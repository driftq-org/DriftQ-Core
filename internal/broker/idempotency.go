package broker

import (
	"errors"
	"sync"
	"time"
)

const (
	IdemScopeProduce = "produce"
	IdemScopeConsume = "consume"
)

const (
	IdemStatusPending   = "PENDING"
	IdemStatusCommitted = "COMMITTED"
	IdemStatusFailed    = "FAILED"
)

var (
	ErrIdempotencyInFlight  = errors.New("idempotency: key already in-flight")
	ErrIdempotencyLeaseHeld = errors.New("idempotency: lease held by another worker")
	ErrIdempotencyLeaseLost = errors.New("idempotency: lease lost or expired")
	ErrIdempotencyNotFound  = errors.New("idempotency: key not found")
	ErrIdempotencyBadLease  = errors.New("idempotency: invalid lease duration")
)

type IdempotencyConsumerHelper struct {
	store *IdempotencyStore
}

type IdempotencyStatus struct {
	Status    string
	Result    []byte
	LastError string
	UpdatedAt time.Time

	// consumer-lease fields (only for Scope="consume")
	Owner      string
	LeaseUntil time.Time
}

type idempotencyKey struct {
	Scope    string // "produce" | "consume"
	TenantID string
	Topic    string
	Group    string // only for consumer scope
	Key      string
}

// IdempotencyStore is a tiny in-memory dedupe map
// MVP behavior:
// - Begin() marks (tenant,topic,idKey) as PENDING atomically
// - Commit() marks it COMMITTED
// - Fail() marks it FAILED
// - Entries expire after ttl (to avoid unbounded growth)
type IdempotencyStore struct {
	mu    sync.Mutex
	ttl   time.Duration
	items map[idempotencyKey]IdempotencyStatus
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}

	return &IdempotencyStore{
		ttl:   ttl,
		items: make(map[idempotencyKey]IdempotencyStatus),
	}
}

// Begin attempts to start a new idempotent operation (PRODUCER SCOPE)
// Returns:
// - alreadyCommitted=true if this key was already committed (caller should treat as success and skip work)
// - err=ErrIdempotencyInFlight if currently pending (caller should reject to avoid duplicates)
// - otherwise it records PENDING and returns (false, nil)
func (s *IdempotencyStore) Begin(tenantID, topic, key string) (alreadyCommitted bool, err error) {
	if key == "" {
		return false, nil // no idempotency requested
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(time.Now())

	k := idempotencyKey{
		Scope:    IdemScopeProduce,
		TenantID: tenantID,
		Topic:    topic,
		Group:    "",
		Key:      key,
	}

	if st, ok := s.items[k]; ok {
		switch st.Status {
		case IdemStatusCommitted:
			return true, nil
		case IdemStatusPending:
			return false, ErrIdempotencyInFlight
		case IdemStatusFailed:
			// allow retry by replacing FAILED with PENDING
		}
	}

	s.items[k] = IdempotencyStatus{
		Status:    IdemStatusPending,
		UpdatedAt: time.Now(),
	}

	return false, nil
}

func (s *IdempotencyStore) Commit(tenantID, topic, key string, result []byte) {
	if key == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := idempotencyKey{
		Scope:    IdemScopeProduce,
		TenantID: tenantID,
		Topic:    topic,
		Group:    "",
		Key:      key,
	}

	s.items[k] = IdempotencyStatus{
		Status:    IdemStatusCommitted,
		Result:    result,
		UpdatedAt: time.Now(),
	}
}

func (s *IdempotencyStore) Fail(tenantID, topic, key string, cause error) {
	if key == "" {
		return
	}

	msg := ""
	if cause != nil {
		msg = cause.Error()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := idempotencyKey{
		Scope:    IdemScopeProduce,
		TenantID: tenantID,
		Topic:    topic,
		Group:    "",
		Key:      key,
	}

	s.items[k] = IdempotencyStatus{
		Status:    IdemStatusFailed,
		LastError: msg,
		UpdatedAt: time.Now(),
	}
}

func (s *IdempotencyStore) cleanupLocked(now time.Time) {
	if s.ttl <= 0 {
		return
	}

	cutoff := now.Add(-s.ttl)
	for k, st := range s.items {
		if st.UpdatedAt.Before(cutoff) {
			delete(s.items, k)
		}
	}
}

func (s *IdempotencyStore) Check(tenantID, topic, key string) (st IdempotencyStatus, ok bool) {
	if key == "" {
		return IdempotencyStatus{}, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(time.Now())

	k := idempotencyKey{
		Scope:    IdemScopeProduce,
		TenantID: tenantID,
		Topic:    topic,
		Group:    "",
		Key:      key,
	}

	st, ok = s.items[k]
	return st, ok
}

func (s *IdempotencyStore) MarkSuccess(tenantID, topic, key string, result []byte) {
	s.Commit(tenantID, topic, key, result)
}

func (s *IdempotencyStore) MarkFailure(tenantID, topic, key string, cause error) {
	s.Fail(tenantID, topic, key, cause)
}

// -------------------------
// CONSUMER LEASE
// -------------------------

func validateLease(lease time.Duration) error {
	if lease <= 0 {
		return ErrIdempotencyBadLease
	}
	return nil
}

// ClaimConsumeLease tries to claim/refresh an idempotency lease for CONSUMER side effects.
// Behavior:
// - If COMMITTED: return (true, result, nil) => already done, caller should Ack and skip side-effect
// - If PENDING and lease valid but owned by someone else: ErrIdempotencyLeaseHeld
// - If PENDING and owned by caller: refresh lease and proceed
// - If PENDING but lease expired: caller can take over (new owner)
// - If FAILED: allow retry (becomes PENDING with a new lease)
func (s *IdempotencyStore) ClaimConsumeLease(tenantID, topic, group, key, owner string, lease time.Duration) (alreadyCommitted bool, priorResult []byte, err error) {
	if key == "" {
		return false, nil, nil // no idempotency requested
	}

	if err := validateLease(lease); err != nil {
		return false, nil, err
	}

	if owner == "" {
		owner = "unknown"
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(now)

	k := idempotencyKey{
		Scope:    IdemScopeConsume,
		TenantID: tenantID,
		Topic:    topic,
		Group:    group,
		Key:      key,
	}

	if st, ok := s.items[k]; ok {
		switch st.Status {
		case IdemStatusCommitted:
			return true, st.Result, nil

		case IdemStatusPending:
			// If lease still valid and owned by someone else -> cannot proceed
			if !st.LeaseUntil.IsZero() && now.Before(st.LeaseUntil) && st.Owner != "" && st.Owner != owner {
				return false, nil, ErrIdempotencyLeaseHeld
			}

			// If same owner (or lease expired), refresh / take over
			st.Status = IdemStatusPending
			st.Owner = owner
			st.LeaseUntil = now.Add(lease)
			st.UpdatedAt = now
			s.items[k] = st
			return false, nil, nil

		case IdemStatusFailed:
			// Allow retry: replace FAILED with PENDING lease
			st.Status = IdemStatusPending
			st.LastError = ""
			st.Owner = owner
			st.LeaseUntil = now.Add(lease)
			st.UpdatedAt = now
			s.items[k] = st
			return false, nil, nil
		}
	}

	// No record: create a new PENDING lease
	s.items[k] = IdempotencyStatus{
		Status:     IdemStatusPending,
		UpdatedAt:  now,
		Owner:      owner,
		LeaseUntil: now.Add(lease),
	}

	return false, nil, nil
}

func (s *IdempotencyStore) RenewConsumeLease(tenantID, topic, group, key, owner string, lease time.Duration) error {
	if key == "" {
		return nil
	}

	if err := validateLease(lease); err != nil {
		return err
	}

	if owner == "" {
		owner = "unknown"
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(now)

	k := idempotencyKey{
		Scope:    IdemScopeConsume,
		TenantID: tenantID,
		Topic:    topic,
		Group:    group,
		Key:      key,
	}

	st, ok := s.items[k]
	if !ok {
		return ErrIdempotencyNotFound
	}

	if st.Status != IdemStatusPending {
		return ErrIdempotencyLeaseLost
	}

	if st.Owner != owner {
		return ErrIdempotencyLeaseHeld
	}

	if !st.LeaseUntil.IsZero() && now.After(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	st.LeaseUntil = now.Add(lease)
	st.UpdatedAt = now
	s.items[k] = st
	return nil
}

func (s *IdempotencyStore) CompleteConsumeSuccess(tenantID, topic, group, key, owner string, result []byte) error {
	if key == "" {
		return nil
	}
	if owner == "" {
		owner = "unknown"
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(now)

	k := idempotencyKey{
		Scope:    IdemScopeConsume,
		TenantID: tenantID,
		Topic:    topic,
		Group:    group,
		Key:      key,
	}

	st, ok := s.items[k]
	if !ok {
		return ErrIdempotencyNotFound
	}

	if st.Status != IdemStatusPending {
		return ErrIdempotencyLeaseLost
	}

	if st.Owner != owner {
		return ErrIdempotencyLeaseHeld
	}

	if !st.LeaseUntil.IsZero() && now.After(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	s.items[k] = IdempotencyStatus{
		Status:    IdemStatusCommitted,
		Result:    result,
		UpdatedAt: now,
		// clear lease fields on commit
		Owner:      "",
		LeaseUntil: time.Time{},
	}
	return nil
}

func (s *IdempotencyStore) CompleteConsumeFailure(tenantID, topic, group, key, owner string, cause error) error {
	if key == "" {
		return nil
	}
	if owner == "" {
		owner = "unknown"
	}

	msg := ""
	if cause != nil {
		msg = cause.Error()
	}

	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked(now)

	k := idempotencyKey{
		Scope:    IdemScopeConsume,
		TenantID: tenantID,
		Topic:    topic,
		Group:    group,
		Key:      key,
	}

	st, ok := s.items[k]
	if !ok {
		return ErrIdempotencyNotFound
	}
	if st.Status != IdemStatusPending {
		return ErrIdempotencyLeaseLost
	}
	if st.Owner != owner {
		return ErrIdempotencyLeaseHeld
	}
	if !st.LeaseUntil.IsZero() && now.After(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	s.items[k] = IdempotencyStatus{
		Status:    IdemStatusFailed,
		LastError: msg,
		UpdatedAt: now,
		// clear lease fields on failure record
		Owner:      "",
		LeaseUntil: time.Time{},
	}
	return nil
}

// -------------------------
// Helpers
// -------------------------

func NewIdempotencyConsumerHelper(store *IdempotencyStore) *IdempotencyConsumerHelper {
	return &IdempotencyConsumerHelper{store: store}
}

// Begin checks PRODUCE-scope committed status (legacy helper; keep it if you want)
func (h *IdempotencyConsumerHelper) Begin(topic string, msg Message) (alreadyDone bool, priorResult []byte, err error) {
	if h == nil || h.store == nil {
		return false, nil, nil
	}
	if msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return false, nil, nil // no idempotency requested
	}

	tenantID := msg.Envelope.TenantID
	key := msg.Envelope.IdempotencyKey

	st, ok := h.store.Check(tenantID, topic, key)
	if !ok {
		return false, nil, nil
	}

	if st.Status == IdemStatusCommitted {
		return true, st.Result, nil
	}

	return false, nil, nil
}

// BeginLease is the CONSUMER Option-B entrypoint. If already committed, caller should skip side-effect and just Ack
func (h *IdempotencyConsumerHelper) BeginLease(topic, group string, msg Message, owner string, lease time.Duration) (alreadyDone bool, priorResult []byte, err error) {
	if h == nil || h.store == nil {
		return false, nil, nil
	}

	if msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return false, nil, nil // no idempotency requested
	}

	return h.store.ClaimConsumeLease(msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, lease)
}

func (h *IdempotencyConsumerHelper) RenewLease(topic, group string, msg Message, owner string, lease time.Duration) error {
	if h == nil || h.store == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return nil
	}

	return h.store.RenewConsumeLease(msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, lease)
}

func (h *IdempotencyConsumerHelper) CompleteSuccessLease(topic, group string, msg Message, owner string, result []byte) error {
	if h == nil || h.store == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return nil
	}

	return h.store.CompleteConsumeSuccess(msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, result)
}

func (h *IdempotencyConsumerHelper) CompleteFailureLease(topic, group string, msg Message, owner string, cause error) error {
	if h == nil || h.store == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return nil
	}

	return h.store.CompleteConsumeFailure(msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, cause)
}

func (h *IdempotencyConsumerHelper) MarkSuccess(topic string, msg Message, result []byte) {
	if h == nil || h.store == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return
	}

	h.store.MarkSuccess(msg.Envelope.TenantID, topic, msg.Envelope.IdempotencyKey, result)
}

func (h *IdempotencyConsumerHelper) MarkFailure(topic string, msg Message, cause error) {
	if h == nil || h.store == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
		return
	}

	h.store.MarkFailure(msg.Envelope.TenantID, topic, msg.Envelope.IdempotencyKey, cause)
}
