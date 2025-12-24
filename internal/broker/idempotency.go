package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
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
	wal   storage.WAL
}

func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	return NewIdempotencyStoreWithWAL(nil, ttl)
}

func NewIdempotencyStoreWithWAL(wal storage.WAL, ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}

	return &IdempotencyStore{
		ttl:   ttl,
		items: make(map[idempotencyKey]IdempotencyStatus),
		wal:   wal,
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
		// This is what makes "don't rerun side effects" actually hold beyond 10 minutes
		if k.Scope == IdemScopeConsume && st.Status == IdemStatusCommitted {
			continue
		}

		// For consume-scope PENDING: do NOT delete if lease is still active
		// If lease expired and it's old, let TTL evict it
		if k.Scope == IdemScopeConsume && st.Status == IdemStatusPending {
			leaseActive := !st.LeaseUntil.IsZero() && now.Before(st.LeaseUntil)
			if leaseActive {
				continue
			}
		}

		// Default TTL eviction
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

func (s *IdempotencyStore) ConsumeBeginLease(tenantID, topic, group, key, owner string, lease time.Duration) (alreadyDone bool, priorResult []byte, err error) {
	if key == "" {
		return false, nil, nil
	}

	// Enforce your lease policy (e.g., >= 250ms)
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

	// Helper to durably record "PENDING + lease" before mutating memory.
	appendPending := func(leaseUntil time.Time) error {
		if s.wal == nil {
			return nil
		}
		updatedAt := now
		lu := leaseUntil
		return s.wal.Append(storage.Entry{
			Type:              storage.RecordTypeConsumeIdempotency,
			TenantID:          tenantID,
			Topic:             topic,
			Group:             group,
			IdempotencyKey:    key,
			IdempotencyScope:  IdemScopeConsume,
			IdempotencyStatus: IdemStatusPending,
			LeaseOwner:        owner,
			LeaseUntil:        &lu,
			Result:            nil,
			LastError:         "",
			UpdatedAt:         &updatedAt,
		})
	}

	if st, ok := s.items[k]; ok {
		switch st.Status {
		case IdemStatusCommitted:
			return true, st.Result, nil

		case IdemStatusPending:
			// Someone else holds a still-valid lease => reject
			leaseActive := !st.LeaseUntil.IsZero() && now.Before(st.LeaseUntil)
			if leaseActive && st.Owner != owner {
				return false, nil, ErrIdempotencyLeaseHeld
			}

			// Lease expired OR same owner => take/refresh
			newUntil := now.Add(lease)
			if err := appendPending(newUntil); err != nil {
				return false, nil, err
			}

			st.Status = IdemStatusPending
			st.Owner = owner
			st.LeaseUntil = newUntil
			st.UpdatedAt = now
			s.items[k] = st
			return false, nil, nil

		case IdemStatusFailed:
			newUntil := now.Add(lease)
			if err := appendPending(newUntil); err != nil {
				return false, nil, err
			}

			st.Status = IdemStatusPending
			st.LastError = ""
			st.Owner = owner
			st.LeaseUntil = newUntil
			st.UpdatedAt = now
			s.items[k] = st
			return false, nil, nil

		default:
			newUntil := now.Add(lease)
			if err := appendPending(newUntil); err != nil {
				return false, nil, err
			}

			st.Status = IdemStatusPending
			st.LastError = ""
			st.Owner = owner
			st.LeaseUntil = newUntil
			st.UpdatedAt = now
			s.items[k] = st
			return false, nil, nil
		}
	}

	// New entry
	newUntil := now.Add(lease)
	if err := appendPending(newUntil); err != nil {
		return false, nil, err
	}

	s.items[k] = IdempotencyStatus{
		Status:     IdemStatusPending,
		UpdatedAt:  now,
		Owner:      owner,
		LeaseUntil: newUntil,
	}

	return false, nil, nil
}

func (s *IdempotencyStore) ConsumeRenewLease(tenantID, topic, group, key, owner string, lease time.Duration) error {
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

	if st.LeaseUntil.IsZero() || !now.Before(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	newUntil := now.Add(lease)

	// Durable renew before mutating memory
	if s.wal != nil {
		updatedAt := now
		lu := newUntil
		if err := s.wal.Append(storage.Entry{
			Type:              storage.RecordTypeConsumeIdempotency,
			TenantID:          tenantID,
			Topic:             topic,
			Group:             group,
			IdempotencyKey:    key,
			IdempotencyScope:  IdemScopeConsume,
			IdempotencyStatus: IdemStatusPending, // still pending, just updated lease
			LeaseOwner:        owner,
			LeaseUntil:        &lu,
			Result:            nil,
			LastError:         "",
			UpdatedAt:         &updatedAt,
		}); err != nil {
			return err
		}
	}

	st.LeaseUntil = newUntil
	st.UpdatedAt = now
	s.items[k] = st

	return nil
}

func (s *IdempotencyStore) ConsumeCommitIfOwner(tenantID, topic, group, key, owner string, result []byte) error {
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

	if st.Status == IdemStatusCommitted {
		return nil
	}

	if st.Status != IdemStatusPending {
		return ErrIdempotencyLeaseLost
	}

	if st.Owner != owner {
		return ErrIdempotencyLeaseHeld
	}

	if st.LeaseUntil.IsZero() || !now.Before(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	if s.wal != nil {
		updatedAt := now
		lu := st.LeaseUntil

		entry := storage.Entry{
			Type:              storage.RecordTypeConsumeIdempotency,
			TenantID:          tenantID,
			Topic:             topic,
			Group:             group,
			IdempotencyKey:    key,
			IdempotencyScope:  IdemScopeConsume,
			IdempotencyStatus: IdemStatusCommitted,

			LeaseOwner: owner,
			LeaseUntil: &lu,

			Result:    result,
			LastError: "",
			UpdatedAt: &updatedAt,
		}

		if err := s.wal.Append(entry); err != nil {
			return err
		}
	}

	s.items[k] = IdempotencyStatus{
		Status:     IdemStatusCommitted,
		Result:     result,
		LastError:  "",
		UpdatedAt:  now,
		Owner:      "",
		LeaseUntil: time.Time{},
	}

	return nil
}

func (s *IdempotencyStore) ConsumeFailIfOwner(tenantID, topic, group, key, owner string, cause error) error {
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

	if st.LeaseUntil.IsZero() || !now.Before(st.LeaseUntil) {
		return ErrIdempotencyLeaseLost
	}

	// Durable failure before mutating memory
	if s.wal != nil {
		updatedAt := now
		if err := s.wal.Append(storage.Entry{
			Type:              storage.RecordTypeConsumeIdempotency,
			TenantID:          tenantID,
			Topic:             topic,
			Group:             group,
			IdempotencyKey:    key,
			IdempotencyScope:  IdemScopeConsume,
			IdempotencyStatus: IdemStatusFailed,
			LeaseOwner:        owner,
			LeaseUntil:        nil, // failure clears lease
			Result:            nil,
			LastError:         msg,
			UpdatedAt:         &updatedAt,
		}); err != nil {
			return err
		}
	}

	s.items[k] = IdempotencyStatus{
		Status:     IdemStatusFailed,
		LastError:  msg,
		UpdatedAt:  now,
		Owner:      "",
		LeaseUntil: time.Time{},
		Result:     nil,
	}

	return nil
}
