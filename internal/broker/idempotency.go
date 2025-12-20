package broker

import (
	"errors"
	"sync"
	"time"
)

const (
	IdemStatusPending   = "PENDING"
	IdemStatusCommitted = "COMMITTED"
	IdemStatusFailed    = "FAILED"
)

var ErrIdempotencyInFlight = errors.New("idempotency: key already in-flight")

type IdempotencyStatus struct {
	Status    string
	Result    []byte
	LastError string
	UpdatedAt time.Time
}

type idempotencyKey struct {
	TenantID string
	Topic    string
	Key      string
}

// IdempotencyStore is a tiny in-memory dedupe map.
// MVP behavior:
// - Begin() marks (tenant,topic,idKey) as PENDING atomically.
// - Commit() marks it COMMITTED.
// - Fail() marks it FAILED.
// - Entries expire after ttl (to avoid unbounded growth).
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

// Begin attempts to start a new idempotent operation
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

	k := idempotencyKey{TenantID: tenantID, Topic: topic, Key: key}
	if st, ok := s.items[k]; ok {
		switch st.Status {
		case IdemStatusCommitted:
			return true, nil
		case IdemStatusPending:
			return false, ErrIdempotencyInFlight
		case IdemStatusFailed:
			// For MVP: allow retry by replacing FAILED with PENDING
			// (later we can respect RetryPolicy/backoff)
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

	k := idempotencyKey{TenantID: tenantID, Topic: topic, Key: key}
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

	k := idempotencyKey{TenantID: tenantID, Topic: topic, Key: key}
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
