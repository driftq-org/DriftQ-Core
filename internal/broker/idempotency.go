package broker

import (
	"errors"
	"sync"
	"time"
)

// This is the state of a (tenant_id, idempotency_key)
type IdempotencyStatus string

const (
	IdempoInProgress IdempotencyStatus = "IN_PROGRESS"
	IdempoSuccess    IdempotencyStatus = "SUCCESS"
	IdempoFailure    IdempotencyStatus = "FAILURE"
)

// IdempotencyRecord is what we store for a key
type IdempotencyRecord struct {
	Status    IdempotencyStatus
	Result    []byte
	LastError string
	UpdatedAt time.Time
}

// Simple in-memory map keyed by tenant_id + idempotency_key
type InMemoryIdempotencyStore struct {
	mu   sync.Mutex
	data map[string]map[string]IdempotencyRecord // tenantID -> key -> record
}

func NewInMemoryIdempotencyStore() *InMemoryIdempotencyStore {
	return &InMemoryIdempotencyStore{
		data: make(map[string]map[string]IdempotencyRecord),
	}
}

// Begin attempts to "claim" the idempotency key
//
// Returns:
// - existing: existing record if one already exists (SUCCESS / FAILURE / IN_PROGRESS)
// - allowed:  true if caller is allowed to execute the side effect now
//
// v1 behavior:
// - No record      -> create IN_PROGRESS, allowed=true
// - SUCCESS        -> allowed=false
// - IN_PROGRESS    -> allowed=false
// - FAILURE        -> allowed=true (allow retry in v1; I will refine with retry_policy later)
func (s *InMemoryIdempotencyStore) Begin(tenantID, key string) (existing *IdempotencyRecord, allowed bool, err error) {
	if tenantID == "" {
		return nil, false, errors.New("tenant_id is required for idempotency")
	}

	if key == "" {
		return nil, false, errors.New("idempotency_key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tenantMap := s.data[tenantID]
	if tenantMap == nil {
		tenantMap = make(map[string]IdempotencyRecord)
		s.data[tenantID] = tenantMap
	}

	if rec, ok := tenantMap[key]; ok {
		// return a copy
		cpy := rec
		cpy.Result = cloneBytes(rec.Result)

		switch rec.Status {
		case IdempoSuccess:
			return &cpy, false, nil
		case IdempoInProgress:
			return &cpy, false, nil
		case IdempoFailure:
			return &cpy, true, nil
		default:
			// unknown -> be conservative: do not allow
			return &cpy, false, nil
		}
	}

	// Create IN_PROGRESS record
	tenantMap[key] = IdempotencyRecord{
		Status:    IdempoInProgress,
		UpdatedAt: time.Now().UTC(),
	}

	return nil, true, nil
}

func (s *InMemoryIdempotencyStore) CompleteSuccess(tenantID, key string, result []byte) error {
	if tenantID == "" {
		return errors.New("tenant_id is required for idempotency")
	}

	if key == "" {
		return errors.New("idempotency_key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tenantMap := s.data[tenantID]
	if tenantMap == nil {
		tenantMap = make(map[string]IdempotencyRecord)
		s.data[tenantID] = tenantMap
	}

	tenantMap[key] = IdempotencyRecord{
		Status:    IdempoSuccess,
		Result:    cloneBytes(result),
		LastError: "",
		UpdatedAt: time.Now().UTC(),
	}

	return nil
}

func (s *InMemoryIdempotencyStore) CompleteFailure(tenantID, key string, lastErr string) error {
	if tenantID == "" {
		return errors.New("tenant_id is required for idempotency")
	}

	if key == "" {
		return errors.New("idempotency_key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tenantMap := s.data[tenantID]
	if tenantMap == nil {
		tenantMap = make(map[string]IdempotencyRecord)
		s.data[tenantID] = tenantMap
	}

	tenantMap[key] = IdempotencyRecord{
		Status:    IdempoFailure,
		Result:    nil,
		LastError: lastErr,
		UpdatedAt: time.Now().UTC(),
	}
	return nil
}

func (s *InMemoryIdempotencyStore) Get(tenantID, key string) (IdempotencyRecord, bool, error) {
	if tenantID == "" {
		return IdempotencyRecord{}, false, errors.New("tenant_id is required for idempotency")
	}

	if key == "" {
		return IdempotencyRecord{}, false, errors.New("idempotency_key is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tenantMap := s.data[tenantID]
	if tenantMap == nil {
		return IdempotencyRecord{}, false, nil
	}

	rec, ok := tenantMap[key]
	if !ok {
		return IdempotencyRecord{}, false, nil
	}

	rec.Result = cloneBytes(rec.Result)
	return rec, true, nil
}

func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)
	return out
}
