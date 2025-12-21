package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Describes what kind of thing this entry represents
type RecordType uint8

const (
	RecordTypeMessage    RecordType = 1
	RecordTypeOffset     RecordType = 2
	RecordTypeTopic      RecordType = 3 // topic/partition metadata (optional for later)
	RecordTypeRetryState RecordType = 4 // (topic, group, partition, offset) -> last_error (+ timestamp)
)

type Entry struct {
	Type      RecordType `json:"type"`
	Topic     string     `json:"topic"`
	Partition int        `json:"partition"`
	Offset    int64      `json:"offset"`

	Group string `json:"group,omitempty"`

	Key   []byte `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`

	// routing metadata
	RoutingLabel string            `json:"routing_label,omitempty"`
	RoutingMeta  map[string]string `json:"routing_meta,omitempty"`

	// envelope fields
	RunID        string            `json:"run_id,omitempty"`
	StepID       string            `json:"step_id,omitempty"`
	ParentStepID string            `json:"parent_step_id,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`

	TargetTopic       string `json:"target_topic,omitempty"`
	PartitionOverride *int   `json:"partition_override,omitempty"`

	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	Deadline       *time.Time `json:"deadline,omitempty"`

	RetryMaxAttempts  int   `json:"retry_max_attempts,omitempty"`
	RetryBackoffMs    int64 `json:"retry_backoff_ms,omitempty"`
	RetryMaxBackoffMs int64 `json:"retry_max_backoff_ms,omitempty"`

	TenantID string `json:"tenant_id,omitempty"`

	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at,omitempty"`
}

type WAL interface {
	Append(e Entry) error     // This one writes new stuff
	Replay() ([]Entry, error) // This one restores state after crash
	Close() error             // This one cleans shutdown or releases resources
}

type FileWAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

func OpenFileWAL(path string) (*FileWAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileWAL{
		f:    f,
		path: path,
	}, nil
}

func (w *FileWAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal WAL entry: %w", err)
	}

	if _, err := w.f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write WAL entry: %w", err)
	}

	// Durability guarantee for MVP
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("fsync WAL: %w", err)
	}

	return nil
}

func (w *FileWAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Rewind to start
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek WAL start: %w", err)
	}

	var entries []Entry
	scanner := bufio.NewScanner(w.f)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("decode WAL entry: %w", err)
		}
		entries = append(entries, e)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan WAL: %w", err)
	}

	// Move back to end for future appends
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("seek WAL end: %w", err)
	}

	return entries, nil
}

func (w *FileWAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
