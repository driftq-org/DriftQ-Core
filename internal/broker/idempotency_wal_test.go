package broker

import (
	"os"
	"testing"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func TestConsumeIdempotencySurvivesRestart(t *testing.T) {
	tmp, err := os.CreateTemp("", "driftq-idem-*.wal")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(path)

	tenant := "t1"
	topic := "idem-demo"
	group := "g1"
	key := "consume-key-123"
	owner := "worker-A"
	lease := 2 * time.Second
	result := []byte("ok")

	// Run 1: write consume-idem COMMITTED to WAL
	w1, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL w1: %v", err)
	}

	b1 := NewInMemoryBrokerWithWAL(w1)
	if b1.idem == nil {
		t.Fatalf("idem store is nil (broker not wiring WAL into idem?)")
	}

	alreadyDone, prior, err := b1.idem.ConsumeBeginLease(tenant, topic, group, key, owner, lease)
	if err != nil {
		t.Fatalf("ConsumeBeginLease: %v", err)
	}

	if alreadyDone {
		t.Fatalf("expected alreadyDone=false, got true (prior=%q)", string(prior))
	}

	if err := b1.idem.ConsumeCommitIfOwner(tenant, topic, group, key, owner, result); err != nil {
		t.Fatalf("ConsumeCommitIfOwner: %v", err)
	}

	_ = w1.Close()

	// Restart: replay WAL into a new broker
	w2, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL w2: %v", err)
	}
	defer w2.Close()

	b2, err := NewInMemoryBrokerFromWAL(w2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	if b2.idem == nil {
		t.Fatalf("idem store is nil after replay")
	}

	alreadyDone2, prior2, err := b2.idem.ConsumeBeginLease(tenant, topic, group, key, "worker-B", lease)
	if err != nil {
		t.Fatalf("ConsumeBeginLease after restart: %v", err)
	}

	if !alreadyDone2 {
		t.Fatalf("expected alreadyDone=true after restart, got false")
	}

	if string(prior2) != string(result) {
		t.Fatalf("expected prior result %q, got %q", string(result), string(prior2))
	}
}

func TestConsumePendingLeaseExpiresOnRestart(t *testing.T) {
	tmp, err := os.CreateTemp("", "driftq-idem-pending-*.wal")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}

	path := tmp.Name()
	_ = tmp.Close()
	defer os.Remove(path)

	tenant := "t1"
	topic := "idem-demo"
	group := "g1"
	key := "consume-key-123"

	// Must pass validateLease(); also needs to be long enough that it would still be active if replay DIDN'T expire PENDING.
	lease := 1 * time.Second

	// Run 1: create a PENDING lease and persist it to WAL (no commit)
	w1, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL w1: %v", err)
	}

	b1 := NewInMemoryBrokerWithWAL(w1)
	if b1.idem == nil {
		t.Fatalf("idem store is nil (broker not wiring WAL into idem?)")
	}

	alreadyDone, _, err := b1.idem.ConsumeBeginLease(tenant, topic, group, key, "worker-A", lease)
	if err != nil {
		t.Fatalf("ConsumeBeginLease run1: %v", err)
	}

	if alreadyDone {
		t.Fatalf("expected alreadyDone=false on first begin, got true")
	}

	_ = w1.Close()

	// --- Restart: replay WAL into new broker ---
	w2, err := storage.OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL w2: %v", err)
	}
	defer w2.Close()

	b2, err := NewInMemoryBrokerFromWAL(w2)
	if err != nil {
		t.Fatalf("NewInMemoryBrokerFromWAL: %v", err)
	}

	if b2.idem == nil {
		t.Fatalf("idem store is nil after replay")
	}

	// Key assertion: after restart, old PENDING lease must be treated as expired,
	// so a different owner should NOT get ErrIdempotencyLeaseHeld
	alreadyDone2, _, err := b2.idem.ConsumeBeginLease(tenant, topic, group, key, "worker-B", lease)
	if err != nil {
		t.Fatalf("ConsumeBeginLease run2: %v", err)
	}

	if alreadyDone2 {
		t.Fatalf("expected alreadyDone=false after restart for PENDING key, got true")
	}
}
