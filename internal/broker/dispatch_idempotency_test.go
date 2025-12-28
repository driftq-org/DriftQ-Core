package broker

import (
	"context"
	"testing"
	"time"
)

func TestDispatchSkipsCommittedConsumeIdempotencyAndAdvancesOffset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Create a consumer stream (this will make dispatch run on produce)
	ch, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerA", 2*time.Second)
	if err != nil {
		t.Fatalf("ConsumeWithLease: %v", err)
	}

	tenantID := "tenant1"
	idk := "idem-1"

	// Precommit consume-scope idempotency (must create the key first by leasing it)
	lease := 2 * time.Second // must be >= 250ms
	if _, _, err := b.idem.ConsumeBeginLease(tenantID, "t1", "g1", idk, "ownerA", lease); err != nil {
		t.Fatalf("ConsumeBeginLease precommit: %v", err)
	}

	if err := b.idem.ConsumeCommitIfOwner(tenantID, "t1", "g1", idk, "ownerA", nil); err != nil {
		t.Fatalf("ConsumeCommitIfOwner precommit: %v", err)
	}

	msg := Message{
		Key:   []byte("k1"),
		Value: []byte("v1"),
		Envelope: &Envelope{
			TenantID:       tenantID,
			IdempotencyKey: idk,
		},
	}

	if err := b.Produce(ctx, "t1", msg); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Should NOT deliver anything
	select {
	case m := <-ch:
		t.Fatalf("expected no delivery, but got offset=%d partition=%d", m.Offset, m.Partition)
	case <-time.After(250 * time.Millisecond):
		// ok
	}

	// And it should have advanced the group's offset for partition 0
	b.mu.RLock()
	defer b.mu.RUnlock()

	got := b.consumerOffsets["t1"]["g1"][0]
	if got != 0 {
		t.Fatalf("expected consumer offset to advance to 0, got %d", got)
	}
}
