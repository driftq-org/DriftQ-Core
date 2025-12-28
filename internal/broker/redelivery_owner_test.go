package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRedeliveryUpdatesOwnerSoAckIfOwnerWorks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b := NewInMemoryBroker()
	b.StartRedeliveryLoop(ctx)

	if err := b.CreateTopic(ctx, "t1", 1); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Two consumers in same group, with short leases so we can see redelivery
	chA, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerA", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease A: %v", err)
	}

	chB, err := b.ConsumeWithLease(ctx, "t1", "g1", "ownerB", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("ConsumeWithLease B: %v", err)
	}

	if err := b.Produce(ctx, "t1", Message{Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	var first Message
	select {
	case first = <-chA:
	case first = <-chB:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for first delivery")
	}

	// DO NOT ack so the wait is long enough for lease + redelivery tick
	time.Sleep(900 * time.Millisecond)

	deadline := time.Now().Add(2 * time.Second)
	gotRedelivery := false

	for time.Now().Before(deadline) {
		select {
		case m := <-chA:
			if m.Offset == first.Offset && m.Partition == first.Partition {
				if err := b.AckIfOwner(ctx, "t1", "g1", m.Partition, m.Offset, "ownerA"); err == nil {
					gotRedelivery = true
					goto DONE
				} else if !errors.Is(err, ErrNotOwner) {
					t.Fatalf("AckIfOwner unexpected err: %v", err)
				}
			}
		case m := <-chB:
			if m.Offset == first.Offset && m.Partition == first.Partition {
				if err := b.AckIfOwner(ctx, "t1", "g1", m.Partition, m.Offset, "ownerB"); err == nil {
					gotRedelivery = true
					goto DONE
				} else if !errors.Is(err, ErrNotOwner) {
					t.Fatalf("AckIfOwner unexpected err: %v", err)
				}
			}
		case <-time.After(100 * time.Millisecond):
			// keep polling
		}
	}

DONE:
	if !gotRedelivery {
		t.Fatalf("never observed a redelivery that could be acked by the receiving owner")
	}
}
