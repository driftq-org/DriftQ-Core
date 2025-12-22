package broker

import (
	"context"
	"time"
)

type ConsumerHandler func(ctx context.Context, msg Message) ([]byte, error)

// RunConsumerWithIdempotency runs a consumer loop that enforces consumer-boundary idempotency
// using Option B (lease + fencing via commit-if-owner).
func (b *InMemoryBroker) RunConsumerWithIdempotency(ctx context.Context, topic, group, owner string, lease time.Duration, handler ConsumerHandler) error {
	if handler == nil {
		return nil
	}

	// Enforce valid lease once, before we even start consuming.
	// (Your store methods also validate, but we should not rely on that.)
	if err := validateLease(lease); err != nil {
		return err
	}

	if owner == "" {
		owner = "unknown"
	}

	ch, err := b.Consume(ctx, topic, group)
	if err != nil {
		return err
	}

	helper := b.IdempotencyHelper()

	// Renew at half-lease, but keep it sane:
	// - never 0
	// - never too tiny (would thrash)
	// - never larger than half the lease (or you risk expiring before renew)
	renewEvery := lease / 2
	const minRenew = 100 * time.Millisecond
	if renewEvery < minRenew {
		renewEvery = minRenew
	}
	if renewEvery >= lease {
		// This should never happen with validateLease, but belt+suspenders
		renewEvery = lease / 2
		if renewEvery <= 0 {
			renewEvery = minRenew
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg, ok := <-ch:
			if !ok {
				return nil
			}

			// If no idempotency requested, just run handler normally.
			if helper == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
				if _, err := handler(ctx, msg); err != nil {
					_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, err.Error())
					continue
				}
				_ = b.Ack(ctx, topic, group, msg.Partition, msg.Offset)
				continue
			}

			// 1) Claim lease (Option B)
			alreadyDone, _, err := helper.store.ConsumeBeginLease(
				msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, lease,
			)
			if err != nil {
				if err == ErrIdempotencyLeaseHeld {
					_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, "idem_lease_held")
					continue
				}
				_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, "idem_begin_failed: "+err.Error())
				continue
			}

			if alreadyDone {
				_ = b.Ack(ctx, topic, group, msg.Partition, msg.Offset)
				continue
			}

			// 2) Run handler while renewing lease
			hctx, cancel := context.WithCancel(ctx)
			renewErr := make(chan error, 1)
			done := make(chan struct{})

			go func() {
				defer close(done)

				t := time.NewTicker(renewEvery)
				defer t.Stop()

				for {
					select {
					case <-hctx.Done():
						return
					case <-t.C:
						if err := helper.store.ConsumeRenewLease(
							msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, lease,
						); err != nil {
							// Best-effort signal; non-blocking if handler already ended.
							select {
							case renewErr <- err:
							default:
							}
							cancel()
							return
						}
					}
				}
			}()

			res, handlerErr := handler(hctx, msg)

			// stop renew loop
			cancel()
			<-done

			select {
			case err := <-renewErr:
				// lease lost while we were running — do NOT Ack.
				_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, "idem_lease_lost: "+err.Error())
				continue
			default:
			}

			if handlerErr != nil {
				_ = helper.store.ConsumeFailIfOwner(
					msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, handlerErr,
				)
				_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, handlerErr.Error())
				continue
			}

			// 3) Commit-if-owner; only Ack if commit succeeds
			if err := helper.store.ConsumeCommitIfOwner(
				msg.Envelope.TenantID, topic, group, msg.Envelope.IdempotencyKey, owner, res,
			); err != nil {
				_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, "idem_commit_failed: "+err.Error())
				continue
			}

			_ = b.Ack(ctx, topic, group, msg.Partition, msg.Offset)
		}
	}
}

func validateLease(lease time.Duration) error {
	const minLease = 250 * time.Millisecond
	const maxLease = 10 * time.Minute // can/should tweak this

	if lease <= 0 {
		return ErrIdempotencyBadLease
	}
	if lease < minLease {
		return ErrIdempotencyBadLease
	}
	if lease > maxLease {
		return ErrIdempotencyBadLease
	}
	return nil
}
