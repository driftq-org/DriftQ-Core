package broker

import (
	"context"
	"strings"
	"time"
)

type ConsumerHandler func(ctx context.Context, msg Message) ([]byte, error)

func (b *InMemoryBroker) RunConsumerWithIdempotency(ctx context.Context, topic, group, owner string, lease time.Duration, handler ConsumerHandler) error {
	if handler == nil {
		return nil
	}

	// If we're running Option-B, the lease must be sane. Validate once, up front
	if err := validateLease(lease); err != nil {
		return err
	}

	owner = strings.TrimSpace(owner)
	if owner == "" {
		owner = "consumer-runner"
	}

	ch, err := b.ConsumeWithLease(ctx, topic, group, owner, lease)
	if err != nil {
		return err
	}

	helper := b.IdempotencyHelper()

	renewEvery := lease / 3
	const minRenew = 100 * time.Millisecond
	if renewEvery < minRenew {
		renewEvery = minRenew
	}

	if renewEvery >= lease {
		renewEvery = lease / 2
		if renewEvery <= 0 {
			renewEvery = minRenew
		}

		if renewEvery >= lease {
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

			// Fast path: no helper or no idempotency requested on this message
			if helper == nil || msg.Envelope == nil || msg.Envelope.IdempotencyKey == "" {
				if _, herr := handler(ctx, msg); herr != nil {
					_ = b.Nack(ctx, topic, group, msg.Partition, msg.Offset, owner, herr.Error())
					continue
				}
				_ = b.AckIfOwner(ctx, topic, group, msg.Partition, msg.Offset, owner)
				continue
			}

			tenantID := msg.Envelope.TenantID
			idKey := msg.Envelope.IdempotencyKey
			part := msg.Partition
			off := msg.Offset

			// 1) Claim lease
			alreadyDone, _, berr := helper.store.ConsumeBeginLease(tenantID, topic, group, idKey, owner, lease)
			if berr != nil {
				if berr == ErrIdempotencyLeaseHeld {
					_ = b.Nack(ctx, topic, group, part, off, owner, "idem_lease_held")
					continue
				}
				_ = b.Nack(ctx, topic, group, part, off, owner, "idem_begin_failed: "+berr.Error())
				continue
			}

			if alreadyDone {
				_ = b.AckIfOwner(ctx, topic, group, part, off, owner)
				continue
			}

			// 2) Run handler while renewing lease
			hctx, cancel := context.WithCancel(ctx)

			renewErr := make(chan error, 1)
			done := make(chan struct{})

			go func(tenantID, topic, group, idKey, owner string) {
				defer close(done)

				t := time.NewTicker(renewEvery)
				defer t.Stop()

				for {
					select {
					case <-hctx.Done():
						return
					case <-t.C:
						if err := helper.store.ConsumeRenewLease(tenantID, topic, group, idKey, owner, lease); err != nil {
							select {
							case renewErr <- err:
							default:
							}
							cancel()
							return
						}
					}
				}
			}(tenantID, topic, group, idKey, owner)

			res, handlerErr := handler(hctx, msg)

			// stop renew loop + wait it out
			cancel()
			<-done

			// If renew failed, we definitely don't Ack.
			select {
			case rerr := <-renewErr:
				_ = b.Nack(ctx, topic, group, part, off, owner, "idem_lease_lost: "+rerr.Error())
				continue
			default:
			}

			if handlerErr != nil {
				if ferr := helper.store.ConsumeFailIfOwner(tenantID, topic, group, idKey, owner, handlerErr); ferr != nil {
					_ = b.Nack(ctx, topic, group, part, off, owner, "idem_fail_failed: "+ferr.Error())
					continue
				}
				_ = b.Nack(ctx, topic, group, part, off, owner, handlerErr.Error())
				continue
			}

			// 3) Commit-if-owner; only Ack if commit succeeds
			if cerr := helper.store.ConsumeCommitIfOwner(tenantID, topic, group, idKey, owner, res); cerr != nil {
				_ = b.Nack(ctx, topic, group, part, off, owner, "idem_commit_failed: "+cerr.Error())
				continue
			}

			_ = b.AckIfOwner(ctx, topic, group, part, off, owner)
		}
	}
}

func validateLease(lease time.Duration) error {
	const minLease = 250 * time.Millisecond
	const maxLease = 10 * time.Minute

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
