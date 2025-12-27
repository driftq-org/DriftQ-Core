package broker

import (
	"context"
	"strings"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

func (b *InMemoryBroker) StartRedeliveryLoop(ctx context.Context) {
	t := time.NewTicker(b.redeliverTick)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.mu.Lock()
				b.redeliverExpiredLocked()
				b.mu.Unlock()
			}
		}
	}()
}

func (b *InMemoryBroker) redeliverExpiredLocked() {
	now := time.Now()

	for topic, byGroup := range b.inFlight {
		groupChans, ok := b.consumerChans[topic]
		if !ok {
			continue
		}

		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		for group, byPart := range byGroup {
			chans := groupChans[group]
			if len(chans) == 0 {
				continue
			}

			leaseForOwner := func(owner string) time.Duration {
				lease := b.ackTimeout
				own := strings.TrimSpace(owner)

				for _, st := range chans {
					if strings.TrimSpace(st.Owner) == own && st.Lease > 0 {
						lease = st.Lease
						break
					}
				}
				return lease
			}

			for partition, inflight := range byPart {
				for offset, e := range inflight {
					if e == nil {
						delete(inflight, offset)
						continue
					}

					// Not yet timed out (use per-consumer lease if available)
					lease := leaseForOwner(e.Owner)
					if now.Sub(e.SentAt) < lease {
						continue
					}

					// If it timed out without an explicit Nack, record ack_timeout as last_error
					if e.LastError == "" {
						e.LastError = "ack_timeout"

						m := e.Msg
						m.LastError = e.LastError
						e.Msg = m

						rs := b.ensureRetryState(topic, group, partition)
						rs[offset] = &retryStateEntry{
							LastError:   e.LastError,
							LastErrorAt: now,
						}

						if b.wal != nil {
							at := now
							_ = b.wal.Append(storage.Entry{
								Type:        storage.RecordTypeRetryState,
								Topic:       topic,
								Group:       group,
								Partition:   partition,
								Offset:      offset,
								LastError:   e.LastError,
								LastErrorAt: &at,
							})
						}
					}

					// Retry scheduling gate
					if !e.NextDeliverAt.IsZero() && now.Before(e.NextDeliverAt) {
						continue
					}

					// Optional retry policy from envelope
					var rp *RetryPolicy
					if e.Msg.Envelope != nil {
						rp = e.Msg.Envelope.RetryPolicy
					}

					// DLQ routing when MaxAttempts reached (STRICT: no drop unless DLQ publish succeeds)
					if rp != nil && rp.MaxAttempts > 0 && e.Attempts >= rp.MaxAttempts {
						dlqMsg := e.Msg
						dlqMsg.Attempts = e.Attempts
						dlqMsg.LastError = e.LastError

						var envCopy *Envelope
						if e.Msg.Envelope != nil {
							tmp := *e.Msg.Envelope
							envCopy = &tmp
						} else {
							envCopy = &Envelope{}
						}

						envCopy.DLQ = &DLQMetadata{
							OriginalTopic:     topic,
							OriginalPartition: partition,
							OriginalOffset:    offset,
							Attempts:          e.Attempts,
							LastError:         e.LastError,
							RoutedAtMs:        now.UnixMilli(),
						}
						dlqMsg.Envelope = envCopy

						if err := b.publishToDLQLocked(context.Background(), topic, dlqMsg); err != nil {
							e.LastError = appendLastError(e.LastError, "dlq_publish_failed: "+err.Error())

							m := e.Msg
							m.LastError = e.LastError
							e.Msg = m

							rs := b.ensureRetryState(topic, group, partition)
							rs[offset] = &retryStateEntry{LastError: e.LastError, LastErrorAt: now}
							if b.wal != nil {
								at := now
								_ = b.wal.Append(storage.Entry{
									Type:        storage.RecordTypeRetryState,
									Topic:       topic,
									Group:       group,
									Partition:   partition,
									Offset:      offset,
									LastError:   e.LastError,
									LastErrorAt: &at,
								})
							}
							continue
						}

						delete(inflight, offset)
						_ = b.advanceOffsetLocked(topic, group, partition, offset)
						b.purgeRetryStateLocked(topic, group, partition, offset)
						continue
					}

					// pick one consumer in the group (round-robin)
					idx := b.rrCursor[topic][group] % len(chans)
					b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
					cs := chans[idx]

					// Use the lease for the target consumer (not the previous owner)
					targetLease := cs.Lease
					if targetLease <= 0 {
						targetLease = b.ackTimeout
					}

					// CONSUME-SCOPE BeginLease gate (consumer-boundary idempotency)
					// IMPORTANT: do this BEFORE changing e.Owner.
					if b.idem != nil && e.Msg.Envelope != nil && e.Msg.Envelope.IdempotencyKey != "" {
						tenantID := e.Msg.Envelope.TenantID
						idk := e.Msg.Envelope.IdempotencyKey

						alreadyDone, _, berr := b.idem.ConsumeBeginLease(tenantID, topic, group, idk, cs.Owner, targetLease)
						if berr != nil {
							reason := "idem_begin_failed: " + berr.Error()
							if berr == ErrIdempotencyLeaseHeld {
								reason = "idem_lease_held"
							}

							// Update error so the next delivery shows *why* it didn't move
							e.LastError = appendLastError(e.LastError, reason)

							m := e.Msg
							m.LastError = e.LastError
							e.Msg = m

							rs := b.ensureRetryState(topic, group, partition)
							rs[offset] = &retryStateEntry{LastError: e.LastError, LastErrorAt: now}
							if b.wal != nil {
								at := now
								_ = b.wal.Append(storage.Entry{
									Type:        storage.RecordTypeRetryState,
									Topic:       topic,
									Group:       group,
									Partition:   partition,
									Offset:      offset,
									LastError:   e.LastError,
									LastErrorAt: &at,
								})
							}

							// Don't spin hot — push next attempt a bit.
							if e.NextDeliverAt.IsZero() || e.NextDeliverAt.Before(now.Add(100*time.Millisecond)) {
								e.NextDeliverAt = now.Add(100 * time.Millisecond)
							}
							continue
						}

						if alreadyDone {
							// Someone already finished it: stop retrying it and advance offset.
							delete(inflight, offset)
							_ = b.advanceOffsetLocked(topic, group, partition, offset)
							b.purgeRetryStateLocked(topic, group, partition, offset)
							continue
						}
					}

					// IMPORTANT: update owner to the consumer we're delivering to
					e.Owner = cs.Owner

					// update inflight bookkeeping (source of truth)
					e.SentAt = now
					e.Attempts++

					// Backoff scheduling (for next retry eligibility)
					if rp != nil && (rp.BackoffMs > 0 || rp.MaxBackoffMs > 0) {
						retryNumber := e.Attempts - 1
						backoff := computeBackoff(rp, retryNumber)
						e.NextDeliverAt = now.Add(backoff)
					} else {
						e.NextDeliverAt = time.Time{}
					}

					// Send message with updated attempts + last_error
					m := e.Msg
					m.Attempts = e.Attempts
					m.LastError = e.LastError
					e.Msg = m

					go func(ch chan Message, m Message) {
						defer func() { _ = recover() }()
						ch <- m
					}(cs.Ch, m)
				}
			}
		}
	}
}
