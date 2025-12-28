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

			leaseForStream := func(st consumerStream) time.Duration {
				if st.Lease > 0 {
					return st.Lease
				}
				return b.ackTimeout
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

					// If consume-scope idempotency is already COMMITTED, skip redelivery and advance offset.
					if b.idem != nil && e.Msg.Envelope != nil && e.Msg.Envelope.IdempotencyKey != "" {
						tenantID := e.Msg.Envelope.TenantID
						idk := e.Msg.Envelope.IdempotencyKey

						if st, ok := b.idem.ConsumeCheck(tenantID, topic, group, idk); ok && st.Status == IdemStatusCommitted {
							delete(inflight, offset)
							_ = b.advanceOffsetLocked(topic, group, partition, offset)
							b.purgeRetryStateLocked(topic, group, partition, offset)
							continue
						}
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
					cs := consumerStream{}
					chosenIdx := -1

					// If idempotency_key exists, MUST BeginLease for the consumer we’re about to deliver to.
					if b.idem != nil && e.Msg.Envelope != nil && e.Msg.Envelope.IdempotencyKey != "" {
						tenantID := e.Msg.Envelope.TenantID
						idk := e.Msg.Envelope.IdempotencyKey

						start := b.rrCursor[topic][group] % len(chans)

						for i := 0; i < len(chans); i++ {
							idx := (start + i) % len(chans)
							cand := chans[idx]

							lease := leaseForStream(cand)

							alreadyDone, _, err := b.idem.ConsumeBeginLease(tenantID, topic, group, idk, cand.Owner, lease)
							if err == ErrIdempotencyLeaseHeld {
								continue // try another consumer
							}
							if err != nil {
								// don’t send; try again next tick
								e.LastError = appendLastError(e.LastError, "idem_begin_failed: "+err.Error())
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

								chosenIdx = -1
								break
							}

							if alreadyDone {
								// Treat as done: advance and stop redelivering it
								delete(inflight, offset)
								_ = b.advanceOffsetLocked(topic, group, partition, offset)
								b.purgeRetryStateLocked(topic, group, partition, offset)
								chosenIdx = -2 // sentinel
								break
							}

							cs = cand
							chosenIdx = idx
							break
						}

						if chosenIdx == -2 {
							continue
						}
						if chosenIdx < 0 {
							continue
						}

						b.rrCursor[topic][group] = (chosenIdx + 1) % len(chans)
					} else {
						idx := b.rrCursor[topic][group] % len(chans)
						b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
						cs = chans[idx]
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
