package broker

import (
	"context"
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

			for partition, inflight := range byPart {
				for offset, e := range inflight {
					if e == nil {
						delete(inflight, offset)
						continue
					}

					// Not yet timed out
					if now.Sub(e.SentAt) < b.ackTimeout {
						continue
					}

					// If it timed out without an explicit Nack, record ack_timeout as last_error
					// But don't overwrite a real error reason (e.g. "boom") if one already exists
					if e.LastError == "" {
						e.LastError = "ack_timeout"

						// update stored msg copy too to keep stuff consistent
						m := e.Msg
						m.LastError = e.LastError
						e.Msg = m

						// update retryState map (so dispatch can seed after restart)
						rs := b.ensureRetryState(topic, group, partition)
						rs[offset] = &retryStateEntry{
							LastError:   e.LastError,
							LastErrorAt: now,
						}

						// persist to WAL
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
						// Build a fresh message that includes the latest attempt/error state
						dlqMsg := e.Msg
						dlqMsg.Attempts = e.Attempts
						dlqMsg.LastError = e.LastError

						var envCopy *Envelope
						if e.Msg.Envelope != nil {
							tmp := *e.Msg.Envelope // do not forget that this is a shallow copy (enough for our DLQ metadata use)
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

						// 1) Publish to DLQ
						if err := b.publishToDLQLocked(context.Background(), topic, dlqMsg); err != nil {
							// IMPORTANT: preserve original error, append DLQ failure info
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

						// 2) Remove from inflight so we stop redelivering it
						delete(inflight, offset)

						// 3) Advance offset (persist) so restart won't resurrect it
						_ = b.advanceOffsetLocked(topic, group, partition, offset)

						// 4) Purge stale retry state for this offset
						b.purgeRetryStateLocked(topic, group, partition, offset)

						continue
					}

					// pick one consumer in the group (round-robin!)
					idx := b.rrCursor[topic][group] % len(chans)
					b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
					ch := chans[idx]

					// update inflight bookkeeping (source of truth)
					e.SentAt = now
					e.Attempts++

					// Backoff scheduling (for the next eligible retry send time)
					if rp != nil && (rp.BackoffMs > 0 || rp.MaxBackoffMs > 0) {
						retryNumber := e.Attempts - 1 // attempt=2 => retry#1, attempt=3 => retry#2 ...
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
					}(ch, m)
				}
			}
		}
	}
}
