package broker

import (
	"context"
	"time"
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
					// Not yet timed out
					if now.Sub(e.SentAt) < b.ackTimeout {
						continue
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

					// Stop retrying once MaxAttempts is reached (temporary behavior until I add DLQ!!)
					if rp != nil && rp.MaxAttempts > 0 && e.Attempts >= rp.MaxAttempts {
						// 1) Remove from inflight so we stop redelivering it
						delete(inflight, offset)

						// Move the offset forward and PERSIST it to the WAL so a restart doesn’t bring this message back to life
						_ = b.advanceOffsetLocked(topic, group, partition, offset)

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

					// Send message with updated attempts
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
