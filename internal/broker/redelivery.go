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

			for _, inflight := range byPart {
				for _, e := range inflight {
					if now.Sub(e.SentAt) < b.ackTimeout {
						continue
					}

					// pick one consumer in the group (round-robin!)
					idx := b.rrCursor[topic][group] % len(chans)
					b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
					ch := chans[idx]

					// update inflight bookkeeping => SOURCE OF TRUTH :)
					e.SentAt = now
					e.Attempts++

					// IMPORTANT: send message with updated attempts
					m := e.Msg
					m.Attempts = e.Attempts
					e.Msg = m // keep stored copy consistent too

					go func(ch chan Message, m Message) {
						defer func() { _ = recover() }()
						ch <- m
					}(ch, m)
				}
			}
		}
	}
}
