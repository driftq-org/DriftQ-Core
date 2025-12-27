package broker

import (
	"time"
)

func (b *InMemoryBroker) dispatchLocked(topic string) {
	ts, ok := b.topics[topic]
	if !ok {
		return
	}

	groupChans, ok := b.consumerChans[topic]
	if !ok {
		return
	}

	for group, chans := range groupChans {
		if len(chans) == 0 {
			continue
		}

		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		nextByPart := b.ensureNextIndex(topic, group)

		for p := range ts.partitions {
			inflight := b.ensureInFlight(topic, group, p)
			if b.maxInFlight > 0 && len(inflight) >= b.maxInFlight {
				continue
			}

			// resume after last ack if we haven't initialized next index yet
			if _, ok := nextByPart[p]; !ok {
				last := int64(-1)
				if byTopic, ok := b.consumerOffsets[topic]; ok {
					if byGroup, ok := byTopic[group]; ok {
						if v, ok := byGroup[p]; ok {
							last = v
						}
					}
				}

				// find first message with Offset > last
				idx := 0
				for idx < len(ts.partitions[p]) && ts.partitions[p][idx].Offset <= last {
					idx++
				}
				nextByPart[p] = idx
			}

			for nextByPart[p] < len(ts.partitions[p]) {
				if b.maxInFlight > 0 && len(inflight) >= b.maxInFlight {
					break
				}

				m := ts.partitions[p][nextByPart[p]]
				nextByPart[p]++

				// pick one consumer in the group (round-robin)
				idx := b.rrCursor[topic][group] % len(chans)
				b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
				ch := chans[idx]

				e, ok := inflight[m.Offset]
				if !ok || e == nil {
					rs := b.ensureRetryState(topic, group, p)

					lastErr := ""
					if st, ok := rs[m.Offset]; ok && st != nil {
						lastErr = st.LastError
					}

					e = &inflightEntry{
						Msg:       m,
						SentAt:    time.Now(),
						Attempts:  1,
						LastError: lastErr,
					}
					inflight[m.Offset] = e
				}

				// Build message to send (Attempts and LastError come from inflight entry)
				send := m
				send.Attempts = e.Attempts
				send.LastError = e.LastError

				go func(ch chan Message, m Message) {
					defer func() { _ = recover() }()
					ch <- m
				}(ch.Ch, send)
			}
		}
	}
}
