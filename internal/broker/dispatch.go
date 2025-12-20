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
				attempt := 0

				if e, ok := inflight[m.Offset]; ok {
					// already in-flight (and shouldn't usually happen), but treat as a re-send attempt
					e.SentAt = time.Now()
					e.Attempts++
					attempt = e.Attempts
				} else {
					inflight[m.Offset] = &inflightEntry{
						Msg:      m,
						SentAt:   time.Now(),
						Attempts: 1,
					}
					attempt = 1
				}

				// IMPORTANT: Attempts is delivery-attempt count, sourced from inflight entry
				send := m
				send.Attempts = attempt

				go func(ch chan Message, m Message) {
					defer func() { _ = recover() }()
					ch <- m
				}(ch, send)

			}
		}
	}
}
