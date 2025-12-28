package broker

import (
	"errors"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
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

			// Resume after last ack if we haven't initialized next index yet
			if _, ok := nextByPart[p]; !ok {
				last := int64(-1)
				if byTopic, ok := b.consumerOffsets[topic]; ok {
					if byGroup, ok := byTopic[group]; ok {
						if v, ok := byGroup[p]; ok {
							last = v
						}
					}
				}

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

				// IMPORTANT: don't advance nextByPart until we either
				// (a) skip/advance, or (b) successfully stage delivery
				m := ts.partitions[p][nextByPart[p]]

				// If consume-scope idempotency is already COMMITTED for this (tenant,topic,group,key),
				// treat this message as already done and advance the group's offset without delivering
				if b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != "" {
					tenantID := m.Envelope.TenantID
					idk := m.Envelope.IdempotencyKey

					if st, ok := b.idem.ConsumeCheck(tenantID, topic, group, idk); ok && st.Status == IdemStatusCommitted {
						if _, ok := b.consumerOffsets[topic]; !ok {
							b.consumerOffsets[topic] = make(map[string]map[int]int64)
						}
						if _, ok := b.consumerOffsets[topic][group]; !ok {
							b.consumerOffsets[topic][group] = make(map[int]int64)
						}

						// IMPORTANT: missing offset should behave like -1 (so offset 0 can advance)
						cur, ok := b.consumerOffsets[topic][group][p]
						if !ok {
							cur = -1
						}

						// Only advance if moving forward
						if m.Offset > cur {
							if b.wal != nil {
								if err := b.wal.Append(storage.Entry{
									Type:      storage.RecordTypeOffset,
									Topic:     topic,
									Group:     group,
									Partition: p,
									Offset:    m.Offset,
								}); err != nil {
									return
								}
							}

							b.consumerOffsets[topic][group][p] = m.Offset
						}

						b.purgeRetryStateLocked(topic, group, p, m.Offset)

						nextByPart[p]++
						continue
					}
				}

				// If it's already in-flight, DO NOT deliver it again here. Redelivery loop owns retries
				if e, ok := inflight[m.Offset]; ok && e != nil {
					nextByPart[p]++
					continue
				}

				// pick one consumer in the group (round-robin), BUT if this message has an idempotency key,
				// we must successfully BeginLease for the chosen owner before delivering.
				start := b.rrCursor[topic][group] % len(chans)

				var (
					cs        consumerStream
					csIndex   = -1
					lease     time.Duration
					alreadyOK bool
				)

				// default lease for this delivery attempt
				leaseDefault := b.ackTimeout
				if leaseDefault <= 0 {
					leaseDefault = 2 * time.Second
				}

				hasIdem := b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != ""
				if !hasIdem {
					csIndex = start
					cs = chans[csIndex]
					b.rrCursor[topic][group] = (csIndex + 1) % len(chans)
					lease = cs.Lease
					if lease <= 0 {
						lease = leaseDefault
					}
				} else {
					tenantID := m.Envelope.TenantID
					idk := m.Envelope.IdempotencyKey

					var beginErr error

					for tries := 0; tries < len(chans); tries++ {
						i := (start + tries) % len(chans)
						cand := chans[i]

						candLease := cand.Lease
						if candLease <= 0 {
							candLease = leaseDefault
						}

						alreadyDone, _, err := b.idem.ConsumeBeginLease(tenantID, topic, group, idk, cand.Owner, candLease)
						if err == nil {
							// success: reserve this key for this owner
							csIndex = i
							cs = cand
							lease = candLease
							alreadyOK = alreadyDone
							b.rrCursor[topic][group] = (i + 1) % len(chans)
							break
						}

						if errors.Is(err, ErrIdempotencyLeaseHeld) {
							continue
						}

						beginErr = err
						break
					}

					// if begin failed with a real error, don't skip the message; try again later
					if beginErr != nil {
						rs := b.ensureRetryState(topic, group, p)
						rs[m.Offset] = &retryStateEntry{
							LastError:   "idem_begin_failed: " + beginErr.Error(),
							LastErrorAt: time.Now(),
						}
						if b.wal != nil {
							at := time.Now()
							_ = b.wal.Append(storage.Entry{
								Type:        storage.RecordTypeRetryState,
								Topic:       topic,
								Group:       group,
								Partition:   p,
								Offset:      m.Offset,
								LastError:   "idem_begin_failed: " + beginErr.Error(),
								LastErrorAt: &at,
							})
						}
						break
					}

					// if all candidates were lease-held, don't advance nextByPart (don't lose the message)
					if csIndex == -1 {
						break
					}

					// If store says "already done", treat like committed skip (Option A)
					if alreadyOK {
						if _, ok := b.consumerOffsets[topic]; !ok {
							b.consumerOffsets[topic] = make(map[string]map[int]int64)
						}
						if _, ok := b.consumerOffsets[topic][group]; !ok {
							b.consumerOffsets[topic][group] = make(map[int]int64)
						}
						cur, ok := b.consumerOffsets[topic][group][p]
						if !ok {
							cur = -1
						}
						if m.Offset > cur {
							if b.wal != nil {
								if err := b.wal.Append(storage.Entry{
									Type:      storage.RecordTypeOffset,
									Topic:     topic,
									Group:     group,
									Partition: p,
									Offset:    m.Offset,
								}); err != nil {
									return
								}
							}
							b.consumerOffsets[topic][group][p] = m.Offset
						}

						b.purgeRetryStateLocked(topic, group, p, m.Offset)

						nextByPart[p]++
						continue
					}
				}

				// seed error from retryState
				rs := b.ensureRetryState(topic, group, p)
				lastErr := ""
				if st, ok := rs[m.Offset]; ok && st != nil {
					lastErr = st.LastError
				}

				e := &inflightEntry{
					Msg:       m,
					SentAt:    time.Now(),
					Attempts:  1,
					LastError: lastErr,
					Owner:     cs.Owner,
				}
				inflight[m.Offset] = e

				// Build message to send (Attempts and LastError come from inflight entry)
				send := m
				send.Attempts = e.Attempts
				send.LastError = e.LastError

				nextByPart[p]++

				go func(ch chan Message, m Message) {
					defer func() { _ = recover() }()
					ch <- m
				}(cs.Ch, send)
			}
		}
	}
}
