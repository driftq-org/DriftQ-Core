package broker

import "github.com/driftq-org/DriftQ-Core/internal/storage"

// NewInMemoryBrokerFromWAL builds a broker, then replays whatever is in the WAL so I can restore topics, partitions, messages and consumer offsets on startup.
func NewInMemoryBrokerFromWAL(wal storage.WAL) (*InMemoryBroker, error) {
	b := NewInMemoryBrokerWithWAL(wal)

	if wal == nil {
		return b, nil
	}

	entries, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	// Rebuild in-memory state from the log
	for _, e := range entries {
		switch e.Type {
		case storage.RecordTypeMessage:
			ts, ok := b.topics[e.Topic]
			if !ok {
				// No idea how many partitions this topic "should" have, so I grow the slice as needed based on whatever the WAL tells me
				ts = &TopicState{
					partitions: make([][]Message, 0),
					nextOffset: 0,
				}
				b.topics[e.Topic] = ts
			}

			for len(ts.partitions) <= e.Partition {
				ts.partitions = append(ts.partitions, nil)
			}

			m := Message{
				Key:       e.Key,
				Partition: e.Partition,
				Value:     e.Value,
				Offset:    e.Offset,
				Envelope:  envelopeFromEntry(e),
			}

			// Restore routing metadata if present.
			if e.RoutingLabel != "" || len(e.RoutingMeta) > 0 {
				m.Routing = &RoutingMetadata{
					Label: e.RoutingLabel,
					Meta:  e.RoutingMeta,
				}
			}

			// Rebuild idempotency committed state from WAL
			if b.idem != nil && m.Envelope != nil && m.Envelope.IdempotencyKey != "" {
				tenantID := m.Envelope.TenantID // FYI: may be ""
				b.idem.Commit(tenantID, e.Topic, m.Envelope.IdempotencyKey, nil)
			}

			ts.partitions[e.Partition] = append(ts.partitions[e.Partition], m)

			if e.Offset >= ts.nextOffset {
				ts.nextOffset = e.Offset + 1
			}

		case storage.RecordTypeOffset:
			if _, ok := b.consumerOffsets[e.Topic]; !ok {
				b.consumerOffsets[e.Topic] = make(map[string]map[int]int64)
			}

			if _, ok := b.consumerOffsets[e.Topic][e.Group]; !ok {
				b.consumerOffsets[e.Topic][e.Group] = make(map[int]int64)
			}

			cur, ok := b.consumerOffsets[e.Topic][e.Group][e.Partition]
			if !ok || e.Offset > cur {
				b.consumerOffsets[e.Topic][e.Group][e.Partition] = e.Offset
			}

		case storage.RecordTypeRetryState:
			// (topic, group, partition, offset) -> last_error (+ timestamp)
			if e.Topic == "" || e.Group == "" {
				// group is required for retry state; ignore malformed entries
				continue
			}

			rs := b.ensureRetryState(e.Topic, e.Group, e.Partition)

			at := retryStateEntry{
				LastError: e.LastError,
			}
			if e.LastErrorAt != nil {
				at.LastErrorAt = *e.LastErrorAt
			}

			// Last-write-wins naturally since WAL is replayed in order
			rs[e.Offset] = &at

		default:
			// ignore unknown record types for now
		}
	}

	// IMPORTANT: purge retry state for anything already acked, so old errors don’t "resurrect" after restart.
	for topic, byGroup := range b.consumerOffsets {
		for group, byPart := range byGroup {
			for partition, ackedOffset := range byPart {
				// Only purge if retry state exists
				if byTopicRS, ok := b.retryState[topic]; ok {
					if byGroupRS, ok := byTopicRS[group]; ok {
						if byPartRS, ok := byGroupRS[partition]; ok {
							for off := range byPartRS {
								if off <= ackedOffset {
									delete(byPartRS, off)
								}
							}

							// cleanup empties (optional, but keeps memory tidy)
							if len(byPartRS) == 0 {
								delete(byGroupRS, partition)
							}

							if len(byGroupRS) == 0 {
								delete(byTopicRS, group)
							}

							if len(byTopicRS) == 0 {
								delete(b.retryState, topic)
							}
						}
					}
				}
			}
		}
	}

	return b, nil
}

func envelopeFromEntry(e storage.Entry) *Envelope {
	// Decide if we should allocate an envelope at all
	has := e.RunID != "" ||
		e.StepID != "" ||
		e.ParentStepID != "" ||
		len(e.Labels) > 0 ||
		e.TargetTopic != "" ||
		e.PartitionOverride != nil ||
		e.IdempotencyKey != "" ||
		e.Deadline != nil ||
		e.RetryMaxAttempts != 0 ||
		e.RetryBackoffMs != 0 ||
		e.RetryMaxBackoffMs != 0 ||
		e.TenantID != "" ||
		e.DLQOriginalTopic != "" ||
		e.DLQOriginalPartition != 0 ||
		e.DLQOriginalOffset != 0 ||
		e.DLQAttempts != 0 ||
		e.DLQLastError != "" ||
		e.DLQRoutedAtMs != 0

	if !has {
		return nil
	}

	env := &Envelope{
		RunID:             e.RunID,
		StepID:            e.StepID,
		ParentStepID:      e.ParentStepID,
		Labels:            e.Labels,
		TargetTopic:       e.TargetTopic,
		PartitionOverride: e.PartitionOverride,
		IdempotencyKey:    e.IdempotencyKey,
		Deadline:          e.Deadline,
		TenantID:          e.TenantID,
	}

	if e.RetryMaxAttempts != 0 || e.RetryBackoffMs != 0 || e.RetryMaxBackoffMs != 0 {
		env.RetryPolicy = &RetryPolicy{
			MaxAttempts:  e.RetryMaxAttempts,
			BackoffMs:    e.RetryBackoffMs,
			MaxBackoffMs: e.RetryMaxBackoffMs,
		}
	}

	if e.DLQOriginalTopic != "" ||
		e.DLQOriginalPartition != 0 ||
		e.DLQOriginalOffset != 0 ||
		e.DLQAttempts != 0 ||
		e.DLQLastError != "" ||
		e.DLQRoutedAtMs != 0 {
		env.DLQ = &DLQMetadata{
			OriginalTopic:     e.DLQOriginalTopic,
			OriginalPartition: e.DLQOriginalPartition,
			OriginalOffset:    e.DLQOriginalOffset,
			Attempts:          e.DLQAttempts,
			LastError:         e.DLQLastError,
			RoutedAtMs:        e.DLQRoutedAtMs,
		}
	}

	return env
}
