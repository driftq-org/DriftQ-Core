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
		default:
			// For now I just ignore unknown record types but this is a TODO for the future
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
		e.TenantID != ""

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

	return env
}
