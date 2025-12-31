package broker

import "context"

type InflightCount struct {
	Topic     string
	Group     string
	Partition int
	Count     int
}

func (b *InMemoryBroker) SnapshotInflightCounts(_ context.Context) []InflightCount {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]InflightCount, 0, 64)

	for topic, byGroup := range b.inFlight {
		for group, byPart := range byGroup {
			for part, inflight := range byPart {
				if inflight == nil {
					continue
				}
				out = append(out, InflightCount{
					Topic:     topic,
					Group:     group,
					Partition: part,
					Count:     len(inflight),
				})
			}
		}
	}

	return out
}
