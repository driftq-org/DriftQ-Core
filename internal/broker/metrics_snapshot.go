package broker

import (
	"sort"
	"strconv"
)

type PartitionMetric struct {
	Topic     string
	Group     string
	Partition int
	Value     int64
}

type MetricsSnapshot struct {
	InFlight    []PartitionMetric
	ConsumerLag []PartitionMetric
}

func (b *InMemoryBroker) MetricsSnapshot() MetricsSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := MetricsSnapshot{}

	for topic, ts := range b.topics {
		numParts := len(ts.partitions)

		// union of groups we care about (offsets, inflight, active consumers)
		groups := map[string]struct{}{}

		if byGroup, ok := b.consumerOffsets[topic]; ok {
			for g := range byGroup {
				groups[g] = struct{}{}
			}
		}

		if byGroup, ok := b.inFlight[topic]; ok {
			for g := range byGroup {
				groups[g] = struct{}{}
			}
		}

		if byGroup, ok := b.consumerChans[topic]; ok {
			for g := range byGroup {
				groups[g] = struct{}{}
			}
		}

		getCommitted := func(group string, partition int) int64 {
			if byGroup, ok := b.consumerOffsets[topic]; ok {
				if byPart, ok := byGroup[group]; ok {
					if off, ok := byPart[partition]; ok {
						return off
					}
				}
			}
			return -1
		}

		lagForPartition := func(part []Message, committed int64) int64 {
			if len(part) == 0 {
				return 0
			}
			i := sort.Search(len(part), func(i int) bool { return part[i].Offset > committed })
			return int64(len(part) - i)
		}

		inflightCount := func(group string, partition int) int64 {
			if byGroup, ok := b.inFlight[topic]; ok {
				if byPart, ok := byGroup[group]; ok {
					if byOff, ok := byPart[partition]; ok {
						return int64(len(byOff))
					}
				}
			}
			return 0
		}

		for group := range groups {
			for p := 0; p < numParts; p++ {
				committed := getCommitted(group, p)
				lag := lagForPartition(ts.partitions[p], committed)
				inflight := inflightCount(group, p)

				out.ConsumerLag = append(out.ConsumerLag, PartitionMetric{
					Topic: topic, Group: group, Partition: p, Value: lag,
				})
				out.InFlight = append(out.InFlight, PartitionMetric{
					Topic: topic, Group: group, Partition: p, Value: inflight,
				})
			}
		}
	}

	_ = strconv.IntSize
	return out
}
