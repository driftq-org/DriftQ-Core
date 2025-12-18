package broker

func (b *InMemoryBroker) ensureInFlight(topic, group string, partition int) map[int64]*inflightEntry {
	if _, ok := b.inFlight[topic]; !ok {
		b.inFlight[topic] = make(map[string]map[int]map[int64]*inflightEntry)
	}

	if _, ok := b.inFlight[topic][group]; !ok {
		b.inFlight[topic][group] = make(map[int]map[int64]*inflightEntry)
	}

	if _, ok := b.inFlight[topic][group][partition]; !ok {
		b.inFlight[topic][group][partition] = make(map[int64]*inflightEntry)
	}

	return b.inFlight[topic][group][partition]
}

func (b *InMemoryBroker) ensureNextIndex(topic, group string) map[int]int {
	if _, ok := b.nextIndex[topic]; !ok {
		b.nextIndex[topic] = make(map[string]map[int]int)
	}
	if _, ok := b.nextIndex[topic][group]; !ok {
		b.nextIndex[topic][group] = make(map[int]int)
	}
	return b.nextIndex[topic][group]
}
