package broker

import "time"

type retryStateEntry struct {
	LastError   string
	LastErrorAt time.Time
}

func (b *InMemoryBroker) ensureRetryState(topic, group string, partition int) map[int64]*retryStateEntry {
	if _, ok := b.retryState[topic]; !ok {
		b.retryState[topic] = make(map[string]map[int]map[int64]*retryStateEntry)
	}

	if _, ok := b.retryState[topic][group]; !ok {
		b.retryState[topic][group] = make(map[int]map[int64]*retryStateEntry)
	}

	if _, ok := b.retryState[topic][group][partition]; !ok {
		b.retryState[topic][group][partition] = make(map[int64]*retryStateEntry)
	}

	return b.retryState[topic][group][partition]
}

func (b *InMemoryBroker) purgeRetryStateLocked(topic, group string, partition int, ackedOffset int64) {
	byPart := b.ensureRetryState(topic, group, partition)
	for off := range byPart {
		if off <= ackedOffset {
			delete(byPart, off)
		}
	}
}
