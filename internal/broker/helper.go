package broker

import (
	"hash/fnv"
	"math"
	"strings"
)

func pickPartition(key []byte, numPartitions int) int {
	if len(key) == 0 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32()) % numPartitions
}

func bufferedCount(partMsgs []Message, slowestAck int64) int {
	i := 0
	for i < len(partMsgs) && partMsgs[i].Offset <= slowestAck {
		i++
	}
	return len(partMsgs) - i
}

func bufferedBytesCount(partMsgs []Message, slowestAck int64) int {
	bytes := 0

	for i := range partMsgs {
		if partMsgs[i].Offset <= slowestAck {
			continue
		}
		bytes += len(partMsgs[i].Key) + len(partMsgs[i].Value)
	}

	return bytes
}

func (b *InMemoryBroker) slowestAckLocked(topic string, partition int) int64 {
	byGroup, ok := b.consumerOffsets[topic]
	if !ok || len(byGroup) == 0 {
		return -1
	}

	slowest := int64(math.MaxInt64)
	seen := false

	for _, byPart := range byGroup {
		off, ok := byPart[partition]
		if !ok {
			off = -1
		}

		if !seen || off < slowest {
			slowest = off
			seen = true
		}
	}

	if !seen {
		return -1
	}

	return slowest
}

func appendLastError(existing, addition string) string {
	addition = strings.TrimSpace(addition)
	if addition == "" {
		return existing
	}

	existing = strings.TrimSpace(existing)
	if existing == "" {
		return addition
	}

	return existing + " | " + addition
}
