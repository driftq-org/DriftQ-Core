package broker

import (
	"context"
	"fmt"
)

func dlqTopicName(topic string) string {
	return fmt.Sprintf("dlq.%s", topic)
}

func (b *InMemoryBroker) publishToDLQLocked(ctx context.Context, topic string, msg Message) error {
	if topic == "" {
		return fmt.Errorf("source topic cannot be empty")
	}

	dlqTopic := dlqTopicName(topic)

	// Ensure DLQ topic exists (same partition count as source topic if possible)
	if _, ok := b.topics[dlqTopic]; !ok {
		partitions := 1
		if ts, ok := b.topics[topic]; ok && ts != nil && len(ts.partitions) > 0 {
			partitions = len(ts.partitions)
		}

		if err := b.createTopicLocked(dlqTopic, partitions); err != nil {
			return err
		}
	}

	// This is minimal payload: original key/value + envelope (optional already present on msg)
	return b.produceLocked(ctx, dlqTopic, msg)
}
