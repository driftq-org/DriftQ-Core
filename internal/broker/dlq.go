package broker

import (
	"context"
	"fmt"
	"time"
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

	// Attach DLQ metadata (without breaking original envelope semantics)
	now := time.Now()

	var env Envelope
	if msg.Envelope != nil {
		env = *msg.Envelope // shallow copy
	} else {
		env = Envelope{}
	}

	// IMPORTANT: prevent the DLQ publish from being redirected by envelope routing controls
	env.TargetTopic = ""
	env.PartitionOverride = nil

	env.DLQ = &DLQMetadata{
		OriginalTopic:     topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		Attempts:          msg.Attempts,
		LastError:         msg.LastError,
		RoutedAtMs:        now.UnixMilli(),
	}

	msg.Envelope = &env

	return b.produceLocked(ctx, dlqTopic, msg)
}
