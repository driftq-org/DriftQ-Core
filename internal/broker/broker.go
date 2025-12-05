package broker

import (
	"context"
	"errors"
	"sort"
	"sync"
)

type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// Broker is the core interface for the MVP message broker
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions int) error
	ListTopics(ctx context.Context) ([]string, error)

	Produce(ctx context.Context, topic string, msg Message) error
	Consume(ctx context.Context, topic string, group string) (<-chan Message, error)

	Ack(ctx context.Context, topic, group string, offset int64) error
}

// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	mu              sync.RWMutex
	topics          map[string][]Message
	consumerOffsets map[string]map[string]int64
}

// NewInMemoryBroker creates a new in-memory broker instance.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		topics:          make(map[string][]Message),
		consumerOffsets: make(map[string]map[string]int64),
	}
}

// CreateTopic creates a topic if it does not already exist. And partitions is ignored for now (TODO: real partitioning later).
func (b *InMemoryBroker) CreateTopic(_ context.Context, name string, partitions int) error {
	if name == "" {
		return errors.New("topic name cannot be empty")
	}
	if partitions <= 0 {
		return errors.New("partitions must be > 0")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[name]; exists {
		return nil
	}

	b.topics[name] = nil // no messages yet
	return nil
}

// ListTopics returns the list of topic names (sorted for stability).
func (b *InMemoryBroker) ListTopics(_ context.Context) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.topics))
	for name := range b.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Produce appends a message to the given topic (in-memory only for now).
func (b *InMemoryBroker) Produce(_ context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	messages, exists := b.topics[topic]
	if !exists {
		return errors.New("topic does not exist (create it first)")
	}

	b.topics[topic] = append(messages, msg)
	return nil
}

// Consume returns a channel that will receive all current messages
// for the given topic, then close. Consumer group is ignored for now.
func (b *InMemoryBroker) Consume(ctx context.Context, topic, group string) (<-chan Message, error) {
	if topic == "" {
		return nil, errors.New("topic cannot be empty")
	}
	if group == "" {
		return nil, errors.New("group cannot be empty (MVP requirement)")
	}

	out := make(chan Message)

	b.mu.RLock()
	messages, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, errors.New("topic does not exist")
	}

	// Figure out where this consumer group should start.
	var startOffset int64 = 0
	if groups, ok := b.consumerOffsets[topic]; ok {
		if last, ok := groups[group]; ok {
			startOffset = last + 1 // resume from next message after last acked
		}
	}

	// Clamp / handle out-of-range.
	if startOffset < 0 || startOffset > int64(len(messages)) {
		startOffset = int64(len(messages)) // nothing to read
	}

	// Copy only the slice from startOffset onward to avoid races.
	msgs := append([]Message(nil), messages[startOffset:]...)
	b.mu.RUnlock()

	// Replay messages asynchronously.
	go func(baseOffset int64, msgs []Message) {
		defer close(out)
		for i, m := range msgs {
			m.Offset = baseOffset + int64(i)

			select {
			case <-ctx.Done():
				return
			case out <- m:
			}
		}
	}(startOffset, msgs)

	return out, nil
}

func (b *InMemoryBroker) Ack(_ context.Context, topic, group string, offset int64) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[topic]; !ok {
		return errors.New("topic does not exist")
	}

	// Get or create group map for this topic.
	groups, ok := b.consumerOffsets[topic]
	if !ok {
		groups = make(map[string]int64)
		b.consumerOffsets[topic] = groups
	}

	// Store the last processed offset for this group.
	groups[group] = offset

	return nil
}
