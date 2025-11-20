package broker

import (
	"context"
	"errors"
	"sort"
	"sync"
)

type Message struct {
	Key   []byte
	Value []byte
}

// Broker is the core interface for the MVP message broker
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions int) error
	ListTopics(ctx context.Context) ([]string, error)

	Produce(ctx context.Context, topic string, msg Message) error
	Consume(ctx context.Context, topic string, group string) (<-chan Message, error)
}

// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	mu     sync.RWMutex
	topics map[string][]Message
}

// NewInMemoryBroker creates a new in-memory broker instance.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		topics: make(map[string][]Message),
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
