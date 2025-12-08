package broker

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
)

type Message struct {
	Offset int64
	Key    []byte
	Value  []byte
}

type topicState struct {
	partitions [][]Message
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
	topics          map[string]*topicState
	consumerOffsets map[string]map[string]int64          // topic -> group -> offset
	consumerChans   map[string]map[string][]chan Message // topic -> group -> list of chans
}

// NewInMemoryBroker creates a new in-memory broker instance.
func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		topics:          make(map[string]*topicState),
		consumerOffsets: make(map[string]map[string]int64),
		consumerChans:   make(map[string]map[string][]chan Message),
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

	b.topics[name] = &topicState{
		partitions: make([][]Message, partitions),
	}

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

func pickPartition(key []byte, numPartitions int) int {
	if len(key) == 0 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32()) % numPartitions
}

// Produce appends a message to the given topic (in-memory only for now)
// and delivers it to any active consumers for that topic :)
func (b *InMemoryBroker) Produce(_ context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	b.mu.Lock()
	ts, exists := b.topics[topic]
	if !exists {
		b.mu.Unlock()
		return errors.New("topic does not exist (create it first)")
	}

	numPartitions := len(ts.partitions)
	part := pickPartition(msg.Key, numPartitions)
	messages := ts.partitions[part]

	msg.Offset = int64(len(messages))
	ts.partitions[part] = append(messages, msg)

	// Notify consumers
	var chansToNotify []chan Message
	if groupChans, ok := b.consumerChans[topic]; ok {
		for _, chans := range groupChans {
			chansToNotify = append(chansToNotify, chans...)
		}
	}

	b.mu.Unlock()

	// Fan-out to active consumers asynchronously.
	go func(m Message, chans []chan Message) {
		for _, ch := range chans {
			// Best-effort: if a consumer is slow, Produce may block here. Note to myself: MVP: Only
			ch <- m
		}
	}(msg, chansToNotify)

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
	ts, exists := b.topics[topic]
	if !exists {
		b.mu.RUnlock()
		return nil, errors.New("topic does not exist")
	}

	// Just for now
	part := 0
	messages := ts.partitions[part]

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
	initial := append([]Message(nil), messages[startOffset:]...)
	b.mu.RUnlock()

	// Register this consumer channel for future streaming!
	b.mu.Lock()
	groupChans, ok := b.consumerChans[topic]
	if !ok {
		groupChans = make(map[string][]chan Message)
		b.consumerChans[topic] = groupChans
	}
	groupChans[group] = append(groupChans[group], out)
	b.mu.Unlock()

	// Goroutine we got:
	//  - sends initial snapshot
	//  - then just waits for ctx cancellation (new messages are pushed by Produce directly into `out`)
	go func(baseOffset int64, initial []Message) {
		defer func() {
			// Unregister this consumer channel.
			b.mu.Lock()
			if groupChans, ok := b.consumerChans[topic]; ok {
				chans := groupChans[group]
				for i, ch := range chans {
					if ch == out {
						groupChans[group] = append(chans[:i], chans[i+1:]...)
						break
					}
				}

				if len(groupChans[group]) == 0 {
					delete(groupChans, group)
				}
			}
			b.mu.Unlock()

			close(out)
		}()

		// Send existing messages first.
		for i, m := range initial {
			m.Offset = baseOffset + int64(i)
			select {
			case <-ctx.Done():
				return
			case out <- m:
			}
		}

		<-ctx.Done()
	}(startOffset, initial)

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
