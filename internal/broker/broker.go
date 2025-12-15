package broker

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// Note: This is for test. This is my "do nothing" brain. It lets me plug something in without changing behavior
type NoopRouter struct{}

// This is where I stash whatever the "brain" decided about this message. v0: super simple. I can extend this later as I learn what I actually need
type RoutingMetadata struct {
	Label string            `json:"label,omitempty"`
	Meta  map[string]string `json:"meta,omitempty"`
}

type Message struct {
	Offset int64
	Key    []byte
	Value  []byte

	Routing *RoutingMetadata
}

type topicState struct {
	partitions [][]Message
	nextOffset int64
}

// RoutingDecision is what the brain tells me to do with a message. For v0 I'm keeping it small
type RoutingDecision struct {
	// High-level label for the message. I can use this for metrics, filtering, or future policy routing
	Label string

	// If set, the brain is asking me to send this to a different topic. If empty, I keep the original topic
	TargetTopic string

	// If set, the brain wants a specific partition. If nil, I fall back to my normal. hash-based partitioning logic
	PartitionOverride *int

	// Extra metadata I want to stash with the message (embedding id, scores, and more stuff)
	Meta map[string]string
}

// Router is the "brain" interface. Core only knows about this, not about Agno/OpenAI or whatever else I use
type Router interface {
	// Route looks at a message and decides how it should be labeled/routed. topic is the topic the producer asked for
	Route(ctx context.Context, topic string, msg Message) (RoutingDecision, error)
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
	consumerOffsets map[string]map[string]map[int]int64  // topic -> group -> offset -> offset
	consumerChans   map[string]map[string][]chan Message // topic -> group -> list of chans

	rrCursor map[string]map[string]int

	wal    storage.WAL
	router Router // If nil, "no brain configured"
}

func (NoopRouter) Route(_ context.Context, _ string, msg Message) (RoutingDecision, error) {
	return RoutingDecision{
		Label:             "",
		TargetTopic:       "",
		PartitionOverride: nil,
		Meta:              make(map[string]string),
	}, nil
}

func (b *InMemoryBroker) SetRouter(r Router) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.router = r
}

func NewInMemoryBroker() *InMemoryBroker {
	return NewInMemoryBrokerWithWALAndRouter(nil, nil)
}

// Creates a broker that uses the given WAL but NO router
func NewInMemoryBrokerWithWAL(wal storage.WAL) *InMemoryBroker {
	return NewInMemoryBrokerWithWALAndRouter(wal, nil)
}

// This now lets me plug in both durability and a brain, so passing nil for either is fine (pure in-memory/no routing)
func NewInMemoryBrokerWithWALAndRouter(wal storage.WAL, r Router) *InMemoryBroker {
	return &InMemoryBroker{
		topics:          make(map[string]*topicState),
		consumerOffsets: make(map[string]map[string]map[int]int64),
		consumerChans:   make(map[string]map[string][]chan Message),
		rrCursor:        make(map[string]map[string]int),
		wal:             wal,
		router:          r,
	}
}

// NewInMemoryBrokerFromWAL builds a broker, then replays whatever is in the WAL so I can restore topics, partitions, messages and consumer offsets on startup.
func NewInMemoryBrokerFromWAL(wal storage.WAL) (*InMemoryBroker, error) {
	b := NewInMemoryBrokerWithWAL(wal)

	if wal == nil {
		return b, nil
	}

	entries, err := wal.Replay()
	if err != nil {
		return nil, err
	}

	// Rebuild in-memory state from the log
	for _, e := range entries {
		switch e.Type {
		case storage.RecordTypeMessage:
			ts, ok := b.topics[e.Topic]
			if !ok {
				// No idea how many partitions this topic "should" have, so I grow the slice as needed based on whatever the WAL tells me
				ts = &topicState{
					partitions: make([][]Message, 0),
					nextOffset: 0,
				}
				b.topics[e.Topic] = ts
			}

			for len(ts.partitions) <= e.Partition {
				ts.partitions = append(ts.partitions, nil)
			}

			m := Message{
				Key:    e.Key,
				Value:  e.Value,
				Offset: e.Offset,
			}

			// Restore routing metadata if present.
			if e.RoutingLabel != "" || len(e.RoutingMeta) > 0 {
				m.Routing = &RoutingMetadata{
					Label: e.RoutingLabel,
					Meta:  e.RoutingMeta,
				}
			}

			ts.partitions[e.Partition] = append(ts.partitions[e.Partition], m)

			if e.Offset >= ts.nextOffset {
				ts.nextOffset = e.Offset + 1
			}

		case storage.RecordTypeOffset:
			if _, ok := b.consumerOffsets[e.Topic]; !ok {
				b.consumerOffsets[e.Topic] = make(map[string]map[int]int64)
			}

			if _, ok := b.consumerOffsets[e.Topic][e.Group]; !ok {
				b.consumerOffsets[e.Topic][e.Group] = make(map[int]int64)
			}

			cur, ok := b.consumerOffsets[e.Topic][e.Group][e.Partition]
			if !ok || e.Offset > cur {
				b.consumerOffsets[e.Topic][e.Group][e.Partition] = e.Offset
			}
		default:
			// For now I just ignore unknown record types but this is a TODO for the future
		}
	}

	return b, nil
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
		nextOffset: 0,
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

	// If I have a router, let it take a look at this message before I do anything
	// This won't affect routing yet — I just stash whatever metadata it returns
	if b.router != nil {
		decision, err := b.router.Route(context.Background(), topic, msg)
		if err == nil {
			// Attach whatever the brain said here
			msg.Routing = &RoutingMetadata{
				Label: decision.Label,
				Meta:  decision.Meta,
			}
		} else {
			// Note to myself:
			// if the router blows up, I just ignore it for now, because v0 is not strict about routing failures.
		}
	}

	b.mu.Lock()
	ts, exists := b.topics[topic]
	if !exists {
		b.mu.Unlock()
		return errors.New("topic does not exist (create it first)")
	}

	// Choose partition
	numPartitions := len(ts.partitions)
	part := pickPartition(msg.Key, numPartitions)
	messages := ts.partitions[part]

	// Assign global topic-wide offset, but DO NOT mutate state yet.
	msg.Offset = ts.nextOffset

	// --- WAL append first (if configured) ---
	if b.wal != nil {
		entry := storage.Entry{
			Type:      storage.RecordTypeMessage,
			Topic:     topic,
			Partition: part,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,

			RoutingLabel: "",
			RoutingMeta:  nil,
		}

		if msg.Routing != nil {
			entry.RoutingLabel = msg.Routing.Label
			entry.RoutingMeta = msg.Routing.Meta
		}

		if err := b.wal.Append(entry); err != nil {
			b.mu.Unlock()
			return err
		}
	}

	ts.nextOffset++
	ts.partitions[part] = append(messages, msg)

	var chansToNotify []chan Message

	if groupChans, ok := b.consumerChans[topic]; ok {
		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		for group, chans := range groupChans {
			if len(chans) == 0 {
				continue
			}
			idx := b.rrCursor[topic][group] % len(chans)
			b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)

			chansToNotify = append(chansToNotify, chans[idx])
		}
	}

	b.mu.Unlock()

	go func(m Message, chans []chan Message) {
		for _, ch := range chans {
			// This keeps the app from crashing if a consumer disconnects mid-send
			func() {
				defer func() { _ = recover() }()
				ch <- m
			}()
		}
	}(msg, chansToNotify)

	return nil
}

// Consume returns a channel that will receive all current messages for the given topic, then close. Consumer group is ignored for now.
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

	// Figure out where this consumer group should start (by offset)
	var startOffset int64 = 0
	if groups, ok := b.consumerOffsets[topic]; ok {
		if parts, ok := groups[group]; ok {
			if last, ok := parts[0]; ok {
				startOffset = last + 1 // This is TEMP: partition 0 only
			}
		}
	}
	// Collect messages from ALL partitions.
	var all []Message
	for _, partMsgs := range ts.partitions {
		all = append(all, partMsgs...)
	}
	b.mu.RUnlock()

	// Filter by offset >= startOffset.
	initial := make([]Message, 0, len(all))
	for _, m := range all {
		if m.Offset >= startOffset {
			initial = append(initial, m)
		}
	}

	// Register this consumer channel for future streaming.
	b.mu.Lock()
	groupChans, ok := b.consumerChans[topic]
	if !ok {
		groupChans = make(map[string][]chan Message)
		b.consumerChans[topic] = groupChans
	}

	sendInitial := len(groupChans[group]) == 0

	groupChans[group] = append(groupChans[group], out)
	b.mu.Unlock()

	// Goroutine:
	//  - sends initial snapshot (optional)
	//  - then just waits for ctx cancellation (new messages are pushed by Produce into `out`)
	go func(initial []Message, sendInitial bool) {
		defer func() {
			// Unregister this consumer channel
			b.mu.Lock()
			if groupChans, ok := b.consumerChans[topic]; ok {
				chans := groupChans[group]
				for i, ch := range chans {
					if ch == out {
						groupChans[group] = append(chans[:i], chans[i+1:]...)
						break
					}
				}

				// If no consumers left for this group, delete the group entry
				if len(groupChans[group]) == 0 {
					delete(groupChans, group)
				}

				if cursors, ok := b.rrCursor[topic]; ok {
					if _, ok := groupChans[group]; !ok {
						// Note: group entry deleted => no consumers
						delete(cursors, group)
					} else if cur, ok := cursors[group]; ok && cur >= len(groupChans[group]) {
						cursors[group] = cur % len(groupChans[group])
					}
				}
			}
			b.mu.Unlock()

			close(out)
		}()

		if sendInitial {
			// Send existing messages first (offsets already set by Produce)
			for _, m := range initial {
				select {
				case <-ctx.Done():
					return
				case out <- m:
				}
			}
		}

		// Wait until client cancels (after snapshot)
		<-ctx.Done()
	}(initial, sendInitial)

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

	// If I have a WAL, log this offset so I can restore group progress on restart
	if b.wal != nil {
		entry := storage.Entry{
			Type:   storage.RecordTypeOffset,
			Topic:  topic,
			Group:  group,
			Offset: offset,
		}

		if err := b.wal.Append(entry); err != nil {
			return err
		}
	}

	groups, ok := b.consumerOffsets[topic]
	if !ok {
		groups = make(map[string]map[int]int64)
		b.consumerOffsets[topic] = groups
	}

	parts, ok := groups[group]
	if !ok {
		parts = make(map[int]int64)
		groups[group] = parts
	}

	// This is TEMP: partition 0 only (until I add partition-aware ack)
	// This is OK for now because /ack doesn't inlcude a partition yet and my Consume currently merges partitions anyway
	parts[0] = offset
	return nil
}
