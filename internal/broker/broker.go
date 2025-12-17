package broker

import (
	"context"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

var ErrBackpressure = errors.New("backpressure: partition is full")

type inflightEntry struct {
	Msg      Message
	SentAt   time.Time
	Attempts int
}

// Note: This is for test. This is my "do nothing" brain. It lets me plug something in without changing behavior
type NoopRouter struct{}

// This is where I stash whatever the "brain" decided about this message. v0: super simple. I can extend this later as I learn what I actually need
type RoutingMetadata struct {
	Label string            `json:"label,omitempty"`
	Meta  map[string]string `json:"meta,omitempty"`
}

type Message struct {
	Offset    int64
	Partition int
	Key       []byte
	Value     []byte
	Routing   *RoutingMetadata
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

	Ack(ctx context.Context, topic, group string, partition int, offset int64) error
}

// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	mu     sync.RWMutex
	topics map[string]*topicState

	// topic -> group -> offset -> offset
	consumerOffsets map[string]map[string]map[int]int64

	// topic -> group -> list of chans
	consumerChans map[string]map[string][]chan Message

	rrCursor map[string]map[string]int

	// max unacked messages allowed per (topic, group, partition)
	maxInFlight int

	// topic -> group -> partition -> set(offset) of currently in-flight (delivered but not acked)
	inFlight map[string]map[string]map[int]map[int64]*inflightEntry

	// topic -> group -> partition -> next index in ts.partitions[partition] to attempt dispatch
	nextIndex map[string]map[string]map[int]int

	wal    storage.WAL
	router Router // If nil, "no brain configured"

	ackTimeout    time.Duration
	redeliverTick time.Duration

	maxPartitionMsgs int
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
		topics:           make(map[string]*topicState),
		consumerOffsets:  make(map[string]map[string]map[int]int64),
		consumerChans:    make(map[string]map[string][]chan Message),
		rrCursor:         make(map[string]map[string]int),
		maxInFlight:      2, // for testing, later can raise if needed!
		inFlight:         make(map[string]map[string]map[int]map[int64]*inflightEntry),
		nextIndex:        make(map[string]map[string]map[int]int),
		wal:              wal,
		router:           r,
		ackTimeout:       2 * time.Second,
		redeliverTick:    250 * time.Millisecond,
		maxPartitionMsgs: 5,
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
				Key:       e.Key,
				Partition: e.Partition,
				Value:     e.Value,
				Offset:    e.Offset,
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

func (b *InMemoryBroker) Produce(_ context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	// If I have a router, let it take a look at this message before I do anything
	// This won't affect routing yet — I just stash whatever metadata it returns
	if b.router != nil {
		decision, err := b.router.Route(context.Background(), topic, msg)
		if err == nil {
			msg.Routing = &RoutingMetadata{
				Label: decision.Label,
				Meta:  decision.Meta,
			}
		}
	}

	b.mu.Lock()
	ts, exists := b.topics[topic]
	if !exists {
		b.mu.Unlock()
		return errors.New("topic does not exist (create it first)")
	}

	numPartitions := len(ts.partitions)
	part := pickPartition(msg.Key, numPartitions)

	if b.maxPartitionMsgs > 0 && len(ts.partitions[part]) >= b.maxPartitionMsgs {
		b.mu.Unlock()
		return ErrBackpressure
	}

	msg.Offset = ts.nextOffset
	msg.Partition = part

	// WAL append first (if configured)
	if b.wal != nil {
		entry := storage.Entry{
			Type:      storage.RecordTypeMessage,
			Topic:     topic,
			Partition: part,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
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

	// Commit to in-memory state
	ts.nextOffset++
	ts.partitions[part] = append(ts.partitions[part], msg)

	// NEW: deliver what we can, respecting maxInFlight
	b.dispatchLocked(topic)

	b.mu.Unlock()
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

	// Register this consumer channel for streaming, and kick backlog dispatch through dispatchLocked() so inFlight entries are created (required for redelivery)
	b.mu.Lock()
	if _, exists := b.topics[topic]; !exists {
		b.mu.Unlock()
		return nil, errors.New("topic does not exist")
	}

	groupChans, ok := b.consumerChans[topic]
	if !ok {
		groupChans = make(map[string][]chan Message)
		b.consumerChans[topic] = groupChans
	}

	// If this is the FIRST consumer for this group, kick backlog dispatch
	kick := len(groupChans[group]) == 0

	groupChans[group] = append(groupChans[group], out)

	if kick {
		b.dispatchLocked(topic)
	}
	b.mu.Unlock()

	// Wait for ctx cancel; messages are pushed by dispatchLocked/Produce/redelivery loop
	go func() {
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

				// Keep rrCursor sane
				if cursors, ok := b.rrCursor[topic]; ok {
					if _, ok := groupChans[group]; !ok {
						delete(cursors, group)
					} else if cur, ok := cursors[group]; ok && cur >= len(groupChans[group]) {
						cursors[group] = cur % len(groupChans[group])
					}
				}
			}
			b.mu.Unlock()

			close(out)
		}()

		<-ctx.Done()
	}()

	return out, nil
}

func (b *InMemoryBroker) Ack(_ context.Context, topic, group string, partition int, offset int64) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	if group == "" {
		return errors.New("group cannot be empty")
	}

	if partition < 0 {
		return errors.New("partition cannot be negative")
	}

	if offset < 0 {
		return errors.New("offset cannot be negative")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ts, ok := b.topics[topic]
	if !ok {
		return errors.New("topic does not exist")
	}

	if partition >= len(ts.partitions) {
		return errors.New("partition out of range")
	}

	// Verify offsets map exists
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

	if cur, ok := parts[partition]; ok && offset <= cur {
		// still remove from inflight if present, ack is "done" even if duplicate/late
		inFlight := b.ensureInFlight(topic, group, partition)
		delete(inFlight, offset)
		b.dispatchLocked(topic)

		return nil
	}

	// WAL append only when we advance progress
	if b.wal != nil {
		entry := storage.Entry{
			Type:      storage.RecordTypeOffset,
			Topic:     topic,
			Group:     group,
			Partition: partition,
			Offset:    offset,
		}

		if err := b.wal.Append(entry); err != nil {
			return err
		}
	}

	inFlight := b.ensureInFlight(topic, group, partition)
	delete(inFlight, offset)

	parts[partition] = offset
	b.dispatchLocked(topic)

	return nil
}

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

func (b *InMemoryBroker) dispatchLocked(topic string) {
	ts, ok := b.topics[topic]
	if !ok {
		return
	}

	groupChans, ok := b.consumerChans[topic]
	if !ok {
		return
	}

	for group, chans := range groupChans {
		if len(chans) == 0 {
			continue
		}

		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		nextByPart := b.ensureNextIndex(topic, group)

		for p := range ts.partitions {
			inflight := b.ensureInFlight(topic, group, p)
			if b.maxInFlight > 0 && len(inflight) >= b.maxInFlight {
				continue
			}

			// resume after last ack if we haven't initialized next index yet
			if _, ok := nextByPart[p]; !ok {
				last := int64(-1)
				if byTopic, ok := b.consumerOffsets[topic]; ok {
					if byGroup, ok := byTopic[group]; ok {
						if v, ok := byGroup[p]; ok {
							last = v
						}
					}
				}

				// find first message with Offset > last
				idx := 0
				for idx < len(ts.partitions[p]) && ts.partitions[p][idx].Offset <= last {
					idx++
				}
				nextByPart[p] = idx
			}

			for nextByPart[p] < len(ts.partitions[p]) {
				if b.maxInFlight > 0 && len(inflight) >= b.maxInFlight {
					break
				}

				m := ts.partitions[p][nextByPart[p]]
				nextByPart[p]++

				// pick one consumer in the group (round-robin)
				idx := b.rrCursor[topic][group] % len(chans)
				b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
				ch := chans[idx]

				if e, ok := inflight[m.Offset]; ok {
					// already in-flight (and shouldn't usually happen), but treat as a re-send attempt
					e.SentAt = time.Now()
					e.Attempts++
				} else {
					inflight[m.Offset] = &inflightEntry{
						Msg:      m,
						SentAt:   time.Now(),
						Attempts: 1,
					}
				}

				go func(ch chan Message, m Message) {
					defer func() { _ = recover() }()
					ch <- m
				}(ch, m)
			}
		}
	}
}

func (b *InMemoryBroker) StartRedeliveryLoop(ctx context.Context) {
	t := time.NewTicker(b.redeliverTick)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.mu.Lock()
				b.redeliverExpiredLocked()
				b.mu.Unlock()
			}
		}
	}()
}

func (b *InMemoryBroker) redeliverExpiredLocked() {
	now := time.Now()

	for topic, byGroup := range b.inFlight {
		groupChans, ok := b.consumerChans[topic]
		if !ok {
			continue
		}
		if _, ok := b.rrCursor[topic]; !ok {
			b.rrCursor[topic] = make(map[string]int)
		}

		for group, byPart := range byGroup {
			chans := groupChans[group]
			if len(chans) == 0 {
				continue
			}

			for _, inflight := range byPart {
				for _, e := range inflight {
					if now.Sub(e.SentAt) < b.ackTimeout {
						continue
					}

					// pick one consumer in the group (round-robin!)
					idx := b.rrCursor[topic][group] % len(chans)
					b.rrCursor[topic][group] = (b.rrCursor[topic][group] + 1) % len(chans)
					ch := chans[idx]

					// update inflight bookkeeping
					e.SentAt = now
					e.Attempts++

					m := e.Msg
					go func(ch chan Message, m Message) {
						defer func() { _ = recover() }()
						ch <- m
					}(ch, m)
				}
			}
		}
	}
}
