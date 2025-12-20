package broker

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/driftq-org/DriftQ-Core/internal/storage"
)

// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	mu     sync.RWMutex
	topics map[string]*TopicState

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

	maxPartitionMsgs  int
	maxPartitionBytes int

	idem *IdempotencyStore
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
		topics:            make(map[string]*TopicState),
		consumerOffsets:   make(map[string]map[string]map[int]int64),
		consumerChans:     make(map[string]map[string][]chan Message),
		rrCursor:          make(map[string]map[string]int),
		maxInFlight:       2, // for testing, later can raise if needed!
		inFlight:          make(map[string]map[string]map[int]map[int64]*inflightEntry),
		nextIndex:         make(map[string]map[string]map[int]int),
		wal:               wal,
		router:            r,
		ackTimeout:        2 * time.Second,
		redeliverTick:     250 * time.Millisecond,
		maxPartitionMsgs:  2,
		maxPartitionBytes: 20,
		idem:              NewIdempotencyStore(10 * time.Minute),
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

	b.topics[name] = &TopicState{
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

func (b *InMemoryBroker) Produce(ctx context.Context, topic string, msg Message) error {
	if topic == "" {
		return errors.New("topic cannot be empty")
	}

	// Router hook: routing metadata + optional routing controls (target_topic/partition_override)
	if b.router != nil {
		decision, err := b.router.Route(ctx, topic, msg)
		if err == nil {
			msg.Routing = &RoutingMetadata{
				Label: decision.Label,
				Meta:  decision.Meta,
			}

			// routing controls -> envelope (router wins)
			if decision.TargetTopic != "" || decision.PartitionOverride != nil {
				if msg.Envelope == nil {
					msg.Envelope = &Envelope{}
				}

				if decision.TargetTopic != "" {
					msg.Envelope.TargetTopic = decision.TargetTopic
				}

				if decision.PartitionOverride != nil {
					msg.Envelope.PartitionOverride = decision.PartitionOverride
				}
			}
		}
	}

	// Deadline enforcement (Reject if already expired)
	if msg.Envelope != nil && msg.Envelope.Deadline != nil {
		if time.Now().After(*msg.Envelope.Deadline) {
			return errors.New("message deadline exceeded")
		}
	}

	// Final topic: envelope target_topic overrides producer topic
	finalTopic := topic
	if msg.Envelope != nil && msg.Envelope.TargetTopic != "" {
		finalTopic = msg.Envelope.TargetTopic
	}

	// Idempotency gate (before any side-effects)
	tenantID := ""
	idemKey := ""
	if msg.Envelope != nil {
		tenantID = msg.Envelope.TenantID
		idemKey = msg.Envelope.IdempotencyKey
	}

	startedIdem := false
	if b.idem != nil && idemKey != "" {
		alreadyCommitted, err := b.idem.Begin(tenantID, finalTopic, idemKey)
		if err != nil {
			// Pending -> reject duplicates
			return err
		}

		if alreadyCommitted {
			// Treat as success, but do NOT produce a duplicate message
			return nil
		}
		startedIdem = true
	}

	// Helper for failures after Begin()
	failIdem := func(cause error) {
		if startedIdem && b.idem != nil && idemKey != "" {
			b.idem.Fail(tenantID, finalTopic, idemKey, cause)
		}
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	ts, exists := b.topics[finalTopic]
	if !exists {
		err := errors.New("topic does not exist (create it first)")
		failIdem(err)
		return err
	}

	numPartitions := len(ts.partitions)

	// Partition selection: override > hash(key)
	part := pickPartition(msg.Key, numPartitions)
	if msg.Envelope != nil && msg.Envelope.PartitionOverride != nil {
		po := *msg.Envelope.PartitionOverride

		if po < 0 || po >= numPartitions {
			err := errors.New("partition_override out of range")
			failIdem(err)
			return err
		}
		part = po
	}

	// Compute slowest ack only if we need it
	slowest := int64(-1)
	if b.maxPartitionMsgs > 0 || b.maxPartitionBytes > 0 {
		slowest = b.slowestAckLocked(finalTopic, part)
	}

	// 1) message-count cap
	if b.maxPartitionMsgs > 0 {
		buffered := bufferedCount(ts.partitions[part], slowest)
		if buffered >= b.maxPartitionMsgs {
			err := &ProducerOverloadError{
				Reason:     "partition_buffer_full",
				RetryAfter: 1 * time.Second,
				Cause:      ErrBackpressure,
			}
			failIdem(err)
			return err
		}
	}

	// 2) bytes cap
	if b.maxPartitionBytes > 0 {
		bufferedBytes := bufferedBytesCount(ts.partitions[part], slowest)
		bufferedBytes += len(msg.Key) + len(msg.Value)

		if bufferedBytes >= b.maxPartitionBytes {
			err := &ProducerOverloadError{
				Reason:     "partition_buffer_bytes_full",
				RetryAfter: 1 * time.Second,
				Cause:      ErrBackpressure,
			}
			failIdem(err)
			return err
		}
	}

	msg.Offset = ts.nextOffset
	msg.Partition = part

	// WAL append first (if configured!)
	if b.wal != nil {
		entry := storage.Entry{
			Type:      storage.RecordTypeMessage,
			Topic:     finalTopic,
			Partition: part,
			Offset:    msg.Offset,
			Key:       msg.Key,
			Value:     msg.Value,
		}

		// routing metadata
		if msg.Routing != nil {
			entry.RoutingLabel = msg.Routing.Label
			entry.RoutingMeta = msg.Routing.Meta
		}

		// envelope fields
		if msg.Envelope != nil {
			entry.RunID = msg.Envelope.RunID
			entry.StepID = msg.Envelope.StepID
			entry.ParentStepID = msg.Envelope.ParentStepID
			entry.Labels = msg.Envelope.Labels

			entry.TargetTopic = msg.Envelope.TargetTopic
			entry.PartitionOverride = msg.Envelope.PartitionOverride

			entry.IdempotencyKey = msg.Envelope.IdempotencyKey
			entry.Deadline = msg.Envelope.Deadline

			if msg.Envelope.RetryPolicy != nil {
				entry.RetryMaxAttempts = msg.Envelope.RetryPolicy.MaxAttempts
				entry.RetryBackoffMs = msg.Envelope.RetryPolicy.BackoffMs
				entry.RetryMaxBackoffMs = msg.Envelope.RetryPolicy.MaxBackoffMs
			}

			entry.TenantID = msg.Envelope.TenantID
		}

		if err := b.wal.Append(entry); err != nil {
			failIdem(err)
			return err
		}
	}

	// Commit to in-memory state
	ts.nextOffset++
	ts.partitions[part] = append(ts.partitions[part], msg)

	// Deliver what we can (respects maxInFlight inside dispatch)
	b.dispatchLocked(finalTopic)

	// Mark idempotency as committed ONLY after successful produce
	if startedIdem && b.idem != nil && idemKey != "" {
		b.idem.Commit(tenantID, finalTopic, idemKey, nil)
	}

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

func (b *InMemoryBroker) IdempotencyHelper() *IdempotencyConsumerHelper {
	if b == nil {
		return nil
	}

	return NewIdempotencyConsumerHelper(b.idem)
}
