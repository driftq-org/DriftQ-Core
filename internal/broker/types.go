package broker

import (
	"context"
	"time"
)

type inflightEntry struct {
	Msg           Message
	SentAt        time.Time
	Attempts      int
	NextDeliverAt time.Time
	LastError     string
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

	Envelope *Envelope
	Routing  *RoutingMetadata
	Attempts int

	LastError string
}

type TopicState struct {
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
	Nack(ctx context.Context, topic, group string, partition int, offset int64, reason string) error
}

type RetryPolicy struct {
	MaxAttempts  int   `json:"max_attempts,omitempty"`
	BackoffMs    int64 `json:"backoff_ms,omitempty"`
	MaxBackoffMs int64 `json:"max_backoff_ms,omitempty"`
}

type Envelope struct {
	RunID        string `json:"run_id,omitempty"`
	StepID       string `json:"step_id,omitempty"`
	ParentStepID string `json:"parent_step_id,omitempty"`

	Labels map[string]string `json:"labels,omitempty"`

	TargetTopic       string `json:"target_topic,omitempty"`
	PartitionOverride *int   `json:"partition_override,omitempty"`

	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	Deadline       *time.Time `json:"deadline,omitempty"`

	RetryPolicy *RetryPolicy `json:"retry_policy,omitempty"`
	TenantID    string       `json:"tenant_id,omitempty"`
}
