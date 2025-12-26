package v1

import "time"

type HealthzResponse struct {
	Status string `json:"status"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type TopicsListResponse struct {
	Topics []string `json:"topics"`
}

type TopicsCreateResponse struct {
	Status     string `json:"status"`
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

type ProduceRequest struct {
	Topic    string    `json:"topic"`
	Key      string    `json:"key,omitempty"`
	Value    string    `json:"value"`
	Envelope *Envelope `json:"envelope,omitempty"`
}

type ProduceResponse struct {
	Status string `json:"status"`
	Topic  string `json:"topic"`
}

type ConsumeItem struct {
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Attempts  int       `json:"attempts"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	LastError string    `json:"last_error"`
	Routing   *Routing  `json:"routing,omitempty"`
	Envelope  *Envelope `json:"envelope,omitempty"`
}

type Routing struct {
	Label string            `json:"label"`
	Meta  map[string]string `json:"meta,omitempty"`
}

type Envelope struct {
	RunID             string       `json:"run_id,omitempty"`
	StepID            string       `json:"step_id,omitempty"`
	ParentStepID      string       `json:"parent_step_id,omitempty"`
	TenantID          string       `json:"tenant_id,omitempty"`
	IdempotencyKey    string       `json:"idempotency_key,omitempty"`
	TargetTopic       string       `json:"target_topic,omitempty"`
	Deadline          *time.Time   `json:"deadline,omitempty"`
	RetryPolicy       *RetryPolicy `json:"retry_policy,omitempty"`
	PartitionOverride *int         `json:"partition_override,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts  int   `json:"max_attempts,omitempty"`
	BackoffMs    int64 `json:"backoff_ms,omitempty"`
	MaxBackoffMs int64 `json:"max_backoff_ms,omitempty"`
}

type AckResponse struct {
	Status    string `json:"status"`
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type NackResponse struct {
	Status    string `json:"status"`
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Owner     string `json:"owner"`
	Reason    string `json:"reason,omitempty"`
}

type ResourceExhaustedResponse struct {
	Error        string `json:"error"`
	Message      string `json:"message"`
	Reason       string `json:"reason,omitempty"`
	RetryAfterMs int    `json:"retry_after_ms,omitempty"`
}

type TopicsCreateRequest struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions,omitempty"` // default = 1 (handler will enforce)
}

type AckRequest struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

type NackRequest struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Owner     string `json:"owner"`
	Reason    string `json:"reason,omitempty"`
}
