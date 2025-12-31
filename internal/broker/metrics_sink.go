package broker

// MetricsSink is an optional hook for emitting broker metrics and keeps broker decoupled from Prometheus/OpenTelemetry...
type MetricsSink interface {
	IncProduceRejected(reason string)
	IncDLQ(topic string, reason string)
}
