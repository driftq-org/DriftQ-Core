package main

import (
	"strconv"

	"github.com/driftq-org/DriftQ-Core/internal/broker"
	"github.com/prometheus/client_golang/prometheus"
)

type brokerSnapshot interface {
	MetricsSnapshot() broker.MetricsSnapshot
}

type BrokerCollector struct {
	broker       brokerSnapshot
	inflightDesc *prometheus.Desc
	lagDesc      *prometheus.Desc
}

func NewBrokerCollector(b brokerSnapshot) *BrokerCollector {
	return &BrokerCollector{
		broker: b,
		inflightDesc: prometheus.NewDesc(
			"inflight_messages",
			"Current number of in-flight messages per topic/group/partition.",
			[]string{"topic", "group", "partition"},
			nil,
		),
		lagDesc: prometheus.NewDesc(
			"consumer_lag",
			"Consumer lag per topic/group/partition (messages after committed offset).",
			[]string{"topic", "group", "partition"},
			nil,
		),
	}
}

func (c *BrokerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inflightDesc
	ch <- c.lagDesc
}

func (c *BrokerCollector) Collect(ch chan<- prometheus.Metric) {
	snap := c.broker.MetricsSnapshot()

	for _, m := range snap.InFlight {
		ch <- prometheus.MustNewConstMetric(
			c.inflightDesc,
			prometheus.GaugeValue,
			float64(m.Value),
			m.Topic,
			m.Group,
			strconv.Itoa(m.Partition),
		)
	}
	for _, m := range snap.ConsumerLag {
		ch <- prometheus.MustNewConstMetric(
			c.lagDesc,
			prometheus.GaugeValue,
			float64(m.Value),
			m.Topic,
			m.Group,
			strconv.Itoa(m.Partition),
		)
	}
}
