package broker

import (
	"context"
)

func (NoopRouter) Route(_ context.Context, _ string, msg Message) (RoutingDecision, error) {
	return RoutingDecision{
		Label:             "",
		TargetTopic:       "",
		PartitionOverride: nil,
		Meta:              make(map[string]string),
	}, nil
}
