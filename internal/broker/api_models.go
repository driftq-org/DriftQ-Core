package broker

type ConsumeResponse struct {
	Offset  int64                  `json:"offset"`
	Key     string                 `json:"key"`
	Value   string                 `json:"value"`
	Routing *RoutingPublicMetadata `json:"routing,omitempty"`
}

type RoutingPublicMetadata struct {
	Label string            `json:"label,omitempty"`
	Meta  map[string]string `json:"meta,omitempty"`
}
