package broker

import (
	"errors"
	"time"
)

var ErrBackpressure = errors.New("backpressure: partition is full")
var ErrProducerOverloaded = errors.New("producer overload")

type ProducerOverloadError struct {
	Reason     string
	RetryAfter time.Duration
	Cause      error
}

func (e *ProducerOverloadError) Error() string {
	if e == nil {
		return "producer overload"
	}
	if e.Reason != "" {
		return "producer overload: " + e.Reason
	}
	return "producer overload"
}

func (e *ProducerOverloadError) Is(target error) bool {
	return target == ErrProducerOverloaded
}

func (e *ProducerOverloadError) Unwrap() error {
	return e.Cause
}
