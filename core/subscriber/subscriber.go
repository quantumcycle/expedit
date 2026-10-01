package subscriber

import (
	"context"

	"github.com/quantumcycle/expedit/core/message"
)

// Subscriber receives messages from a broker.
type Subscriber interface {
	// Receive calls handler for each received message, and blocks until ctx is done or receiving fails.
	//
	// The message is acked when handler returns nil, and nacked when it returns an error or panics. Cancelling ctx
	// does not cancel the context of the messages, so in-flight handlers can finish: when ctx is done, Receive stops
	// receiving, waits for the in-flight handlers and returns nil. When receiving fails, Receive waits for the
	// in-flight handlers and returns the error.
	Receive(ctx context.Context, handler message.HandlerFunc) error
}
