package middleware

import (
	"context"
	"github.com/quantumcycle/expedit/core/message"
	"time"
)

// ContextTimeout creates a new context with the given timeout and sets it on the message
func ContextTimeout(timeout time.Duration) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) error {
			ctx, cancel := context.WithTimeout(msg.Context(), timeout)
			defer func() {
				cancel()
			}()

			msg.SetContext(ctx)
			return next(msg)
		}
	}
}

// Throttle applies a global throttle to the handler. This will throttle the whole publisher or subscriber to that
// number of messages
func Throttle(max int, perDuration time.Duration) Middleware {
	ticker := time.NewTicker(perDuration / time.Duration(max))
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) error {
			<-ticker.C
			return next(msg)
		}
	}
}
