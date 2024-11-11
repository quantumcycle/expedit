package middleware

import (
	"context"
	"github.com/quantumcycle/expedit/core/message"
	"time"
)

// ContextTimeout creates a new context with the given timeout and sets it on the message
func ContextTimeout(timeout time.Duration) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
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
		return func(msg *message.Message) error {
			<-ticker.C
			return next(msg)
		}
	}
}

// MessageApplicableFn is a function that will return true if a message is applicable. Used by middleware to determine
// which messages are applicable.
type MessageApplicableFn = func(msg *message.Message) bool

// ConditionalSkip is a middleware that will skip messages based on a condition
// This is useful, for example, to create de-duplicator using Redis or a similar datastore.
// This is just a specialized version of ConditionalExecute.
func ConditionalSkip(shouldSkipFn MessageApplicableFn) Middleware {
	return ConditionalExecute(shouldSkipFn, func(msg *message.Message) error {
		return nil
	})
}

// ConditionalExecute is a middleware that will execute a function based on a condition
// This is useful to create middlewares like Poison queues. If the function returns true, it's sent to the exec function
// that would send it to a poison queue.
func ConditionalExecute(shouldExec MessageApplicableFn, exec func(msg *message.Message) error) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			if shouldExec(msg) {
				return exec(msg)
			}
			return next(msg)
		}
	}
}
