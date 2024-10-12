package middleware

import "github.com/quantumcycle/expedit/core/message"

// Middleware to take a context key from the context and set it as message metadata
// Useful to take something like a correlation ID from the context and set it as metadata
func ContextKeyToMetadata(metadataKey string, ctxKey any) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			if idCtx := msg.Context().Value(ctxKey); idCtx != nil {
				if id, isStr := idCtx.(string); isStr {
					msg.Metadata[metadataKey] = id
				}
			}
			return next(msg)
		}
	}
}
