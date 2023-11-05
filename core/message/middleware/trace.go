package middleware

import "github.com/quantumcycle/expedit/core/message"

func CorrelationIDFromContext(metadataKey string, ctxKey any) Middleware {
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
