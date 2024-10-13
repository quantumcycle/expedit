package google

import (
	"encoding/json"
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
)

func UnmarshallPayloadFromJson[T any](payloadType T) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			if _, ok := msg.Payload.([]byte); !ok {
				return fmt.Errorf("payload is not a byte array")
			}
			payload := new(T)
			err := json.Unmarshal(msg.Payload.([]byte), payload)
			if err != nil {
				return err
			}
			msg.Payload = *payload
			return next(msg)
		}
	}
}

func MarshallPayloadToJson() middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			payload, err := json.Marshal(msg.Payload)
			if err != nil {
				return err
			}
			msg.Payload = payload
			return next(msg)
		}
	}
}
