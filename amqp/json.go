package amqp

import (
	"encoding/json"
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
)

// UnmarshallPayloadFromJson unmarshalls a JSON byte payload into the specified type.
// This middleware expects the message payload to be a []byte containing JSON data.
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

// MarshallPayloadToJson marshalls the message payload into JSON bytes.
// This middleware converts any payload type into a JSON byte array.
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