package redis

import (
	"encoding/json"
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"reflect"
)

func UnmarshallMapPayloadFromJson[T any](mapKey string, payloadType T) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			if _, ok := msg.Payload.(map[string]interface{}); !ok {
				return fmt.Errorf("payload is not a map[string]interface{}")
			}
			payload := new(T)
			payloadKey, exists := msg.Payload.(map[string]interface{})[mapKey]
			if !exists {
				return fmt.Errorf("payload key %s not found in message", mapKey)
			}
			_, okByte := payloadKey.([]byte)
			_, okString := payloadKey.(string)
			if !okByte && !okString {
				var keyType string
				if payloadKey == nil {
					keyType = "nil"
				} else {
					keyType = reflect.TypeOf(payloadKey).Name()
				}
				return fmt.Errorf("payload key %s is of type [%s]. It must be either a byte array or string",
					mapKey, keyType)
			}
			var value []byte
			if okByte {
				value = payloadKey.([]byte)
			}
			if okString {
				value = []byte(payloadKey.(string))
			}
			err := json.Unmarshal(value, payload)
			if err != nil {
				return err
			}
			msg.Payload = *payload
			return next(msg)
		}
	}
}

func MarshallPayloadToJsonMap(mapKey string) XAddValuesMarshaller {
	return func(msg *message.Message) map[string]interface{} {
		payload, err := json.Marshal(msg.Payload)
		if err != nil {
			return nil
		}
		mp := make(map[string]interface{})
		mp[mapKey] = payload
		return mp
	}
}
