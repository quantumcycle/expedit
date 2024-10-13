package message

import (
	"errors"
	"fmt"
	"reflect"
)

type HandlerFunc func(msg *Message) error

type HandlerFuncWithPayload[T any] func(msg *Message, payload T) error

func HandleWithPayload[T any](handler HandlerFuncWithPayload[T]) HandlerFunc {
	return func(msg *Message) error {
		payload, ok := msg.Payload.(T)
		if !ok {
			payloadType := reflect.TypeOf(payload).Name()
			expectedType := new(T)
			expectedTypeName := reflect.TypeOf(expectedType).Name()
			return errors.New(fmt.Sprintf("payload type [%s] does not match handler expected payload type [%s]",
				payloadType, expectedTypeName))
		}
		return handler(msg, payload)
	}
}
