package message

import (
	"context"
	"maps"
	"reflect"
)

type Metadata map[string]interface{}
type Payload any

// Message is the unit published and received. A received message is acknowledged from the result of the handler
// that processes it, see subscriber.Subscriber.
type Message struct {
	ID       string
	Metadata Metadata
	Payload  Payload
	ctx      context.Context
}

func NewMessage(ctx context.Context, payload Payload) *Message {
	return &Message{
		ID:       "",
		Metadata: make(map[string]interface{}),
		Payload:  payload,
		ctx:      ctx,
	}
}

func (m *Message) WithMetadata(key string, value interface{}) *Message {
	m.Metadata[key] = value
	return m
}

func (m *Message) Context() context.Context {
	return m.ctx
}

func (m *Message) SetContext(ctx context.Context) *Message {
	m.ctx = ctx
	return m
}

// Copy returns a shallow copy of the message, with its own metadata map.
func (m *Message) Copy() *Message {
	msg := NewMessage(m.ctx, m.Payload)
	msg.ID = m.ID
	msg.Metadata = maps.Clone(m.Metadata)
	return msg
}

// Equals compare, that two messages are equal.
func (m *Message) Equals(toCompare *Message) bool {
	if m.ID != toCompare.ID {
		return false
	}
	if len(m.Metadata) != len(toCompare.Metadata) {
		return false
	}
	for key, value := range m.Metadata {
		toCompareValue, ok := toCompare.Metadata[key]
		if !ok || !reflect.DeepEqual(value, toCompareValue) {
			return false
		}
	}
	return reflect.DeepEqual(m.Payload, toCompare.Payload)
}
