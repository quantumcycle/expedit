package message

import (
	"context"
	"maps"
	"reflect"
	"sync"
)

type Metadata map[string]string

type State int

const (
	Processing State = iota
	Ack
	Nack
)

type Message[T any] struct {
	ID        string   `json:"id"`
	Metadata  Metadata `json:"metadata"`
	Payload   T        `json:"payload"`
	ctx       context.Context
	mutex     sync.Mutex
	state     State
	stateChan chan State
}

func NewMessage[T any](ctx context.Context, UUID string, payload T) *Message[T] {
	return &Message[T]{
		ID:        UUID,
		Metadata:  make(map[string]string),
		Payload:   payload,
		ctx:       ctx,
		state:     Processing,
		stateChan: make(chan State, 1),
	}
}

func (m *Message[T]) WithMetadata(key, value string) *Message[T] {
	m.Metadata[key] = value
	return m
}

func (m *Message[T]) Ack() bool {
	if m.state == Ack {
		return true
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == Nack {
		return false
	}

	m.state = Ack
	m.stateChan <- Ack
	return true
}

func (m *Message[T]) Nack() bool {
	if m.state == Nack {
		return true
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == Ack {
		return false
	}

	m.state = Nack
	m.stateChan <- Nack
	return true
}

func (m *Message[T]) StateChange() <-chan State {
	return m.stateChan
}

func (m *Message[T]) State() State {
	return m.state
}

func (m *Message[T]) Context() context.Context {
	return m.ctx
}

func (m *Message[T]) SetContext(ctx context.Context) *Message[T] {
	m.ctx = ctx
	return m
}

func (m *Message[T]) Copy() *Message[T] {
	msg := NewMessage(m.ctx, m.ID, m.Payload)
	msg.Metadata = maps.Clone(m.Metadata)
	return msg
}

// Equals compare, that two messages are equal. Acks/Nacks are not compared.
func (m *Message[T]) Equals(toCompare *Message[T]) bool {
	if m.ID != toCompare.ID {
		return false
	}
	if len(m.Metadata) != len(toCompare.Metadata) {
		return false
	}
	for key, value := range m.Metadata {
		if value != toCompare.Metadata[key] {
			return false
		}
	}
	return reflect.DeepEqual(m.Payload, toCompare.Payload)
}
