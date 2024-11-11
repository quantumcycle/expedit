package message

import (
	"context"
	"maps"
	"reflect"
	"sync"
)

type Metadata map[string]string
type Payload any

type State int

const (
	Processing State = iota
	Ack
	Nack
)

type Message struct {
	ID        string
	Metadata  Metadata
	Payload   Payload
	ctx       context.Context
	mutex     sync.Mutex
	state     State
	stateChan []chan State
}

func NewMessage(ctx context.Context, id string, payload Payload) *Message {
	return &Message{
		ID:       id,
		Metadata: make(map[string]string),
		Payload:  payload,
		ctx:      ctx,
		state:    Processing,
	}
}

func (m *Message) WithMetadata(key, value string) *Message {
	m.Metadata[key] = value
	return m
}

func (m *Message) Ack() bool {
	if m.state == Ack {
		return true
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == Nack {
		return false
	}

	m.state = Ack
	for _, ch := range m.stateChan {
		ch <- Ack
	}
	return true
}

func (m *Message) Nack() bool {
	if m.state == Nack {
		return true
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == Ack {
		return false
	}

	m.state = Nack
	for _, ch := range m.stateChan {
		ch <- Nack
	}
	return true
}

func (m *Message) StateChange() <-chan State {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	ch := make(chan State, 1)
	m.stateChan = append(m.stateChan, ch)
	return ch
}

func (m *Message) State() State {
	return m.state
}

func (m *Message) Context() context.Context {
	return m.ctx
}

func (m *Message) SetContext(ctx context.Context) *Message {
	m.ctx = ctx
	return m
}

func (m *Message) Copy() *Message {
	msg := NewMessage(m.ctx, m.ID, m.Payload)
	msg.Metadata = maps.Clone(m.Metadata)
	return msg
}

// Equals compare, that two messages are equal. Acks/Nacks are not compared.
func (m *Message) Equals(toCompare *Message) bool {
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

func (m *Message) Destroy() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, ch := range m.stateChan {
		close(ch)
	}
	m.stateChan = nil
}
