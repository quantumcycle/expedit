package message

import (
	"bytes"
	"context"
	"maps"
	"sync"
)

type Metadata map[string]string
type Payload []byte

type State int

const (
	Processing State = iota
	Ack
	Nack
)

type Message struct {
	ID        string   `json:"id"`
	Metadata  Metadata `json:"metadata"`
	Payload   Payload  `json:"payload"`
	ctx       context.Context
	mutex     sync.Mutex
	state     State
	stateChan chan State
}

func NewMessage(ctx context.Context, UUID string, payload Payload) *Message {
	return &Message{
		ID:        UUID,
		Metadata:  make(map[string]string),
		Payload:   payload,
		ctx:       ctx,
		state:     Processing,
		stateChan: make(chan State, 1),
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
	m.stateChan <- Ack
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
	m.stateChan <- Nack
	return true
}

func (m *Message) StateChange() <-chan State {
	return m.stateChan
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
	return bytes.Equal(m.Payload, toCompare.Payload)
}
