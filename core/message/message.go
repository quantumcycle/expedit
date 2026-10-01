package message

import (
	"context"
	"maps"
	"reflect"
	"sync"
)

type Metadata map[string]interface{}
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

func NewMessage(ctx context.Context, payload Payload) *Message {
	return &Message{
		ID:       "",
		Metadata: make(map[string]interface{}),
		Payload:  payload,
		ctx:      ctx,
		state:    Processing,
	}
}

func (m *Message) WithMetadata(key string, value interface{}) *Message {
	m.Metadata[key] = value
	return m
}

func (m *Message) Ack() bool {
	return m.transition(Ack, Nack)
}

func (m *Message) Nack() bool {
	return m.transition(Nack, Ack)
}

// transition moves the message from Processing to the target state and notifies the state listeners.
// It returns true if the message is in the target state, and false if it is already in the opposite state.
func (m *Message) transition(target, opposite State) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.state == target {
		return true
	}
	if m.state == opposite {
		return false
	}

	m.state = target
	for _, ch := range m.stateChan {
		ch <- target
	}
	return true
}

// StateChange returns a channel that receives the state the message transitions to (Ack or Nack). If the message
// already left the Processing state, the channel immediately holds the current state, so a late subscriber never
// misses it.
func (m *Message) StateChange() <-chan State {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	ch := make(chan State, 1)
	if m.state != Processing {
		ch <- m.state
	}
	m.stateChan = append(m.stateChan, ch)
	return ch
}

func (m *Message) State() State {
	m.mutex.Lock()
	defer m.mutex.Unlock()
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
	msg := NewMessage(m.ctx, m.Payload)
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
