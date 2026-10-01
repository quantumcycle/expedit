package publisher

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"io"
	"sync"
)

type Publisher interface {
	io.Closer
	Publish(message *message.Message) error
}

var ErrClosed = errors.New("publisher is closed")

// Destination is analogous to a topic in google pubsub, a stream in redis or kafka or a queue in RabbitMQ.
type Destination string
type RoutingFunc func(msg *message.Message) (Destination, error)

// ConstantDestination is a routing function implementation that always returns the same destination.
func ConstantDestination(d Destination) RoutingFunc {
	return func(msg *message.Message) (Destination, error) {
		return d, nil
	}
}

type MessageBoolOptFunc func(msg *message.Message) (bool, error)

func ConstantBoolMsgFn(value bool) MessageBoolOptFunc {
	return func(msg *message.Message) (bool, error) {
		return value, nil
	}
}

type MessageStringOptFunc func(msg *message.Message) (string, error)

func ConstantStringMsgFn(s string) MessageStringOptFunc {
	return func(msg *message.Message) (string, error) {
		return s, nil
	}
}

type MessageIntOptFunc func(msg *message.Message) (int, error)

func ConstantIntMsgFn(i int) MessageIntOptFunc {
	return func(msg *message.Message) (int, error) {
		return i, nil
	}
}

type MessagesPublisherImpl[T any] interface {
	io.Closer
	Publish(ctx context.Context, message T) error
	GetMessageID(message T) string
}

// MessagePublisher is a helper struct to help with the implementation of a Publisher implementation.
// T is the generic type of a message in the implementation
// It keeps a list of internal publishers, one for each destination.
type MessagePublisher[T any] struct {
	RoutingFunc             RoutingFunc
	MessageMarshaller       func(msg *message.Message) (T, error)
	GetDestinationPublisher func(d Destination) (MessagesPublisherImpl[T], error)

	lock       sync.RWMutex
	publishers map[Destination]MessagesPublisherImpl[T]
	closed     bool
}

func (p *MessagePublisher[T]) Publish(message *message.Message) error {
	destName, err := p.RoutingFunc(message)
	if err != nil {
		return err
	}
	pub, err := p.getPublisher(destName)
	if err != nil {
		return err
	}
	msgImpl, err := p.MessageMarshaller(message)
	if err != nil {
		return err
	}

	err = pub.Publish(message.Context(), msgImpl)
	if err != nil {
		return err
	}

	// Try to get the generated message ID from the underlying publisher implementation
	if id := pub.GetMessageID(msgImpl); id != "" {
		message.ID = id
	}
	return nil
}

// getPublisher returns the cached publisher of a destination, creating it on first use.
func (p *MessagePublisher[T]) getPublisher(dest Destination) (MessagesPublisherImpl[T], error) {
	p.lock.RLock()
	closed := p.closed
	pub, ok := p.publishers[dest]
	p.lock.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if ok {
		return pub, nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	// Re-check, since the state may have changed between releasing the read lock and taking the write lock
	if p.closed {
		return nil, ErrClosed
	}
	if pub, ok := p.publishers[dest]; ok {
		return pub, nil
	}

	pub, err := p.GetDestinationPublisher(dest)
	if err != nil {
		return nil, err
	}
	if p.publishers == nil {
		p.publishers = make(map[Destination]MessagesPublisherImpl[T])
	}
	p.publishers[dest] = pub
	return pub, nil
}

// Close closes every destination publisher. All of them are closed even if some fail, and the errors are returned
// joined together.
func (p *MessagePublisher[T]) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	var errs []error
	for _, pub := range p.publishers {
		if err := pub.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	p.publishers = nil
	return errors.Join(errs...)
}
