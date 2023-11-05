package google

import (
	"cloud.google.com/go/pubsub"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"sync"
	"time"
)

var ErrClosed = errors.New("publisher is closed")
var DefaultPublishTimeout = 60 * time.Second

type OrderingKeyProvider func(message *message.Message) string

type PublisherOption struct {
	// RoutingFunc is a function that returns a topic name for a given message. It's a required option.
	RoutingFunc RoutingFunc
	// MessageMarshaller is a function that converts a message to a google Message. DefaultMessageMarshaller is used if not provided.
	Marshaller MessageMarshaller
	// OrderingKeyProvider is a function that returns an ordering key for a given message. If not provided, no ordering key is used.
	OrderingKeyProvider OrderingKeyProvider
	// PublishTimeout is a timeout for publishing a message. DefaultPublishTimeout is used if not provided.
	PublishTimeout time.Duration
}

type DestinationTopic string

type RoutingFunc func(msg *message.Message) (DestinationTopic, error)

func ConstantTopic(topic string) RoutingFunc {
	return func(msg *message.Message) (DestinationTopic, error) {
		return DestinationTopic(topic), nil
	}
}

type MessageMarshaller func(msg *message.Message) (*pubsub.Message, error)

func DefaultMessageMarshaller(msg *message.Message) (*pubsub.Message, error) {
	return &pubsub.Message{
		Data: msg.Payload,
	}, nil
}

type GooglePublisher struct {
	client              *pubsub.Client
	publishTimeout      time.Duration
	orderingKeyProvider OrderingKeyProvider
	routingFunc         RoutingFunc
	marshaller          MessageMarshaller
	lock                sync.RWMutex
	topics              map[DestinationTopic]*pubsub.Topic
	closed              bool
}

func NewGooglePublisher(c *pubsub.Client, opts PublisherOption) (*GooglePublisher, error) {
	if opts.RoutingFunc == nil {
		return nil, errors.New("routing function is required")
	}
	if opts.PublishTimeout == 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.Marshaller == nil {
		opts.Marshaller = DefaultMessageMarshaller
	}
	p := &GooglePublisher{
		client:         c,
		routingFunc:    opts.RoutingFunc,
		publishTimeout: opts.PublishTimeout,
		topics:         make(map[DestinationTopic]*pubsub.Topic),
		lock:           sync.RWMutex{},
		closed:         false,
		marshaller:     opts.Marshaller,
	}
	return p, nil
}

func (p *GooglePublisher) Publish(message *message.Message) error {
	if p.closed {
		return ErrClosed
	}
	topicName, err := p.routingFunc(message)
	if err != nil {
		return err
	}
	topic, err := p.topic(topicName)
	if err != nil {
		return err
	}
	pubMsg, err := p.marshaller(message)
	if err != nil {
		return err
	}
	topic.Publish(message.Context(), pubMsg)
	return nil
}

func (p *GooglePublisher) topic(topic DestinationTopic) (t *pubsub.Topic, err error) {
	p.lock.RLock()
	t, ok := p.topics[topic]
	p.lock.RUnlock()
	if ok {
		return t, nil
	}

	p.lock.Lock()
	defer func() {
		if err == nil {
			p.topics[topic] = t
		}
		p.lock.Unlock()
	}()

	return p.client.Topic(string(topic)), nil
}

func (p *GooglePublisher) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true

	p.lock.Lock()
	for _, t := range p.topics {
		t.Stop()
	}
	p.lock.Unlock()
	return p.client.Close()
}
