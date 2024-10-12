package google

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"time"
)

var DefaultPublishTimeout = 60 * time.Second

type OrderingKeyProvider func(message *message.Message) string
type AttributesProvider func(message *message.Message) map[string]string

func MetadataAsAttributes(msg *message.Message) map[string]string {
	return msg.Metadata
}

type PublisherOption struct {
	// OrderingKeyProvider is a function that returns an ordering key for a given message. If not provided, no ordering key is used.
	OrderingKeyProvider OrderingKeyProvider
	// AttributesProvider is a function that returns attributes for a given message. If not provided, no attributes are used.
	// A provider to set the attribute on the pubsub message.
	// By default, it's using MetadataAsAttributes which converts all metadata entries as attributes.
	AttributesProvider AttributesProvider
	// PublishTimeout is a timeout for publishing a message. DefaultPublishTimeout is used if not provided.
	PublishTimeout time.Duration
}

type PayloadMarshaller func(message *message.Message) ([]byte, error)

func JSONPayloadMarshaller(message *message.Message) ([]byte, error) {
	return json.Marshal(message.Payload)
}

type MessageMarshaller func(msg *message.Message) (*pubsub.Message, error)

type Publisher struct {
	internalPublisher *publisher.MessagePublisher[*pubsub.Message]
}

type PubsubTopic struct {
	topic *pubsub.Topic
}

func (p PubsubTopic) Close() error {
	p.topic.Stop()
	return nil
}

func (p PubsubTopic) Publish(ctx context.Context, message *pubsub.Message) error {
	r := p.topic.Publish(ctx, message)
	if _, err := r.Get(ctx); err != nil {
		return err
	}
	return nil
}

func NewGooglePublisher(
	c *pubsub.Client,
	routingFunc publisher.RoutingFunc,
	payloadMarshaller PayloadMarshaller,
	opts PublisherOption) (*Publisher, error) {
	if c == nil {
		return nil, errors.New("client is required")
	}
	if routingFunc == nil {
		return nil, errors.New("routing function is required")
	}
	if payloadMarshaller == nil {
		return nil, errors.New("payloadMarshaller is required")
	}
	if opts.PublishTimeout == 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.AttributesProvider == nil {
		opts.AttributesProvider = MetadataAsAttributes
	}

	internalPublisher := &publisher.MessagePublisher[*pubsub.Message]{
		RoutingFunc: routingFunc,
		MessageMarshaller: func(msg *message.Message) (*pubsub.Message, error) {
			msgPayload, err := payloadMarshaller(msg)
			if err != nil {
				return nil, err
			}
			msgImpl := &pubsub.Message{
				Data: msgPayload,
			}
			if opts.OrderingKeyProvider != nil {
				msgImpl.OrderingKey = opts.OrderingKeyProvider(msg)
			}
			if opts.AttributesProvider != nil {
				msgImpl.Attributes = opts.AttributesProvider(msg)
			}
			return msgImpl, nil
		},
		PublishTimeout: opts.PublishTimeout,
		GetDestinationPublisher: func(dest publisher.Destination) (publisher.MessageImplPublisher[*pubsub.Message], error) {
			pubTopic := c.Topic(string(dest))
			if opts.OrderingKeyProvider != nil {
				pubTopic.EnableMessageOrdering = true
			}
			return PubsubTopic{
				topic: pubTopic,
			}, nil
		},
	}

	p := &Publisher{
		internalPublisher: internalPublisher,
	}
	return p, nil
}

func (p *Publisher) Publish(message *message.Message) error {
	return p.internalPublisher.Publish(message)
}

func (p *Publisher) Close() error {
	return p.internalPublisher.Close()
}
