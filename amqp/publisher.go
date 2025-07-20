package amqp

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageModifierFn func(msg *amqp.Publishing) error
type HeadersProvider func(message *message.Message) map[string]interface{}
type RoutingKeyFunc func(message *message.Message) (string, error)

func ConstantRoutingKey(routingKey string) RoutingKeyFunc {
	return func(message *message.Message) (string, error) {
		return routingKey, nil
	}
}

func MetadataAsHeaders(msg *message.Message) map[string]interface{} {
	return msg.Metadata
}

type PublisherOption func(*PublisherOptions)

type PublisherOptions struct {
	headersProvider HeadersProvider
	mandatoryMsgOpt *publisher.MessageBoolOptFunc
	immediateMsgOpt *publisher.MessageBoolOptFunc
	messageModifier *MessageModifierFn
}

func WithMandatoryMsgFn(mandatoryOpt publisher.MessageBoolOptFunc) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.mandatoryMsgOpt = &mandatoryOpt
	}
}

func WithImmediateMsgFn(immediateOpt publisher.MessageBoolOptFunc) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.immediateMsgOpt = &immediateOpt
	}
}

func WithMessageModifier(modifier MessageModifierFn) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.messageModifier = &modifier
	}
}

func WithHeadersProvider(provider HeadersProvider) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.headersProvider = provider
	}
}

type Publisher struct {
	internalPublisher *publisher.MessagePublisher[*AMQPMessage]
}

func (p *Publisher) Publish(message *message.Message) error {
	return p.internalPublisher.Publish(message)
}

func (p *Publisher) Close() error {
	return p.internalPublisher.Close()
}

type DefaultMessageOptions struct {
	ContentType  string
	Priority     uint8
	DeliveryMode uint8
}

type AMQPMessage struct {
	Publishing *amqp.Publishing
	Immediate  bool
	Mandatory  bool
	RoutingKey string
}

type ExchangeDestination struct {
	channel  *ReconnectingChannel
	exchange publisher.Destination
	opts     PublisherOptions
}

func (d ExchangeDestination) Close() error {
	return d.channel.Close()
}

func (d ExchangeDestination) Publish(ctx context.Context, message *AMQPMessage) error {
	return d.channel.Publish(string(d.exchange),
		message.RoutingKey,
		message.Mandatory,
		message.Immediate,
		*message.Publishing)
}

func (d ExchangeDestination) GetMessageID(message *AMQPMessage) string {
	if message.Publishing == nil {
		return ""
	}
	return message.Publishing.MessageId
}

func NewAMQPPublisher(
	channel *ReconnectingChannel,
	exchangeRoutingFn publisher.RoutingFunc,
	routingKeyFn RoutingKeyFunc,
	defaultMsgOptions DefaultMessageOptions,
	opts ...PublisherOption) (*Publisher, error) {
	if exchangeRoutingFn == nil {
		return nil, errors.New("exchange routing function required")
	}

	if routingKeyFn == nil {
		return nil, errors.New("routing key function required")
	}

	if defaultMsgOptions.ContentType == "" {
		return nil, errors.New("message default content type is required")
	}

	if defaultMsgOptions.Priority < 0 || defaultMsgOptions.Priority > 9 {
		return nil, errors.New("message default priority is required to be between 0 and 9")
	}

	if defaultMsgOptions.DeliveryMode < amqp.Transient || defaultMsgOptions.DeliveryMode > amqp.Persistent {
		return nil, errors.New("message default delivery mode is required to be either transient or persistent")
	}

	options := PublisherOptions{
		// Default is to use metadata as headers
		headersProvider: MetadataAsHeaders,
	}
	for _, opt := range opts {
		opt(&options)
	}

	internalPublisher := &publisher.MessagePublisher[*AMQPMessage]{
		RoutingFunc: exchangeRoutingFn,
		MessageMarshaller: func(msg *message.Message) (*AMQPMessage, error) {
			if _, ok := msg.Payload.([]byte); !ok {
				return nil, errors.New("payload must be []byte. Use a middleware to convert the payload to []byte")
			}
			publishing := &amqp.Publishing{
				ContentType:  defaultMsgOptions.ContentType,
				Priority:     defaultMsgOptions.Priority,
				DeliveryMode: defaultMsgOptions.DeliveryMode,
				Body:         msg.Payload.([]byte),
			}
			routingKey, err := routingKeyFn(msg)
			if err != nil {
				return nil, err
			}

			if options.headersProvider != nil {
				publishing.Headers = options.headersProvider(msg)
			}

			if options.messageModifier != nil {
				err := (*options.messageModifier)(publishing)
				if err != nil {
					return nil, err
				}
			}

			var immediate bool
			if options.immediateMsgOpt != nil {
				b, err := (*options.immediateMsgOpt)(msg)
				if err != nil {
					return nil, err
				}
				immediate = b
			}

			var mandatory bool
			if options.mandatoryMsgOpt != nil {
				b, err := (*options.mandatoryMsgOpt)(msg)
				if err != nil {
					return nil, err
				}
				mandatory = b
			}

			return &AMQPMessage{
				RoutingKey: routingKey,
				Publishing: publishing,
				Immediate:  immediate,
				Mandatory:  mandatory,
			}, nil
		},
		GetDestinationPublisher: func(dest publisher.Destination) (publisher.MessagesPublisherImpl[*AMQPMessage], error) {
			return ExchangeDestination{
				channel:  channel,
				exchange: dest,
				opts:     options,
			}, nil
		},
	}

	p := &Publisher{
		internalPublisher: internalPublisher,
	}
	return p, nil
}
