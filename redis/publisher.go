package redis

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/redis/go-redis/v9"
	"time"
)

var DefaultPublishTimeout = 60 * time.Second

type PublisherOption func(*PublisherOptions)

// PublisherOptions holds configuration options for a Publisher.
type PublisherOptions struct {
	idGenerator        IDGenerator
	maxlen             int64
	approx             bool
	publishTimeout     time.Duration
	metadataMarshaller XAddValuesMarshaller
}

// WithIDGenerator is a function that returns an ID for a given message to be send to redis. If not provided, redis will generate the ID.
func WithIDGenerator(idGen IDGenerator) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.idGenerator = idGen
	}
}

// WithMaxlen is a redis stream concept to limit the size of a stream. Old entries are evicted once maxlen is reached. If not provided, no limit is used.
func WithMaxlen(maxlen int64) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.maxlen = maxlen
	}
}

// WithApprox Approx is an optimization flag in redis stream, because it's costly to evict from a stream up to Maxlen.
// With approx=true, redis will evict up to Maxlen, but not exactly Maxlen, so there could be a bit more than Maxlen items in the stream. If not provided, approx=false is used.
func WithApprox(approx bool) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.approx = approx
	}
}

// WithPublishTimeout is a timeout for publishing a message. DefaultPublishTimeout is used if not provided.
func WithPublishTimeout(timeout time.Duration) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.publishTimeout = timeout
	}
}

// WithMetadataMarshaller is a function that returns a map of values representing the metadata part of the message
// to be send to redis. If not provided, the metadata string map of the message is used as is. This is useful if you
// want to process the metadata, for example, to add a prefix to all keys.
func WithMetadataMarshaller(marshaller XAddValuesMarshaller) PublisherOption {
	return func(opts *PublisherOptions) {
		opts.metadataMarshaller = marshaller
	}
}

type IDGenerator func(msg *message.Message) (string, error)
type XAddValuesMarshaller func(msg *message.Message) map[string]interface{}

type Publisher struct {
	internalPublisher *publisher.MessagePublisher[*redis.XAddArgs]
}

type Stream struct {
	client *redis.Client
	name   string
}

func (s Stream) Close() error {
	//nothing to do
	return nil
}

func (s Stream) Publish(ctx context.Context, message *redis.XAddArgs) error {
	message.Stream = s.name
	cmd := s.client.XAdd(ctx, message)
	return cmd.Err()
}

func (s Stream) GetMessageID(message *redis.XAddArgs) string {
	if message == nil {
		return ""
	}
	return message.ID
}

func NewRedisPublisher(
	c *redis.Client,
	routingFunc publisher.RoutingFunc,
	payloadMarshaller XAddValuesMarshaller,
	opts ...PublisherOption) (*Publisher, error) {
	if c == nil {
		return nil, errors.New("client is required")
	}
	if routingFunc == nil {
		return nil, errors.New("routing function is required")
	}
	if payloadMarshaller == nil {
		return nil, errors.New("payloadMarshaller is required")
	}

	options := PublisherOptions{
		publishTimeout: DefaultPublishTimeout,
	}

	for _, opt := range opts {
		opt(&options)
	}

	internalPublisher := &publisher.MessagePublisher[*redis.XAddArgs]{
		RoutingFunc: routingFunc,
		MessageMarshaller: func(msg *message.Message) (*redis.XAddArgs, error) {
			values := make(map[string]interface{})

			if options.metadataMarshaller != nil {
				metadataValues := options.metadataMarshaller(msg)
				for k, v := range metadataValues {
					values[k] = v
				}
			} else {
				for k, v := range msg.Metadata {
					values[k] = v
				}
			}

			payloadValues := payloadMarshaller(msg)
			for k, v := range payloadValues {
				values[k] = v
			}

			//redis will use * to generate an ID when id is empty string
			var id = msg.ID
			if options.idGenerator != nil {
				var err error
				id, err = options.idGenerator(msg)
				if err != nil {
					return nil, err
				}
			}
			return &redis.XAddArgs{
				Values: values,
				MaxLen: options.maxlen,
				Approx: options.approx,
				ID:     id,
			}, nil
		},
		PublishTimeout: options.publishTimeout,
		GetDestinationPublisher: func(dest publisher.Destination) (publisher.MessagesPublisherImpl[*redis.XAddArgs], error) {
			st := Stream{
				client: c,
				name:   string(dest),
			}
			return st, nil
		},
	}

	return &Publisher{
		internalPublisher: internalPublisher,
	}, nil
}

func (p *Publisher) Publish(message *message.Message) error {
	return p.internalPublisher.Publish(message)
}

func (p *Publisher) Close() error {
	return p.internalPublisher.Close()
}
