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

type PublisherOption struct {
	// IDGenerator is a function that returns an ID for a given message to be send to redis. If not provided, redis will generate the ID.
	IDGenerator IDGenerator
	// Maxlen is a redis stream concept to limit the size of a stream. Old entries are evicted once maxlen is reached. If not provided, no limit is used.
	Maxlen int64
	// Approx is an optimization flag in redis stream, because it's costly to evict from a stream up to Maxlen. With approx=true, redis will evict up to Maxlen, but not exactly Maxlen, so there could be a bit more than Maxlen items in the stream. If not provided, approx=false is used.
	Approx bool
	// PublishTimeout is a timeout for publishing a message. DefaultPublishTimeout is used if not provided.
	PublishTimeout time.Duration
}

type IDGenerator func(msg *message.Message) (string, error)
type MessageMarshaller func(msg *message.Message) (map[string]interface{}, error)

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

func NewRedisPublisher(
	c *redis.Client,
	routingFunc publisher.RoutingFunc,
	marshaller MessageMarshaller,
	opts PublisherOption) (*Publisher, error) {
	if c == nil {
		return nil, errors.New("client is required")
	}
	if routingFunc == nil {
		return nil, errors.New("routing function is required")
	}
	if marshaller == nil {
		return nil, errors.New("marshaller is required")
	}
	if opts.PublishTimeout == 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}

	internalPublisher := &publisher.MessagePublisher[*redis.XAddArgs]{
		RoutingFunc: routingFunc,
		MessageMarshaller: func(msg *message.Message) (*redis.XAddArgs, error) {
			values, err := marshaller(msg)
			if err != nil {
				return nil, err
			}
			var id = ""
			if opts.IDGenerator != nil {
				id, err = opts.IDGenerator(msg)
				if err != nil {
					return nil, err
				}
			}
			return &redis.XAddArgs{
				Values: values,
				MaxLen: opts.Maxlen,
				Approx: opts.Approx,
				ID:     id,
			}, nil
		},
		PublishTimeout: opts.PublishTimeout,
		GetDestinationPublisher: func(dest publisher.Destination) (publisher.MessageImplPublisher[*redis.XAddArgs], error) {
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
