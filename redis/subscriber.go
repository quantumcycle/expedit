package redis

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/lithammer/shortuuid/v3"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

var StreamDoesntExistErr = errors.New("stream does not exist")

type MessageUnmarshaller func(ctx context.Context, msg *redis.XMessage) (*message.Message, error)

type SubscriberOption struct {
	// OnUnmarshallingError is a function that is called when an error occurs while unmarshalling a message.
	// If this option is provided, the callback is responsible for handling the error and optionally calling msg.Nack().
	// If this option is not provided, the default behaviour is to call msg.Nack().
	OnUnmarshallingError func(msg MessageWrapper, err error)

	// OnProcessingTimeout is a function that is called when a message processing times out.
	OnProcessingTimeout func(ctx context.Context, msg MessageWrapper)

	// ConsumerGroup identifies the consumer group to which the subscriber belongs.
	ConsumerGroup string

	// ConsumerGroupCreateStreamIfMissing will create the stream if it does not exist in consumer group mode.
	ConsumerGroupCreateStreamIfMissing bool

	// ConsumerGroupStartID is the position the consumer of a consumer group should start from
	// Using ConsumerGroupStartFromBeginning "0" means the consumer group will consume from the very first message.
	// Using ConsumerGroupStartFromLatest "$" means the consumer group will consume from the latest message.
	ConsumerGroupStartID StartPosition

	// StartID is the ID of the last message that was processed by the subscriber. This is used only when not using
	// consumer groups. The default is "$" which means the subscriber will start from the latest message.
	StartID StartPosition

	// ProcessingTimeout will dictate how long a message will be processed before it is nacked.
	// 0 means no timeout, wait forever. Keep in mind that GCP uses the "Acknowledgement deadline" to determine if a
	// message needs to be redelivered. ProcessingTimeout has no impact on the "Acknowledgement deadline".
	// Default value is 600 seconds, which is the max value of the GCP "Acknowledgement deadline".
	ProcessingTimeout time.Duration
}

type Subscriber struct {
	internalSubscriber *subscriber.MessageSubscriber[*pubsub.Message]
}

type MessageWrapper struct {
	stream        string
	consumerGroup string
	msg           *redis.XMessage
}

type StartPosition string

const (
	StartFromBeginning StartPosition = "0"
	StartFromLatest    StartPosition = "$"
)

//TODO: still need to implement XCLAIM to get messages from Pending entries list

func NewRedisSubscriber(
	c *redis.Client,
	stream string,
	unmarshaller MessageUnmarshaller,
	opts SubscriberOption) (*Subscriber, error) {

	if c == nil {
		return nil, errors.New("client is required")
	}
	if unmarshaller == nil {
		return nil, errors.New("unmarshaller is required")
	}

	if opts.ProcessingTimeout <= 0 {
		opts.ProcessingTimeout = 600 * time.Second
	}

	if opts.ConsumerGroup != "" && opts.ConsumerGroupStartID == "" {
		opts.ConsumerGroupStartID = StartFromLatest
	}

	if opts.ConsumerGroup == "" && opts.StartID == "" {
		opts.StartID = StartFromLatest
	}

	processor := subscriber.MessageProcessor[MessageWrapper]{
		Ack: func(ctx context.Context, wrapper MessageWrapper) {
			c.XAck(ctx, wrapper.stream, wrapper.consumerGroup, wrapper.msg.ID)
		},
		Nack: func(ctx context.Context, wrapper MessageWrapper) {
			//TODO: see the XCLAIM comment above
		},
		MessageUnmarshall: func(ctx context.Context, wrapper MessageWrapper) (*message.Message, error) {
			return unmarshaller(ctx, wrapper.msg)
		},
		OnUnmarshallingError: opts.OnUnmarshallingError,
		ProcessingTimeout:    opts.ProcessingTimeout,
		OnProcessingTimeout:  opts.OnProcessingTimeout,
	}
	internalSubscriber := subscriber.MessageSubscriber[*pubsub.Message]{
		InitializeFn: func(ctx context.Context, outputCh chan *message.Message) error {
			if opts.ConsumerGroup != "" {
				var err error
				if opts.ConsumerGroupCreateStreamIfMissing {
					err = c.XGroupCreateMkStream(ctx, stream, opts.ConsumerGroup, string(opts.ConsumerGroupStartID)).Err()
				} else {
					err = c.XGroupCreate(ctx, stream, opts.ConsumerGroup, string(opts.ConsumerGroupStartID)).Err()
				}
				if err != nil {
					if strings.Contains(err.Error(), "The XGROUP subcommand requires the key to exist") {
						return StreamDoesntExistErr
					}
					//Getting this error when the consumer group already exists is fine
					if !strings.Contains(err.Error(), "BUSYGROUP") {
						return err
					}
				}
			}

			uniqueID := shortuuid.New()
			go func() {
				startID := string(opts.StartID)
				for {
					var err error
					var entries []redis.XStream

					if opts.ConsumerGroup != "" {
						entries, err = c.XReadGroup(ctx, &redis.XReadGroupArgs{
							Group:    opts.ConsumerGroup,
							Consumer: uniqueID,
							Streams:  []string{stream, ">"},
							Count:    1,
							Block:    0,
							NoAck:    false,
						}).Result()
					} else {
						entries, err = c.XRead(ctx, &redis.XReadArgs{
							Streams: []string{stream, startID},
							Count:   1,
							Block:   0,
						}).Result()
					}
					if err != nil {
						return
					}
					if (len(entries) == 0) || (len(entries[0].Messages) == 0) {
						continue
					}
					msgs := entries[0].Messages
					for _, msg := range msgs {
						processor.ProcessMessage(ctx, MessageWrapper{
							stream:        stream,
							consumerGroup: opts.ConsumerGroup,
							msg:           &msg,
						}, outputCh)
						startID = msg.ID
					}
				}
			}()
			return nil
		},
	}

	return &Subscriber{
		internalSubscriber: &internalSubscriber,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	return s.internalSubscriber.Subscribe(ctx)
}

func (s *Subscriber) Close() error {
	return s.internalSubscriber.Close()
}
