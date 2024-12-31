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

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	onUnmarshallingError               func(msg MessageWrapper, err error)
	onProcessingTimeout                func(ctx context.Context, msg MessageWrapper)
	consumerGroup                      string
	consumerGroupCreateStreamIfMissing bool
	consumerGroupStartID               StartPosition
	startID                            StartPosition
	processingTimeout                  time.Duration
	metadataExtractor                  func(wrapper MessageWrapper) map[string]string
	payloadExtractor                   func(wrapper MessageWrapper) map[string]interface{}
}

// WithUnmarshallingErrorHandler takes a function that is called when an error occurs while unmarshalling a message.
// If this option is provided, the callback is responsible for handling the error and optionally calling msg.Nack().
// If this option is not provided, the default behaviour is to call msg.Nack().
func WithUnmarshallingErrorHandler(handler func(msg MessageWrapper, err error)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onUnmarshallingError = handler
	}
}

// WithProcessingTimeoutHandler takes a function that is called when a message processing times out.
func WithProcessingTimeoutHandler(handler func(ctx context.Context, msg MessageWrapper)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onProcessingTimeout = handler
	}
}

// WithConsumerGroup identifies the consumer group to which the subscriber belongs.
func WithConsumerGroup(group string) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroup = group
	}
}

// WithConsumerGroupCreateStreamIfMissing will create the stream if it does not exist in consumer group mode.
func WithConsumerGroupCreateStreamIfMissing(create bool) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroupCreateStreamIfMissing = create
	}
}

// WithConsumerGroupStartID is the position the consumer of a consumer group should start from
// Using ConsumerGroupStartFromBeginning "0" means the consumer group will consume from the very first message.
// Using ConsumerGroupStartFromLatest "$" means the consumer group will consume from the latest message.
func WithConsumerGroupStartID(startID StartPosition) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroupStartID = startID
	}
}

// WithStartID is the ID of the last message that was processed by the subscriber. This is used only when not using
// consumer groups. The default is "$" which means the subscriber will start from the latest message.
func WithStartID(startID StartPosition) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.startID = startID
	}
}

// WithProcessingTimeout will dictate how long a message will be processed before it is nacked.
// 0 means no timeout, wait forever. Keep in mind that GCP uses the "Acknowledgement deadline" to determine if a
// message needs to be redelivered. ProcessingTimeout has no impact on the "Acknowledgement deadline".
// Default value is 600 seconds, which is the max value of the GCP "Acknowledgement deadline".
func WithProcessingTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.processingTimeout = timeout
	}
}

// WithMetadataExtractor is a function that extracts metadata from a redis message.
// If not provided, no metadata will be extracted. The extractor usually needs to be aligned with the
// marshaller used by the publisher.
func WithMetadataExtractor(extractor func(wrapper MessageWrapper) map[string]string) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.metadataExtractor = extractor
	}
}

// WithPayloadExtractor is a function that extracts payload from a redis message.
// If not provided, all the values in the redis message are converted into a map as payload.
func WithPayloadExtractor(extractor func(wrapper MessageWrapper) map[string]interface{}) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.payloadExtractor = extractor
	}
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
	opts ...SubscriberOption) (*Subscriber, error) {

	if c == nil {
		return nil, errors.New("client is required")
	}

	options := &SubscriberOptions{
		processingTimeout:    600 * time.Second,
		consumerGroupStartID: StartFromLatest,
		startID:              StartFromLatest,
	}

	for _, opt := range opts {
		opt(options)
	}

	processor := subscriber.MessageProcessor[MessageWrapper]{
		Ack: func(ctx context.Context, wrapper MessageWrapper) {
			//TODO: Xack seems to be only useful for consumer groups, should we add a IF to only apply on consumer groups?
			_, err := c.XAck(ctx, wrapper.stream, wrapper.consumerGroup, wrapper.msg.ID).Result()
			if err != nil {
				//TODO, handle this error instead of panicking
				//Retry a couple of time? If it doesn't work, it will be redelivered
				panic(err)
			}
		},
		Nack: func(ctx context.Context, wrapper MessageWrapper) {
			//TODO: see the XCLAIM comment above
		},
		MessageUnmarshall: func(ctx context.Context, wrapper MessageWrapper) (*message.Message, error) {
			metadata := map[string]string{}
			if options.metadataExtractor != nil {
				metadata = options.metadataExtractor(wrapper)
			}
			payload := map[string]interface{}{}
			if options.payloadExtractor != nil {
				payload = options.payloadExtractor(wrapper)
			} else {
				payload = wrapper.msg.Values
			}
			msg := message.NewMessage(ctx, payload)
			msg.ID = wrapper.msg.ID
			msg.Metadata = metadata
			return msg, nil
		},
		OnUnmarshallingError: options.onUnmarshallingError,
		ProcessingTimeout:    options.processingTimeout,
		OnProcessingTimeout:  options.onProcessingTimeout,
	}
	internalSubscriber := subscriber.MessageSubscriber[*pubsub.Message]{
		InitializeFn: func(ctx context.Context, outputCh chan *message.Message) error {
			if options.consumerGroup != "" {
				var err error
				if options.consumerGroupCreateStreamIfMissing {
					err = c.XGroupCreateMkStream(ctx, stream, options.consumerGroup, string(options.consumerGroupStartID)).Err()
				} else {
					err = c.XGroupCreate(ctx, stream, options.consumerGroup, string(options.consumerGroupStartID)).Err()
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
				startID := string(options.startID)
				for {
					var err error
					var entries []redis.XStream

					if options.consumerGroup != "" {
						entries, err = c.XReadGroup(ctx, &redis.XReadGroupArgs{
							Group:    options.consumerGroup,
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
							consumerGroup: options.consumerGroup,
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
