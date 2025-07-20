package google

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"strconv"
	"time"
)

const DefaultProcessingTimeout = 600 * time.Second

type MessageUnmarshaller func(ctx context.Context, msg *pubsub.Message) (*message.Message, error)

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	onProcessingTimeout func(ctx context.Context, msg *pubsub.Message)
	processingTimeout   time.Duration
	receiveSettings     pubsub.ReceiveSettings
	parseAttributes     bool
}

// WithProcessingTimeoutHandler is a function that is called when a message processing times out.
func WithProcessingTimeoutHandler(handler func(ctx context.Context, msg *pubsub.Message)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onProcessingTimeout = handler
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

// WithReceiveSettings is a set of options to pass the underlying gcp pubsub.Subscription
func WithReceiveSettings(settings pubsub.ReceiveSettings) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.receiveSettings = settings
	}
}

// WithParseAttributes is a flag to indicate if the attributes should be parsed or not, meaning that boolean true/false,
// integers and floats are going to be their respective types. The default is to just keep everything as strings.
func WithParseAttributes(parseAttributes bool) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.parseAttributes = parseAttributes
	}
}

type Subscriber struct {
	internalSubscriber *subscriber.MessageSubscriber[*pubsub.Message]
}

func NewGoogleSubscriber(
	c *pubsub.Client,
	subscription string,
	opts ...SubscriberOption) (*Subscriber, error) {

	if c == nil {
		return nil, errors.New("client is required")
	}

	options := SubscriberOptions{
		processingTimeout: DefaultProcessingTimeout,
	}

	for _, opt := range opts {
		opt(&options)
	}

	processor := subscriber.MessageProcessor[*pubsub.Message]{
		Ack: func(ctx context.Context, msg *pubsub.Message) error {
			msg.Ack()
			return nil
		},
		Nack: func(ctx context.Context, msg *pubsub.Message) error {
			msg.Nack()
			return nil
		},
		MessageUnmarshall: func(ctx context.Context, pubMsg *pubsub.Message) *message.Message {
			metadata := make(map[string]interface{})
			for k, v := range pubMsg.Attributes {
				if options.parseAttributes {
					metadata[k] = parseAsPrimitiveType(v)
				} else {
					metadata[k] = v
				}
			}

			msg := message.NewMessage(ctx, pubMsg.Data)
			msg.ID = pubMsg.ID
			msg.Metadata = metadata
			return msg
		},
		ProcessingTimeout:   options.processingTimeout,
		OnProcessingTimeout: options.onProcessingTimeout,
	}
	internalSubscriber := subscriber.MessageSubscriber[*pubsub.Message]{
		InitializeFn: func(ctx context.Context, outputCh chan *message.Message) error {
			sub := c.Subscription(subscription)
			if ok, err := sub.Exists(ctx); !ok || err != nil {
				return errors.New("subscription does not exist")
			}
			sub.ReceiveSettings = options.receiveSettings
			go func() {
				sub.Receive(ctx,
					func(ctx context.Context, pubMsg *pubsub.Message) {
						processor.ProcessMessage(ctx, pubMsg, outputCh)
					})
			}()
			return nil
		},
	}

	return &Subscriber{
		internalSubscriber: &internalSubscriber,
	}, nil
}

// parseAsPrimitiveType will try to parse the value as a primitive type, if it fails, it will return the original value.
// It supports boolean, integers and floats. Otherwise, it will return the original value as string
func parseAsPrimitiveType(v string) interface{} {
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}

	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		return i
	}

	f, err := strconv.ParseFloat(v, 64)
	if err == nil {
		return f
	}

	return v
}

func (s *Subscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	return s.internalSubscriber.Subscribe(ctx)
}

func (s *Subscriber) Close() error {
	return s.internalSubscriber.Close()
}
