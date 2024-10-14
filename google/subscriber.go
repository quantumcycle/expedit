package google

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"time"
)

const DefaultProcessingTimeout = 600 * time.Second

type MessageUnmarshaller func(ctx context.Context, msg *pubsub.Message) (*message.Message, error)

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	onUnmarshallingError func(msg *pubsub.Message, err error)
	onProcessingTimeout  func(ctx context.Context, msg *pubsub.Message)
	processingTimeout    time.Duration
	receiveSettings      pubsub.ReceiveSettings
}

// WithUnmarshallingErrorHandler is a function that is called when an error occurs while unmarshalling a message.
// If this option is provided, the callback is responsible for handling the error and optionally calling msg.Nack().
// If this option is not provided, the default behaviour is to call msg.Nack().
func WithUnmarshallingErrorHandler(handler func(msg *pubsub.Message, err error)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onUnmarshallingError = handler
	}
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
		Ack: func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
		},
		Nack: func(ctx context.Context, msg *pubsub.Message) {
			msg.Nack()
		},
		MessageUnmarshall: func(ctx context.Context, pubMsg *pubsub.Message) (*message.Message, error) {
			msg := message.NewMessage(ctx, pubMsg.ID, pubMsg.Data)
			msg.Metadata = pubMsg.Attributes
			return msg, nil
		},
		OnUnmarshallingError: options.onUnmarshallingError,
		ProcessingTimeout:    options.processingTimeout,
		OnProcessingTimeout:  options.onProcessingTimeout,
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

func (s *Subscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	return s.internalSubscriber.Subscribe(ctx)
}

func (s *Subscriber) Close() error {
	return s.internalSubscriber.Close()
}
