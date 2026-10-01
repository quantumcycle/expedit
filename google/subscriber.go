package google

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"errors"
	"fmt"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"strconv"
	"time"
)

const DefaultProcessingTimeout = 600 * time.Second

type MessageUnmarshaller func(ctx context.Context, msg *pubsub.Message) (*message.Message, error)

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	processingTimeout time.Duration
	receiveSettings   pubsub.ReceiveSettings
	parseAttributes   bool
}

// WithProcessingTimeout is the deadline of the context of each message, see subscriber.DispatchOptions.
// 0 means no deadline. The Pub/Sub client extends the "Acknowledgement deadline" while the handler runs, up to
// ReceiveSettings.MaxExtension. Default value is 600 seconds, which is the max value of the GCP
// "Acknowledgement deadline".
func WithProcessingTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.processingTimeout = timeout
	}
}

// WithReceiveSettings is a set of options to pass the underlying gcp pubsub.Subscriber. Its
// MaxOutstandingMessages and MaxOutstandingBytes limit how many messages are handled at the same time.
// Leave ShutdownOptions nil to keep the shutdown behavior of Receive (wait for the in-flight handlers): with
// ShutdownOptions set, the Pub/Sub client may stop waiting for them, and nack them.
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

// ErrSubscriptionNotFound is returned by Receive when the subscription does not exist.
var ErrSubscriptionNotFound = errors.New("subscription does not exist")

type Subscriber struct {
	client       *pubsub.Client
	subscription string
	options      SubscriberOptions
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

	return &Subscriber{
		client:       c,
		subscription: subscription,
		options:      options,
	}, nil
}

// Receive implements subscriber.Subscriber. The Pub/Sub client limits the concurrency, see WithReceiveSettings.
func (s *Subscriber) Receive(ctx context.Context, handler message.HandlerFunc) error {
	d := subscriber.NewDispatcher(handler, subscriber.DispatchOptions{ProcessingTimeout: s.options.processingTimeout})
	sub := s.client.Subscriber(s.subscription)
	sub.ReceiveSettings = s.options.receiveSettings

	// Receive returns nil when ctx is done, after all the callbacks returned
	err := sub.Receive(ctx, func(ctx context.Context, pubMsg *pubsub.Message) {
		d.Handle(subscriber.Delivery{
			Message: s.toMessage(ctx, pubMsg),
			Ack: func(context.Context) error {
				pubMsg.Ack()
				return nil
			},
			Nack: func(context.Context) error {
				pubMsg.Nack()
				return nil
			},
		})
	})
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s: %w", ErrSubscriptionNotFound, s.subscription, err)
	}
	return err
}

func (s *Subscriber) toMessage(ctx context.Context, pubMsg *pubsub.Message) *message.Message {
	metadata := make(map[string]interface{}, len(pubMsg.Attributes))
	for k, v := range pubMsg.Attributes {
		if s.options.parseAttributes {
			metadata[k] = parseAsPrimitiveType(v)
		} else {
			metadata[k] = v
		}
	}

	msg := message.NewMessage(ctx, pubMsg.Data)
	msg.ID = pubMsg.ID
	msg.Metadata = metadata
	return msg
}

// parseAsPrimitiveType will try to parse the value as a primitive type, if it fails, it will return the original value.
// It supports boolean, integers and floats. Otherwise, it will return the original value as string
func parseAsPrimitiveType(v string) interface{} {
	// Integers come before booleans, because strconv.ParseBool accepts "0" and "1"
	i, err := strconv.ParseInt(v, 10, 64)
	if err == nil {
		return i
	}

	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}

	f, err := strconv.ParseFloat(v, 64)
	if err == nil {
		return f
	}

	return v
}
