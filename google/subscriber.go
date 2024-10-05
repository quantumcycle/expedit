package google

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"time"
)

type MessageUnmarshaller func(ctx context.Context, msg *pubsub.Message) (*message.Message, error)

type SubscriberOption struct {
	// OnUnmarshallingError is a function that is called when an error occurs while unmarshalling a message.
	// If this option is provided, the callback is responsible for handling the error and optionally calling msg.Nack().
	// If this option is not provided, the default behaviour is to call msg.Nack().
	OnUnmarshallingError func(msg *pubsub.Message, err error)

	// OnProcessingTimeout is a function that is called when a message processing times out.
	OnProcessingTimeout func(ctx context.Context, msg *pubsub.Message)

	// ProcessingTimeout will dictate how long a message will be processed before it is nacked.
	// 0 means no timeout, wait forever. Keep in mind that GCP uses the "Acknowledgement deadline" to determine if a
	// message needs to be redelivered. ProcessingTimeout has no impact on the "Acknowledgement deadline".
	// Default value is 600 seconds, which is the max value of the GCP "Acknowledgement deadline".
	ProcessingTimeout time.Duration

	// ReceiveSettings is a set of options to pass the underlying gcp pubsub.Subscription
	ReceiveSettings pubsub.ReceiveSettings
}

type Subscriber struct {
	internalSubscriber *subscriber.MessageSubscriber[*pubsub.Message]
}

type PayloadUnmarshaller func(msg *pubsub.Message) (message.Payload, error)

func NewGoogleSubscriber(
	c *pubsub.Client,
	subscription string,
	unmarshaller PayloadUnmarshaller,
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

	processor := subscriber.MessageProcessor[*pubsub.Message]{
		Ack: func(ctx context.Context, msg *pubsub.Message) {
			msg.Ack()
		},
		Nack: func(ctx context.Context, msg *pubsub.Message) {
			msg.Nack()
		},
		MessageUnmarshall: func(ctx context.Context, pubMsg *pubsub.Message) (*message.Message, error) {
			payload, err := unmarshaller(pubMsg)
			if err != nil {
				return nil, err
			}
			msg := message.NewMessage(ctx, pubMsg.ID, payload)
			msg.Metadata = pubMsg.Attributes
			return msg, nil
		},
		OnUnmarshallingError: opts.OnUnmarshallingError,
		ProcessingTimeout:    opts.ProcessingTimeout,
		OnProcessingTimeout:  opts.OnProcessingTimeout,
	}
	internalSubscriber := subscriber.MessageSubscriber[*pubsub.Message]{
		InitializeFn: func(ctx context.Context, outputCh chan *message.Message) error {
			sub := c.Subscription(subscription)
			if ok, err := sub.Exists(ctx); !ok || err != nil {
				return errors.New("subscription does not exist")
			}
			sub.ReceiveSettings = opts.ReceiveSettings
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
