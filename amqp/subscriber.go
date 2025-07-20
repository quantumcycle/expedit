package amqp

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	autoAck             bool
	noRequeueOnNack     bool
	exclusive           bool
	processingTimeout   time.Duration
	onProcessingTimeout func(ctx context.Context, msg *amqp.Delivery)
}

// WithAutoAck will automatically ack the message when it's received.
func WithAutoAck() SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.autoAck = true
	}
}

// WithNoRequeueOnNack will not requeue the message when it's nacked.
func WithNoRequeueOnNack() SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.noRequeueOnNack = true
	}
}

// WithExclusive will make this subscriber exclusive to the target queue.
func WithExclusive() SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.exclusive = true
	}
}

// WithProcessingTimeout will dictate how long a message will be processed before it is nacked.
// 0 means no timeout, wait forever.
func WithProcessingTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.processingTimeout = timeout
	}
}

// WithProcessingTimeoutHandler is a function that is called when a message processing times out.
func WithProcessingTimeoutHandler(handler func(ctx context.Context, msg *amqp.Delivery)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onProcessingTimeout = handler
	}
}

type Subscriber struct {
	internalSubscriber *subscriber.MessageSubscriber[*amqp.Delivery]
}

func NewAMQPSubscriber(channel *ReconnectingChannel, queue string, opts ...SubscriberOption) (*Subscriber, error) {
	if channel == nil {
		return nil, errors.New("channel is required")
	}

	options := SubscriberOptions{
		autoAck: false,
	}
	for _, opt := range opts {
		opt(&options)
	}

	ackFn := func(ctx context.Context, msgImpl *amqp.Delivery) error {
		if options.autoAck {
			return nil
		}
		return msgImpl.Ack(false)
	}
	nackFn := func(ctx context.Context, msgImpl *amqp.Delivery) error {
		if options.autoAck {
			return nil
		}
		return msgImpl.Nack(false, !options.noRequeueOnNack)
	}
	processor := subscriber.MessageProcessor[*amqp.Delivery]{
		Ack:  ackFn,
		Nack: nackFn,
		MessageUnmarshall: func(ctx context.Context, msgImpl *amqp.Delivery) *message.Message {
			msg := message.NewMessage(ctx, msgImpl.Body)
			msg.ID = msgImpl.MessageId
			msg.Metadata = message.Metadata(msgImpl.Headers)
			return msg
		},
		ProcessingTimeout:   options.processingTimeout,
		OnProcessingTimeout: options.onProcessingTimeout,
	}

	internalSubscriber := subscriber.MessageSubscriber[*amqp.Delivery]{
		InitializeFn: func(ctx context.Context, outputCh chan *message.Message) error {
			msgsCh, err := channel.Consume(queue, "",
				options.autoAck,
				options.exclusive,
				false,
				false,
				nil)
			if err != nil {
				return err
			}
			go func() {
				for {
					select {
					case msg := <-msgsCh:
						processor.ProcessMessage(ctx, &msg, outputCh)
					case <-ctx.Done():
						//TODO test the context cancelled case
						return
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
