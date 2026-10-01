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
	autoAck           bool
	noRequeueOnNack   bool
	exclusive         bool
	processingTimeout time.Duration
	maxInFlight       int
	onAckError        func(msg *message.Message, err error)
	onNackError       func(msg *message.Message, err error)
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

// WithProcessingTimeout is the deadline of the context of each message, see subscriber.DispatchOptions.
// 0 means no deadline, which is the default.
func WithProcessingTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.processingTimeout = timeout
	}
}

// WithMaxInFlight is the maximum number of messages handled at the same time, see subscriber.DispatchOptions.
// Default is subscriber.DefaultMaxInFlight. Set the prefetch count of the channel (Qos) to at least this value, so
// the broker does not deliver more messages than can be handled.
func WithMaxInFlight(maxInFlight int) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.maxInFlight = maxInFlight
	}
}

// WithAckErrorHandler is called when acking a message fails. If not provided, the error is ignored.
func WithAckErrorHandler(handler func(msg *message.Message, err error)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onAckError = handler
	}
}

// WithNackErrorHandler is called when nacking a message fails. If not provided, the error is ignored.
func WithNackErrorHandler(handler func(msg *message.Message, err error)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onNackError = handler
	}
}

// ErrChannelClosed is returned by Receive when the deliveries stop while ctx is not done.
var ErrChannelClosed = errors.New("amqp channel closed")

type Subscriber struct {
	channel *ReconnectingChannel
	queue   string
	options SubscriberOptions
}

func NewAMQPSubscriber(channel *ReconnectingChannel, queue string, opts ...SubscriberOption) (*Subscriber, error) {
	if channel == nil {
		return nil, errors.New("channel is required")
	}

	options := SubscriberOptions{
		maxInFlight: subscriber.DefaultMaxInFlight,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &Subscriber{
		channel: channel,
		queue:   queue,
		options: options,
	}, nil
}

// Receive implements subscriber.Subscriber. When ctx is done, the consumer is cancelled. The messages the broker
// already delivered to the channel but that were not handled yet stay unacked until the channel is closed, and are
// then requeued by the broker. The prefetch count of the channel bounds how many they are.
func (s *Subscriber) Receive(ctx context.Context, handler message.HandlerFunc) error {
	deliveries, err := s.channel.Consume(ctx, s.queue, "", s.options.autoAck, s.options.exclusive, false, false, nil)
	if err != nil {
		return err
	}
	d := subscriber.NewDispatcher(handler, subscriber.DispatchOptions{
		MaxInFlight:       s.options.maxInFlight,
		ProcessingTimeout: s.options.processingTimeout,
		OnAckError:        s.options.onAckError,
		OnNackError:       s.options.onNackError,
	})
	defer d.Wait()

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return ErrChannelClosed
			}
			d.Dispatch(ctx, s.delivery(ctx, delivery))
		}
	}
}

func (s *Subscriber) delivery(ctx context.Context, delivery amqp.Delivery) subscriber.Delivery {
	msg := message.NewMessage(ctx, delivery.Body)
	msg.ID = delivery.MessageId
	if delivery.Headers != nil {
		msg.Metadata = message.Metadata(delivery.Headers)
	}
	return subscriber.Delivery{
		Message: msg,
		Ack: func(context.Context) error {
			if s.options.autoAck {
				return nil
			}
			return delivery.Ack(false)
		},
		Nack: func(context.Context) error {
			if s.options.autoAck {
				return nil
			}
			return delivery.Nack(false, !s.options.noRequeueOnNack)
		},
	}
}
