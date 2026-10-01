package subscriber

import (
	"context"

	"github.com/quantumcycle/expedit/core/message"
)

// ChannelSubscriber receives the messages sent to a Go channel, for example by a ChannelPublisher. A nacked message
// is handled again. Messages nacked during a shutdown are dropped.
type ChannelSubscriber struct {
	inputCh     <-chan *message.Message
	maxInFlight int
}

func NewChannelSubscriber(inputCh <-chan *message.Message, maxInFlight int) *ChannelSubscriber {
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	return &ChannelSubscriber{
		inputCh:     inputCh,
		maxInFlight: maxInFlight,
	}
}

// Receive handles the messages of the input channel until ctx is done or the input channel is closed and all its
// messages are handled. It always returns nil.
func (c *ChannelSubscriber) Receive(ctx context.Context, handler message.HandlerFunc) error {
	d := NewDispatcher(handler, DispatchOptions{MaxInFlight: c.maxInFlight})
	// A message is either in flight, waiting for a slot in Dispatch, or waiting here to be handled again, so at most
	// maxInFlight+1 messages are ever in retry and sending to it never blocks.
	retry := make(chan *message.Message, c.maxInFlight+1)
	dispatch := func(msg *message.Message) {
		// Handle a copy, so a new attempt is not affected by the changes of the previous one
		d.Dispatch(ctx, Delivery{
			Message: msg.Copy(),
			Ack:     func(context.Context) error { return nil },
			Nack: func(context.Context) error {
				retry <- msg
				return nil
			},
		})
	}

	for {
		select {
		case <-ctx.Done():
			d.Wait()
			return nil
		case msg := <-retry:
			dispatch(msg)
		case msg, ok := <-c.inputCh:
			if !ok {
				return c.drain(ctx, d, retry, dispatch)
			}
			if msg != nil {
				dispatch(msg)
			}
		}
	}
}

// drain handles the nacked messages again until none is left, after the input channel is closed.
func (c *ChannelSubscriber) drain(ctx context.Context, d *Dispatcher, retry chan *message.Message, dispatch func(*message.Message)) error {
	for {
		d.Wait()
		if ctx.Err() != nil {
			return nil
		}
		select {
		case msg := <-retry:
			dispatch(msg)
		default:
			return nil
		}
	}
}
