//This code is based on the following project, and is subject to the same MIT license
//https://github.com/isayme/go-amqp-reconnect

// Original license:
// -----------------------------------------------------------------------------------
// MIT License
//
// # Copyright (c) 2018 iSayme
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
// -----------------------------------------------------------------------------------
package amqp

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync/atomic"
	"time"
)

// ErrQueueNotFound is returned when consuming from a queue that does not exist.
var ErrQueueNotFound = errors.New("queue does not exist")

type ReconnectingChannel struct {
	*amqp.Channel
	connection *ReconnectingConnection
	closed     int32
}

func (ch *ReconnectingChannel) watchDisconnects() {
	go func() {
		var reconnectionAttempts int
		for {
			_, ok := <-ch.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || ch.IsClosed() {
				ch.Channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			reconnectionAttempts = 0

			// reconnect if not closed by developer
			for {
				delay := ch.connection.opts.retryStrategy(reconnectionAttempts)
				time.Sleep(delay)
				reconnectionAttempts++

				newCh, err := ch.connection.Connection.Channel()
				if err == nil {
					ch.Channel = newCh
					break
				}
			}
		}

	}()
}

// IsClosed indicate closed by developer
func (ch *ReconnectingChannel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *ReconnectingChannel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer.
// Use ConsumeWithContext to be able to stop consuming without closing the channel.
func (ch *ReconnectingChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return ch.ConsumeWithContext(context.Background(), queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

// ConsumeWithContext wrap amqp.Channel.ConsumeWithContext and keeps consuming after a reconnection. The returned
// deliveries channel is closed, and the consumer is cancelled, when ctx is done or when the channel is closed by the
// developer. It is also closed when the broker cancels the consumer while the channel stays open, for example
// because the queue was deleted.
//
// The first consume is done synchronously, so its error is returned and no RPC of this consumer runs concurrently
// with the next calls on the channel. amqp091 matches RPC responses in order, so concurrent RPCs on the same channel
// can receive each other's responses.
func (ch *ReconnectingChannel) ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	consumingOn := ch.Channel
	d, err := consumingOn.ConsumeWithContext(ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
			return nil, fmt.Errorf("%w: %s", ErrQueueNotFound, queue)
		}
		return nil, err
	}

	deliveries := make(chan amqp.Delivery)
	go func() {
		defer close(deliveries)
		var reconnectionAttempts int
		for {
			for msg := range d {
				select {
				case deliveries <- msg:
				case <-ctx.Done():
					return
				}
			}

			// The broker cancelled the consumer, there is nothing to reconnect
			if !consumingOn.IsClosed() {
				return
			}

			// The channel was lost, consume again once reconnected, until it succeeds
			for {
				// The closed flag is set before the channel is closed, so it is reliable here. Do not talk to a
				// channel that was closed in the meantime, the broker would drop the connection.
				if ctx.Err() != nil || ch.IsClosed() {
					return
				}
				select {
				case <-time.After(ch.connection.opts.retryStrategy(reconnectionAttempts)):
				case <-ctx.Done():
					return
				}
				reconnectionAttempts++
				consumingOn = ch.Channel
				d, err = consumingOn.ConsumeWithContext(ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
				if err == nil {
					reconnectionAttempts = 0
					break
				}
			}
		}
	}()

	return deliveries, nil
}
