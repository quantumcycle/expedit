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
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync/atomic"
	"time"
)

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

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *ReconnectingChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	// Check queue existence with limited retry for timing issues during test setup
	// Only retry for a short period to handle race conditions in tests
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		_, err := ch.Channel.QueueDeclarePassive(queue, false, false, false, false, nil)
		if err == nil {
			break // Queue exists, continue
		}
		if i == maxRetries-1 {
			// Return original error message for compatibility
			return nil, errors.New("queue does not exist")
		}
		// Brief wait only for potential timing issues
		time.Sleep(50 * time.Millisecond)
	}

	go func() {
		var reconnectionAttempts int
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				delay := ch.connection.opts.retryStrategy(reconnectionAttempts)
				time.Sleep(delay)
				reconnectionAttempts++
				continue
			}

			reconnectionAttempts = 0
			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(3 * time.Second)
			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}
