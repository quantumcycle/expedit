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
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type ReconnectingConnection struct {
	*amqp.Connection
	url  string
	opts ConnectionOptions
}

func (c *ReconnectingConnection) watchDisconnects() {
	go func() {
		connection := c.Connection
		var reconnectionAttempts int
		for {
			if _, ok := <-connection.NotifyClose(make(chan *amqp.Error)); !ok {
				// exit this goroutine if closed by developer
				break
			}
			reconnectionAttempts = 0

			// reconnect if not closed by developer
			for {
				delay := c.opts.retryStrategy(reconnectionAttempts)
				time.Sleep(delay)
				reconnectionAttempts++

				conn, err := amqp.Dial(c.url)
				if err == nil {
					c.Connection = conn
					break
				}
			}
		}
	}()
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *ReconnectingConnection) Channel() (*ReconnectingChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &ReconnectingChannel{
		Channel:    ch,
		connection: c,
	}

	channel.watchDisconnects()

	return channel, nil
}

type ConnectionOptions struct {
	retryStrategy RetryDelayStrategy
}

type ConnectionOption func(*ConnectionOptions)

type RetryDelayStrategy func(iterationCount int) time.Duration

func WithReconnectionRetry(s RetryDelayStrategy) ConnectionOption {
	return func(opts *ConnectionOptions) {
		opts.retryStrategy = s
	}
}

// DefaultRetryStrategy is just to wait 1 sec between reconnection attempts
var DefaultRetryStrategy = func(iterationCount int) time.Duration {
	return 1 * time.Second
}

func Dial(url string, opts ...ConnectionOption) (*ReconnectingConnection, error) {
	return DialConfig(url, amqp.Config{
		Locale: "en_US",
	}, opts...)
}

func DialConfig(url string, config amqp.Config, opts ...ConnectionOption) (*ReconnectingConnection, error) {
	options := ConnectionOptions{
		retryStrategy: DefaultRetryStrategy,
	}
	for _, opt := range opts {
		opt(&options)
	}

	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		return nil, err
	}
	connection := &ReconnectingConnection{
		Connection: conn,
		url:        url,
		opts:       options,
	}
	connection.watchDisconnects()
	return connection, nil
}
