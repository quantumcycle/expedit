package amqp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/amqp/testrabbit"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	amqpgo "github.com/rabbitmq/amqp091-go"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

type simpleToxiproxy struct {
	client *toxiproxy.Client
	proxy  *toxiproxy.Proxy
}

func (t *simpleToxiproxy) SimulateDisconnect() {
	if t.proxy != nil {
		_ = t.proxy.Disable()
		time.Sleep(500 * time.Millisecond)
		_ = t.proxy.Enable()
	}
}

func setupToxiproxy() (*simpleToxiproxy, error) {
	client := toxiproxy.NewClient("localhost:8474")

	maxRetries := 5
	var proxy *toxiproxy.Proxy
	var err error

	for i := 0; i < maxRetries; i++ {
		_, err = client.Proxies()
		if err != nil {
			if i == maxRetries-1 {
				return nil, fmt.Errorf("toxiproxy not available after %d attempts: %w", maxRetries, err)
			}
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			continue
		}

		proxy, err = client.CreateProxy("rabbitmq", "localhost:25672", "rabbitmq:5672")
		if err != nil {
			proxy, err = client.Proxy("rabbitmq")
			if err != nil {
				if i == maxRetries-1 {
					return nil, fmt.Errorf("failed to setup toxiproxy proxy after %d attempts: %w", maxRetries, err)
				}
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
		}

		if proxy != nil {
			break
		}
	}

	if proxy == nil {
		return nil, fmt.Errorf("failed to create or retrieve toxiproxy proxy")
	}

	return &simpleToxiproxy{
		client: client,
		proxy:  proxy,
	}, nil
}

func (t *simpleToxiproxy) Cleanup() {
	if t.proxy != nil {
		err := t.proxy.Delete()
		if err != nil {
			fmt.Printf("Warning: Failed to delete toxiproxy proxy: %v\n", err)
		}
	}
}

func createTestConnectionWithToxiproxy(toxi *simpleToxiproxy) (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, error) {
	config := amqpgo.Config{
		Vhost:      "/",
		Properties: amqpgo.NewConnectionProperties(),
	}

	connectionURIs := []string{"amqp://guest:guest@localhost:5672/"}
	if toxi != nil {
		connectionURIs = []string{"amqp://guest:guest@localhost:25672/", "amqp://guest:guest@localhost:5672/"}
	}

	maxRetries := 5
	baseDelay := 50 * time.Millisecond
	maxDelay := 2 * time.Second

	var conn *amqp.ReconnectingConnection
	var err error
	connected := false

	for _, uri := range connectionURIs {
		for i := 0; i < maxRetries; i++ {
			conn, err = amqp.DialConfig(uri, config)
			if err == nil {
				connected = true
				break
			}

			delay := time.Duration(i+1) * baseDelay
			if delay > maxDelay {
				delay = maxDelay
			}
			time.Sleep(delay)
		}
		if connected {
			break
		}
	}

	if !connected {
		return nil, nil, fmt.Errorf("failed to connect to RabbitMQ after trying all connection options: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to create channel: %w", err)
	}

	return conn, channel, nil
}

// closeOnce returns a function closing ch at most once, and registers it to run when the test ends, so a failing test
// does not leave handlers blocked.
func closeOnce(t *testing.T, ch chan struct{}) func() {
	var once sync.Once
	closeCh := func() { once.Do(func() { close(ch) }) }
	t.Cleanup(closeCh)
	return closeCh
}

func countTo(count *atomic.Int32) message.HandlerFunc {
	return func(*message.Message) error {
		count.Add(1)
		return nil
	}
}

func TestAMQPSubscriber(t *testing.T) {
	t.Run("should return an error if the channel is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)

		_, err := amqp.NewAMQPSubscriber(nil, "test-queue")
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("channel is required"))
	})

	t.Run("should return an error if the queue does not exist", func(t *testing.T) {
		g := NewGomegaWithT(t)
		_, channel := newTestChannel(t)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		sub, err := amqp.NewAMQPSubscriber(channel, "non-existing-queue")
		g.Expect(err).NotTo(HaveOccurred())

		err = sub.Receive(ctx, func(*message.Message) error { return nil })
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError(amqp.ErrQueueNotFound))
	})

	t.Run("when using a direct queue", func(t *testing.T) {
		t.Run("should receives all messages sent to the queue", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-direct-queue")

			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			received := newStringSet()
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				received.Add(string(msg.Payload.([]byte)))
				return nil
			})

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				queue.PublishBytes([]byte(fmt.Sprintf("test message %d", i)), nil)
			}

			g.Eventually(received.Len, 5*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))
			for i := 0; i < expectedMsgCount; i++ {
				g.Expect(received.Count(fmt.Sprintf("test message %d", i))).To(Equal(1))
			}
		})
	})

	t.Run("when testing network disconnection with toxiproxy", func(t *testing.T) {
		t.Run("should reconnect and receive all the messages", func(t *testing.T) {
			g := NewGomegaWithT(t)
			toxi, err := setupToxiproxy()
			if err != nil {
				t.Skip("Toxiproxy not available, skipping network disconnection test")
			}
			t.Cleanup(toxi.Cleanup)

			// The admin connection goes directly to the broker, the connection under test goes through the proxy
			adminConn, _ := newTestChannel(t)
			conn, channel, err := createTestConnectionWithToxiproxy(toxi)
			g.Expect(err).NotTo(HaveOccurred())
			t.Cleanup(func() {
				_ = channel.Close()
				_ = conn.Close()
			})

			queue := newTestQueue(t, adminConn, channel, "test-disconnect-queue")

			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			// A message can be delivered twice if its ack is lost with the connection, so count distinct messages
			received := newStringSet()
			startReceive(t, adminConn, sub, queue.QueueName, func(msg *message.Message) error {
				received.Add(string(msg.Payload.([]byte)))
				return nil
			})

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				if i == 3 {
					toxi.SimulateDisconnect()
				}

				body := []byte(fmt.Sprintf("test message %d", i))
				// Publishing fails until the channel is reconnected
				g.Eventually(func() error {
					return channel.Publish("", queue.QueueName, false, false, amqpgo.Publishing{
						Body:         body,
						ContentType:  "text/plain",
						DeliveryMode: amqpgo.Persistent,
					})
				}, 15*time.Second, 100*time.Millisecond).Should(Succeed())
			}

			g.Eventually(received.Len, 15*time.Second, 100*time.Millisecond).Should(Equal(expectedMsgCount))
		})
	})

	t.Run("when using a fanout exchange", func(t *testing.T) {
		t.Run("should receive messages published to the fanout exchange", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)

			exchange := testrabbit.CreateFanoutExchange(channel, "test-fanout-exchange", "queue1", "queue2")
			t.Cleanup(exchange.Delete)

			subscriber1, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			g.Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue2"])
			g.Expect(err).NotTo(HaveOccurred())

			var msgCount1, msgCount2 atomic.Int32
			startReceive(t, conn, subscriber1, exchange.LogicalToActual["queue1"], countTo(&msgCount1))
			startReceive(t, conn, subscriber2, exchange.LogicalToActual["queue2"], countTo(&msgCount2))

			expectedMsgCount := 5
			for i := 0; i < expectedMsgCount; i++ {
				exchange.PublishBytes([]byte(fmt.Sprintf("fanout test message %d", i)), nil)
			}

			g.Eventually(msgCount1.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(expectedMsgCount))
			g.Eventually(msgCount2.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(expectedMsgCount))
		})

		t.Run("should handle multiple concurrent subscribers on the same fanout queue", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)

			exchange := testrabbit.CreateFanoutExchange(channel, "test-fanout-exchange", "queue1", "queue2")
			t.Cleanup(exchange.Delete)
			queueName := exchange.LogicalToActual["queue1"]

			subscriber1, err := amqp.NewAMQPSubscriber(channel, queueName)
			g.Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, queueName)
			g.Expect(err).NotTo(HaveOccurred())

			received := newStringSet()
			var msgCount1, msgCount2 atomic.Int32
			record := func(count *atomic.Int32) message.HandlerFunc {
				return func(msg *message.Message) error {
					count.Add(1)
					received.Add(string(msg.Payload.([]byte)))
					return nil
				}
			}
			startReceive(t, conn, subscriber1, queueName, record(&msgCount1))
			startReceive(t, conn, subscriber2, queueName, record(&msgCount2))

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				exchange.PublishBytes([]byte(fmt.Sprintf("concurrent fanout test message %d", i)), nil)
			}

			g.Eventually(func() int {
				return int(msgCount1.Load() + msgCount2.Load())
			}, 5*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount),
				"subscriber1: %d, subscriber2: %d", msgCount1.Load(), msgCount2.Load())
			g.Expect(received.Len()).To(Equal(expectedMsgCount))
		})
	})

	t.Run("when using a topic exchange", func(t *testing.T) {
		t.Run("should receive messages that match the topic pattern", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)

			exchange := testrabbit.CreateTopicExchange(channel, "test-topic-exchange",
				"logs.*", "events.#", "alerts.critical")
			t.Cleanup(exchange.Delete)

			var logsCount, eventsCount, alertsCount atomic.Int32
			for pattern, count := range map[string]*atomic.Int32{
				"logs.*":          &logsCount,
				"events.#":        &eventsCount,
				"alerts.critical": &alertsCount,
			} {
				queueName := exchange.PatternToQueue[pattern]
				sub, err := amqp.NewAMQPSubscriber(channel, queueName)
				g.Expect(err).NotTo(HaveOccurred())
				startReceive(t, conn, sub, queueName, countTo(count))
			}

			testMessages := []struct {
				routing string
				content string
			}{
				{"logs.info", "info log message"},
				{"logs.error", "error log message"},
				{"events.user.created", "user created event"},
				{"events.order.processed", "order processed event"},
				{"alerts.critical", "critical alert"},
				{"alerts.warning", "warning alert"},
				{"unmatched.routing", "should not match any pattern"},
			}
			for _, msgData := range testMessages {
				exchange.PublishBytes([]byte(msgData.content), nil, msgData.routing)
			}

			g.Eventually(logsCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
			g.Eventually(eventsCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
			g.Eventually(alertsCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(1))
		})

		t.Run("should handle complex topic patterns with wildcards", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)

			exchange := testrabbit.CreateTopicExchange(channel, "complex-topic",
				"*.critical", "#.error", "system.#")
			t.Cleanup(exchange.Delete)

			var criticalCount, errorCount, systemCount atomic.Int32
			for pattern, count := range map[string]*atomic.Int32{
				"*.critical": &criticalCount,
				"#.error":    &errorCount,
				"system.#":   &systemCount,
			} {
				queueName := exchange.PatternToQueue[pattern]
				sub, err := amqp.NewAMQPSubscriber(channel, queueName)
				g.Expect(err).NotTo(HaveOccurred())
				startReceive(t, conn, sub, queueName, countTo(count))
			}

			testMessages := []struct {
				routing string
				content string
			}{
				{"app.critical", "app critical issue"},
				{"db.critical", "database critical issue"},
				{"app.logs.error", "application error"},
				{"system.health", "system health check"},
				{"system.metrics.cpu", "cpu metrics"},
				{"network.connection.error", "network error"},
			}
			for _, msgData := range testMessages {
				exchange.PublishBytes([]byte(msgData.content), nil, msgData.routing)
			}

			g.Eventually(criticalCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
			g.Eventually(errorCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
			g.Eventually(systemCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
		})
	})

	t.Run("when using a headers exchange", func(t *testing.T) {
		t.Run("should receive messages that match header bindings", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)

			exchange := testrabbit.CreateHeadersExchange(channel, "test-headers-exchange",
				testrabbit.HeaderBinding{
					BindingKey: "error-critical",
					Headers:    amqpgo.Table{"type": "error", "level": "critical"},
					MatchType:  "all",
				},
				testrabbit.HeaderBinding{
					BindingKey: "any-urgent",
					Headers:    amqpgo.Table{"priority": "urgent", "category": "alert", "service": "auth"},
					MatchType:  "any",
				})
			t.Cleanup(exchange.Delete)

			var errorCriticalCount, anyUrgentCount atomic.Int32
			for binding, count := range map[string]*atomic.Int32{
				"error-critical": &errorCriticalCount,
				"any-urgent":     &anyUrgentCount,
			} {
				queueName := exchange.HeadersToQueue[binding]
				sub, err := amqp.NewAMQPSubscriber(channel, queueName)
				g.Expect(err).NotTo(HaveOccurred())
				startReceive(t, conn, sub, queueName, countTo(count))
			}

			testMessages := []struct {
				content string
				headers map[string]interface{}
			}{
				{"exact match", map[string]interface{}{"type": "error", "level": "critical"}},
				{"urgent only", map[string]interface{}{"priority": "urgent"}},
				{"category only", map[string]interface{}{"category": "alert"}},
				{"service only", map[string]interface{}{"service": "auth"}},
				{"multiple matches", map[string]interface{}{"type": "error", "level": "critical", "priority": "urgent"}},
				{"partial match", map[string]interface{}{"type": "error", "level": "warning"}},
				{"no match", map[string]interface{}{"unrelated": "header"}},
			}
			for _, msgData := range testMessages {
				exchange.PublishBytes([]byte(msgData.content), msgData.headers)
			}

			g.Eventually(errorCriticalCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(2))
			g.Eventually(anyUrgentCount.Load, 5*time.Second, 50*time.Millisecond).Should(BeEquivalentTo(4))
		})
	})

	t.Run("message conversion", func(t *testing.T) {
		t.Run("should give the message an empty, non-nil metadata when the delivery has no headers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-no-headers-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			received := make(chan *message.Message, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				received <- msg
				return nil
			})

			queue.PublishBytes([]byte("no headers"), nil)

			var msg *message.Message
			g.Eventually(received, 5*time.Second).Should(Receive(&msg))
			g.Expect(msg.Metadata).NotTo(BeNil())
			g.Expect(msg.Metadata).To(BeEmpty())
			g.Expect(string(msg.Payload.([]byte))).To(Equal("no headers"))
		})

		t.Run("should give the message the headers as metadata, and the message id", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-headers-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			received := make(chan *message.Message, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				received <- msg
				return nil
			})

			g.Expect(channel.Publish("", queue.QueueName, false, false, amqpgo.Publishing{
				Body:      []byte("with headers"),
				MessageId: "message-1",
				Headers:   amqpgo.Table{"source": "test"},
			})).To(Succeed())

			var msg *message.Message
			g.Eventually(received, 5*time.Second).Should(Receive(&msg))
			g.Expect(msg.ID).To(Equal("message-1"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("source", "test"))
		})
	})

	t.Run("acknowledgement", func(t *testing.T) {
		t.Run("should redeliver a message when the handler returns an error", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-nack-requeue-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			var attempts atomic.Int32
			startReceive(t, conn, sub, queue.QueueName, func(*message.Message) error {
				if attempts.Add(1) == 1 {
					return errors.New("first attempt fails")
				}
				return nil
			})

			queue.PublishBytes([]byte("retry me"), nil)

			g.Eventually(attempts.Load, 5*time.Second, 20*time.Millisecond).Should(BeEquivalentTo(2))
			// The second attempt was acked, so nothing is left in the queue
			g.Eventually(func() int {
				ready, _ := queueState(g, conn, queue.QueueName)
				return ready
			}, 5*time.Second, 20*time.Millisecond).Should(Equal(0))
			g.Consistently(attempts.Load, 300*time.Millisecond, 50*time.Millisecond).Should(BeEquivalentTo(2))
		})

		t.Run("should not redeliver a message nacked with WithNoRequeueOnNack", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-nack-no-requeue-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName, amqp.WithNoRequeueOnNack())
			g.Expect(err).NotTo(HaveOccurred())

			var attempts atomic.Int32
			startReceive(t, conn, sub, queue.QueueName, func(*message.Message) error {
				attempts.Add(1)
				return errors.New("always fails")
			})

			queue.PublishBytes([]byte("drop me"), nil)

			g.Eventually(attempts.Load, 5*time.Second, 20*time.Millisecond).Should(BeEquivalentTo(1))
			g.Consistently(func() int {
				ready, _ := queueState(g, conn, queue.QueueName)
				return ready + int(attempts.Load()) - 1
			}, 500*time.Millisecond, 50*time.Millisecond).Should(Equal(0))
		})

		t.Run("should call the ack error handler when acking fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-ack-error-queue")

			ackErrors := make(chan error, 1)
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName,
				amqp.WithAckErrorHandler(func(msg *message.Message, err error) { ackErrors <- err }))
			g.Expect(err).NotTo(HaveOccurred())

			handling := make(chan struct{})
			release := make(chan struct{})
			run := startReceive(t, conn, sub, queue.QueueName, func(*message.Message) error {
				close(handling)
				<-release
				return nil
			})
			releaseHandler := closeOnce(t, release)

			queue.PublishBytes([]byte("ack fails"), nil)
			g.Eventually(handling, 5*time.Second).Should(BeClosed())

			// The ack runs on the closed channel
			g.Expect(channel.Close()).To(Succeed())
			releaseHandler()

			g.Eventually(ackErrors, 5*time.Second).Should(Receive(HaveOccurred()))
			g.Expect(run.Wait()).To(MatchError(amqp.ErrChannelClosed))
		})
	})

	t.Run("subscriber options", func(t *testing.T) {
		t.Run("should handle WithAutoAck option", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-options-queue")

			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName, amqp.WithAutoAck())
			g.Expect(err).NotTo(HaveOccurred())

			var attempts atomic.Int32
			received := make(chan string, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				attempts.Add(1)
				received <- string(msg.Payload.([]byte))
				// With auto ack the broker already forgot the message, so the nack does nothing
				return errors.New("fails")
			})

			queue.PublishBytes([]byte("auto-ack test"), nil)

			g.Eventually(received, 5*time.Second).Should(Receive(Equal("auto-ack test")))
			g.Consistently(func() int {
				ready, _ := queueState(g, conn, queue.QueueName)
				return ready + int(attempts.Load()) - 1
			}, 500*time.Millisecond, 50*time.Millisecond).Should(Equal(0))
		})

		t.Run("should put the WithProcessingTimeout deadline on the context of the message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-timeout-queue")

			timeout := time.Minute
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName, amqp.WithProcessingTimeout(timeout))
			g.Expect(err).NotTo(HaveOccurred())

			received := time.Now()
			deadlines := make(chan time.Time, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				deadline, ok := msg.Context().Deadline()
				if !ok {
					return errors.New("the context has no deadline")
				}
				deadlines <- deadline
				return nil
			})

			queue.PublishBytes([]byte("timeout test"), nil)

			var deadline time.Time
			g.Eventually(deadlines, 5*time.Second).Should(Receive(&deadline))
			g.Expect(deadline).To(BeTemporally(">", received))
			g.Expect(deadline).To(BeTemporally("<=", time.Now().Add(timeout)))
		})

		t.Run("should not put a deadline on the context of the message by default", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-no-timeout-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			hasDeadline := make(chan bool, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				_, ok := msg.Context().Deadline()
				hasDeadline <- ok
				return nil
			})

			queue.PublishBytes([]byte("no timeout"), nil)

			g.Eventually(hasDeadline, 5*time.Second).Should(Receive(BeFalse()))
		})

		t.Run("should nack and redeliver a message whose processing timed out", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-timeout-nack-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName, amqp.WithProcessingTimeout(100*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			var attempts atomic.Int32
			firstAttemptErr := make(chan error, 1)
			startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				if attempts.Add(1) == 1 {
					<-msg.Context().Done()
					firstAttemptErr <- msg.Context().Err()
					return msg.Context().Err()
				}
				return nil
			})

			queue.PublishBytes([]byte("too slow"), nil)

			g.Eventually(firstAttemptErr, 5*time.Second).Should(Receive(MatchError(context.DeadlineExceeded)))
			g.Eventually(attempts.Load, 5*time.Second, 20*time.Millisecond).Should(BeEquivalentTo(2))
		})

		t.Run("should handle at most WithMaxInFlight messages at the same time", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-max-in-flight-queue")

			maxInFlight := 3
			totalMessages := 9
			// The prefetch is at least MaxInFlight, so the broker delivers more messages than are handled
			g.Expect(channel.Qos(totalMessages, 0, false)).To(Succeed())
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName, amqp.WithMaxInFlight(maxInFlight))
			g.Expect(err).NotTo(HaveOccurred())

			var running, highestRunning, done atomic.Int32
			gate := make(chan struct{})
			openGate := closeOnce(t, gate)
			startReceive(t, conn, sub, queue.QueueName, func(*message.Message) error {
				n := running.Add(1)
				for {
					highest := highestRunning.Load()
					if n <= highest || highestRunning.CompareAndSwap(highest, n) {
						break
					}
				}
				<-gate
				running.Add(-1)
				done.Add(1)
				return nil
			})

			for i := 0; i < totalMessages; i++ {
				queue.PublishBytes([]byte(fmt.Sprintf("message %d", i)), nil)
			}

			g.Eventually(running.Load, 5*time.Second, 10*time.Millisecond).Should(BeEquivalentTo(maxInFlight))
			g.Consistently(highestRunning.Load, 300*time.Millisecond, 10*time.Millisecond).Should(BeEquivalentTo(maxInFlight))

			openGate()
			g.Eventually(done.Load, 5*time.Second, 10*time.Millisecond).Should(BeEquivalentTo(totalMessages))
			g.Expect(highestRunning.Load()).To(BeEquivalentTo(maxInFlight))
		})
	})

	t.Run("shutdown", func(t *testing.T) {
		t.Run("should return nil only after the in-flight handlers returned, and ack their messages", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-graceful-shutdown-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			handling := make(chan struct{})
			release := make(chan struct{})
			var handlerReturned atomic.Bool
			handlerCtxErr := make(chan error, 1)
			releaseHandler := closeOnce(t, release)
			run := startReceive(t, conn, sub, queue.QueueName, func(msg *message.Message) error {
				close(handling)
				<-release
				handlerCtxErr <- msg.Context().Err()
				handlerReturned.Store(true)
				return nil
			})

			queue.PublishBytes([]byte("in flight"), nil)
			g.Eventually(handling, 5*time.Second).Should(BeClosed())

			stopped := make(chan error, 1)
			go func() { stopped <- run.Stop() }()

			// Receive waits for the handler, which is not told about the shutdown
			g.Consistently(run.Finished(), 300*time.Millisecond, 20*time.Millisecond).ShouldNot(BeClosed())
			releaseHandler()

			g.Eventually(stopped, 5*time.Second).Should(Receive(BeNil()))
			g.Expect(handlerReturned.Load()).To(BeTrue())
			g.Expect(<-handlerCtxErr).NotTo(HaveOccurred(), "the shutdown must not cancel the context of the message")

			// The message was acked: closing the channel requeues the unacked messages, and there are none
			g.Expect(channel.Close()).To(Succeed())
			ready, _ := queueState(g, conn, queue.QueueName)
			g.Expect(ready).To(Equal(0))
		})

		t.Run("should return nil when the context is cancelled while no message is handled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "test-idle-shutdown-queue")
			sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			run := startReceive(t, conn, sub, queue.QueueName, func(*message.Message) error { return nil })

			g.Expect(run.Stop()).To(Succeed())
			// The consumer is cancelled on the broker
			g.Eventually(func() int {
				_, consumers := queueState(g, conn, queue.QueueName)
				return consumers
			}, 5*time.Second, 20*time.Millisecond).Should(Equal(0))
		})
	})

	t.Run("JSON unmarshalling integration", func(t *testing.T) {
		t.Run("should unmarshal JSON payload to struct", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "json-test-queue")

			type TestData struct {
				Name   string `json:"name"`
				Age    int    `json:"age"`
				Active bool   `json:"active"`
				Scores []int  `json:"scores"`
			}

			expectedData := TestData{
				Name:   "Jane Doe",
				Age:    25,
				Active: true,
				Scores: []int{90, 85, 88},
			}

			jsonData, err := json.Marshal(expectedData)
			g.Expect(err).NotTo(HaveOccurred())

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			receivedData := make(chan TestData, 1)
			router.AddHandler("default").Handle(func(msg *message.Message) error {
				receivedData <- msg.Payload.(TestData)
				return nil
			})

			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(TestData{}))
			startReceiveFunc(t, conn, queue.QueueName, subEngine.Start)

			queue.PublishBytes(jsonData, amqpgo.Table{"content-type": "application/json"})

			g.Eventually(receivedData, 5*time.Second).Should(Receive(Equal(expectedData)))
		})

		t.Run("should handle round-trip JSON marshalling and unmarshalling", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel := newTestChannel(t)
			queue := newTestQueue(t, conn, channel, "json-test-queue")

			type Product struct {
				ID          string   `json:"id"`
				Name        string   `json:"name"`
				Price       float64  `json:"price"`
				Tags        []string `json:"tags"`
				InStock     bool     `json:"in_stock"`
				Description *string  `json:"description,omitempty"`
			}

			description := "A great product"
			originalProduct := Product{
				ID:          "prod-789",
				Name:        "Test Product",
				Price:       99.99,
				Tags:        []string{"electronics", "gadget"},
				InStock:     true,
				Description: &description,
			}

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination("")),
				amqp.ConstantRoutingKey(queue.QueueName),
				amqp.DefaultMessageOptions{
					ContentType:  "application/json",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(context.Background(), originalProduct)
			msg.ID = "round-trip-test"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			receivedProduct := make(chan Product, 1)
			router.AddHandler("default").Handle(func(msg *message.Message) error {
				receivedProduct <- msg.Payload.(Product)
				return nil
			})

			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(Product{}))
			startReceiveFunc(t, conn, queue.QueueName, subEngine.Start)

			g.Eventually(receivedProduct, 5*time.Second).Should(Receive(Equal(originalProduct)))
		})
	})
}
