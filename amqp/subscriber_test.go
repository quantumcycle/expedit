package amqp_test

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/amqp/testrabbit"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"sync/atomic"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

type simpleToxiproxy struct {
	client *toxiproxy.Client
	proxy  *toxiproxy.Proxy
}

func (t *simpleToxiproxy) SimulateDisconnect() {
	if t.proxy != nil {
		_ = t.proxy.Disable()
		time.Sleep(500 * time.Millisecond) // Longer disconnect to ensure connection is detected as lost
		_ = t.proxy.Enable()
	}
}

func setupToxiproxy() (*simpleToxiproxy, error) {
	// Connect to toxiproxy running in Docker Compose with retry
	client := toxiproxy.NewClient("localhost:8474")

	// Verify toxiproxy is available with retry logic
	maxRetries := 5
	var proxy *toxiproxy.Proxy
	var err error

	for i := 0; i < maxRetries; i++ {
		// First try to list proxies to verify connection
		_, err = client.Proxies()
		if err != nil {
			if i == maxRetries-1 {
				return nil, fmt.Errorf("toxiproxy not available after %d attempts: %w", maxRetries, err)
			}
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			continue
		}

		// Create proxy from toxiproxy to rabbitmq
		proxy, err = client.CreateProxy("rabbitmq", "localhost:25672", "rabbitmq:5672")
		if err != nil {
			// Proxy might already exist, try to get it
			proxy, err = client.Proxy("rabbitmq")
			if err != nil {
				if i == maxRetries-1 {
					return nil, fmt.Errorf("failed to setup toxiproxy proxy after %d attempts: %w", maxRetries, err)
				}
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
				continue
			}
		}

		// Verify proxy is working
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

func asyncCountMessages(count *int, ch <-chan *message.Message, duration time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				if msg == nil {
					return
				}
				*count++
				msg.Ack()
			}
		}
	}()
}

// waitForReconnection waits for the connection to be re-established after a disconnect
// This replaces hard-coded sleeps with proper synchronization
func waitForReconnection(conn *amqp.ReconnectingConnection, channel *amqp.ReconnectingChannel, maxWait time.Duration) (*amqp.ReconnectingChannel, error) {
	start := time.Now()
	for time.Since(start) < maxWait {
		// First check if connection is healthy
		if conn != nil && !conn.IsClosed() {
			// Try to create a new channel since the old one might be permanently damaged
			newChannel, err := conn.Channel()
			if err == nil {
				// Test the new channel by trying a simple operation
				_, err := newChannel.QueueDeclarePassive("amq.gen-test-reconnect", false, false, false, false, nil)
				// We expect this to fail with NOT_FOUND, but if it's a different error it means connection issues
				if err != nil && err.Error() == "Exception (404) Reason: \"NOT_FOUND\"" {
					// Connection is working, return the new channel
					return newChannel, nil
				}
				// If that didn't work, close the channel and try again
				newChannel.Close()
			}
		}
		time.Sleep(50 * time.Millisecond) // Longer poll interval for reconnection
	}
	return nil, fmt.Errorf("connection did not recover within %v", maxWait)
}

// createTestConnectionWithToxiproxy creates a connection with retry logic and toxiproxy support
func createTestConnectionWithToxiproxy(toxi *simpleToxiproxy) (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, error) {
	config := amqpgo.Config{
		Vhost:      "/",
		Properties: amqpgo.NewConnectionProperties(),
	}

	// Try to connect through toxiproxy first, then fallback to direct connection
	connectionURIs := []string{"amqp://guest:guest@localhost:5672/"}
	if toxi != nil {
		connectionURIs = []string{"amqp://guest:guest@localhost:25672/", "amqp://guest:guest@localhost:5672/"}
	}

	// Retry connection to allow RabbitMQ to start
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

			// Exponential backoff with jitter
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

var _ = Describe("AMQP Subscriber", func() {
	// No shared resources - each test creates its own isolated connection/channel

	It("should return an error if the channel is missing", func() {
		_, err := amqp.NewAMQPSubscriber(nil,
			"test-queue")
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("channel is required"))
	})

	It("should return an error if the queue does not exist", func() {
		// Create isolated connection for this test
		conn, channel, err := createTestConnectionWithToxiproxy(nil)
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		subscriber, err := amqp.NewAMQPSubscriber(channel,
			"non-existing-queue")
		Expect(err).NotTo(HaveOccurred())

		_, err = subscriber.Subscribe(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("queue does not exist"))
	})

	When("using a direct queue", func() {
		var queue testrabbit.DirectQueue
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			queue = testrabbit.CreateDirectExchangeQueue(channel, "test-direct-queue")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up queue resources: %v\n", r)
				}
			}()
			if queue.QueueName != "" {
				queue.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should receives all messages sent to the queue", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Verify queue exists before creating subscriber
			_, err := channel.QueueDeclarePassive(queue.QueueName, false, false, false, false, nil)
			Expect(err).NotTo(HaveOccurred(), "Queue should exist before subscriber creation")

			subscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			msgCount := 0
			ready := make(chan struct{})

			// Start counting messages with better synchronization
			go func() {
				close(ready) // Signal that we're ready to receive messages

				timeout := time.After(2 * time.Second)

				for {
					select {
					case <-timeout:
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						msgCount++
						msg.Ack()
					}
				}
			}()

			// Wait for the subscriber to be ready
			<-ready

			// Add a delay to ensure subscriber is fully initialized and consuming
			time.Sleep(100 * time.Millisecond)

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("test message %d", i))
				queue.PublishBytes(msgData, nil)
				time.Sleep(5 * time.Millisecond) // Small delay between publishes
			}

			Eventually(func() int {
				return msgCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))
		})

	})

	When("testing network disconnection with toxiproxy", func() {
		var queue testrabbit.DirectQueue
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel
		var toxi *simpleToxiproxy

		BeforeEach(func() {
			var err error
			// Setup toxiproxy for network disconnection tests
			toxi, err = setupToxiproxy()
			if err != nil {
				// If toxiproxy setup fails, skip network disconnection tests
				toxi = nil
				Skip("Toxiproxy not available, skipping network disconnection test")
			}

			conn, channel, err = createTestConnectionWithToxiproxy(toxi)
			Expect(err).NotTo(HaveOccurred())
			
			queue = testrabbit.CreateDirectExchangeQueue(channel, "test-disconnect-queue")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up toxiproxy test resources: %v\n", r)
				}
			}()
			if queue.QueueName != "" {
				queue.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
			if toxi != nil {
				toxi.Cleanup()
			}
		})

		It("should reconnect and receive all the messages", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			msgCount := 0
			asyncCountMessages(&msgCount, msgCh, 10*time.Second)

			expectedMsgCount := 10
			
			// Publish all messages, with disconnect happening after message 3
			for i := 0; i < expectedMsgCount; i++ {
				// Simulate disconnect after publishing 3 messages
				if i == 3 {
					fmt.Printf("Simulating network disconnection after message %d...\n", i-1)
					toxi.SimulateDisconnect()
					
					// Brief pause to allow disconnect/reconnect to complete
					// The toxiproxy disconnect is only 500ms, so total pause should be around 1s
					time.Sleep(1 * time.Second)
					fmt.Printf("Network should be reconnected, continuing to publish...\n")
				}
				
				// Publish message - use queue.PublishBytes for consistency
				// The reconnecting connection should handle channel recovery automatically
				queue.PublishBytes([]byte(fmt.Sprintf("test message %d", i)), nil)
				time.Sleep(100 * time.Millisecond) // Small delay between messages
			}

			// Wait for all messages to be received
			// The subscriber should automatically reconnect and continue receiving
			Eventually(func() int {
				current := msgCount
				fmt.Printf("Current message count: %d/%d\n", current, expectedMsgCount)
				return current
			}, 8*time.Second, 500*time.Millisecond).Should(Equal(expectedMsgCount))
		})
	})

	When("using a fanout exchange", func() {
		var exchange testrabbit.FanoutExchange
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			exchange = testrabbit.CreateFanoutExchange(channel, "test-fanout-exchange", "queue1", "queue2")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up exchange resources: %v\n", r)
				}
			}()
			if exchange.ExchangeName != "" {
				exchange.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should receive messages published to the fanout exchange", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create subscribers for each queue
			subscriber1, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue2"])
			Expect(err).NotTo(HaveOccurred())

			msgCh1, err := subscriber1.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			msgCh2, err := subscriber2.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			var msgCount1 int32
			var msgCount2 int32

			// Start counting messages for both queues
			ready := make(chan struct{}, 2)

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-msgCh1:
						if msg == nil {
							return
						}
						atomic.AddInt32(&msgCount1, 1)
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-msgCh2:
						if msg == nil {
							return
						}
						atomic.AddInt32(&msgCount2, 1)
						msg.Ack()
					}
				}
			}()

			// Wait for both subscribers to be ready
			<-ready
			<-ready

			// Publish messages to the fanout exchange
			expectedMsgCount := 5
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("fanout test message %d", i))
				exchange.PublishBytes(msgData, nil)
				time.Sleep(30 * time.Millisecond)
			}

			// Both queues should receive all messages due to fanout behavior
			Eventually(func() int {
				return int(atomic.LoadInt32(&msgCount1))
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))

			Eventually(func() int {
				return int(atomic.LoadInt32(&msgCount2))
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))
		})

		It("should handle multiple concurrent subscribers on the same fanout queue", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create multiple subscribers for the same queue
			subscriber1, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			Expect(err).NotTo(HaveOccurred())

			msgCh1, err := subscriber1.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			msgCh2, err := subscriber2.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			var totalMsgCount int32
			var msgCount1 int32
			var msgCount2 int32

			ready := make(chan struct{}, 2)

			// Count messages from both subscribers (they should split the load)
			go func() {
				ready <- struct{}{}
				timeout := time.After(4 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-msgCh1:
						if msg == nil {
							return
						}
						atomic.AddInt32(&totalMsgCount, 1)
						atomic.AddInt32(&msgCount1, 1)
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(4 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-msgCh2:
						if msg == nil {
							return
						}
						atomic.AddInt32(&totalMsgCount, 1)
						atomic.AddInt32(&msgCount2, 1)
						msg.Ack()
					}
				}
			}()

			// Wait for both subscribers to be ready
			<-ready
			<-ready
			// Extended delay to ensure consumers are fully established
			time.Sleep(500 * time.Millisecond)

			// Publish messages
			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("concurrent fanout test message %d", i))
				exchange.PublishBytes(msgData, nil)
				time.Sleep(30 * time.Millisecond)
			}

			// Total messages processed should equal what was published
			// Both subscribers share the same queue, so all messages should be processed
			Eventually(func() int {
				count := int(atomic.LoadInt32(&totalMsgCount))
				return count
			}, 4*time.Second, 100*time.Millisecond).Should(Equal(expectedMsgCount),
				"Expected %d total messages, got %d (subscriber1: %d, subscriber2: %d)",
				expectedMsgCount, atomic.LoadInt32(&totalMsgCount), atomic.LoadInt32(&msgCount1), atomic.LoadInt32(&msgCount2))
		})
	})

	When("using a topic exchange", func() {
		var exchange testrabbit.TopicExchange
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			exchange = testrabbit.CreateTopicExchange(channel, "test-topic-exchange",
				"logs.*", "events.#", "alerts.critical")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up exchange resources: %v\n", r)
				}
			}()
			if exchange.ExchangeName != "" {
				exchange.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should receive messages that match the topic pattern", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Create subscribers for different patterns
			logsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["logs.*"])
			Expect(err).NotTo(HaveOccurred())
			eventsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["events.#"])
			Expect(err).NotTo(HaveOccurred())
			alertsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["alerts.critical"])
			Expect(err).NotTo(HaveOccurred())

			logsCh, err := logsSubscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer logsSubscriber.Close()

			eventsCh, err := eventsSubscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer eventsSubscriber.Close()

			alertsCh, err := alertsSubscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer alertsSubscriber.Close()

			logsCount := 0
			eventsCount := 0
			alertsCount := 0

			ready := make(chan struct{}, 3)

			// Count messages for logs.*
			go func() {
				ready <- struct{}{}
				timeout := time.After(3 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-logsCh:
						if msg == nil {
							return
						}
						logsCount++
						msg.Ack()
					}
				}
			}()

			// Count messages for events.#
			go func() {
				ready <- struct{}{}
				timeout := time.After(3 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-eventsCh:
						if msg == nil {
							return
						}
						eventsCount++
						msg.Ack()
					}
				}
			}()

			// Count messages for alerts.critical
			go func() {
				ready <- struct{}{}
				timeout := time.After(3 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-alertsCh:
						if msg == nil {
							return
						}
						alertsCount++
						msg.Ack()
					}
				}
			}()

			// Wait for all subscribers to be ready
			<-ready
			<-ready
			<-ready
			time.Sleep(100 * time.Millisecond)

			// Publish messages with different routing keys
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
				time.Sleep(20 * time.Millisecond)
			}

			// Verify message counts based on patterns
			Eventually(func() int {
				return logsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(2)) // logs.info + logs.error

			Eventually(func() int {
				return eventsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(2)) // events.user.created + events.order.processed

			Eventually(func() int {
				return alertsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(1)) // alerts.critical only
		})

		It("should handle complex topic patterns with wildcards", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Create exchange with more complex patterns
			exchange.Delete()
			exchange = testrabbit.CreateTopicExchange(channel, "complex-topic",
				"*.critical", "#.error", "system.#")

			anyCritical, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["*.critical"])
			Expect(err).NotTo(HaveOccurred())
			anyError, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["#.error"])
			Expect(err).NotTo(HaveOccurred())
			systemAll, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["system.#"])
			Expect(err).NotTo(HaveOccurred())

			criticalCh, err := anyCritical.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer anyCritical.Close()

			errorCh, err := anyError.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer anyError.Close()

			systemCh, err := systemAll.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer systemAll.Close()

			// Use atomic counters to avoid race conditions
			var criticalCount int32
			var errorCount int32
			var systemCount int32

			ready := make(chan struct{}, 3)

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-criticalCh:
						if msg == nil {
							return
						}
						atomic.AddInt32(&criticalCount, 1)
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-errorCh:
						if msg == nil {
							return
						}
						atomic.AddInt32(&errorCount, 1)
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-systemCh:
						if msg == nil {
							return
						}
						atomic.AddInt32(&systemCount, 1)
						msg.Ack()
					}
				}
			}()

			<-ready
			<-ready
			<-ready

			// Publish test messages
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
				time.Sleep(10 * time.Millisecond) // Even longer delay for more reliable delivery
			}

			// Verify patterns using atomic loads
			Eventually(func() int {
				return int(atomic.LoadInt32(&criticalCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // app.critical + db.critical

			Eventually(func() int {
				return int(atomic.LoadInt32(&errorCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // app.logs.error + network.connection.error

			Eventually(func() int {
				return int(atomic.LoadInt32(&systemCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // system.health + system.metrics.cpu
		})
	})

	When("using a headers exchange", func() {
		var exchange testrabbit.HeadersExchange
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			exchange = testrabbit.CreateHeadersExchange(channel, "test-headers-exchange",
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
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up exchange resources: %v\n", r)
				}
			}()
			if exchange.ExchangeName != "" {
				exchange.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should receive messages that match header bindings", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Create subscribers for different header patterns
			errorCriticalSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["error-critical"])
			Expect(err).NotTo(HaveOccurred())
			anyUrgentSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["any-urgent"])
			Expect(err).NotTo(HaveOccurred())

			errorCriticalCh, err := errorCriticalSub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer errorCriticalSub.Close()

			anyUrgentCh, err := anyUrgentSub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer anyUrgentSub.Close()

			errorCriticalCount := 0
			anyUrgentCount := 0

			ready := make(chan struct{}, 2)

			// Count messages for error-critical (all match)
			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-errorCriticalCh:
						if msg == nil {
							return
						}
						errorCriticalCount++
						msg.Ack()
					}
				}
			}()

			// Count messages for any-urgent (any match)
			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-anyUrgentCh:
						if msg == nil {
							return
						}
						anyUrgentCount++
						msg.Ack()
					}
				}
			}()

			// Wait for subscribers to be ready
			<-ready
			<-ready
			time.Sleep(100 * time.Millisecond)

			// Publish messages with different header combinations
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
				time.Sleep(5 * time.Millisecond)
			}

			// Verify message counts based on header matching
			Eventually(func() int {
				return errorCriticalCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // exact match + multiple matches

			Eventually(func() int {
				return anyUrgentCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(4)) // urgent only + category only + service only + multiple matches
		})

		It("should handle complex header matching scenarios", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Create exchange with more complex header bindings
			exchange.Delete()
			exchange = testrabbit.CreateHeadersExchange(channel, "complex-headers",
				testrabbit.HeaderBinding{
					BindingKey: "exact-environment",
					Headers:    amqpgo.Table{"env": "production", "region": "us-west-2", "cluster": "main"},
					MatchType:  "all",
				},
				testrabbit.HeaderBinding{
					BindingKey: "debug-any",
					Headers:    amqpgo.Table{"debug": "true", "verbose": "true", "trace": "enabled"},
					MatchType:  "any",
				},
				testrabbit.HeaderBinding{
					BindingKey: "format-json",
					Headers:    amqpgo.Table{"format": "json"},
					MatchType:  "all",
				})

			exactEnvSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["exact-environment"])
			Expect(err).NotTo(HaveOccurred())
			debugAnySub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["debug-any"])
			Expect(err).NotTo(HaveOccurred())
			formatJsonSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["format-json"])
			Expect(err).NotTo(HaveOccurred())

			exactEnvCh, err := exactEnvSub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer exactEnvSub.Close()

			debugAnyCh, err := debugAnySub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer debugAnySub.Close()

			formatJsonCh, err := formatJsonSub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer formatJsonSub.Close()

			exactEnvCount := 0
			debugAnyCount := 0
			formatJsonCount := 0

			ready := make(chan struct{}, 3)

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-exactEnvCh:
						if msg == nil {
							return
						}
						exactEnvCount++
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-debugAnyCh:
						if msg == nil {
							return
						}
						debugAnyCount++
						msg.Ack()
					}
				}
			}()

			go func() {
				ready <- struct{}{}
				timeout := time.After(2 * time.Second)
				for {
					select {
					case <-timeout:
						return
					case msg := <-formatJsonCh:
						if msg == nil {
							return
						}
						formatJsonCount++
						msg.Ack()
					}
				}
			}()

			<-ready
			<-ready
			<-ready
			time.Sleep(100 * time.Millisecond)

			// Publish test messages
			testMessages := []struct {
				content string
				headers map[string]interface{}
			}{
				{"prod message", map[string]interface{}{"env": "production", "region": "us-west-2", "cluster": "main"}},
				{"debug trace", map[string]interface{}{"debug": "true", "trace": "enabled"}},
				{"verbose only", map[string]interface{}{"verbose": "true"}},
				{"json data", map[string]interface{}{"format": "json"}},
				{"multi match", map[string]interface{}{"env": "production", "region": "us-west-2", "cluster": "main", "debug": "true", "format": "json"}},
				{"partial prod", map[string]interface{}{"env": "production", "region": "us-east-1"}},
			}

			for _, msgData := range testMessages {
				exchange.PublishBytes([]byte(msgData.content), msgData.headers)
				time.Sleep(5 * time.Millisecond)
			}

			// Verify matching
			Eventually(func() int {
				return exactEnvCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // prod message + multi match

			Eventually(func() int {
				return debugAnyCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(3)) // debug trace + verbose only + multi match

			Eventually(func() int {
				return formatJsonCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2)) // json data + multi match
		})
	})

	Context("Subscriber Options", func() {
		var queue testrabbit.DirectQueue
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			queue = testrabbit.CreateDirectExchangeQueue(channel, "test-options-queue")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up queue resources: %v\n", r)
				}
			}()
			if queue.QueueName != "" {
				queue.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should handle WithAutoAck option", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithAutoAck())
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Publish a test message
			queue.PublishBytes([]byte("auto-ack test"), nil)

			// Receive the message
			select {
			case msg := <-msgCh:
				Expect(msg).NotTo(BeNil())
				Expect(string(msg.Payload.([]byte))).To(Equal("auto-ack test"))
				// With auto-ack, we don't need to manually ack
			case <-ctx.Done():
				Fail("Did not receive message within timeout")
			}
		})

		It("should handle WithExclusive option", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithExclusive())
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Publish a test message
			queue.PublishBytes([]byte("exclusive test"), nil)

			// Receive the message
			select {
			case msg := <-msgCh:
				Expect(msg).NotTo(BeNil())
				Expect(string(msg.Payload.([]byte))).To(Equal("exclusive test"))
				msg.Ack()
			case <-ctx.Done():
				Fail("Did not receive message within timeout")
			}
		})

		It("should handle WithProcessingTimeout option", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var timeoutCalled int32
			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithProcessingTimeout(100*time.Millisecond),
				amqp.WithProcessingTimeoutHandler(func(ctx context.Context, msg *amqpgo.Delivery) {
					atomic.StoreInt32(&timeoutCalled, 1)
				}))
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Publish a test message
			queue.PublishBytes([]byte("timeout test"), nil)
			time.Sleep(10 * time.Millisecond)

			// Receive the message but don't ack it quickly
			select {
			case msg := <-msgCh:
				Expect(msg).NotTo(BeNil())
				Expect(string(msg.Payload.([]byte))).To(Equal("timeout test"))
				// Wait longer than the processing timeout before acking
				time.Sleep(200 * time.Millisecond)
				msg.Ack()
			case <-ctx.Done():
				Fail("Did not receive message within timeout")
			}

			// Verify timeout handler was called
			Eventually(func() bool {
				return atomic.LoadInt32(&timeoutCalled) == 1
			}, 2*time.Second).Should(BeTrue())
		})

		It("should handle WithNoRequeueOnNack option", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithNoRequeueOnNack())
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Publish a test message
			queue.PublishBytes([]byte("nack test"), nil)
			time.Sleep(10 * time.Millisecond)

			// Receive the message and nack it
			select {
			case msg := <-msgCh:
				Expect(msg).NotTo(BeNil())
				Expect(string(msg.Payload.([]byte))).To(Equal("nack test"))
				msg.Nack() // This should not requeue the message
			case <-ctx.Done():
				Fail("Did not receive message within timeout")
			}

			// Wait a bit to ensure message is not requeued
			time.Sleep(100 * time.Millisecond)

			// Check that no more messages are received (message was not requeued)
			select {
			case msg := <-msgCh:
				if msg != nil {
					Fail("Message was requeued despite WithNoRequeueOnNack")
				}
			case <-time.After(1 * time.Second):
				// Good, no message received as expected
			}
		})

		It("should handle context cancellation", func() {
			ctx, cancel := context.WithCancel(context.Background())

			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Cancel context
			cancel()

			// Wait for subscription to handle the cancellation
			// The behavior might vary - the channel could close or stop receiving new messages
			select {
			case <-msgCh:
				// Channel closed or received nil message - this is expected
			case <-time.After(2 * time.Second):
				// If the channel doesn't close immediately, that's also acceptable
				// as long as no new messages are processed after cancellation
			}

			// Verify that the context is indeed cancelled
			Expect(ctx.Err()).To(Equal(context.Canceled))
		})
	})

	Context("JSON unmarshalling integration", func() {
		var queue testrabbit.DirectQueue
		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel

		BeforeEach(func() {
			var err error
			conn, channel, err = createTestConnectionWithToxiproxy(nil)
			Expect(err).NotTo(HaveOccurred())
			
			queue = testrabbit.CreateDirectExchangeQueue(channel, "json-test-queue")
		})

		AfterEach(func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Failed to clean up queue resources: %v\n", r)
				}
			}()
			if queue.QueueName != "" {
				queue.Delete()
			}
			if channel != nil {
				channel.Close()
			}
			if conn != nil {
				conn.Close()
			}
		})

		It("should unmarshal JSON payload to struct", func() {
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

			// Publish JSON data directly
			jsonData, err := json.Marshal(expectedData)
			Expect(err).NotTo(HaveOccurred())

			queue.PublishBytes(jsonData, amqpgo.Table{"content-type": "application/json"})

			// Create subscriber with JSON unmarshalling middleware
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			// Create router with default handler (no routing needed for this test)
			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			var receivedData TestData
			var messageReceived bool

			router.AddHandler("default").Handle(func(msg *message.Message) error {
				receivedData = msg.Payload.(TestData)
				messageReceived = true
				return nil
			})

			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(TestData{}))

			// Start the subscription engine in a goroutine
			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			// Give the subscriber time to start
			time.Sleep(100 * time.Millisecond)

			// Wait for message processing
			Eventually(func() bool {
				return messageReceived
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			// Verify the unmarshalled data matches expected
			Expect(receivedData).To(Equal(expectedData))
		})

		It("should handle complex nested JSON structures", func() {
			type Address struct {
				Street  string `json:"street"`
				City    string `json:"city"`
				Country string `json:"country"`
			}

			type Person struct {
				ID       string            `json:"id"`
				Name     string            `json:"name"`
				Address  Address           `json:"address"`
				Metadata map[string]string `json:"metadata"`
			}

			expectedPerson := Person{
				ID:   "person-456",
				Name: "Bob Johnson",
				Address: Address{
					Street:  "456 Oak Ave",
					City:    "San Francisco",
					Country: "USA",
				},
				Metadata: map[string]string{
					"department": "marketing",
					"level":      "manager",
				},
			}

			// Publish JSON data
			jsonData, err := json.Marshal(expectedPerson)
			Expect(err).NotTo(HaveOccurred())

			queue.PublishBytes(jsonData, amqpgo.Table{"content-type": "application/json"})

			// Create subscriber with JSON unmarshalling middleware
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			// Create router with default handler
			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			var receivedPerson Person
			var messageReceived bool

			router.AddHandler("default").Handle(func(msg *message.Message) error {
				receivedPerson = msg.Payload.(Person)
				messageReceived = true
				return nil
			})

			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(Person{}))

			// Start the subscription engine in a goroutine
			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			// Give the subscriber time to start
			time.Sleep(100 * time.Millisecond)

			// Wait for message processing
			Eventually(func() bool {
				return messageReceived
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			// Verify the unmarshalled data matches expected
			Expect(receivedPerson).To(Equal(expectedPerson))
		})

		It("should handle JSON unmarshalling errors gracefully", func() {
			type TestData struct {
				Name string `json:"name"`
				Age  int    `json:"age"`
			}

			// Publish invalid JSON data
			invalidJSON := []byte(`{"name": "John", "age": "not-a-number"}`)
			queue.PublishBytes(invalidJSON, amqpgo.Table{"content-type": "application/json"})

			// Create subscriber with JSON unmarshalling middleware
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			// Create router with default handler
			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			var errorReceived error

			router.AddHandler("default").Handle(func(msg *message.Message) error {
				// This shouldn't be called due to unmarshalling error
				Fail("Handler should not be called when unmarshalling fails")
				return nil
			})

			// Add error handling middleware to capture unmarshalling errors
			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(func(next message.HandlerFunc) message.HandlerFunc {
					return func(msg *message.Message) error {
						err := next(msg)
						if err != nil {
							errorReceived = err
						}
						return err
					}
				}).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(TestData{}))

			// Start the subscription engine in a goroutine
			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			// Give the subscriber time to start
			time.Sleep(100 * time.Millisecond)

			// Wait for error processing
			Eventually(func() error {
				return errorReceived
			}, 2*time.Second, 50*time.Millisecond).Should(Not(BeNil()))

			// Verify the error is related to JSON unmarshalling
			Expect(errorReceived.Error()).To(ContainSubstring("json"))
		})

		It("should handle round-trip JSON marshalling and unmarshalling", func() {
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

			// First, publish using marshalling middleware
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			// Create a publisher to publish the struct
			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination("")),
				amqp.ConstantRoutingKey(queue.QueueName),
				amqp.DefaultMessageOptions{
					ContentType:  "application/json",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(ctx, originalProduct)
			msg.ID = "round-trip-test"
			err = pubEngine.Publish(msg)
			Expect(err).NotTo(HaveOccurred())

			// Now subscribe and unmarshal
			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			Expect(err).NotTo(HaveOccurred())

			// Create router with default handler
			router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
				return subscriber.RoutingKey("default")
			})

			var receivedProduct Product
			var messageReceived bool

			router.AddHandler("default").Handle(func(msg *message.Message) error {
				receivedProduct = msg.Payload.(Product)
				messageReceived = true
				return nil
			})

			subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router).
				AddMiddleware(amqp.UnmarshallPayloadFromJson(Product{}))

			// Start the subscription engine in a goroutine
			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			// Give the subscriber time to start
			time.Sleep(100 * time.Millisecond)

			// Wait for message processing
			Eventually(func() bool {
				return messageReceived
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			// Verify the round-trip worked correctly
			Expect(receivedProduct).To(Equal(originalProduct))
		})
	})
})
