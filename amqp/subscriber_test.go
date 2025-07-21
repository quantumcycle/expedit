package amqp_test

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestAMQPSubscriber(t *testing.T) {
	t.Run("should return an error if the channel is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)

		_, err := amqp.NewAMQPSubscriber(nil, "test-queue")
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("channel is required"))
	})

	t.Run("should return an error if the queue does not exist", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnectionWithToxiproxy(nil)
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		subscriber, err := amqp.NewAMQPSubscriber(channel, "non-existing-queue")
		g.Expect(err).NotTo(HaveOccurred())

		_, err = subscriber.Subscribe(ctx)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("queue does not exist"))
	})

	t.Run("when using a direct queue", func(t *testing.T) {
		t.Run("should receives all messages sent to the queue", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-direct-queue")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			_, err = channel.QueueDeclarePassive(queue.QueueName, false, false, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred(), "Queue should exist before subscriber creation")

			subscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			msgCount := 0
			ready := make(chan struct{})

			go func() {
				close(ready)

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

			<-ready
			time.Sleep(100 * time.Millisecond)

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("test message %d", i))
				queue.PublishBytes(msgData, nil)
				time.Sleep(5 * time.Millisecond)
			}

			g.Eventually(func() int {
				return msgCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))
		})
	})

	t.Run("when testing network disconnection with toxiproxy", func(t *testing.T) {
		t.Run("should reconnect and receive all the messages", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			toxi, err := setupToxiproxy()
			if err != nil {
				t.Skip("Toxiproxy not available, skipping network disconnection test")
			}

			conn, channel, err := createTestConnectionWithToxiproxy(toxi)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-disconnect-queue")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			msgCount := 0
			asyncCountMessages(&msgCount, msgCh, 10*time.Second)

			expectedMsgCount := 10

			for i := 0; i < expectedMsgCount; i++ {
				if i == 3 {
					fmt.Printf("Simulating network disconnection after message %d...\n", i-1)
					toxi.SimulateDisconnect()

					time.Sleep(1 * time.Second)
					fmt.Printf("Network should be reconnected, continuing to publish...\n")
				}

				queue.PublishBytes([]byte(fmt.Sprintf("test message %d", i)), nil)
				time.Sleep(100 * time.Millisecond)
			}

			g.Eventually(func() int {
				current := msgCount
				fmt.Printf("Current message count: %d/%d\n", current, expectedMsgCount)
				return current
			}, 8*time.Second, 500*time.Millisecond).Should(Equal(expectedMsgCount))
		})
	})

	t.Run("when using a fanout exchange", func(t *testing.T) {
		t.Run("should receive messages published to the fanout exchange", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			exchange := testrabbit.CreateFanoutExchange(channel, "test-fanout-exchange", "queue1", "queue2")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			subscriber1, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			g.Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue2"])
			g.Expect(err).NotTo(HaveOccurred())

			msgCh1, err := subscriber1.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			msgCh2, err := subscriber2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			var msgCount1 int32
			var msgCount2 int32

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

			<-ready
			<-ready

			expectedMsgCount := 5
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("fanout test message %d", i))
				exchange.PublishBytes(msgData, nil)
				time.Sleep(30 * time.Millisecond)
			}

			g.Eventually(func() int {
				return int(atomic.LoadInt32(&msgCount1))
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))

			g.Eventually(func() int {
				return int(atomic.LoadInt32(&msgCount2))
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(expectedMsgCount))
		})

		t.Run("should handle multiple concurrent subscribers on the same fanout queue", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			exchange := testrabbit.CreateFanoutExchange(channel, "test-fanout-exchange", "queue1", "queue2")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			subscriber1, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			g.Expect(err).NotTo(HaveOccurred())
			subscriber2, err := amqp.NewAMQPSubscriber(channel, exchange.LogicalToActual["queue1"])
			g.Expect(err).NotTo(HaveOccurred())

			msgCh1, err := subscriber1.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			msgCh2, err := subscriber2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			var totalMsgCount int32
			var msgCount1 int32
			var msgCount2 int32

			ready := make(chan struct{}, 2)

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

			<-ready
			<-ready
			time.Sleep(500 * time.Millisecond)

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				msgData := []byte(fmt.Sprintf("concurrent fanout test message %d", i))
				exchange.PublishBytes(msgData, nil)
				time.Sleep(30 * time.Millisecond)
			}

			g.Eventually(func() int {
				count := int(atomic.LoadInt32(&totalMsgCount))
				return count
			}, 4*time.Second, 100*time.Millisecond).Should(Equal(expectedMsgCount),
				"Expected %d total messages, got %d (subscriber1: %d, subscriber2: %d)",
				expectedMsgCount, atomic.LoadInt32(&totalMsgCount), atomic.LoadInt32(&msgCount1), atomic.LoadInt32(&msgCount2))
		})
	})

	t.Run("when using a topic exchange", func(t *testing.T) {
		t.Run("should receive messages that match the topic pattern", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			exchange := testrabbit.CreateTopicExchange(channel, "test-topic-exchange",
				"logs.*", "events.#", "alerts.critical")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			logsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["logs.*"])
			g.Expect(err).NotTo(HaveOccurred())
			eventsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["events.#"])
			g.Expect(err).NotTo(HaveOccurred())
			alertsSubscriber, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["alerts.critical"])
			g.Expect(err).NotTo(HaveOccurred())

			logsCh, err := logsSubscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer logsSubscriber.Close()

			eventsCh, err := eventsSubscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer eventsSubscriber.Close()

			alertsCh, err := alertsSubscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer alertsSubscriber.Close()

			logsCount := 0
			eventsCount := 0
			alertsCount := 0

			ready := make(chan struct{}, 3)

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

			<-ready
			<-ready
			<-ready
			time.Sleep(100 * time.Millisecond)

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

			g.Eventually(func() int {
				return logsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(2))

			g.Eventually(func() int {
				return eventsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(2))

			g.Eventually(func() int {
				return alertsCount
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(1))
		})

		t.Run("should handle complex topic patterns with wildcards", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			exchange := testrabbit.CreateTopicExchange(channel, "complex-topic",
				"*.critical", "#.error", "system.#")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			anyCritical, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["*.critical"])
			g.Expect(err).NotTo(HaveOccurred())
			anyError, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["#.error"])
			g.Expect(err).NotTo(HaveOccurred())
			systemAll, err := amqp.NewAMQPSubscriber(channel, exchange.PatternToQueue["system.#"])
			g.Expect(err).NotTo(HaveOccurred())

			criticalCh, err := anyCritical.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer anyCritical.Close()

			errorCh, err := anyError.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer anyError.Close()

			systemCh, err := systemAll.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer systemAll.Close()

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
				time.Sleep(10 * time.Millisecond)
			}

			g.Eventually(func() int {
				return int(atomic.LoadInt32(&criticalCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2))

			g.Eventually(func() int {
				return int(atomic.LoadInt32(&errorCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2))

			g.Eventually(func() int {
				return int(atomic.LoadInt32(&systemCount))
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2))
		})
	})

	t.Run("when using a headers exchange", func(t *testing.T) {
		t.Run("should receive messages that match header bindings", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

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

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			errorCriticalSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["error-critical"])
			g.Expect(err).NotTo(HaveOccurred())
			anyUrgentSub, err := amqp.NewAMQPSubscriber(channel, exchange.HeadersToQueue["any-urgent"])
			g.Expect(err).NotTo(HaveOccurred())

			errorCriticalCh, err := errorCriticalSub.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer errorCriticalSub.Close()

			anyUrgentCh, err := anyUrgentSub.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer anyUrgentSub.Close()

			errorCriticalCount := 0
			anyUrgentCount := 0

			ready := make(chan struct{}, 2)

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

			<-ready
			<-ready
			time.Sleep(100 * time.Millisecond)

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

			g.Eventually(func() int {
				return errorCriticalCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(2))

			g.Eventually(func() int {
				return anyUrgentCount
			}, 2*time.Second, 50*time.Millisecond).Should(Equal(4))
		})
	})

	t.Run("subscriber options", func(t *testing.T) {
		t.Run("should handle WithAutoAck option", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-options-queue")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithAutoAck())
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			queue.PublishBytes([]byte("auto-ack test"), nil)

			select {
			case msg := <-msgCh:
				g.Expect(msg).NotTo(BeNil())
				g.Expect(string(msg.Payload.([]byte))).To(Equal("auto-ack test"))
			case <-ctx.Done():
				g.Fail("Did not receive message within timeout")
			}
		})

		t.Run("should handle WithProcessingTimeout option", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-options-queue")

			defer func() {
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
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var timeoutCalled int32
			subscriber, err := amqp.NewAMQPSubscriber(channel,
				queue.QueueName,
				amqp.WithProcessingTimeout(100*time.Millisecond),
				amqp.WithProcessingTimeoutHandler(func(ctx context.Context, msg *amqpgo.Delivery) {
					atomic.StoreInt32(&timeoutCalled, 1)
				}))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			queue.PublishBytes([]byte("timeout test"), nil)
			time.Sleep(10 * time.Millisecond)

			select {
			case msg := <-msgCh:
				g.Expect(msg).NotTo(BeNil())
				g.Expect(string(msg.Payload.([]byte))).To(Equal("timeout test"))
				time.Sleep(200 * time.Millisecond)
				msg.Ack()
			case <-ctx.Done():
				g.Fail("Did not receive message within timeout")
			}

			g.Eventually(func() bool {
				return atomic.LoadInt32(&timeoutCalled) == 1
			}, 2*time.Second).Should(BeTrue())
		})
	})

	t.Run("JSON unmarshalling integration", func(t *testing.T) {
		t.Run("should unmarshal JSON payload to struct", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "json-test-queue")

			defer func() {
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
			}()

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

			queue.PublishBytes(jsonData, amqpgo.Table{"content-type": "application/json"})

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

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

			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			time.Sleep(100 * time.Millisecond)

			g.Eventually(func() bool {
				return messageReceived
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			g.Expect(receivedData).To(Equal(expectedData))
		})

		t.Run("should handle round-trip JSON marshalling and unmarshalling", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var err error
			conn, channel, err := createTestConnectionWithToxiproxy(nil)
			g.Expect(err).NotTo(HaveOccurred())

			queue := testrabbit.CreateDirectExchangeQueue(channel, "json-test-queue")

			defer func() {
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
			}()

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

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

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

			msg := message.NewMessage(ctx, originalProduct)
			msg.ID = "round-trip-test"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			amqpSubscriber, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
			g.Expect(err).NotTo(HaveOccurred())

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

			go func() {
				err := subEngine.Start(ctx)
				if err != nil {
					fmt.Printf("Subscription engine error: %v\n", err)
				}
			}()

			time.Sleep(100 * time.Millisecond)

			g.Eventually(func() bool {
				return messageReceived
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			g.Expect(receivedProduct).To(Equal(originalProduct))
		})
	})
}