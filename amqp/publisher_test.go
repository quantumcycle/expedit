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
	amqpgo "github.com/rabbitmq/amqp091-go"
)

func expectNbMessages(g Gomega, msg, exchName string, ch <-chan amqpgo.Delivery, nb int32, timeout time.Duration) {
	var allReceived []amqpgo.Delivery
	var count int32

	g.Eventually(func() int32 {
		for {
			select {
			case receivedMsg := <-ch:
				allReceived = append(allReceived, receivedMsg)
				atomic.AddInt32(&count, 1)
			default:
				return atomic.LoadInt32(&count)
			}
		}
	}, timeout, 50*time.Millisecond).Should(Equal(nb),
		fmt.Sprintf("exchange: %s -> %s (received %d messages, expected %d)",
			exchName, msg, len(allReceived), nb))
}

func cleanupResources(exchange interface{ Delete() }, channel *amqp.ReconnectingChannel, conn *amqp.ReconnectingConnection, resourceType string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Warning: Failed to clean up %s resources: %v\n", resourceType, r)
		}
	}()

	if exchange != nil {
		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("Warning: %s cleanup attempt %d failed: %v\n", resourceType, i+1, r)
					}
				}()
				exchange.Delete()
				return
			}()
			if i < maxRetries-1 {
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			}
		}
	}

	time.Sleep(50 * time.Millisecond)

	if channel != nil && !channel.IsClosed() {
		if err := channel.Close(); err != nil {
			fmt.Printf("Warning: Failed to close channel: %v\n", err)
		}
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			fmt.Printf("Warning: Failed to close connection: %v\n", err)
		}
	}
}

func createTestConnection() (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, error) {
	config := amqpgo.Config{
		Vhost:      "/",
		Properties: amqpgo.NewConnectionProperties(),
	}

	maxRetries := 15
	baseDelay := 100 * time.Millisecond
	maxDelay := 5 * time.Second

	var conn *amqp.ReconnectingConnection
	var err error

	for i := 0; i < maxRetries; i++ {
		conn, err = amqp.DialConfig("amqp://guest:guest@localhost:5672/", config)
		if err == nil {
			break
		}

		if i == maxRetries-1 {
			return nil, nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w. Please ensure RabbitMQ is running via 'task amqp:du'", maxRetries, err)
		}

		delay := time.Duration(i+1) * baseDelay
		if delay > maxDelay {
			delay = maxDelay
		}
		fmt.Printf("RabbitMQ connection attempt %d/%d failed, retrying in %v: %v\n", i+1, maxRetries, delay, err)
		time.Sleep(delay)
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("failed to create channel: %w", err)
	}

	return conn, channel, nil
}

func setupDirectRoutingExchange(exchangeName string, routingKeys ...string) (testrabbit.DirectRoutingExchange, func()) {
	conn, channel, err := createTestConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create test connection: %w", err))
	}

	exchange := testrabbit.CreateDirectRoutingExchange(channel, exchangeName, routingKeys...)

	cleanup := func() {
		cleanupResources(exchange, channel, conn, "direct routing exchange")
	}

	return exchange, cleanup
}

func setupFanoutExchange(exchangeName string, queues ...string) (testrabbit.FanoutExchange, func()) {
	conn, channel, err := createTestConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create test connection: %w", err))
	}

	exchange := testrabbit.CreateFanoutExchange(channel, exchangeName, queues...)

	cleanup := func() {
		cleanupResources(exchange, channel, conn, "fanout exchange")
	}

	return exchange, cleanup
}

func setupTopicExchange(exchangeName string, patterns ...string) (testrabbit.TopicExchange, func()) {
	conn, channel, err := createTestConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create test connection: %w", err))
	}

	exchange := testrabbit.CreateTopicExchange(channel, exchangeName, patterns...)

	cleanup := func() {
		cleanupResources(exchange, channel, conn, "topic exchange")
	}

	return exchange, cleanup
}

func setupHeadersExchange(exchangeName string, bindings ...testrabbit.HeaderBinding) (testrabbit.HeadersExchange, func()) {
	conn, channel, err := createTestConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create test connection: %w", err))
	}

	exchange := testrabbit.CreateHeadersExchange(channel, exchangeName, bindings...)

	cleanup := func() {
		cleanupResources(exchange, channel, conn, "headers exchange")
	}

	return exchange, cleanup
}

func setupPublisherConnection() (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, func()) {
	conn, channel, err := createTestConnection()
	if err != nil {
		panic(fmt.Errorf("failed to create publisher connection: %w", err))
	}

	cleanup := func() {
		_ = channel.Close()
		_ = conn.Close()
	}

	return conn, channel, cleanup
}

func TestAMQPPublisher(t *testing.T) {
	t.Run("should return an error if the exchange function is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			nil,
			amqp.ConstantRoutingKey("test-routing"),
			amqp.DefaultMessageOptions{})
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("exchange routing function required"))
	})

	t.Run("should return an error if the routing key function is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			publisher.ConstantDestination("test-exchange"),
			nil,
			amqp.DefaultMessageOptions{})
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("routing key function required"))
	})

	t.Run("should return an error if the default content type is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			publisher.ConstantDestination("test-exchange"),
			amqp.ConstantRoutingKey("test-routing"),
			amqp.DefaultMessageOptions{
				ContentType:  "",
				Priority:     0,
				DeliveryMode: 0,
			})
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("message default content type is required"))
	})

	t.Run("should return an error if the default priority is invalid", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			publisher.ConstantDestination("test-exchange"),
			amqp.ConstantRoutingKey("test-routing"),
			amqp.DefaultMessageOptions{
				ContentType:  "text/plain",
				Priority:     100,
				DeliveryMode: 0,
			})
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("message default priority is required to be between 0 and 9"))
	})

	t.Run("should return an error if the default delivery mode is invalid", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			publisher.ConstantDestination("test-exchange"),
			amqp.ConstantRoutingKey("test-routing"),
			amqp.DefaultMessageOptions{
				ContentType:  "text/plain",
				Priority:     0,
				DeliveryMode: 100,
			})
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("message default delivery mode is required to be either transient or persistent"))
	})

	t.Run("when using a direct routing exchange", func(t *testing.T) {
		t.Run("should use the routing function to determine the target exchange", func(t *testing.T) {
			g := NewGomegaWithT(t)
			exchange, cleanup := setupDirectRoutingExchange("exchange-routing", "routing1", "routing2")
			defer cleanup()

			routingFn := func(msg *message.Message) (string, error) {
				return fmt.Sprintf("%v", msg.Metadata["destination"]), nil
			}

			msgs1 := exchange.Consume("routing1")
			msgs2 := exchange.Consume("routing2")

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				routingFn,
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			msg1 := message.NewMessage(context.Background(), []byte("msg1")).
				WithMetadata("destination", "routing1")
			msg1.ID = "test-msg-1"
			err = pubEngine.Publish(msg1)
			g.Expect(err).NotTo(HaveOccurred())

			msg2 := message.NewMessage(context.Background(), []byte("msg2")).
				WithMetadata("destination", "routing2")
			msg2.ID = "test-msg-2"
			err = pubEngine.Publish(msg2)
			g.Expect(err).NotTo(HaveOccurred())

			expectNbMessages(g, "routing1 messages", exchange.ExchangeName, msgs1, 1, 4*time.Second)
			expectNbMessages(g, "routing2 messages", exchange.ExchangeName, msgs2, 1, 4*time.Second)
		})
	})

	t.Run("when using a fanout exchange", func(t *testing.T) {
		t.Run("should broadcast messages to all queues bound to the fanout exchange", func(t *testing.T) {
			g := NewGomegaWithT(t)
			exchange, cleanup := setupFanoutExchange("fanout-exchange", "queue1", "queue2", "queue3")
			defer cleanup()

			msgs1 := exchange.Consume("queue1")
			msgs2 := exchange.Consume("queue2")
			msgs3 := exchange.Consume("queue3")

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey(""),
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			msg := message.NewMessage(context.Background(), []byte("fanout broadcast"))
			msg.ID = "fanout-test-msg"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			expectNbMessages(g, "queue1 messages", exchange.ExchangeName, msgs1, 1, 2*time.Second)
			expectNbMessages(g, "queue2 messages", exchange.ExchangeName, msgs2, 1, 2*time.Second)
			expectNbMessages(g, "queue3 messages", exchange.ExchangeName, msgs3, 1, 2*time.Second)
		})

		t.Run("should broadcast multiple messages to all queues", func(t *testing.T) {
			g := NewGomegaWithT(t)
			exchange, cleanup := setupFanoutExchange("fanout-exchange", "queue1", "queue2", "queue3")
			defer cleanup()

			msgs1 := exchange.Consume("queue1")
			msgs2 := exchange.Consume("queue2")
			msgs3 := exchange.Consume("queue3")

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey(""),
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			numMessages := 5
			for i := 0; i < numMessages; i++ {
				msg := message.NewMessage(context.Background(), []byte(fmt.Sprintf("fanout message %d", i)))
				msg.ID = fmt.Sprintf("fanout-test-msg-%d", i)
				err = pubEngine.Publish(msg)
				g.Expect(err).NotTo(HaveOccurred())
			}

			expectNbMessages(g, "queue1 messages", exchange.ExchangeName, msgs1, int32(numMessages), 5*time.Second)
			expectNbMessages(g, "queue2 messages", exchange.ExchangeName, msgs2, int32(numMessages), 5*time.Second)
			expectNbMessages(g, "queue3 messages", exchange.ExchangeName, msgs3, int32(numMessages), 5*time.Second)
		})
	})

	t.Run("when using a topic exchange", func(t *testing.T) {
		t.Run("with log and event patterns", func(t *testing.T) {
			t.Run("should route messages based on topic patterns", func(t *testing.T) {
				g := NewGomegaWithT(t)
				exchange, cleanup := setupTopicExchange("topic-exchange", "logs.*", "logs.error", "events.#", "events.user.created")
				defer cleanup()

				logsAll := exchange.Consume("logs.*")
				logsError := exchange.Consume("logs.error")
				eventsAll := exchange.Consume("events.#")
				eventsUserCreated := exchange.Consume("events.user.created")

				routingFn := func(msg *message.Message) (string, error) {
					return fmt.Sprintf("%v", msg.Metadata["routing_key"]), nil
				}

				_, channel, cleanupPublisher := setupPublisherConnection()
				defer cleanupPublisher()

				pub, err := amqp.NewAMQPPublisher(
					channel,
					publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
					routingFn,
					amqp.DefaultMessageOptions{
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					})
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				msg1 := message.NewMessage(context.Background(), []byte("info message")).
					WithMetadata("routing_key", "logs.info")
				msg1.ID = "topic-test-1"
				err = pubEngine.Publish(msg1)
				g.Expect(err).NotTo(HaveOccurred())

				msg2 := message.NewMessage(context.Background(), []byte("error message")).
					WithMetadata("routing_key", "logs.error")
				msg2.ID = "topic-test-2"
				err = pubEngine.Publish(msg2)
				g.Expect(err).NotTo(HaveOccurred())

				msg3 := message.NewMessage(context.Background(), []byte("user created")).
					WithMetadata("routing_key", "events.user.created")
				msg3.ID = "topic-test-3"
				err = pubEngine.Publish(msg3)
				g.Expect(err).NotTo(HaveOccurred())

				msg4 := message.NewMessage(context.Background(), []byte("user deleted")).
					WithMetadata("routing_key", "events.user.deleted")
				msg4.ID = "topic-test-4"
				err = pubEngine.Publish(msg4)
				g.Expect(err).NotTo(HaveOccurred())

				expectNbMessages(g, "logs all messages", exchange.ExchangeName, logsAll, 2, 3*time.Second)
				expectNbMessages(g, "logs error messages", exchange.ExchangeName, logsError, 1, 3*time.Second)
				expectNbMessages(g, "all events messages", exchange.ExchangeName, eventsAll, 2, 3*time.Second)
				expectNbMessages(g, "user creates events messages", exchange.ExchangeName, eventsUserCreated, 1, 3*time.Second)
			})
		})

		t.Run("with wildcard patterns", func(t *testing.T) {
			t.Run("should handle wildcard patterns correctly", func(t *testing.T) {
				g := NewGomegaWithT(t)
				exchange, cleanup := setupTopicExchange("wildcard-topic", "*.urgent", "audit.*", "#", "system.#")
				defer cleanup()

				urgentAny := exchange.Consume("*.urgent")
				auditAny := exchange.Consume("audit.*")
				allMessages := exchange.Consume("#")
				systemAll := exchange.Consume("system.#")

				routingFn := func(msg *message.Message) (string, error) {
					return fmt.Sprintf("%v", msg.Metadata["routing_key"]), nil
				}

				_, channel, cleanupPublisher := setupPublisherConnection()
				defer cleanupPublisher()

				pub, err := amqp.NewAMQPPublisher(
					channel,
					publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
					routingFn,
					amqp.DefaultMessageOptions{
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					})
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				messages := []struct {
					routing string
					content string
				}{
					{"log.urgent", "urgent log"},
					{"email.urgent", "urgent email"},
					{"audit.login", "login audit"},
					{"audit.logout", "logout audit"},
					{"system.health.check", "health check"},
					{"unmatched.message", "should only go to #"},
				}

				for _, msgData := range messages {
					msg := message.NewMessage(context.Background(), []byte(msgData.content)).
						WithMetadata("routing_key", msgData.routing)
					msg.ID = fmt.Sprintf("wildcard-test-%s", msgData.routing)
					err = pubEngine.Publish(msg)
					g.Expect(err).NotTo(HaveOccurred())
					time.Sleep(50 * time.Millisecond)
				}

				expectNbMessages(g, "urgent messages", exchange.ExchangeName, urgentAny, 2, 5*time.Second)
				expectNbMessages(g, "audit messages", exchange.ExchangeName, auditAny, 2, 5*time.Second)
				expectNbMessages(g, "all messages", exchange.ExchangeName, allMessages, 6, 5*time.Second)
				expectNbMessages(g, "system messages", exchange.ExchangeName, systemAll, 1, 5*time.Second)
			})
		})
	})

	t.Run("when using a headers exchange", func(t *testing.T) {
		t.Run("with all and any match bindings", func(t *testing.T) {
			t.Run("should route messages based on header matching with 'all' strategy", func(t *testing.T) {
				g := NewGomegaWithT(t)
				exchange, cleanup := setupHeadersExchange("headers-exchange",
					testrabbit.HeaderBinding{
						BindingKey: "exact-match",
						Headers:    amqpgo.Table{"type": "error", "severity": "high"},
						MatchType:  "all",
					},
					testrabbit.HeaderBinding{
						BindingKey: "any-match",
						Headers:    amqpgo.Table{"category": "logs", "source": "app"},
						MatchType:  "any",
					},
					testrabbit.HeaderBinding{
						BindingKey: "priority-only",
						Headers:    amqpgo.Table{"priority": "urgent"},
						MatchType:  "all",
					})
				defer cleanup()

				exactMatch := exchange.Consume("exact-match")
				anyMatch := exchange.Consume("any-match")
				priorityOnly := exchange.Consume("priority-only")

				_, channel, cleanupPublisher := setupPublisherConnection()
				defer cleanupPublisher()

				pub, err := amqp.NewAMQPPublisher(
					channel,
					publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
					amqp.ConstantRoutingKey(""),
					amqp.DefaultMessageOptions{
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					})
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				msg1 := message.NewMessage(context.Background(), []byte("exact match")).
					WithMetadata("type", "error").
					WithMetadata("severity", "high")
				msg1.ID = "headers-test-1"
				err = pubEngine.Publish(msg1)
				g.Expect(err).NotTo(HaveOccurred())

				msg2 := message.NewMessage(context.Background(), []byte("any match")).
					WithMetadata("category", "logs").
					WithMetadata("level", "info")
				msg2.ID = "headers-test-2"
				err = pubEngine.Publish(msg2)
				g.Expect(err).NotTo(HaveOccurred())

				msg3 := message.NewMessage(context.Background(), []byte("priority urgent")).
					WithMetadata("priority", "urgent")
				msg3.ID = "headers-test-3"
				err = pubEngine.Publish(msg3)
				g.Expect(err).NotTo(HaveOccurred())

				msg4 := message.NewMessage(context.Background(), []byte("multiple matches")).
					WithMetadata("type", "error").
					WithMetadata("severity", "high").
					WithMetadata("priority", "urgent")
				msg4.ID = "headers-test-4"
				err = pubEngine.Publish(msg4)
				g.Expect(err).NotTo(HaveOccurred())

				expectNbMessages(g, "exact match messages", exchange.ExchangeName, exactMatch, 2, 2*time.Second)
				expectNbMessages(g, "any match messages", exchange.ExchangeName, anyMatch, 1, 2*time.Second)
				expectNbMessages(g, "priority only messages", exchange.ExchangeName, priorityOnly, 2, 2*time.Second)
			})
		})

		t.Run("with complex any match patterns", func(t *testing.T) {
			t.Run("should handle 'any' match type correctly", func(t *testing.T) {
				g := NewGomegaWithT(t)
				exchange, cleanup := setupHeadersExchange("any-headers",
					testrabbit.HeaderBinding{
						BindingKey: "multi-any",
						Headers:    amqpgo.Table{"env": "prod", "region": "us-east", "team": "backend"},
						MatchType:  "any",
					},
					testrabbit.HeaderBinding{
						BindingKey: "debug-only",
						Headers:    amqpgo.Table{"debug": "true"},
						MatchType:  "all",
					})
				defer cleanup()

				multiAny := exchange.Consume("multi-any")
				debugOnly := exchange.Consume("debug-only")

				_, channel, cleanupPublisher := setupPublisherConnection()
				defer cleanupPublisher()

				pub, err := amqp.NewAMQPPublisher(
					channel,
					publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
					amqp.ConstantRoutingKey(""),
					amqp.DefaultMessageOptions{
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					})
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				messages := []struct {
					id       string
					content  string
					metadata map[string]interface{}
				}{
					{"any-1", "prod env", map[string]interface{}{"env": "prod"}},
					{"any-2", "us-east region", map[string]interface{}{"region": "us-east"}},
					{"any-3", "backend team", map[string]interface{}{"team": "backend"}},
					{"debug-1", "debug message", map[string]interface{}{"debug": "true"}},
					{"no-match", "no headers match", map[string]interface{}{"unrelated": "value"}},
					{"multi-1", "multiple headers", map[string]interface{}{"env": "prod", "debug": "true"}},
				}

				for _, msgData := range messages {
					msg := message.NewMessage(context.Background(), []byte(msgData.content))
					for k, v := range msgData.metadata {
						msg = msg.WithMetadata(k, v)
					}
					msg.ID = msgData.id
					err = pubEngine.Publish(msg)
					g.Expect(err).NotTo(HaveOccurred())
					time.Sleep(10 * time.Millisecond)
				}

				expectNbMessages(g, "multi any messages", exchange.ExchangeName, multiAny, 4, 10*time.Second)
				expectNbMessages(g, "debug only messages", exchange.ExchangeName, debugOnly, 2, 10*time.Second)
			})
		})
	})

	t.Run("when publishing with JSON marshalling", func(t *testing.T) {
		t.Run("should marshal struct payload to JSON before publishing", func(t *testing.T) {
			g := NewGomegaWithT(t)
			exchange, cleanup := setupDirectRoutingExchange("json-test-exchange", "json-routing")
			defer cleanup()

			type TestData struct {
				Name   string `json:"name"`
				Age    int    `json:"age"`
				Active bool   `json:"active"`
				Scores []int  `json:"scores"`
			}

			testData := TestData{
				Name:   "John Doe",
				Age:    30,
				Active: true,
				Scores: []int{85, 92, 78},
			}

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey("json-routing"),
				amqp.DefaultMessageOptions{
					ContentType:  "application/json",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(context.Background(), testData)
			msg.ID = "json-test-msg"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			msgs := exchange.Consume("json-routing")

			var receivedMsg *amqpgo.Delivery
			g.Eventually(func() bool {
				select {
				case msg := <-msgs:
					d := amqpgo.Delivery(msg)
					receivedMsg = &d
					return true
				default:
					return false
				}
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			g.Expect(receivedMsg).NotTo(BeNil())

			var receivedData TestData
			err = json.Unmarshal(receivedMsg.Body, &receivedData)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(receivedData).To(Equal(testData))
		})

		t.Run("should handle complex nested JSON structures", func(t *testing.T) {
			g := NewGomegaWithT(t)
			exchange, cleanup := setupDirectRoutingExchange("json-test-exchange", "json-routing")
			defer cleanup()

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

			testPerson := Person{
				ID:   "person-123",
				Name: "Alice Smith",
				Address: Address{
					Street:  "123 Main St",
					City:    "New York",
					Country: "USA",
				},
				Metadata: map[string]string{
					"department": "engineering",
					"level":      "senior",
				},
			}

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey("json-routing"),
				amqp.DefaultMessageOptions{
					ContentType:  "application/json",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(context.Background(), testPerson)
			msg.ID = "json-nested-test"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			msgs := exchange.Consume("json-routing")

			var receivedMsg *amqpgo.Delivery
			g.Eventually(func() bool {
				select {
				case msg := <-msgs:
					d := amqpgo.Delivery(msg)
					receivedMsg = &d
					return true
				default:
					return false
				}
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			g.Expect(receivedMsg).NotTo(BeNil())

			var receivedPerson Person
			err = json.Unmarshal(receivedMsg.Body, &receivedPerson)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(receivedPerson).To(Equal(testPerson))
		})
	})
}