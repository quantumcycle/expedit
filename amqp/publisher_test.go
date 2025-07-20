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
	amqpgo "github.com/rabbitmq/amqp091-go"
	"sync/atomic"
	"time"
)

func ExpectNbMessages[T any](msg, exchName string, ch <-chan T, nb int32, timeout time.Duration) {
	var allReceived []T
	var count int32

	// Use Eventually to check the count, but accumulate messages properly
	Eventually(func() int32 {
		// Only drain new messages that have arrived since last check
		for {
			select {
			case receivedMsg := <-ch:
				allReceived = append(allReceived, receivedMsg)
				atomic.AddInt32(&count, 1)
			default:
				// No more messages available right now, return current count
				return atomic.LoadInt32(&count)
			}
		}
	}, timeout, 50*time.Millisecond).Should(Equal(nb),
		fmt.Sprintf("exchange: %s -> %s (received %d messages, expected %d)",
			exchName, msg, len(allReceived), nb))
}

// cleanupResources safely cleans up AMQP resources with retry logic
func cleanupResources(exchange interface{ Delete() }, channel *amqp.ReconnectingChannel, conn *amqp.ReconnectingConnection, resourceType string) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Warning: Failed to clean up %s resources: %v\n", resourceType, r)
		}
	}()

	// Clean up exchange/queue first, with retry logic for failed deletions
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
				return // Success
			}()
			if i < maxRetries-1 {
				time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
			}
		}
	}

	// Small delay to ensure cleanup operations complete
	time.Sleep(50 * time.Millisecond)

	// Close channel and connection
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

// createTestConnection creates a fresh AMQP connection with retry logic
func createTestConnection() (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, error) {
	config := amqpgo.Config{
		Vhost:      "/",
		Properties: amqpgo.NewConnectionProperties(),
	}

	// Exponential backoff retry connection to allow RabbitMQ to start
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

		// Exponential backoff with jitter
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

// setupDirectRoutingExchange creates a fresh connection, channel, and direct routing exchange
// Returns the exchange, and a cleanup function that should be called with defer
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

// setupFanoutExchange creates a fresh connection, channel, and fanout exchange
// Returns the exchange, and a cleanup function that should be called with defer
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

// setupTopicExchange creates a fresh connection, channel, and topic exchange
// Returns the exchange, and a cleanup function that should be called with defer
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

// setupHeadersExchange creates a fresh connection, channel, and headers exchange
// Returns the exchange, and a cleanup function that should be called with defer
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

// setupPublisherConnection creates a fresh connection and channel for publisher use
// Returns the connection, channel, and a cleanup function that should be called with defer
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

var _ = Describe("AMQP Publisher", func() {
	// No shared resources - each test creates its own isolated connection/channel

	It("should return an error if the exchange function is missing", func() {
		// Create isolated connection for this test
		conn, channel, err := createTestConnection()
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			nil,
			amqp.ConstantRoutingKey("test-routing"),
			amqp.DefaultMessageOptions{})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("exchange routing function required"))
	})

	It("should return an error if the routing key function is missing", func() {
		// Create isolated connection for this test
		conn, channel, err := createTestConnection()
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			channel.Close()
			conn.Close()
		}()

		_, err = amqp.NewAMQPPublisher(
			channel,
			publisher.ConstantDestination("test-exchange"),
			nil,
			amqp.DefaultMessageOptions{})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("routing key function required"))
	})

	It("should return an error if the default content type is missing", func() {
		conn, channel, err := createTestConnection()
		Expect(err).NotTo(HaveOccurred())
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
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("message default content type is required"))
	})

	It("should return an error if the default priority is invalid", func() {
		conn, channel, err := createTestConnection()
		Expect(err).NotTo(HaveOccurred())
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
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("message default priority is required to be between 0 and 9"))
	})

	It("should return an error if the default delivery mode is invalid", func() {
		conn, channel, err := createTestConnection()
		Expect(err).NotTo(HaveOccurred())
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
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("message default delivery mode is required to be either transient or persistent"))
	})

	When("using a direct routing exchange", func() {

		It("should use the routing function to determine the target exchange", func() {
			exchange, cleanup := setupDirectRoutingExchange("exchange-routing", "routing1", "routing2")
			defer cleanup()

			//ctx := context.Background()
			routingFn := func(msg *message.Message) (string, error) {
				return fmt.Sprintf("%v", msg.Metadata["destination"]), nil
			}

			msgs1 := exchange.Consume("routing1")
			msgs2 := exchange.Consume("routing2")

			// Create a new connection for the publisher to avoid conflicts with the exchange setup
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
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			msg1 := message.NewMessage(context.Background(), []byte("msg1")).
				WithMetadata("destination", "routing1")
			msg1.ID = "test-msg-1"
			err = pubEngine.Publish(msg1)
			Expect(err).NotTo(HaveOccurred())

			msg2 := message.NewMessage(context.Background(), []byte("msg2")).
				WithMetadata("destination", "routing2")
			msg2.ID = "test-msg-2"
			err = pubEngine.Publish(msg2)
			Expect(err).NotTo(HaveOccurred())

			ExpectNbMessages("routing1 messages", exchange.ExchangeName, msgs1, 1, 4*time.Second)
			ExpectNbMessages("routing2 messages", exchange.ExchangeName, msgs2, 1, 4*time.Second)
		})

	})

	When("using a fanout exchange", func() {

		It("should broadcast messages to all queues bound to the fanout exchange", func() {
			exchange, cleanup := setupFanoutExchange("fanout-exchange", "queue1", "queue2", "queue3")
			defer cleanup()

			// Set up consumers for all queues
			msgs1 := exchange.Consume("queue1")
			msgs2 := exchange.Consume("queue2")
			msgs3 := exchange.Consume("queue3")

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey(""), // Fanout ignores routing key
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			// Publish a single message
			msg := message.NewMessage(context.Background(), []byte("fanout broadcast"))
			msg.ID = "fanout-test-msg"
			err = pubEngine.Publish(msg)
			Expect(err).NotTo(HaveOccurred())

			// Verify all queues receive the message
			ExpectNbMessages("queue1 messages", exchange.ExchangeName, msgs1, 1, 2*time.Second)
			ExpectNbMessages("queue2 messages", exchange.ExchangeName, msgs2, 1, 2*time.Second)
			ExpectNbMessages("queue3 messages", exchange.ExchangeName, msgs3, 1, 2*time.Second)
		})

		It("should broadcast multiple messages to all queues", func() {
			exchange, cleanup := setupFanoutExchange("fanout-exchange", "queue1", "queue2", "queue3")
			defer cleanup()

			// Set up consumers for all queues
			msgs1 := exchange.Consume("queue1")
			msgs2 := exchange.Consume("queue2")
			msgs3 := exchange.Consume("queue3")

			_, channel, cleanupPublisher := setupPublisherConnection()
			defer cleanupPublisher()

			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
				amqp.ConstantRoutingKey(""), // Fanout ignores routing key
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Persistent,
				})
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			// Publish multiple messages
			numMessages := 5
			for i := 0; i < numMessages; i++ {
				msg := message.NewMessage(context.Background(), []byte(fmt.Sprintf("fanout message %d", i)))
				msg.ID = fmt.Sprintf("fanout-test-msg-%d", i)
				err = pubEngine.Publish(msg)
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify all queues receive all messages
			ExpectNbMessages("queue1 messages", exchange.ExchangeName, msgs1, int32(numMessages), 5*time.Second)
			ExpectNbMessages("queue2 messages", exchange.ExchangeName, msgs2, int32(numMessages), 5*time.Second)
			ExpectNbMessages("queue3 messages", exchange.ExchangeName, msgs3, int32(numMessages), 5*time.Second)
		})

	})

	When("using a topic exchange", func() {

		Context("with log and event patterns", func() {

			It("should route messages based on topic patterns", func() {
				exchange, cleanup := setupTopicExchange("topic-exchange", "logs.*", "logs.error", "events.#", "events.user.created")
				defer cleanup()

				// Set up consumers for different patterns
				logsAll := exchange.Consume("logs.*")                        // Should match logs.info, logs.error, etc.
				logsError := exchange.Consume("logs.error")                  // Should match only logs.error
				eventsAll := exchange.Consume("events.#")                    // Should match all events.*
				eventsUserCreated := exchange.Consume("events.user.created") // Should match only events.user.created

				// Create routing function that uses message metadata
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
				Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				// Test logs.info - should match logs.* only
				msg1 := message.NewMessage(context.Background(), []byte("info message")).
					WithMetadata("routing_key", "logs.info")
				msg1.ID = "topic-test-1"
				err = pubEngine.Publish(msg1)
				Expect(err).NotTo(HaveOccurred())

				// Test logs.error - should match both logs.* and logs.error
				msg2 := message.NewMessage(context.Background(), []byte("error message")).
					WithMetadata("routing_key", "logs.error")
				msg2.ID = "topic-test-2"
				err = pubEngine.Publish(msg2)
				Expect(err).NotTo(HaveOccurred())

				// Test events.user.created - should match both events.# and events.user.created
				msg3 := message.NewMessage(context.Background(), []byte("user created")).
					WithMetadata("routing_key", "events.user.created")
				msg3.ID = "topic-test-3"
				err = pubEngine.Publish(msg3)
				Expect(err).NotTo(HaveOccurred())

				// Test events.user.deleted - should match events.# only
				msg4 := message.NewMessage(context.Background(), []byte("user deleted")).
					WithMetadata("routing_key", "events.user.deleted")
				msg4.ID = "topic-test-4"
				err = pubEngine.Publish(msg4)
				Expect(err).NotTo(HaveOccurred())

				// Verify message routing with increased timeouts
				ExpectNbMessages("logs all messages", exchange.ExchangeName, logsAll, 2, 3*time.Second)                      // logs.info + logs.error
				ExpectNbMessages("logs error messages", exchange.ExchangeName, logsError, 1, 3*time.Second)                  // logs.error only
				ExpectNbMessages("all events messages", exchange.ExchangeName, eventsAll, 2, 3*time.Second)                  // events.user.created + events.user.deleted
				ExpectNbMessages("user creates events messages", exchange.ExchangeName, eventsUserCreated, 1, 3*time.Second) // events.user.created only
			})
		})

		Context("with wildcard patterns", func() {

			It("should handle wildcard patterns correctly", func() {
				exchange, cleanup := setupTopicExchange("wildcard-topic", "*.urgent", "audit.*", "#", "system.#")
				defer cleanup()

				urgentAny := exchange.Consume("*.urgent") // Should match any.urgent
				auditAny := exchange.Consume("audit.*")   // Should match audit.anything
				allMessages := exchange.Consume("#")      // Should match everything
				systemAll := exchange.Consume("system.#") // Should match system.anything.deep

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
				Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				// Publish various messages
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
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(50 * time.Millisecond)
				}

				// Verify routing patterns with increased timeouts
				ExpectNbMessages("urgent messages", exchange.ExchangeName, urgentAny, 2, 5*time.Second) // log.urgent + email.urgent
				ExpectNbMessages("audit messages", exchange.ExchangeName, auditAny, 2, 5*time.Second)   // audit.login + audit.logout
				ExpectNbMessages("all messages", exchange.ExchangeName, allMessages, 6, 5*time.Second)  // All messages
				ExpectNbMessages("system messages", exchange.ExchangeName, systemAll, 1, 5*time.Second) // system.health.check
			})
		})

	})

	When("using a headers exchange", func() {

		Context("with all and any match bindings", func() {

			It("should route messages based on header matching with 'all' strategy", func() {
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

				// Set up consumers
				exactMatch := exchange.Consume("exact-match")
				anyMatch := exchange.Consume("any-match")
				priorityOnly := exchange.Consume("priority-only")

				_, channel, cleanupPublisher := setupPublisherConnection()
				defer cleanupPublisher()

				// Create publisher that uses message metadata as headers
				pub, err := amqp.NewAMQPPublisher(
					channel,
					publisher.ConstantDestination(publisher.Destination(exchange.ExchangeName)),
					amqp.ConstantRoutingKey(""), // Headers exchange ignores routing key
					amqp.DefaultMessageOptions{
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					})
				Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				// Message that should match exact-match (all headers match)
				msg1 := message.NewMessage(context.Background(), []byte("exact match")).
					WithMetadata("type", "error").
					WithMetadata("severity", "high")
				msg1.ID = "headers-test-1"
				err = pubEngine.Publish(msg1)
				Expect(err).NotTo(HaveOccurred())

				// Message that should match any-match (one header matches)
				msg2 := message.NewMessage(context.Background(), []byte("any match")).
					WithMetadata("category", "logs").
					WithMetadata("level", "info")
				msg2.ID = "headers-test-2"
				err = pubEngine.Publish(msg2)
				Expect(err).NotTo(HaveOccurred())

				// Message that should match priority-only
				msg3 := message.NewMessage(context.Background(), []byte("priority urgent")).
					WithMetadata("priority", "urgent")
				msg3.ID = "headers-test-3"
				err = pubEngine.Publish(msg3)
				Expect(err).NotTo(HaveOccurred())

				// Message that should match both exact-match and priority-only
				msg4 := message.NewMessage(context.Background(), []byte("multiple matches")).
					WithMetadata("type", "error").
					WithMetadata("severity", "high").
					WithMetadata("priority", "urgent")
				msg4.ID = "headers-test-4"
				err = pubEngine.Publish(msg4)
				Expect(err).NotTo(HaveOccurred())

				// Verify message routing
				ExpectNbMessages("exact match messages", exchange.ExchangeName, exactMatch, 2, 2*time.Second)     // msg1 + msg4 (both have type=error AND severity=high)
				ExpectNbMessages("any match messages", exchange.ExchangeName, anyMatch, 1, 2*time.Second)         // msg2 (has category=logs)
				ExpectNbMessages("priority only messages", exchange.ExchangeName, priorityOnly, 2, 2*time.Second) // msg3 + msg4 (both have priority=urgent)
			})
		})

		Context("with complex any match patterns", func() {

			It("should handle 'any' match type correctly", func() {
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
				Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)

				// Test various header combinations
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
					Expect(err).NotTo(HaveOccurred())
					// Small delay between messages to ensure proper delivery
					time.Sleep(10 * time.Millisecond)
				}

				// Verify routing:
				// multi-any should get: any-1, any-2, any-3, multi-1 (4 messages)
				// debug-only should get: debug-1, multi-1 (2 messages)
				ExpectNbMessages("multi:* messages", exchange.ExchangeName, multiAny, 4, 10*time.Second)
				ExpectNbMessages("debug only messages", exchange.ExchangeName, debugOnly, 2, 10*time.Second)
			})
		})

	})

	When("publishing with JSON marshalling", func() {

		It("should marshal struct payload to JSON before publishing", func() {
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
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(context.Background(), testData)
			msg.ID = "json-test-msg"
			err = pubEngine.Publish(msg)
			Expect(err).NotTo(HaveOccurred())

			// Consume the message to verify JSON marshalling worked
			msgs := exchange.Consume("json-routing")

			// Wait for and collect the message directly
			var receivedMsg *amqpgo.Delivery
			Eventually(func() bool {
				select {
				case msg := <-msgs:
					receivedMsg = &msg
					return true
				default:
					return false
				}
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			Expect(receivedMsg).NotTo(BeNil())

			// Unmarshal the received JSON to verify it's correct
			var receivedData TestData
			err = json.Unmarshal(receivedMsg.Body, &receivedData)
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedData).To(Equal(testData))
		})

		It("should handle complex nested JSON structures", func() {
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
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub).
				AddMiddleware(amqp.MarshallPayloadToJson())

			msg := message.NewMessage(context.Background(), testPerson)
			msg.ID = "json-nested-test"
			err = pubEngine.Publish(msg)
			Expect(err).NotTo(HaveOccurred())

			// Consume and verify
			msgs := exchange.Consume("json-routing")

			// Wait for and collect the message directly
			var receivedMsg *amqpgo.Delivery
			Eventually(func() bool {
				select {
				case msg := <-msgs:
					receivedMsg = &msg
					return true
				default:
					return false
				}
			}, 2*time.Second, 50*time.Millisecond).Should(BeTrue())

			Expect(receivedMsg).NotTo(BeNil())

			var receivedPerson Person
			err = json.Unmarshal(receivedMsg.Body, &receivedPerson)
			Expect(err).NotTo(HaveOccurred())
			Expect(receivedPerson).To(Equal(testPerson))
		})

	})

})
