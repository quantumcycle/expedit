package amqp_test

import (
	"context"
	"testing"
	"time"

	"github.com/lithammer/shortuuid/v3"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	amqpgo "github.com/rabbitmq/amqp091-go"
)

func TestAMQPHeaders(t *testing.T) {
	t.Run("HeadersAsMetadata", func(t *testing.T) {
		t.Run("should convert empty AMQP headers to empty metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{}
			metadata := amqp.HeadersAsMetadata(headers)

			g.Expect(metadata).To(BeEmpty())
		})

		t.Run("should convert string headers to metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"content-type": "application/json",
				"source":       "api-gateway",
			}
			metadata := amqp.HeadersAsMetadata(headers)

			g.Expect(metadata).To(HaveKeyWithValue("content-type", "application/json"))
			g.Expect(metadata).To(HaveKeyWithValue("source", "api-gateway"))
		})

		t.Run("should convert numeric headers to string metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"retry-count": 3,
				"timeout":     5000,
				"version":     1.2,
			}
			metadata := amqp.HeadersAsMetadata(headers)

			g.Expect(metadata).To(HaveKeyWithValue("retry-count", "3"))
			g.Expect(metadata).To(HaveKeyWithValue("timeout", "5000"))
			g.Expect(metadata).To(HaveKeyWithValue("version", "1.2"))
		})

		t.Run("should convert boolean headers to string metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"is-urgent":    true,
				"is-processed": false,
			}
			metadata := amqp.HeadersAsMetadata(headers)

			g.Expect(metadata).To(HaveKeyWithValue("is-urgent", "true"))
			g.Expect(metadata).To(HaveKeyWithValue("is-processed", "false"))
		})

		t.Run("should handle nil values in headers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"nullable-field": nil,
				"normal-field":   "value",
			}
			metadata := amqp.HeadersAsMetadata(headers)

			g.Expect(metadata).To(HaveKeyWithValue("nullable-field", "<nil>"))
			g.Expect(metadata).To(HaveKeyWithValue("normal-field", "value"))
		})
	})

	t.Run("PrefixedHeadersProvider", func(t *testing.T) {
		t.Run("should add prefix to all metadata keys", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("type", "error").
				WithMetadata("severity", "high")

			provider := amqp.PrefixedHeadersProvider("app.")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("app.type", "error"))
			g.Expect(headers).To(HaveKeyWithValue("app.severity", "high"))
			g.Expect(headers).NotTo(HaveKey("type"))
			g.Expect(headers).NotTo(HaveKey("severity"))
		})

		t.Run("should handle empty prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key", "value")

			provider := amqp.PrefixedHeadersProvider("")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("key", "value"))
		})

		t.Run("should handle empty metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test"))

			provider := amqp.PrefixedHeadersProvider("prefix.")
			headers := provider(msg)

			g.Expect(headers).To(BeEmpty())
		})
	})

	t.Run("FilteredHeadersProvider", func(t *testing.T) {
		t.Run("should include only keys that match the filter", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("app.name", "myapp").
				WithMetadata("app.version", "1.0").
				WithMetadata("system.id", "abc123").
				WithMetadata("user.id", "user456")

			filter := func(key string) bool {
				return key == "app.name" || key == "user.id"
			}

			provider := amqp.FilteredHeadersProvider(filter)
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("app.name", "myapp"))
			g.Expect(headers).To(HaveKeyWithValue("user.id", "user456"))
			g.Expect(headers).NotTo(HaveKey("app.version"))
			g.Expect(headers).NotTo(HaveKey("system.id"))
		})

		t.Run("should return empty headers when no keys match filter", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("excluded", "value")

			filter := func(key string) bool {
				return false
			}

			provider := amqp.FilteredHeadersProvider(filter)
			headers := provider(msg)

			g.Expect(headers).To(BeEmpty())
		})

		t.Run("should include all keys when filter always returns true", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key1", "value1").
				WithMetadata("key2", "value2")

			filter := func(key string) bool {
				return true
			}

			provider := amqp.FilteredHeadersProvider(filter)
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("key1", "value1"))
			g.Expect(headers).To(HaveKeyWithValue("key2", "value2"))
		})
	})

	t.Run("ExcludePrefixHeadersProvider", func(t *testing.T) {
		t.Run("should exclude keys that start with the specified prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("internal.trace", "abc123").
				WithMetadata("internal.debug", "verbose").
				WithMetadata("user.name", "john").
				WithMetadata("app.version", "1.0")

			provider := amqp.ExcludePrefixHeadersProvider("internal.")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("user.name", "john"))
			g.Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
			g.Expect(headers).NotTo(HaveKey("internal.trace"))
			g.Expect(headers).NotTo(HaveKey("internal.debug"))
		})

		t.Run("should include all keys when no keys match the prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("user.name", "john").
				WithMetadata("app.version", "1.0")

			provider := amqp.ExcludePrefixHeadersProvider("internal.")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("user.name", "john"))
			g.Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
		})

		t.Run("should exclude all keys when all keys match the prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("temp.file1", "abc").
				WithMetadata("temp.file2", "def")

			provider := amqp.ExcludePrefixHeadersProvider("temp.")
			headers := provider(msg)

			g.Expect(headers).To(BeEmpty())
		})
	})

	t.Run("OnlyPrefixHeadersProvider", func(t *testing.T) {
		t.Run("should include only keys that start with the specified prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("app.name", "myapp").
				WithMetadata("app.version", "1.0").
				WithMetadata("user.id", "123").
				WithMetadata("system.debug", "true")

			provider := amqp.OnlyPrefixHeadersProvider("app.")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("app.name", "myapp"))
			g.Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
			g.Expect(headers).NotTo(HaveKey("user.id"))
			g.Expect(headers).NotTo(HaveKey("system.debug"))
		})

		t.Run("should return empty headers when no keys match the prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("user.id", "123").
				WithMetadata("system.debug", "true")

			provider := amqp.OnlyPrefixHeadersProvider("app.")
			headers := provider(msg)

			g.Expect(headers).To(BeEmpty())
		})

		t.Run("should include all keys when all keys match the prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("log.level", "info").
				WithMetadata("log.source", "api")

			provider := amqp.OnlyPrefixHeadersProvider("log.")
			headers := provider(msg)

			g.Expect(headers).To(HaveKeyWithValue("log.level", "info"))
			g.Expect(headers).To(HaveKeyWithValue("log.source", "api"))
		})
	})

	t.Run("ExtractPrefixedMetadata", func(t *testing.T) {
		t.Run("should extract headers with prefix and remove the prefix from keys", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"app.name":    "myapp",
				"app.version": "1.0",
				"user.id":     "123",
				"system.os":   "linux",
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			g.Expect(metadata).To(HaveKeyWithValue("name", "myapp"))
			g.Expect(metadata).To(HaveKeyWithValue("version", "1.0"))
			g.Expect(metadata).NotTo(HaveKey("user.id"))
			g.Expect(metadata).NotTo(HaveKey("system.os"))
			g.Expect(metadata).NotTo(HaveKey("app.name"))
			g.Expect(metadata).NotTo(HaveKey("app.version"))
		})

		t.Run("should return empty metadata when no headers match the prefix", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"user.id":   "123",
				"system.os": "linux",
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			g.Expect(metadata).To(BeEmpty())
		})

		t.Run("should handle empty headers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			g.Expect(metadata).To(BeEmpty())
		})

		t.Run("should convert header values to strings", func(t *testing.T) {
			g := NewGomegaWithT(t)
			headers := amqpgo.Table{
				"config.port":    8080,
				"config.enabled": true,
				"config.ratio":   0.75,
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "config.")

			g.Expect(metadata).To(HaveKeyWithValue("port", "8080"))
			g.Expect(metadata).To(HaveKeyWithValue("enabled", "true"))
			g.Expect(metadata).To(HaveKeyWithValue("ratio", "0.75"))
		})
	})

	t.Run("MergeHeadersToMetadata", func(t *testing.T) {
		t.Run("should merge headers into existing message metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("existing", "value")

			headers := amqpgo.Table{
				"new-key":     "new-value",
				"another-key": "another-value",
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			g.Expect(msg.Metadata).To(HaveKeyWithValue("existing", "value"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("new-key", "new-value"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("another-key", "another-value"))
		})

		t.Run("should override existing metadata values when keys conflict", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key", "original-value").
				WithMetadata("other", "keep-this")

			headers := amqpgo.Table{
				"key": "overridden-value",
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			g.Expect(msg.Metadata).To(HaveKeyWithValue("key", "overridden-value"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("other", "keep-this"))
		})

		t.Run("should handle empty headers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("existing", "value")

			headers := amqpgo.Table{}

			amqp.MergeHeadersToMetadata(msg, headers)

			g.Expect(msg.Metadata).To(HaveKeyWithValue("existing", "value"))
			g.Expect(msg.Metadata).To(HaveLen(1))
		})

		t.Run("should convert header values to strings", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), []byte("test"))

			headers := amqpgo.Table{
				"number":  42,
				"boolean": true,
				"float":   3.14,
				"null":    nil,
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			g.Expect(msg.Metadata).To(HaveKeyWithValue("number", "42"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("boolean", "true"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("float", "3.14"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("null", "<nil>"))
		})
	})

	t.Run("Integration Tests", func(t *testing.T) {
		t.Run("should publish with a header provider", func(t *testing.T) {
			g := NewGomegaWithT(t)
			
			var conn *amqp.ReconnectingConnection
			var channel *amqp.ReconnectingChannel
			var testExchange string
			var testQueue string

			// Setup
			var err error
			config := amqpgo.Config{
				Vhost:      "/",
				Properties: amqpgo.NewConnectionProperties(),
			}

			// Connect to RabbitMQ with retry
			maxRetries := 10
			for i := 0; i < maxRetries; i++ {
				conn, err = amqp.DialConfig("amqp://guest:guest@localhost:5672/", config)
				if err == nil {
					break
				}
				if i == maxRetries-1 {
					t.Fatalf("RabbitMQ not available after %d retries: %v", maxRetries, err)
				}
				time.Sleep(100 * time.Millisecond)
			}

			t.Cleanup(func() {
				if channel != nil && !channel.IsClosed() {
					// Clean up test resources
					if testQueue != "" {
						channel.QueueDelete(testQueue, false, false, false)
					}
					if testExchange != "" {
						channel.ExchangeDelete(testExchange, false, false)
					}
					channel.Close()
				}
				if conn != nil {
					conn.Close()
				}
			})

			channel, err = conn.Channel()
			g.Expect(err).NotTo(HaveOccurred())

			// Create unique test exchange and queue names
			randomPart := shortuuid.New()
			testExchange = "test-headers-exchange-" + randomPart
			testQueue = "test-headers-queue-" + randomPart

			err = channel.ExchangeDeclare(testExchange, "direct", false, true, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())

			_, err = channel.QueueDeclare(testQueue, false, true, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())

			err = channel.QueueBind(testQueue, "test", testExchange, false, nil)
			g.Expect(err).NotTo(HaveOccurred())

			// Create publisher with prefixed headers provider
			pub, err := amqp.NewAMQPPublisher(
				channel,
				publisher.ConstantDestination(publisher.Destination(testExchange)),
				amqp.ConstantRoutingKey("test"),
				amqp.DefaultMessageOptions{
					ContentType:  "text/plain",
					Priority:     0,
					DeliveryMode: amqpgo.Transient,
				},
				amqp.WithHeadersProvider(amqp.PrefixedHeadersProvider("app.")))
			g.Expect(err).NotTo(HaveOccurred())

			// Create subscriber
			sub, err := amqp.NewAMQPSubscriber(channel, testQueue)
			g.Expect(err).NotTo(HaveOccurred())

			// Subscribe and get message channel
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			msgCh, err := sub.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer sub.Close()

			// Collect received messages with ready signal
			receivedMessages := make(chan *message.Message, 1)
			ready := make(chan struct{}, 1)
			go func() {
				ready <- struct{}{}
				for msg := range msgCh {
					msg.Ack()
					receivedMessages <- msg
					break
				}
			}()
			<-ready
			time.Sleep(100 * time.Millisecond)

			// Publish message with metadata
			testMsg := message.NewMessage(context.Background(), []byte("test message")).
				WithMetadata("service", "auth").
				WithMetadata("version", "1.2.3")
			testMsg.ID = "integration-test-1"

			pubEngine := publisher.NewPublishingEngine(pub)
			err = pubEngine.Publish(testMsg)
			g.Expect(err).NotTo(HaveOccurred())

			// Verify received message has prefixed headers as metadata
			var receivedMsg *message.Message
			g.Eventually(receivedMessages, 2*time.Second).Should(Receive(&receivedMsg))

			g.Expect(receivedMsg.Metadata).To(HaveKeyWithValue("app.service", "auth"))
			g.Expect(receivedMsg.Metadata).To(HaveKeyWithValue("app.version", "1.2.3"))
			g.Expect(receivedMsg.Metadata).NotTo(HaveKey("service"))
			g.Expect(receivedMsg.Metadata).NotTo(HaveKey("version"))
		})
	})
}