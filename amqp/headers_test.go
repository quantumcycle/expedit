package amqp_test

import (
	"context"
	"github.com/lithammer/shortuuid/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"time"
)

var _ = Describe("AMQP Headers", func() {

	Describe("HeadersAsMetadata", func() {

		It("should convert empty AMQP headers to empty metadata", func() {
			headers := amqpgo.Table{}
			metadata := amqp.HeadersAsMetadata(headers)

			Expect(metadata).To(BeEmpty())
		})

		It("should convert string headers to metadata", func() {
			headers := amqpgo.Table{
				"content-type": "application/json",
				"source":       "api-gateway",
			}
			metadata := amqp.HeadersAsMetadata(headers)

			Expect(metadata).To(HaveKeyWithValue("content-type", "application/json"))
			Expect(metadata).To(HaveKeyWithValue("source", "api-gateway"))
		})

		It("should convert numeric headers to string metadata", func() {
			headers := amqpgo.Table{
				"retry-count": 3,
				"timeout":     5000,
				"version":     1.2,
			}
			metadata := amqp.HeadersAsMetadata(headers)

			Expect(metadata).To(HaveKeyWithValue("retry-count", "3"))
			Expect(metadata).To(HaveKeyWithValue("timeout", "5000"))
			Expect(metadata).To(HaveKeyWithValue("version", "1.2"))
		})

		It("should convert boolean headers to string metadata", func() {
			headers := amqpgo.Table{
				"is-urgent":    true,
				"is-processed": false,
			}
			metadata := amqp.HeadersAsMetadata(headers)

			Expect(metadata).To(HaveKeyWithValue("is-urgent", "true"))
			Expect(metadata).To(HaveKeyWithValue("is-processed", "false"))
		})

		It("should handle nil values in headers", func() {
			headers := amqpgo.Table{
				"nullable-field": nil,
				"normal-field":   "value",
			}
			metadata := amqp.HeadersAsMetadata(headers)

			Expect(metadata).To(HaveKeyWithValue("nullable-field", "<nil>"))
			Expect(metadata).To(HaveKeyWithValue("normal-field", "value"))
		})

	})

	Describe("PrefixedHeadersProvider", func() {

		It("should add prefix to all metadata keys", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("type", "error").
				WithMetadata("severity", "high")

			provider := amqp.PrefixedHeadersProvider("app.")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("app.type", "error"))
			Expect(headers).To(HaveKeyWithValue("app.severity", "high"))
			Expect(headers).NotTo(HaveKey("type"))
			Expect(headers).NotTo(HaveKey("severity"))
		})

		It("should handle empty prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key", "value")

			provider := amqp.PrefixedHeadersProvider("")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("key", "value"))
		})

		It("should handle empty metadata", func() {
			msg := message.NewMessage(context.Background(), []byte("test"))

			provider := amqp.PrefixedHeadersProvider("prefix.")
			headers := provider(msg)

			Expect(headers).To(BeEmpty())
		})

	})

	Describe("FilteredHeadersProvider", func() {

		It("should include only keys that match the filter", func() {
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

			Expect(headers).To(HaveKeyWithValue("app.name", "myapp"))
			Expect(headers).To(HaveKeyWithValue("user.id", "user456"))
			Expect(headers).NotTo(HaveKey("app.version"))
			Expect(headers).NotTo(HaveKey("system.id"))
		})

		It("should return empty headers when no keys match filter", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("excluded", "value")

			filter := func(key string) bool {
				return false
			}

			provider := amqp.FilteredHeadersProvider(filter)
			headers := provider(msg)

			Expect(headers).To(BeEmpty())
		})

		It("should include all keys when filter always returns true", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key1", "value1").
				WithMetadata("key2", "value2")

			filter := func(key string) bool {
				return true
			}

			provider := amqp.FilteredHeadersProvider(filter)
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("key1", "value1"))
			Expect(headers).To(HaveKeyWithValue("key2", "value2"))
		})

	})

	Describe("ExcludePrefixHeadersProvider", func() {

		It("should exclude keys that start with the specified prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("internal.trace", "abc123").
				WithMetadata("internal.debug", "verbose").
				WithMetadata("user.name", "john").
				WithMetadata("app.version", "1.0")

			provider := amqp.ExcludePrefixHeadersProvider("internal.")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("user.name", "john"))
			Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
			Expect(headers).NotTo(HaveKey("internal.trace"))
			Expect(headers).NotTo(HaveKey("internal.debug"))
		})

		It("should include all keys when no keys match the prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("user.name", "john").
				WithMetadata("app.version", "1.0")

			provider := amqp.ExcludePrefixHeadersProvider("internal.")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("user.name", "john"))
			Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
		})

		It("should exclude all keys when all keys match the prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("temp.file1", "abc").
				WithMetadata("temp.file2", "def")

			provider := amqp.ExcludePrefixHeadersProvider("temp.")
			headers := provider(msg)

			Expect(headers).To(BeEmpty())
		})

	})

	Describe("OnlyPrefixHeadersProvider", func() {

		It("should include only keys that start with the specified prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("app.name", "myapp").
				WithMetadata("app.version", "1.0").
				WithMetadata("user.id", "123").
				WithMetadata("system.debug", "true")

			provider := amqp.OnlyPrefixHeadersProvider("app.")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("app.name", "myapp"))
			Expect(headers).To(HaveKeyWithValue("app.version", "1.0"))
			Expect(headers).NotTo(HaveKey("user.id"))
			Expect(headers).NotTo(HaveKey("system.debug"))
		})

		It("should return empty headers when no keys match the prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("user.id", "123").
				WithMetadata("system.debug", "true")

			provider := amqp.OnlyPrefixHeadersProvider("app.")
			headers := provider(msg)

			Expect(headers).To(BeEmpty())
		})

		It("should include all keys when all keys match the prefix", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("log.level", "info").
				WithMetadata("log.source", "api")

			provider := amqp.OnlyPrefixHeadersProvider("log.")
			headers := provider(msg)

			Expect(headers).To(HaveKeyWithValue("log.level", "info"))
			Expect(headers).To(HaveKeyWithValue("log.source", "api"))
		})

	})

	Describe("ExtractPrefixedMetadata", func() {

		It("should extract headers with prefix and remove the prefix from keys", func() {
			headers := amqpgo.Table{
				"app.name":    "myapp",
				"app.version": "1.0",
				"user.id":     "123",
				"system.os":   "linux",
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			Expect(metadata).To(HaveKeyWithValue("name", "myapp"))
			Expect(metadata).To(HaveKeyWithValue("version", "1.0"))
			Expect(metadata).NotTo(HaveKey("user.id"))
			Expect(metadata).NotTo(HaveKey("system.os"))
			Expect(metadata).NotTo(HaveKey("app.name"))
			Expect(metadata).NotTo(HaveKey("app.version"))
		})

		It("should return empty metadata when no headers match the prefix", func() {
			headers := amqpgo.Table{
				"user.id":   "123",
				"system.os": "linux",
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			Expect(metadata).To(BeEmpty())
		})

		It("should handle empty headers", func() {
			headers := amqpgo.Table{}

			metadata := amqp.ExtractPrefixedMetadata(headers, "app.")

			Expect(metadata).To(BeEmpty())
		})

		It("should convert header values to strings", func() {
			headers := amqpgo.Table{
				"config.port":    8080,
				"config.enabled": true,
				"config.ratio":   0.75,
			}

			metadata := amqp.ExtractPrefixedMetadata(headers, "config.")

			Expect(metadata).To(HaveKeyWithValue("port", "8080"))
			Expect(metadata).To(HaveKeyWithValue("enabled", "true"))
			Expect(metadata).To(HaveKeyWithValue("ratio", "0.75"))
		})

	})

	Describe("MergeHeadersToMetadata", func() {

		It("should merge headers into existing message metadata", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("existing", "value")

			headers := amqpgo.Table{
				"new-key":     "new-value",
				"another-key": "another-value",
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			Expect(msg.Metadata).To(HaveKeyWithValue("existing", "value"))
			Expect(msg.Metadata).To(HaveKeyWithValue("new-key", "new-value"))
			Expect(msg.Metadata).To(HaveKeyWithValue("another-key", "another-value"))
		})

		It("should override existing metadata values when keys conflict", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("key", "original-value").
				WithMetadata("other", "keep-this")

			headers := amqpgo.Table{
				"key": "overridden-value",
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			Expect(msg.Metadata).To(HaveKeyWithValue("key", "overridden-value"))
			Expect(msg.Metadata).To(HaveKeyWithValue("other", "keep-this"))
		})

		It("should handle empty headers", func() {
			msg := message.NewMessage(context.Background(), []byte("test")).
				WithMetadata("existing", "value")

			headers := amqpgo.Table{}

			amqp.MergeHeadersToMetadata(msg, headers)

			Expect(msg.Metadata).To(HaveKeyWithValue("existing", "value"))
			Expect(msg.Metadata).To(HaveLen(1))
		})

		It("should convert header values to strings", func() {
			msg := message.NewMessage(context.Background(), []byte("test"))

			headers := amqpgo.Table{
				"number":  42,
				"boolean": true,
				"float":   3.14,
				"null":    nil,
			}

			amqp.MergeHeadersToMetadata(msg, headers)

			Expect(msg.Metadata).To(HaveKeyWithValue("number", "42"))
			Expect(msg.Metadata).To(HaveKeyWithValue("boolean", "true"))
			Expect(msg.Metadata).To(HaveKeyWithValue("float", "3.14"))
			Expect(msg.Metadata).To(HaveKeyWithValue("null", "<nil>"))
		})

	})

	Describe("Integration Tests", func() {

		var conn *amqp.ReconnectingConnection
		var channel *amqp.ReconnectingChannel
		var testExchange string
		var testQueue string

		BeforeEach(func() {
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
					Fail("RabbitMQ not available")
				}
				time.Sleep(100 * time.Millisecond)
			}

			channel, err = conn.Channel()
			Expect(err).NotTo(HaveOccurred())

			// Create unique test exchange and queue names
			randomPart := shortuuid.New()
			testExchange = "test-headers-exchange-" + randomPart
			testQueue = "test-headers-queue-" + randomPart

			err = channel.ExchangeDeclare(testExchange, "direct", false, true, false, false, nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = channel.QueueDeclare(testQueue, false, true, false, false, nil)
			Expect(err).NotTo(HaveOccurred())

			err = channel.QueueBind(testQueue, "test", testExchange, false, nil)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
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

		It("should publish with a header provider", func() {
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
			Expect(err).NotTo(HaveOccurred())

			// Create subscriber
			sub, err := amqp.NewAMQPSubscriber(channel, testQueue)
			Expect(err).NotTo(HaveOccurred())

			// Subscribe and get message channel
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			msgCh, err := sub.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
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
			Expect(err).NotTo(HaveOccurred())

			// Verify received message has prefixed headers as metadata
			var receivedMsg *message.Message
			Eventually(receivedMessages, 2*time.Second).Should(Receive(&receivedMsg))

			Expect(receivedMsg.Metadata).To(HaveKeyWithValue("app.service", "auth"))
			Expect(receivedMsg.Metadata).To(HaveKeyWithValue("app.version", "1.2.3"))
			Expect(receivedMsg.Metadata).NotTo(HaveKey("service"))
			Expect(receivedMsg.Metadata).NotTo(HaveKey("version"))
		})
	})
})
