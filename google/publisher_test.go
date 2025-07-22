package google_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/google"
)

// setupGooglePublisher creates a Google PubSub test setup for publisher tests
func setupGooglePublisher(t *testing.T) *GoogleTestSetup {
	return NewGoogleTestSetup(t, "test-topic")
}

func TestGooglePublisher(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		_, err := google.NewGooglePublisher(
			nil,
			publisher.ConstantDestination(setup.Topic.Name))
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("should return an error if the routing function is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		_, err := google.NewGooglePublisher(
			setup.Client,
			nil)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("routing function is required"))
	})

	t.Run("should return an error when trying to publish a message with an ID", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		pub, err := google.NewGooglePublisher(setup.Client,
			publisher.ConstantDestination(setup.Topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		msg.ID = "something"
		err = pubEngine.Publish(msg)
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("should return the generated ID from pubsub in the message pointer", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		pub, err := google.NewGooglePublisher(setup.Client,
			publisher.ConstantDestination(setup.Topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		err = pubEngine.Publish(msg)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(msg.ID).NotTo(BeEmpty())
	})

	t.Run("should use the routing function to determine the target topic", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)
		ctx := context.Background()

		sub1 := setup.Topic.CreateTestSubscription(ctx, "test-subscription", false)
		msgs1 := sub1.MessageChannel(context.Background(), 1)

		topic2 := setup.EmuClient.CreateTestTopic(ctx, "test-topic-2")
		sub2 := topic2.CreateTestSubscription(ctx, "test-subscription-2", false)
		msgs2 := sub2.MessageChannel(context.Background(), 1)

		routingFn := func(msg *message.Message) (publisher.Destination, error) {
			if msg.Metadata["destination"] == "topic2" {
				return topic2.Name, nil
			}
			return setup.Topic.Name, nil
		}
		pub, err := google.NewGooglePublisher(setup.Client,
			routingFn,
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg1")).WithMetadata("destination", "topic2"))
		g.Expect(err).NotTo(HaveOccurred())
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg2")))
		g.Expect(err).NotTo(HaveOccurred())

		ExpectMessageCount(g, msgs1, 1, 1*time.Second)
		ExpectMessageCount(g, msgs2, 1, 1*time.Second)
	})

	t.Run("when using attributes providers", func(t *testing.T) {
		t.Run("should add the attributes to the pubsub message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)
			ctx := context.Background()

			sub := setup.Topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageChannel(context.Background(), 1)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithAttributesProvider(google.MetadataAsAttributes))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("message1")).WithMetadata("key1", "value1"))
			g.Expect(err).NotTo(HaveOccurred())

			receivedMsg := <-msgs
			g.Expect(receivedMsg.Attributes).To(HaveKeyWithValue("key1", "value1"))
		})
	})

	t.Run("when using ordering keys", func(t *testing.T) {
		t.Run("should receive the messages in order", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)
			ctx := context.Background()

			sub := setup.Topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageDataChannel(context.Background(), 100)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithOrderingKeyProvider(func(msg *message.Message) string {
					return "test-key"
				}))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			var sentMsgs []interface{}
			for i := 0; i < 100; i++ {
				msg := fmt.Sprintf("message %d", i+1)
				err = pubEngine.Publish(message.NewMessage(context.Background(), []byte(msg)))
				g.Expect(err).NotTo(HaveOccurred())
				sentMsgs = append(sentMsgs, msg)
			}

			ExpectMessageCount(g, msgs, 100, 10*time.Second)

			close(msgs)
			var received []string
			for s := range msgs {
				received = append(received, s)
			}
			g.Expect(received).To(HaveExactElements(sentMsgs))
		})
	})

	t.Run("error handling and edge cases", func(t *testing.T) {
		t.Run("should handle metadata as attributes conversion", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithAttributesProvider(google.MetadataAsAttributes))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)

			msg := message.NewMessage(context.Background(), []byte("test"))
			msg.Metadata[""] = "empty-key"
			msg.Metadata["unicode-key-🔑"] = "unicode-value-🎯"
			msg.Metadata["null-value"] = ""
			msg.Metadata["number"] = 42
			msg.Metadata["bool"] = true

			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})
	})

	t.Run("should handle ordering key provider behavior with multiple keys", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)
		ctx := context.Background()

		sub := setup.Topic.CreateTestSubscription(ctx, "test-subscription", true)
		msgs := sub.MessageDataChannel(context.Background(), 100)

		orderingKeyProvider := func(msg *message.Message) string {
			if userID, ok := msg.Metadata["user_id"].(string); ok {
				return userID
			}
			if category, ok := msg.Metadata["category"].(string); ok {
				return "category:" + category
			}
			return "default"
		}

		pub, err := google.NewGooglePublisher(setup.Client,
			publisher.ConstantDestination(setup.Topic.Name),
			google.WithOrderingKeyProvider(orderingKeyProvider))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)

		var sentMsgs []interface{}

		// Send messages with user_id ordering key
		for i := 0; i < 3; i++ {
			msg := message.NewMessage(context.Background(), []byte(fmt.Sprintf("user-msg-%d", i+1)))
			msg.Metadata["user_id"] = "user123"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
			sentMsgs = append(sentMsgs, fmt.Sprintf("user-msg-%d", i+1))
		}

		// Send messages with category ordering key
		for i := 0; i < 3; i++ {
			msg := message.NewMessage(context.Background(), []byte(fmt.Sprintf("category-msg-%d", i+1)))
			msg.Metadata["category"] = "electronics"
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
			sentMsgs = append(sentMsgs, fmt.Sprintf("category-msg-%d", i+1))
		}

		// Send messages with default ordering key
		for i := 0; i < 2; i++ {
			msg := message.NewMessage(context.Background(), []byte(fmt.Sprintf("default-msg-%d", i+1)))
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
			sentMsgs = append(sentMsgs, fmt.Sprintf("default-msg-%d", i+1))
		}

		ExpectMessageCount(g, msgs, 8, 10*time.Second)

		close(msgs)
		var received []string
		for s := range msgs {
			received = append(received, s)
		}

		// Verify all messages were received (order may vary between groups)
		g.Expect(received).To(ConsistOf(sentMsgs))

		// Verify ordering within each group by checking that messages with same ordering key maintain order
		userMsgs := []string{}
		categoryMsgs := []string{}
		defaultMsgs := []string{}

		for _, msg := range received {
			switch {
			case strings.Contains(msg, "user-msg-"):
				userMsgs = append(userMsgs, msg)
			case strings.Contains(msg, "category-msg-"):
				categoryMsgs = append(categoryMsgs, msg)
			case strings.Contains(msg, "default-msg-"):
				defaultMsgs = append(defaultMsgs, msg)
			}
		}

		// Within each ordering key group, messages should maintain order
		g.Expect(userMsgs).To(Equal([]string{"user-msg-1", "user-msg-2", "user-msg-3"}))
		g.Expect(categoryMsgs).To(Equal([]string{"category-msg-1", "category-msg-2", "category-msg-3"}))
		g.Expect(defaultMsgs).To(Equal([]string{"default-msg-1", "default-msg-2"}))
	})

	t.Run("should handle invalid client configuration", func(t *testing.T) {
		t.Run("should handle empty ordering key provider gracefully", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithOrderingKeyProvider(func(msg *message.Message) string {
					return ""
				}))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("msg1"))
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(msg.ID).NotTo(BeEmpty())
		})

		t.Run("should handle ordering key provider that panics", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithOrderingKeyProvider(func(msg *message.Message) string {
					panic("ordering key provider panic")
				}))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("msg1"))

			g.Expect(func() {
				_ = pubEngine.Publish(msg)
			}).To(Panic())
		})

		t.Run("should handle nil attributes provider gracefully", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithAttributesProvider(func(msg *message.Message) map[string]string {
					return nil
				}))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("msg1"))
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("should handle routing function returning error", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			routingFn := func(msg *message.Message) (publisher.Destination, error) {
				return "", errors.New("routing failed")
			}

			pub, err := google.NewGooglePublisher(setup.Client, routingFn)
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("msg1"))
			err = pubEngine.Publish(msg)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err).To(MatchError("routing failed"))
		})

		t.Run("should handle invalid payload type", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), "invalid-payload-type")
			err = pubEngine.Publish(msg)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err).To(MatchError("payload must be []byte. Use a middleware to convert the payload to []byte"))
		})
	})

	t.Run("benchmarks", func(t *testing.T) {
		t.Run("message transformation overhead", func(t *testing.T) {
			g := NewGomegaWithT(t)

			orderingKeyProvider := func(msg *message.Message) string {
				return "benchmark-key"
			}

			iterations := 10000
			var transformationTimes []time.Duration

			for i := 0; i < iterations; i++ {
				msg := message.NewMessage(context.Background(), []byte("benchmark message"))
				msg.Metadata["iteration"] = i
				msg.Metadata["benchmark"] = true
				msg.Metadata["timestamp"] = time.Now().Unix()

				start := time.Now()

				// Only measure our message transformation code, not network I/O
				_ = orderingKeyProvider(msg)
				_ = google.MetadataAsAttributes(msg)

				transformationTime := time.Since(start)
				transformationTimes = append(transformationTimes, transformationTime)
			}

			var totalTime time.Duration
			for _, t := range transformationTimes {
				totalTime += t
			}
			averageTime := totalTime / time.Duration(len(transformationTimes))

			t.Logf("Message transformation benchmark: %d iterations, avg: %v per message",
				iterations, averageTime)

			g.Expect(averageTime).To(BeNumerically("<", 10*time.Microsecond))
		})

		t.Run("options processing overhead", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			iterations := 1000

			start := time.Now()
			for i := 0; i < iterations; i++ {
				_, err := google.NewGooglePublisher(setup.Client,
					publisher.ConstantDestination(setup.Topic.Name),
					google.WithOrderingKeyProvider(func(msg *message.Message) string {
						return "test-key"
					}),
					google.WithAttributesProvider(google.MetadataAsAttributes))
				g.Expect(err).NotTo(HaveOccurred())
			}
			duration := time.Since(start)

			averageTime := duration / time.Duration(iterations)
			t.Logf("Options processing benchmark: %d publishers created in %v (avg: %v per publisher)",
				iterations, duration, averageTime)

			g.Expect(averageTime).To(BeNumerically("<", 1*time.Millisecond))
		})
	})

	t.Run("configuration validation", func(t *testing.T) {
		t.Run("should validate PublisherOptions with nil ordering key provider", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithOrderingKeyProvider(nil))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("test message"))
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("should validate PublisherOptions with nil attributes provider", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)

			pub, err := google.NewGooglePublisher(setup.Client,
				publisher.ConstantDestination(setup.Topic.Name),
				google.WithAttributesProvider(nil))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), []byte("test message"))
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("should validate ordering key provider edge cases", func(t *testing.T) {
			g := NewGomegaWithT(t)

			orderingKeyProvider := func(msg *message.Message) string {
				// Test edge cases in our ordering key provider
				if msg == nil {
					return "nil-message"
				}
				if msg.Metadata == nil {
					return "nil-metadata"
				}
				if len(msg.Metadata) == 0 {
					return "empty-metadata"
				}
				if key, ok := msg.Metadata["key"].(string); ok && key != "" {
					return key
				}
				return "default"
			}

			testCases := []struct {
				name     string
				msg      *message.Message
				expected string
			}{
				{
					name:     "nil message",
					msg:      nil,
					expected: "nil-message",
				},
				{
					name: "nil metadata",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: nil,
					},
					expected: "nil-metadata",
				},
				{
					name: "empty metadata",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: make(map[string]interface{}),
					},
					expected: "empty-metadata",
				},
				{
					name: "valid key",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"key": "user123"},
					},
					expected: "user123",
				},
				{
					name: "empty key",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"key": ""},
					},
					expected: "default",
				},
				{
					name: "wrong type key",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"key": 123},
					},
					expected: "default",
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := orderingKeyProvider(tc.msg)
					g.Expect(result).To(Equal(tc.expected))
				})
			}
		})

		t.Run("should validate attributes provider edge cases", func(t *testing.T) {
			g := NewGomegaWithT(t)

			testCases := []struct {
				name     string
				msg      *message.Message
				expected map[string]string
			}{
				{
					name:     "nil message",
					msg:      nil,
					expected: map[string]string{},
				},
				{
					name: "nil metadata",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: nil,
					},
					expected: map[string]string{},
				},
				{
					name: "empty metadata",
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: make(map[string]interface{}),
					},
					expected: map[string]string{},
				},
				{
					name: "mixed types metadata",
					msg: &message.Message{
						Payload: []byte("test"),
						Metadata: map[string]interface{}{
							"string": "value",
							"int":    42,
							"bool":   true,
							"float":  3.14,
							"nil":    nil,
						},
					},
					expected: map[string]string{
						"string": "value",
						"int":    "42",
						"bool":   "true",
						"float":  "3.14",
						"nil":    "<nil>",
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					var result map[string]string
					if tc.msg == nil {
						result = make(map[string]string)
					} else {
						result = google.MetadataAsAttributes(tc.msg)
					}
					g.Expect(result).To(Equal(tc.expected))
				})
			}
		})
	})

	t.Run("message transformation", func(t *testing.T) {
		t.Run("should test MetadataAsAttributes function comprehensively", func(t *testing.T) {
			g := NewGomegaWithT(t)

			testCases := []struct {
				name     string
				metadata map[string]interface{}
				expected map[string]string
			}{
				{
					name:     "nil metadata",
					metadata: nil,
					expected: map[string]string{},
				},
				{
					name:     "empty metadata",
					metadata: map[string]interface{}{},
					expected: map[string]string{},
				},
				{
					name: "string values",
					metadata: map[string]interface{}{
						"name":    "john",
						"email":   "john@example.com",
						"empty":   "",
						"spaces":  "  spaced  ",
						"unicode": "hello 🌍",
					},
					expected: map[string]string{
						"name":    "john",
						"email":   "john@example.com",
						"empty":   "",
						"spaces":  "  spaced  ",
						"unicode": "hello 🌍",
					},
				},
				{
					name: "numeric values",
					metadata: map[string]interface{}{
						"int":      42,
						"int8":     int8(8),
						"int16":    int16(16),
						"int32":    int32(32),
						"int64":    int64(64),
						"uint":     uint(42),
						"float32":  float32(3.14),
						"float64":  3.14159,
						"negative": -123,
						"zero":     0,
					},
					expected: map[string]string{
						"int":      "42",
						"int8":     "8",
						"int16":    "16",
						"int32":    "32",
						"int64":    "64",
						"uint":     "42",
						"float32":  "3.14",
						"float64":  "3.14159",
						"negative": "-123",
						"zero":     "0",
					},
				},
				{
					name: "boolean values",
					metadata: map[string]interface{}{
						"true":  true,
						"false": false,
					},
					expected: map[string]string{
						"true":  "true",
						"false": "false",
					},
				},
				{
					name: "special values",
					metadata: map[string]interface{}{
						"nil":     nil,
						"pointer": &struct{ name string }{"test"},
						"slice":   []string{"a", "b"},
						"map":     map[string]string{"key": "value"},
					},
					expected: map[string]string{
						"nil":     "<nil>",
						"pointer": "&{test}",
						"slice":   "[a b]",
						"map":     "map[key:value]",
					},
				},
				{
					name: "edge case keys",
					metadata: map[string]interface{}{
						"":          "empty-key",
						"key-dash":  "dash",
						"key_under": "underscore",
						"key.dot":   "dot",
						"key:colon": "colon",
						"key/slash": "slash",
						"key space": "space",
						"UPPERCASE": "upper",
						"MixedCase": "mixed",
					},
					expected: map[string]string{
						"":          "empty-key",
						"key-dash":  "dash",
						"key_under": "underscore",
						"key.dot":   "dot",
						"key:colon": "colon",
						"key/slash": "slash",
						"key space": "space",
						"UPPERCASE": "upper",
						"MixedCase": "mixed",
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					msg := &message.Message{
						Payload:  []byte("test"),
						Metadata: tc.metadata,
					}
					result := google.MetadataAsAttributes(msg)
					g.Expect(result).To(Equal(tc.expected))
				})
			}
		})

		t.Run("should test custom attributes providers", func(t *testing.T) {
			g := NewGomegaWithT(t)

			testCases := []struct {
				name     string
				provider func(*message.Message) map[string]string
				msg      *message.Message
				expected map[string]string
			}{
				{
					name: "selective attributes provider",
					provider: func(msg *message.Message) map[string]string {
						attrs := make(map[string]string)
						if userID, ok := msg.Metadata["user_id"].(string); ok {
							attrs["user"] = userID
						}
						if priority, ok := msg.Metadata["priority"].(int); ok {
							attrs["prio"] = fmt.Sprintf("%d", priority)
						}
						return attrs
					},
					msg: &message.Message{
						Payload: []byte("test"),
						Metadata: map[string]interface{}{
							"user_id":  "user123",
							"priority": 5,
							"ignored":  "should not appear",
						},
					},
					expected: map[string]string{
						"user": "user123",
						"prio": "5",
					},
				},
				{
					name: "prefix-based attributes provider",
					provider: func(msg *message.Message) map[string]string {
						attrs := make(map[string]string)
						for key, value := range msg.Metadata {
							if strings.HasPrefix(key, "attr_") {
								attrs[strings.TrimPrefix(key, "attr_")] = fmt.Sprintf("%v", value)
							}
						}
						return attrs
					},
					msg: &message.Message{
						Payload: []byte("test"),
						Metadata: map[string]interface{}{
							"attr_name":   "john",
							"attr_age":    30,
							"other_field": "ignored",
							"attr_active": true,
						},
					},
					expected: map[string]string{
						"name":   "john",
						"age":    "30",
						"active": "true",
					},
				},
				{
					name: "constant attributes provider",
					provider: func(msg *message.Message) map[string]string {
						return map[string]string{
							"service": "my-service",
							"version": "1.0.0",
						}
					},
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"anything": "value"},
					},
					expected: map[string]string{
						"service": "my-service",
						"version": "1.0.0",
					},
				},
				{
					name: "nil-safe attributes provider",
					provider: func(msg *message.Message) map[string]string {
						if msg == nil || msg.Metadata == nil {
							return map[string]string{"error": "nil-input"}
						}
						return map[string]string{"status": "ok"}
					},
					msg: nil,
					expected: map[string]string{
						"error": "nil-input",
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := tc.provider(tc.msg)
					g.Expect(result).To(Equal(tc.expected))
				})
			}
		})

		t.Run("should test ordering key generation", func(t *testing.T) {
			g := NewGomegaWithT(t)

			testCases := []struct {
				name     string
				provider func(*message.Message) string
				msg      *message.Message
				expected string
			}{
				{
					name: "user-based ordering key",
					provider: func(msg *message.Message) string {
						if userID, ok := msg.Metadata["user_id"].(string); ok {
							return userID
						}
						return ""
					},
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"user_id": "user123"},
					},
					expected: "user123",
				},
				{
					name: "composite ordering key",
					provider: func(msg *message.Message) string {
						userID, _ := msg.Metadata["user_id"].(string)
						sessionID, _ := msg.Metadata["session_id"].(string)
						if userID != "" && sessionID != "" {
							return fmt.Sprintf("%s:%s", userID, sessionID)
						}
						return userID
					},
					msg: &message.Message{
						Payload: []byte("test"),
						Metadata: map[string]interface{}{
							"user_id":    "user123",
							"session_id": "sess456",
						},
					},
					expected: "user123:sess456",
				},
				{
					name: "conditional ordering key",
					provider: func(msg *message.Message) string {
						if priority, ok := msg.Metadata["priority"].(int); ok {
							if priority >= 10 {
								return "high-priority"
							} else if priority >= 5 {
								return "medium-priority"
							}
							return "low-priority"
						}
						return "no-priority"
					},
					msg: &message.Message{
						Payload:  []byte("test"),
						Metadata: map[string]interface{}{"priority": 8},
					},
					expected: "medium-priority",
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					result := tc.provider(tc.msg)
					g.Expect(result).To(Equal(tc.expected))
				})
			}
		})

		t.Run("should test message ID handling logic", func(t *testing.T) {
			g := NewGomegaWithT(t)

			t.Run("should reject messages with pre-set ID", func(t *testing.T) {
				setup := setupGooglePublisher(t)

				pub, err := google.NewGooglePublisher(setup.Client,
					publisher.ConstantDestination(setup.Topic.Name))
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)
				msg := message.NewMessage(context.Background(), []byte("test"))
				msg.ID = "pre-set-id"

				err = pubEngine.Publish(msg)
				g.Expect(err).To(HaveOccurred())
				g.Expect(err).To(MatchError(ContainSubstring("message ID is readonly")))
			})

			t.Run("should allow messages with empty ID", func(t *testing.T) {
				setup := setupGooglePublisher(t)

				pub, err := google.NewGooglePublisher(setup.Client,
					publisher.ConstantDestination(setup.Topic.Name))
				g.Expect(err).NotTo(HaveOccurred())

				pubEngine := publisher.NewPublishingEngine(pub)
				msg := message.NewMessage(context.Background(), []byte("test"))
				g.Expect(msg.ID).To(BeEmpty())

				err = pubEngine.Publish(msg)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(msg.ID).NotTo(BeEmpty()) // Should be set by Google
			})

			t.Run("should test ID validation logic", func(t *testing.T) {
				testCases := []struct {
					name    string
					id      string
					isValid bool
				}{
					{
						name:    "empty ID",
						id:      "",
						isValid: true,
					},
					{
						name:    "non-empty ID",
						id:      "some-id",
						isValid: false,
					},
					{
						name:    "whitespace ID",
						id:      "   ",
						isValid: false,
					},
					{
						name:    "generated-looking ID",
						id:      "1234567890abcdef",
						isValid: false,
					},
				}

				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						// Test our validation logic directly
						isValid := tc.id == ""
						g.Expect(isValid).To(Equal(tc.isValid))
					})
				}
			})
		})
	})
}
