package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
)

func asyncCountMessages(count *int, ch <-chan *message.Message, duration time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if ok {
					fmt.Printf("Received message %v\n", msg.Payload)
					*count++
				}
			}
		}
	}()
}

func newStreamName() string {
	return fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
}

type redisSubscriberTestSetup struct {
	client *redis.Client
}

func setupRedisSubscriber(t *testing.T) *redisSubscriberTestSetup {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:29379",
	})

	t.Cleanup(func() {
		client.Close()
	})

	return &redisSubscriberTestSetup{
		client: client,
	}
}

func TestRedisSubscriber(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)

		_, err := subredis.NewRedisSubscriber(nil, newStreamName())
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("when using consumer groups", func(t *testing.T) {
		t.Run("should return an error if the stream doesnt exist", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := fmt.Sprintf("non-existing-stream-%d", time.Now().UnixNano())

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			g.Expect(err).NotTo(HaveOccurred())

			_, err = subscriber.Subscribe(ctx)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err).To(MatchError(subredis.StreamDoesntExistErr))
		})

		t.Run("should dispatch messages to different consumers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := newStreamName()

			subscriber1, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh1, err := subscriber1.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			subscriber2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh2, err := subscriber2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				setup.client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": "payload" + strconv.Itoa(i+1),
					},
				})
			}

			msg1Count := 0
			asyncCountMessages(&msg1Count, msgCh1, 3*time.Second)
			msg2Count := 0
			asyncCountMessages(&msg2Count, msgCh2, 3*time.Second)

			g.Eventually(func() int {
				return msg1Count + msg2Count
			}, 3*time.Second).Should(Equal(expectedMsgCount))

			g.Expect(msg1Count).To(BeNumerically(">", 0))
			g.Expect(msg2Count).To(BeNumerically(">", 0))
		})

		t.Run("should handle message nack and recovery between consumers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()

			// First consumer that will nack the message
			nackingConsumer, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(1*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			msgChNack, err := nackingConsumer.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			// Add a test message
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "test-nack-message",
				},
			})

			nackedMessageID := ""
			consumer1ProcessCount := 0

			// nackingConsumer receives and nacks the message
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgChNack:
						if !ok {
							return
						}
						consumer1ProcessCount++
						fmt.Printf("Nacking consumer processing message %d: %v (ID: %s)\n", consumer1ProcessCount, msg.Payload, msg.ID)
						nackedMessageID = msg.ID
						msg.Nack() // Always nack to leave it pending
					}
				}
			}()

			// Wait for consumer 1 to nack the message
			g.Eventually(func() int { return consumer1ProcessCount }, 5*time.Second).Should(Equal(1))

			// Close consumer 1
			nackingConsumer.Close()

			// Wait for message to become idle
			time.Sleep(2 * time.Second)

			// Second consumer that should claim the pending message
			consumer2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithPendingMessageIdleTimeout(1*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh2, err := consumer2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer consumer2.Close()

			consumer2ProcessCount := 0

			// Consumer 2 should claim and ack the pending message
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh2:
						if !ok {
							return
						}
						consumer2ProcessCount++
						fmt.Printf("Consumer 2 claimed message %d: %v (ID: %s)\n", consumer2ProcessCount, msg.Payload, msg.ID)
						msg.Ack()
					}
				}
			}()

			// Consumer 2 should claim and process the pending message
			g.Eventually(func() int { return consumer2ProcessCount }, 8*time.Second).Should(Equal(1))

			g.Expect(nackedMessageID).NotTo(BeEmpty())
		})

		t.Run("should claim pending messages from failed consumers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()

			// Create a consumer that will simulate failure by not acking messages
			failingSubscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(2*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			failingMsgCh, err := failingSubscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			// Add test messages
			for i := 0; i < 3; i++ {
				setup.client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": fmt.Sprintf("pending-test-%d", i),
					},
				})
			}

			// Let the failing consumer receive messages but not ack them
			failingMsgCount := 0
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-failingMsgCh:
						if ok {
							failingMsgCount++
							fmt.Printf("Failing consumer received message: %v (not acking)\n", msg.Payload)
							// Don't ack - simulate consumer failure
						}
					}
				}
			}()

			// Wait for failing consumer to receive messages
			g.Eventually(func() int { return failingMsgCount }, 5*time.Second).Should(Equal(3))

			// Close the failing consumer to simulate crash
			failingSubscriber.Close()

			// Wait for messages to become idle
			time.Sleep(3 * time.Second)

			// Create a recovery consumer that should claim the pending messages
			recoverySubscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithPendingMessageIdleTimeout(2*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			recoveryMsgCh, err := recoverySubscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer recoverySubscriber.Close()

			recoveryMsgCount := 0
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-recoveryMsgCh:
						if ok {
							recoveryMsgCount++
							fmt.Printf("Recovery consumer claimed message: %v\n", msg.Payload)
							msg.Ack()
						}
					}
				}
			}()

			// Recovery consumer should claim and process the pending messages
			g.Eventually(func() int { return recoveryMsgCount }, 8*time.Second).Should(Equal(3))
		})
	})

	t.Run("when not using consumer groups", func(t *testing.T) {
		t.Run("should receives all messages sent to the subscription", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				setup.client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": "payload" + strconv.Itoa(i+1),
					},
				})
			}

			msgCount := 0
			asyncCountMessages(&msgCount, msgCh, 3*time.Second)

			g.Eventually(func() int {
				return msgCount
			}, 3*time.Second).Should(Equal(expectedMsgCount))
		})

		t.Run("should cancel message context once the processing is done", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "payload0",
				},
			})

			processCount := 0
			waitCh := make(chan bool)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh:
						if !ok {
							return
						}
						msgCtxDone := msg.Context().Done()
						msg.Ack()
						g.Eventually(msgCtxDone, 3*time.Second).Should(BeClosed())
						processCount++
						waitCh <- true
					}
				}
			}()

			g.Eventually(func() int { return processCount }, 3*time.Second).Should(Equal(1))
		})
	})

	t.Run("configuration options", func(t *testing.T) {
		t.Run("WithProcessingTimeoutHandler should call timeout handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream := newStreamName()

			timeoutCalled := false
			timeoutHandler := func(ctx context.Context, msg subredis.MessageWrapper) {
				timeoutCalled = true
			}

			// Add a message first to create the stream
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "timeout-test",
				},
			})

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithProcessingTimeout(1*time.Second),
				subredis.WithProcessingTimeoutHandler(timeoutHandler))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Receive message but don't ack it (simulate long processing)
			select {
			case msg := <-msgCh:
				// Don't ack the message, let it timeout
				_ = msg
			case <-time.After(3 * time.Second):
				g.Fail("Should have received a message")
			}

			// Wait for timeout handler to be called
			g.Eventually(func() bool { return timeoutCalled }, 3*time.Second).Should(BeTrue())
		})

		t.Run("WithProcessingTimeout should timeout messages", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream := newStreamName()

			// Add a message first to create the stream
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "timeout-test",
				},
			})

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithProcessingTimeout(500*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Receive message but don't ack it
			select {
			case msg := <-msgCh:
				// Don't ack, let it timeout
				msgCtx := msg.Context()
				// Wait for context to be cancelled due to timeout
				g.Eventually(msgCtx.Done(), 2*time.Second).Should(BeClosed())
			case <-time.After(3 * time.Second):
				g.Fail("Should have received a message")
			}
		})

		t.Run("WithMetadataExtractor should extract custom metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			// Add message with metadata prefix first
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload":   "test",
					"meta_user": "john",
					"meta_role": "admin",
					"data":      "regular_data",
				},
			})

			// Use the existing PrefixMetadataExtractor utility but convert types
			metadataExtractor := func(wrapper subredis.MessageWrapper) map[string]interface{} {
				stringMetadata := subredis.PrefixMetadataExtractor("meta_")(wrapper)
				metadata := make(map[string]interface{})
				for k, v := range stringMetadata {
					metadata[k] = v
				}
				return metadata
			}

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithMetadataExtractor(metadataExtractor))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Verify metadata extraction
			select {
			case msg := <-msgCh:
				g.Expect(msg.Metadata).To(HaveKey("user"))
				g.Expect(msg.Metadata).To(HaveKey("role"))
				g.Expect(msg.Metadata["user"]).To(Equal("john"))
				g.Expect(msg.Metadata["role"]).To(Equal("admin"))
				g.Expect(msg.Metadata).NotTo(HaveKey("data")) // Should not include non-meta keys
				msg.Ack()
			case <-time.After(3 * time.Second):
				g.Fail("Should have received a message")
			}
		})

		t.Run("WithPayloadExtractor should extract custom payload", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			// Add message with data prefix first
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"data_field1": "value1",
					"data_field2": "value2",
					"meta_user":   "john",
				},
			})

			// Use the existing PrefixPayloadExtractor utility
			payloadExtractor := subredis.PrefixPayloadExtractor("data_")

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithPayloadExtractor(payloadExtractor))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			// Verify payload extraction
			select {
			case msg := <-msgCh:
				payload, ok := msg.Payload.(map[string]interface{})
				g.Expect(ok).To(BeTrue())
				g.Expect(payload).To(HaveKey("field1"))
				g.Expect(payload).To(HaveKey("field2"))
				g.Expect(payload["field1"]).To(Equal("value1"))
				g.Expect(payload["field2"]).To(Equal("value2"))
				g.Expect(payload).NotTo(HaveKey("meta_user")) // Should not include non-data keys
				msg.Ack()
			case <-time.After(3 * time.Second):
				g.Fail("Should have received a message")
			}
		})

		t.Run("WithPendingMessageIdleTimeout should configure timeout", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			stream := newStreamName()

			// Create first consumer with short idle timeout
			consumer1, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithPendingMessageIdleTimeout(1*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh1, err := consumer1.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			// Add message
			setup.client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "pending-timeout-test",
				},
			})

			// Consumer 1 receives but doesn't ack
			consumer1ProcessCount := 0
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh1:
						if !ok {
							return
						}
						consumer1ProcessCount++
						// Don't ack, leave it pending
						_ = msg
					}
				}
			}()

			// Wait for consumer 1 to receive message
			g.Eventually(func() int { return consumer1ProcessCount }, 3*time.Second).Should(Equal(1))

			// Close consumer 1
			consumer1.Close()

			// Wait longer than idle timeout
			time.Sleep(2 * time.Second)

			// Create consumer 2 that should claim the pending message
			consumer2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithPendingMessageIdleTimeout(1*time.Second))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh2, err := consumer2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer consumer2.Close()

			consumer2ProcessCount := 0
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh2:
						if !ok {
							return
						}
						consumer2ProcessCount++
						msg.Ack()
					}
				}
			}()

			// Consumer 2 should claim the pending message
			g.Eventually(func() int { return consumer2ProcessCount }, 8*time.Second).Should(Equal(1))
		})

		t.Run("WithPendingMessageBatchSize should configure batch size", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithPendingMessageBatchSize option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithPendingMessageBatchSize(2))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("WithConsumerGroupStartID should configure start position", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithConsumerGroupStartID option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("WithStartID should configure start position for non-consumer group", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithStartID option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})
	})
}
