package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/loadtest"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
)

// Shared utilities for Redis load testing

func generateTestPayload(testID string, messageNum int, size int) map[string]interface{} {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}

	return map[string]interface{}{
		"test_id":     testID,
		"message_num": messageNum,
		"timestamp":   time.Now().UnixNano(),
		"data":        string(data),
	}
}

type loadTestSetup struct {
	client *redis.Client
}

func setupRedisLoadTest(t *testing.T) *loadTestSetup {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:29379",
		PoolSize: 20, // Support concurrent operations
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := client.Ping(ctx).Err()
	if err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return &loadTestSetup{
		client: client,
	}
}

func TestRedisLoadTest(t *testing.T) {
	t.Run("basic throughput test", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisLoadTest(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		var totalSentCount int64
		var totalProcessedCount int64

		sent := loadtest.NewRecorder()
		received := loadtest.NewRecorder()

		nbStreams := 3
		nbPublishersPerStream := 1
		nbConsumersPerStream := 2
		nbMessagesToSendPerPublisher := 1000

		streams := make([]string, nbStreams)
		for i := 0; i < nbStreams; i++ {
			streams[i] = fmt.Sprintf("load-test-stream-%d-%d", i, time.Now().UnixNano())
		}

		// Create subscribers for each stream
		for streamIndex, stream := range streams {
			consumerGroup := fmt.Sprintf("load-test-group-%d", streamIndex)

			for j := 0; j < nbConsumersPerStream; j++ {
				subscriber, err := subredis.NewRedisSubscriber(setup.client,
					stream,
					subredis.WithConsumerGroup(consumerGroup),
					subredis.WithConsumerGroupCreateStreamIfMissing(true),
					subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
				g.Expect(err).NotTo(HaveOccurred())

				msgCh, err := subscriber.Subscribe(ctx)
				g.Expect(err).NotTo(HaveOccurred())

				go func(consumerIndex int, streamIndex int, stream string) {
					defer subscriber.Close()
					consumerID := fmt.Sprintf("stream-%d-consumer-%d", streamIndex, consumerIndex)

					for {
						select {
						case <-ctx.Done():
							return
						case msg, ok := <-msgCh:
							if !ok {
								return
							}

							// Record before counting, so the records are complete once the count is reached
							received.Record(consumerID, msg.ID)
							atomic.AddInt64(&totalProcessedCount, 1)
							msg.Ack()
						}
					}
				}(j+1, streamIndex, stream)
			}
		}

		// Create publishers
		var publisherWg sync.WaitGroup
		for streamIndex, stream := range streams {
			for pubIndex := 0; pubIndex < nbPublishersPerStream; pubIndex++ {
				publisherWg.Add(1)
				go func(streamIndex int, pubIndex int, stream string) {
					defer publisherWg.Done()

					pub, err := subredis.NewRedisPublisher(setup.client,
						publisher.ConstantDestination(publisher.Destination(stream)),
						simpleMarshaller)
					if err != nil {
						g.Expect(err).NotTo(HaveOccurred())
						return
					}

					pubEngine := publisher.NewPublishingEngine(pub)
					publisherID := fmt.Sprintf("stream-%d-pub-%d", streamIndex, pubIndex)

					for j := 0; j < nbMessagesToSendPerPublisher; j++ {
						payload := generateTestPayload(publisherID, j+1, 100)
						msg := message.NewMessage(ctx, payload)

						err := pubEngine.Publish(msg)
						g.Expect(err).NotTo(HaveOccurred())

						atomic.AddInt64(&totalSentCount, 1)
						sent.Record(publisherID, msg.ID)
					}
				}(streamIndex, pubIndex, stream)
			}
		}

		// Wait for all publishers to complete
		publisherWg.Wait()

		// Wait for all messages to be processed
		totalExpected := int64(nbStreams * nbPublishersPerStream * nbMessagesToSendPerPublisher)
		g.Eventually(func() int64 {
			return atomic.LoadInt64(&totalProcessedCount)
		}, 45*time.Second).Should(Equal(totalExpected))

		// Verify message distribution
		for streamIndex := 0; streamIndex < nbStreams; streamIndex++ {
			consumer1ID := fmt.Sprintf("stream-%d-consumer-%d", streamIndex, 1)
			consumer2ID := fmt.Sprintf("stream-%d-consumer-%d", streamIndex, 2)

			consumer1Msgs := received.IDs(consumer1ID)
			consumer2Msgs := received.IDs(consumer2ID)

			totalMsgsForStream := len(consumer1Msgs) + len(consumer2Msgs)
			expectedMsgsForStream := nbPublishersPerStream * nbMessagesToSendPerPublisher

			g.Expect(totalMsgsForStream).To(Equal(expectedMsgsForStream))

			// Verify no message duplication
			duplicates := loadtest.Duplicates(consumer1Msgs, consumer2Msgs)
			g.Expect(duplicates).To(BeEmpty())
		}
	})

	t.Run("fault tolerance with XCLAIM", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisLoadTest(t)

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		stream := fmt.Sprintf("fault-test-stream-%d", time.Now().UnixNano())
		consumerGroup := "fault-test-group"

		var totalSentCount int64
		var totalProcessedCount int64
		var nackCount int64

		received := loadtest.NewRecorder()
		nbMessagesToSend := 300
		nackRate := 10 // 10% nack rate for failing consumers

		// Create failing consumers that will nack messages then crash
		nbFailingConsumers := 2
		failingConsumersDone := make(chan struct{}, nbFailingConsumers)

		for j := 0; j < nbFailingConsumers; j++ {
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(consumerGroup),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(2*time.Second))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			go func(consumerIndex int) {
				defer func() {
					subscriber.Close()
					failingConsumersDone <- struct{}{}
				}()

				consumerID := fmt.Sprintf("failing-consumer-%d", consumerIndex)
				processedByThisConsumer := 0

				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh:
						if !ok {
							return
						}

						processedByThisConsumer++

						// Simulate failure - nack messages and crash after processing some
						if loadtest.RandomInt(1, 100) <= nackRate || processedByThisConsumer > 50 {
							atomic.AddInt64(&nackCount, 1)
							msg.Nack()
							if processedByThisConsumer > 50 {
								return // Simulate consumer crash
							}
							continue
						}

						// Record before counting, so the records are complete once the count is reached
						received.Record(consumerID, msg.ID)
						atomic.AddInt64(&totalProcessedCount, 1)
						msg.Ack()
					}
				}
			}(j + 1)
		}

		// Publish messages
		pub, err := subredis.NewRedisPublisher(setup.client,
			publisher.ConstantDestination(publisher.Destination(stream)),
			simpleMarshaller)
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)

		go func() {
			for j := 0; j < nbMessagesToSend; j++ {
				payload := generateTestPayload("fault-test", j+1, 100)
				msg := message.NewMessage(ctx, payload)

				err := pubEngine.Publish(msg)
				g.Expect(err).NotTo(HaveOccurred())
				atomic.AddInt64(&totalSentCount, 1)

				// Small delay to allow consumers to process
				if j%20 == 0 {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}()

		// Wait for failing consumers to crash
		for i := 0; i < nbFailingConsumers; i++ {
			select {
			case <-failingConsumersDone:
			case <-time.After(20 * time.Second):
				t.Fatalf("Failing consumer %d didn't finish within timeout", i+1)
			}
		}

		// Wait a bit for messages to become pending
		time.Sleep(3 * time.Second)

		// Create recovery consumers that should claim pending messages
		nbRecoveryConsumers := 2
		for j := 0; j < nbRecoveryConsumers; j++ {
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(consumerGroup),
				subredis.WithPendingMessageIdleTimeout(1*time.Second))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			go func(consumerIndex int) {
				defer subscriber.Close()
				consumerID := fmt.Sprintf("recovery-consumer-%d", consumerIndex)

				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh:
						if !ok {
							return
						}

						// Record before counting, so the records are complete once the count is reached
						received.Record(consumerID, msg.ID)
						atomic.AddInt64(&totalProcessedCount, 1)
						msg.Ack()
					}
				}
			}(j + 1)
		}

		// Wait for all messages to be processed
		g.Eventually(func() int64 {
			return atomic.LoadInt64(&totalProcessedCount)
		}, 30*time.Second).Should(Equal(int64(nbMessagesToSend)))

		// Verify we had some nacks (simulated failures)
		g.Expect(atomic.LoadInt64(&nackCount)).To(BeNumerically(">", 0))

		// Verify recovery consumers processed some messages
		recoveryConsumer1Msgs := received.IDs("recovery-consumer-1")
		recoveryConsumer2Msgs := received.IDs("recovery-consumer-2")
		totalRecoveryMsgs := len(recoveryConsumer1Msgs) + len(recoveryConsumer2Msgs)

		g.Expect(totalRecoveryMsgs).To(BeNumerically(">", 0),
			"Recovery consumers should have processed pending messages")
	})

	t.Run("high concurrency stress test", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisLoadTest(t)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		var totalSentCount int64

		// Keyed by consumer
		received := loadtest.NewRecorder()
		// Keyed by stream. Redis does not redeliver a nacked message before the pending idle timeout, so a nacked
		// message is usually not received again during the test.
		nacked := loadtest.NewRecorder()

		nbStreams := 2
		nbPublishers := 3
		nbConsumersPerStream := 2
		nbMessagesToSendPerPublisher := 100
		nackRate := 2 // 2% random nack rate

		streams := make([]string, nbStreams)
		for i := 0; i < nbStreams; i++ {
			streams[i] = fmt.Sprintf("stress-test-stream-%d-%d", i, time.Now().UnixNano())
		}

		// Create subscribers
		for streamIndex, stream := range streams {
			consumerGroup := fmt.Sprintf("stress-test-group-%d", streamIndex)

			for j := 0; j < nbConsumersPerStream; j++ {
				subscriber, err := subredis.NewRedisSubscriber(setup.client,
					stream,
					subredis.WithConsumerGroup(consumerGroup),
					subredis.WithConsumerGroupCreateStreamIfMissing(true),
					subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
					subredis.WithPendingMessageIdleTimeout(3*time.Second))
				g.Expect(err).NotTo(HaveOccurred())

				msgCh, err := subscriber.Subscribe(ctx)
				g.Expect(err).NotTo(HaveOccurred())

				go func(consumerIndex int, streamIndex int) {
					defer subscriber.Close()
					consumerID := fmt.Sprintf("stress-stream-%d-consumer-%d", streamIndex, consumerIndex)

					for {
						select {
						case <-ctx.Done():
							return
						case msg, ok := <-msgCh:
							if !ok {
								return
							}

							// Simulate occasional errors
							if loadtest.RandomInt(1, 100) <= nackRate {
								nacked.Record(strconv.Itoa(streamIndex), msg.ID)
								msg.Nack()
								continue
							}

							received.Record(consumerID, msg.ID)
							msg.Ack()
						}
					}
				}(j+1, streamIndex)
			}
		}

		// Create publishers
		var publisherWg sync.WaitGroup
		for pubIndex := 0; pubIndex < nbPublishers; pubIndex++ {
			publisherWg.Add(1)
			go func(pubIndex int) {
				defer publisherWg.Done()

				// Round-robin assignment to streams
				streamIndex := pubIndex % nbStreams
				stream := streams[streamIndex]

				pub, err := subredis.NewRedisPublisher(setup.client,
					publisher.ConstantDestination(publisher.Destination(stream)),
					simpleMarshaller)
				if err != nil {
					g.Expect(err).NotTo(HaveOccurred())
					return
				}

				pubEngine := publisher.NewPublishingEngine(pub)

				for j := 0; j < nbMessagesToSendPerPublisher; j++ {
					payload := generateTestPayload(fmt.Sprintf("stress-pub-%d", pubIndex), j+1, 200)
					msg := message.NewMessage(ctx, payload)

					err := pubEngine.Publish(msg)
					g.Expect(err).NotTo(HaveOccurred())
					atomic.AddInt64(&totalSentCount, 1)

					// Small delay every 25 messages to prevent overwhelming
					if j%25 == 0 {
						time.Sleep(5 * time.Millisecond)
					}
				}
			}(pubIndex)
		}

		// Wait for all publishers to complete
		publisherWg.Wait()

		consumerIDs := func(streamIndex int) []string {
			ids := make([]string, 0, nbConsumersPerStream)
			for j := 1; j <= nbConsumersPerStream; j++ {
				ids = append(ids, fmt.Sprintf("stress-stream-%d-consumer-%d", streamIndex, j))
			}
			return ids
		}
		// seen returns the IDs of the stream messages that were either processed or nacked
		seen := func(streamIndex int) map[string]bool {
			ids := map[string]bool{}
			for _, consumerID := range consumerIDs(streamIndex) {
				for _, id := range received.IDs(consumerID) {
					ids[id] = true
				}
			}
			for _, id := range nacked.IDs(strconv.Itoa(streamIndex)) {
				ids[id] = true
			}
			return ids
		}

		g.Expect(atomic.LoadInt64(&totalSentCount)).To(Equal(int64(nbPublishers * nbMessagesToSendPerPublisher)))

		for streamIndex := 0; streamIndex < nbStreams; streamIndex++ {
			// Calculate how many publishers were assigned to this stream (round-robin)
			publishersForStream := 0
			for pubIndex := 0; pubIndex < nbPublishers; pubIndex++ {
				if pubIndex%nbStreams == streamIndex {
					publishersForStream++
				}
			}
			expectedMsgsForStream := publishersForStream * nbMessagesToSendPerPublisher

			// Every message must be handled, either processed or nacked
			g.Eventually(func() int {
				return len(seen(streamIndex))
			}, 30*time.Second).Should(Equal(expectedMsgsForStream), "Stream %d should have handled all its messages", streamIndex)

			var streamConsumerCounts [][]string
			for _, consumerID := range consumerIDs(streamIndex) {
				streamConsumerCounts = append(streamConsumerCounts, received.IDs(consumerID))
			}

			// Verify no duplicates between consumers in same stream
			for i := 0; i < len(streamConsumerCounts); i++ {
				for j := i + 1; j < len(streamConsumerCounts); j++ {
					duplicates := loadtest.Duplicates(streamConsumerCounts[i], streamConsumerCounts[j])
					g.Expect(duplicates).To(BeEmpty(),
						"Found duplicates between consumers %d and %d in stream %d", i+1, j+1, streamIndex)
				}
			}
		}
	})
}
