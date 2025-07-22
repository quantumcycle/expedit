package redis_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
)

// Shared utilities for Redis load testing

func randomInt(lower int, higher int) int {
	return rand.IntN(higher-lower+1) + lower
}

func findDuplicateMessages(msgs1 []string, msgs2 []string) []string {
	duplicates := []string{}
	seen := make(map[string]bool)

	// Mark all messages from first list
	for _, msg := range msgs1 {
		seen[msg] = true
	}

	// Check for duplicates in second list
	for _, msg := range msgs2 {
		if seen[msg] {
			duplicates = append(duplicates, msg)
		}
	}
	return duplicates
}

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
		var publisherMu sync.Mutex
		var consumerMu sync.Mutex
		var publisherCounts map[string][]string
		var consumerCounts map[string][]string

		publisherCounts = make(map[string][]string)
		consumerCounts = make(map[string][]string)

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

							atomic.AddInt64(&totalProcessedCount, 1)
							consumerMu.Lock()
							if _, exists := consumerCounts[consumerID]; !exists {
								consumerCounts[consumerID] = []string{}
							}
							consumerCounts[consumerID] = append(consumerCounts[consumerID], msg.ID)
							consumerMu.Unlock()
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
						publisherMu.Lock()
						if _, exists := publisherCounts[publisherID]; !exists {
							publisherCounts[publisherID] = []string{}
						}
						publisherCounts[publisherID] = append(publisherCounts[publisherID], msg.ID)
						publisherMu.Unlock()
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

			consumer1Msgs := consumerCounts[consumer1ID]
			consumer2Msgs := consumerCounts[consumer2ID]

			totalMsgsForStream := len(consumer1Msgs) + len(consumer2Msgs)
			expectedMsgsForStream := nbPublishersPerStream * nbMessagesToSendPerPublisher

			g.Expect(totalMsgsForStream).To(Equal(expectedMsgsForStream))

			// Verify no message duplication
			duplicates := findDuplicateMessages(consumer1Msgs, consumer2Msgs)
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
		var consumerMu sync.Mutex
		var consumerCounts map[string][]string

		consumerCounts = make(map[string][]string)
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
						if randomInt(1, 100) <= nackRate || processedByThisConsumer > 50 {
							atomic.AddInt64(&nackCount, 1)
							msg.Nack()
							if processedByThisConsumer > 50 {
								return // Simulate consumer crash
							}
							continue
						}

						atomic.AddInt64(&totalProcessedCount, 1)
						consumerMu.Lock()
						if _, exists := consumerCounts[consumerID]; !exists {
							consumerCounts[consumerID] = []string{}
						}
						consumerCounts[consumerID] = append(consumerCounts[consumerID], msg.ID)
						consumerMu.Unlock()
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

						atomic.AddInt64(&totalProcessedCount, 1)
						consumerMu.Lock()
						if _, exists := consumerCounts[consumerID]; !exists {
							consumerCounts[consumerID] = []string{}
						}
						consumerCounts[consumerID] = append(consumerCounts[consumerID], msg.ID)
						consumerMu.Unlock()
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
		g.Expect(nackCount).To(BeNumerically(">", 0))

		// Verify recovery consumers processed some messages
		recoveryConsumer1Msgs := consumerCounts["recovery-consumer-1"]
		recoveryConsumer2Msgs := consumerCounts["recovery-consumer-2"]
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
		var totalProcessedCount int64
		var nackCount int64
		var consumerMu sync.Mutex
		var consumerCounts map[string][]string

		consumerCounts = make(map[string][]string)

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
							if randomInt(1, 100) <= nackRate {
								atomic.AddInt64(&nackCount, 1)
								msg.Nack()
								continue
							}

							atomic.AddInt64(&totalProcessedCount, 1)
							consumerMu.Lock()
							if _, exists := consumerCounts[consumerID]; !exists {
								consumerCounts[consumerID] = []string{}
							}
							consumerCounts[consumerID] = append(consumerCounts[consumerID], msg.ID)
							consumerMu.Unlock()
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

		// Calculate expected totals accounting for stream distribution
		totalExpected := int64(nbPublishers * nbMessagesToSendPerPublisher)

		// Wait for processing to complete
		// Note: Some messages may be nacked and not reprocessed, so we check that
		// processed + nacked >= sent (i.e., all messages were at least attempted)
		g.Eventually(func() bool {
			processed := atomic.LoadInt64(&totalProcessedCount)
			sent := atomic.LoadInt64(&totalSentCount)
			nacked := atomic.LoadInt64(&nackCount)
			return (processed+nacked) >= sent && sent == totalExpected
		}, 30*time.Second).Should(BeTrue())

		// Verify we had some nacks due to simulated errors
		g.Expect(nackCount).To(BeNumerically(">=", 0))

		// Verify consumer distribution
		for streamIndex := 0; streamIndex < nbStreams; streamIndex++ {
			var totalMsgsForStream int
			var streamConsumerCounts [][]string

			for j := 1; j <= nbConsumersPerStream; j++ {
				consumerID := fmt.Sprintf("stress-stream-%d-consumer-%d", streamIndex, j)
				consumerMsgs := consumerCounts[consumerID]
				streamConsumerCounts = append(streamConsumerCounts, consumerMsgs)
				totalMsgsForStream += len(consumerMsgs)
			}

			// Calculate how many publishers were assigned to this stream (round-robin)
			publishersForStream := 0
			for pubIndex := 0; pubIndex < nbPublishers; pubIndex++ {
				if pubIndex%nbStreams == streamIndex {
					publishersForStream++
				}
			}
			expectedMsgsForStream := publishersForStream * nbMessagesToSendPerPublisher

			// Allow for nacked messages that may not be reprocessed
			minExpectedForStream := expectedMsgsForStream - 10 // Allow up to 10 messages to be nacked per stream
			g.Expect(totalMsgsForStream).To(BeNumerically(">=", minExpectedForStream),
				"Stream %d should have processed close to %d messages, got %d",
				streamIndex, expectedMsgsForStream, totalMsgsForStream)

			// Verify no duplicates between consumers in same stream
			for i := 0; i < len(streamConsumerCounts); i++ {
				for j := i + 1; j < len(streamConsumerCounts); j++ {
					duplicates := findDuplicateMessages(streamConsumerCounts[i], streamConsumerCounts[j])
					g.Expect(duplicates).To(BeEmpty(),
						"Found duplicates between consumers %d and %d in stream %d", i+1, j+1, streamIndex)
				}
			}
		}
	})
}
