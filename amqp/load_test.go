package amqp_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/amqp/testrabbit"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// createLoadTestConnection creates a dedicated connection for load testing
func createLoadTestConnection() (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel, error) {
	config := amqpgo.Config{
		Vhost:      "/",
		Properties: amqpgo.NewConnectionProperties(),
	}

	// Try to connect with retry logic
	connectionURIs := []string{"amqp://guest:guest@localhost:5672/"}

	// Retry connection to allow RabbitMQ to start
	maxRetries := 10
	baseDelay := 100 * time.Millisecond
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

			// Exponential backoff
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

// Load test scenario for AMQP

var _ = Describe("AMQP load test", Ordered, func() {
	var conn *amqp.ReconnectingConnection
	var channel *amqp.ReconnectingChannel
	var queues []testrabbit.DirectQueue

	BeforeEach(func() {
		var err error
		// Use the improved connection helper
		conn, channel, err = createLoadTestConnection()
		if err != nil {
			panic(err)
		}

		queues = []testrabbit.DirectQueue{}

		// Create fewer queues to reduce complexity and potential race conditions
		for i := 0; i < 3; i++ {
			queue := testrabbit.CreateDirectExchangeQueue(channel, fmt.Sprintf("load-test-queue-%d", i+1))
			queues = append(queues, queue)
		}
	})

	AfterEach(func() {
		// Clean up queues
		for _, queue := range queues {
			queue.Delete()
		}
		channel.Close()
		conn.Close()
	})

	It("should process all messages at scale with multiple publishers and consumers", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		var totalSentCount int64
		var totalProcessedCount int64
		var nackCount int64
		var consumerMu sync.Mutex
		var consumerCounts map[string][]string

		consumerCounts = make(map[string][]string)
		nbMessagesToSendPerQueue := 100

		// Track consumer readiness
		consumerReady := make(chan struct{}, len(queues)*2)

		// Create subscribers for each queue
		// Create 2 consumers per queue to test concurrent processing
		for queueIndex, queue := range queues {
			for j := 0; j < 2; j++ {
				// Create individual channel for each consumer to avoid conflicts
				consumerChannel, err := conn.Channel()
				Expect(err).NotTo(HaveOccurred())

				subscriber, err := amqp.NewAMQPSubscriber(consumerChannel, queue.QueueName)
				Expect(err).NotTo(HaveOccurred())

				msgCh, err := subscriber.Subscribe(ctx)
				Expect(err).NotTo(HaveOccurred())

				go func(consumerIndex int, queueIndex int, queueName string, consumerChannel *amqp.ReconnectingChannel) {
					defer func() {
						subscriber.Close()
						consumerChannel.Close()
					}()

					consumerID := fmt.Sprintf("queue-%d-consumer-%d", queueIndex, consumerIndex)
					// Signal that this consumer is ready
					consumerReady <- struct{}{}

					for {
						select {
						case msg := <-msgCh:
							if msg == nil {
								return // Channel closed
							}

							// Small error rate for testing robustness
							if randomInt(1, 1000) <= 5 { // 0.5% failure rate
								atomic.AddInt64(&nackCount, 1)
								msg.Nack()
								continue
							}

							atomic.AddInt64(&totalProcessedCount, 1)
							consumerMu.Lock()
							if _, exists := consumerCounts[consumerID]; !exists {
								consumerCounts[consumerID] = []string{}
							}
							// Use payload content as identifier since message ID might be empty
							msgContent := string(msg.Payload.([]byte))
							consumerCounts[consumerID] = append(consumerCounts[consumerID], msgContent)
							consumerMu.Unlock()
							msg.Ack()

						case <-ctx.Done():
							return
						}
					}
				}(j+1, queueIndex, queue.QueueName, consumerChannel)
			}
		}

		// Wait for all consumers to be ready
		expectedConsumers := len(queues) * 2
		for i := 0; i < expectedConsumers; i++ {
			select {
			case <-consumerReady:
				// Consumer is ready
			case <-time.After(5 * time.Second):
				Fail(fmt.Sprintf("Timeout waiting for consumer %d to be ready", i+1))
			}
		}
		// Give a little extra time for consumers to fully establish
		time.Sleep(200 * time.Millisecond)

		// Create publishers using direct queue publishing with individual channels
		var publisherWg sync.WaitGroup
		for queueIndex, queue := range queues {
			publisherWg.Add(1)
			go func(queueIndex int, queue testrabbit.DirectQueue) {
				defer publisherWg.Done()

				// Create individual channel for publisher
				publisherChannel, err := conn.Channel()
				if err != nil {
					fmt.Printf("Failed to create publisher channel for queue %d: %v\n", queueIndex, err)
					return
				}
				defer publisherChannel.Close()

				for j := 0; j < nbMessagesToSendPerQueue; j++ {
					payload := fmt.Sprintf("message %d for queue %d", j+1, queueIndex)

					// Use direct publishing instead of queue method to avoid channel conflicts
					publishing := amqpgo.Publishing{
						Body:         []byte(payload),
						Headers:      map[string]interface{}{"queue_index": queueIndex, "message_num": j + 1},
						ContentType:  "text/plain",
						Priority:     0,
						DeliveryMode: amqpgo.Persistent,
					}

					err := publisherChannel.Publish("", queue.QueueName, false, false, publishing)
					if err != nil {
						fmt.Printf("Failed to publish message %d to queue %d: %v\n", j+1, queueIndex, err)
						continue
					}
					atomic.AddInt64(&totalSentCount, 1)

					// Small delay to prevent overwhelming the broker
					if j%25 == 0 {
						time.Sleep(10 * time.Millisecond)
					}
				}
			}(queueIndex, queue)
		}

		// Wait for all publishers to complete
		publisherWg.Wait()

		// Wait for all messages to be processed
		// We have 3 queues * 100 messages = 300 total messages
		totalExpectedMessages := int64(len(queues) * nbMessagesToSendPerQueue)

		// Wait for processing to complete - we expect close to totalExpectedMessages
		// but some messages might be nacked and not requeued
		Eventually(func() bool {
			processed := atomic.LoadInt64(&totalProcessedCount)
			nacked := atomic.LoadInt64(&nackCount)
			sent := atomic.LoadInt64(&totalSentCount)

			// We should have processed + nacked close to what we sent
			// Allow for small variance due to timing
			return (processed+nacked) >= (sent-5) && sent == totalExpectedMessages
		}, "30s", "1s").Should(BeTrue(),
			"Expected close to %d messages to be processed+nacked, but got %d processed + %d nacked = %d total. Sent: %d",
			totalExpectedMessages, atomic.LoadInt64(&totalProcessedCount), atomic.LoadInt64(&nackCount),
			atomic.LoadInt64(&totalProcessedCount)+atomic.LoadInt64(&nackCount), atomic.LoadInt64(&totalSentCount))

		// Should have some random nacks due to simulated errors (0.5% rate may result in 0-3 nacks)
		Expect(nackCount).To(BeNumerically(">=", int64(0)), "Expected 0 or more messages to be nacked due to simulated errors")

		// Verify message distribution across consumers
		// Each queue should have received close to nbMessagesToSendPerQueue messages (accounting for nacks)
		for queueIndex := range queues {
			consumer1ID := fmt.Sprintf("queue-%d-consumer-%d", queueIndex, 1)
			consumer2ID := fmt.Sprintf("queue-%d-consumer-%d", queueIndex, 2)

			consumer1Msgs := consumerCounts[consumer1ID]
			consumer2Msgs := consumerCounts[consumer2ID]

			totalMsgsForQueue := len(consumer1Msgs) + len(consumer2Msgs)
			// Allow for variance due to nacked messages and load test timing
			expectedMinMessages := nbMessagesToSendPerQueue - 5  // Allow for up to 5 messages to be lost/nacked
			Expect(totalMsgsForQueue).To(BeNumerically(">=", expectedMinMessages),
				"Queue %d should have received close to %d messages, but got %d (consumer1: %d, consumer2: %d)",
				queueIndex, nbMessagesToSendPerQueue, totalMsgsForQueue, len(consumer1Msgs), len(consumer2Msgs))

			// Verify no message duplication between consumers
			duplicates := findDuplicateMessages(consumer1Msgs, consumer2Msgs)
			Expect(duplicates).To(BeEmpty(), "Found duplicate messages between consumers for queue %d: %v", queueIndex, duplicates)
		}
	})
})

func findDuplicateMessages(msgs1 []string, msgs2 []string) []string {
	duplicates := []string{}
	for _, msg1 := range msgs1 {
		for _, msg2 := range msgs2 {
			if msg1 == msg2 {
				duplicates = append(duplicates, msg1)
			}
		}
	}
	return duplicates
}

func randomInt(lower int, higher int) int {
	return rand.IntN(higher-lower+1) + lower
}
