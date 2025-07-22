package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/google"
)

// setupGoogleSubscriber creates a Google PubSub test setup for subscriber tests
func setupGoogleSubscriber(t *testing.T) *GoogleTestSetup {
	return NewGoogleTestSetup(t, "test-topic")
}

func TestGoogleSubscriber(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)

		_, err := google.NewGoogleSubscriber(nil, "test-subscription")
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("should return an error if the subscription doesnt exist", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		subscriber, err := google.NewGoogleSubscriber(setup.Client, "non-existing-subscription")
		g.Expect(err).NotTo(HaveOccurred())

		_, err = subscriber.Subscribe(ctx)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("subscription does not exist"))
	})

	t.Run("should receives all messages sent to the subscription", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		msgCount := 0
		AsyncCountMessages(&msgCount, msgCh, 5*time.Second)

		expectedMsgCount := 10
		for i := 0; i < expectedMsgCount; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
		}
		g.Eventually(func() int {
			return msgCount
		}, 3*time.Second).Should(Equal(expectedMsgCount))
	})

	t.Run("when parse attributes is enabled", func(t *testing.T) {
		t.Run("should convert bool", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			subscriptionName := UniqueSubscriptionName("test-subscription")
			subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			var att1Val interface{}
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						att1Val = msg.Metadata["att1"]
					}
				}
			}()

			attrs := make(map[string]string)
			attrs["att1"] = "true"
			setup.Topic.PublishBytes(ctx, []byte("payload"), attrs)

			g.Eventually(func() interface{} {
				return att1Val
			}).Should(Not(BeNil()))

			g.Expect(att1Val).To(Equal(true))
		})

		t.Run("should convert float", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			subscriptionName := UniqueSubscriptionName("test-subscription")
			subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			var att1Val interface{}
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						att1Val = msg.Metadata["att1"]
					}
				}
			}()

			attrs := make(map[string]string)
			attrs["att1"] = "10.231"
			setup.Topic.PublishBytes(ctx, []byte("payload"), attrs)

			g.Eventually(func() interface{} {
				return att1Val
			}).Should(Not(BeNil()))

			g.Expect(att1Val).To(Equal(10.231))
		})

		t.Run("should convert integer", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			subscriptionName := UniqueSubscriptionName("test-subscription")
			subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			var att1Val interface{}
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						att1Val = msg.Metadata["att1"]
					}
				}
			}()

			attrs := make(map[string]string)
			attrs["att1"] = "10"
			setup.Topic.PublishBytes(ctx, []byte("payload"), attrs)

			g.Eventually(func() interface{} {
				return att1Val
			}).Should(Not(BeNil()))

			g.Expect(att1Val).To(Equal(int64(10)))
		})

		t.Run("should keep string as is", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			subscriptionName := UniqueSubscriptionName("test-subscription")
			subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			var att1Val interface{}
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						att1Val = msg.Metadata["att1"]
					}
				}
			}()

			attrs := make(map[string]string)
			attrs["att1"] = "hello"
			setup.Topic.PublishBytes(ctx, []byte("payload"), attrs)

			g.Eventually(func() interface{} {
				return att1Val
			}).Should(Not(BeNil()))

			g.Expect(att1Val).To(Equal("hello"))
		})
	})

	t.Run("should nack messages that are not ack/nacked after the processing timeout", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		timeoutOccurred := false
		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithProcessingTimeout(1*time.Second),
			google.WithProcessingTimeoutHandler(func(ctx context.Context, msg *pubsub.Message) {
				timeoutOccurred = true
			}))
		g.Expect(err).NotTo(HaveOccurred())

		nackOccurred := false
		msgCh, err := subscriber.Subscribe(ctx)
		defer subscriber.Close()
		g.Expect(err).NotTo(HaveOccurred())
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					nextState := <-msg.StateChange()
					if nextState == message.Nack {
						nackOccurred = true
					}
				}
			}
		}()

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() bool {
			return timeoutOccurred && nackOccurred
		}, 5*time.Second).Should(Equal(true))
	})

	t.Run("should receive the message ids that were published", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		defer subscriber.Close()
		g.Expect(err).NotTo(HaveOccurred())

		idReceived := make(map[string]bool)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					idReceived[msg.ID] = true
				}
			}
		}()

		expectedIds := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			id := setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
			expectedIds = append(expectedIds, id)
		}

		g.Eventually(func() []string {
			keys := make([]string, 0, len(idReceived))
			for k := range idReceived {
				keys = append(keys, k)
			}
			return keys
		}, 5*time.Second).Should(ContainElements(expectedIds))
	})

	t.Run("should relay the ack or nack to gcp messages", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		defer subscriber.Close()
		g.Expect(err).NotTo(HaveOccurred())

		nackDone := false
		processCount := 0
		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					processCount++
					if !nackDone {
						nackDone = true
						msg.Nack()
					} else {
						msg.Ack()
					}
				}
			}
		}()

		nbMsg := 100
		for i := 0; i < nbMsg; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload1"), nil)
		}

		g.Eventually(func() int {
			return processCount
		}, 5*time.Second).Should(Equal(nbMsg + 1))
	})

	t.Run("should cancel message context once the processing is done", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		processCount := 0
		waitCh := make(chan bool)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					msgCtxDone := msg.Context().Done()
					msg.Ack()
					processCount++
					g.Eventually(msgCtxDone, 3*time.Second).Should(BeClosed())
					waitCh <- true
				}
			}
		}()
		<-waitCh

		g.Expect(processCount).To(Equal(1))
	})

	t.Run("should propagate context cancellation properly", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		parentCtx := context.Background()
		ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		var msgContext context.Context
		contextReceivedCh := make(chan bool)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					msgContext = msg.Context()
					msg.Ack()
					contextReceivedCh <- true
					return
				}
			}
		}()

		<-contextReceivedCh
		g.Expect(msgContext).NotTo(BeNil())

		cancel()

		g.Eventually(func() error {
			return msgContext.Err()
		}, 2*time.Second).Should(Not(BeNil()))
		g.Expect(msgContext.Err()).To(Equal(context.Canceled))
	})

	t.Run("should accept WithReceiveSettings option and process messages correctly", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		receiveSettings := pubsub.ReceiveSettings{
			NumGoroutines:          3,
			MaxOutstandingMessages: 50,
			MaxOutstandingBytes:    1024 * 1024,
		}

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithReceiveSettings(receiveSettings))
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		processedCount := 0
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					processedCount++
					msg.Ack()
				}
			}
		}()

		for i := 0; i < 20; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
		}

		g.Eventually(func() int {
			return processedCount
		}, 5*time.Second).Should(Equal(20))
	})

	t.Run("should accept WithProcessingTimeout and WithProcessingTimeoutHandler options", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		timeoutHandlerCalled := false
		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithProcessingTimeout(300*time.Second),
			google.WithProcessingTimeoutHandler(func(ctx context.Context, msg *pubsub.Message) {
				timeoutHandlerCalled = true
			}))
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		processedCount := 0
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					processedCount++
					msg.Ack()
				}
			}
		}()

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() int {
			return processedCount
		}, 3*time.Second).Should(Equal(1))
		g.Expect(timeoutHandlerCalled).To(BeFalse())
	})

	t.Run("should accept subscriber options with various edge case values", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscriptionName := UniqueSubscriptionName("test-subscription")
		subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

		receiveSettings := pubsub.ReceiveSettings{
			NumGoroutines:          -1,
			MaxOutstandingMessages: -1,
			MaxOutstandingBytes:    -1,
		}

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithReceiveSettings(receiveSettings),
			google.WithProcessingTimeout(-1*time.Second),
			google.WithParseAttributes(true),
			google.WithProcessingTimeoutHandler(nil))
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		processedCount := 0
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-msgCh:
					if msg == nil {
						return
					}
					processedCount++
					msg.Ack()
				}
			}
		}()

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() int {
			return processedCount
		}, 3*time.Second).Should(Equal(1))
	})

	t.Run("benchmarks", func(t *testing.T) {
		t.Run("message transformation overhead", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			subscriptionName := UniqueSubscriptionName("test-subscription")
			subscription := setup.Topic.CreateTestSubscription(ctx, subscriptionName, false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name,
				google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			iterations := 1000
			processedCount := 0
			transformationTimes := make([]time.Duration, 0, iterations)

			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg := <-msgCh:
						if msg == nil {
							return
						}
						start := time.Now()

						_ = msg.ID
						_ = msg.Payload
						_ = msg.Metadata["iteration"]
						_ = msg.Metadata["benchmark"]
						_ = msg.Metadata["timestamp"]

						transformationTime := time.Since(start)
						transformationTimes = append(transformationTimes, transformationTime)

						processedCount++
						msg.Ack()
					}
				}
			}()

			publishStart := time.Now()
			for i := 0; i < iterations; i++ {
				attrs := map[string]string{
					"iteration": fmt.Sprintf("%d", i),
					"benchmark": "true",
					"timestamp": fmt.Sprintf("%d", time.Now().Unix()),
				}
				setup.Topic.PublishBytes(ctx, []byte("benchmark message"), attrs)
			}
			publishDuration := time.Since(publishStart)

			g.Eventually(func() int {
				return processedCount
			}, 20*time.Second).Should(Equal(iterations))

			var totalTransformTime time.Duration
			for _, dur := range transformationTimes {
				totalTransformTime += dur
			}
			avgTransformTime := totalTransformTime / time.Duration(len(transformationTimes))

			t.Logf("Message transformation benchmark: %d messages processed", processedCount)
			t.Logf("Publishing took: %v (avg: %v per message)",
				publishDuration, publishDuration/time.Duration(iterations))
			t.Logf("Avg transformation time: %v per message", avgTransformTime)

			g.Expect(avgTransformTime).To(BeNumerically("<", 1*time.Millisecond))
		})

		t.Run("options processing overhead", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			iterations := 1000

			start := time.Now()
			for i := 0; i < iterations; i++ {
				receiveSettings := pubsub.ReceiveSettings{
					NumGoroutines:          10,
					MaxOutstandingMessages: 100,
					MaxOutstandingBytes:    1024 * 1024,
				}

				_, err := google.NewGoogleSubscriber(setup.Client,
					"test-subscription",
					google.WithReceiveSettings(receiveSettings),
					google.WithProcessingTimeout(300*time.Second),
					google.WithParseAttributes(true),
					google.WithProcessingTimeoutHandler(func(ctx context.Context, msg *pubsub.Message) {
						msg.Nack()
					}))
				g.Expect(err).NotTo(HaveOccurred())
			}
			duration := time.Since(start)

			averageTime := duration / time.Duration(iterations)
			t.Logf("Options processing benchmark: %d subscribers created in %v (avg: %v per subscriber)",
				iterations, duration, averageTime)

			g.Expect(averageTime).To(BeNumerically("<", 1*time.Millisecond))
		})
	})

	t.Run("configuration validation", func(t *testing.T) {
		t.Run("should validate SubscriberOptions with nil timeout handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithProcessingTimeoutHandler(nil))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("should validate SubscriberOptions with zero timeout", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithProcessingTimeout(0))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("should validate invalid timeout values", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			testCases := []struct {
				name    string
				timeout time.Duration
				valid   bool
			}{
				{
					name:    "negative timeout",
					timeout: -1 * time.Second,
					valid:   true, // Our code should accept this (treated as no timeout)
				},
				{
					name:    "zero timeout",
					timeout: 0,
					valid:   true, // Disables timeout
				},
				{
					name:    "very small timeout",
					timeout: 1 * time.Nanosecond,
					valid:   true,
				},
				{
					name:    "normal timeout",
					timeout: 30 * time.Second,
					valid:   true,
				},
				{
					name:    "very large timeout",
					timeout: 24 * time.Hour,
					valid:   true,
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					subscriber, err := google.NewGoogleSubscriber(setup.Client,
						"test-subscription",
						google.WithProcessingTimeout(tc.timeout))

					if tc.valid {
						g.Expect(err).NotTo(HaveOccurred())
						g.Expect(subscriber).NotTo(BeNil())
					} else {
						g.Expect(err).To(HaveOccurred())
					}
				})
			}
		})

		t.Run("should validate SubscriberOptions with empty ReceiveSettings", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			emptySettings := pubsub.ReceiveSettings{}
			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithReceiveSettings(emptySettings))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("should validate SubscriberOptions with ParseAttributes flag", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())

			subscriber2, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithParseAttributes(false))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber2).NotTo(BeNil())
		})

		t.Run("should validate combined SubscriberOptions", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)

			settings := pubsub.ReceiveSettings{
				NumGoroutines:          5,
				MaxOutstandingMessages: 100,
			}

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				"test-subscription",
				google.WithReceiveSettings(settings),
				google.WithProcessingTimeout(60*time.Second),
				google.WithParseAttributes(true),
				google.WithProcessingTimeoutHandler(func(ctx context.Context, msg *pubsub.Message) {
					// Custom timeout handler
				}))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})
	})

}
