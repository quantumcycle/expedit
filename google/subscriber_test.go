package google_test

import (
	"cloud.google.com/go/pubsub/v2"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	t.Run("should return an error matching ErrSubscriptionNotFound if the subscription doesnt exist", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		subscriber, err := google.NewGoogleSubscriber(setup.Client, "non-existing-subscription")
		g.Expect(err).NotTo(HaveOccurred())

		errCh := StartReceive(t, ctx, subscriber, func(msg *message.Message) error { return nil })

		g.Eventually(errCh, 5*time.Second).Should(Receive(MatchError(google.ErrSubscriptionNotFound)))
	})

	t.Run("should receives all messages sent to the subscription", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		var msgCount atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			msgCount.Add(1)
			return nil
		})

		expectedMsgCount := 10
		for i := 0; i < expectedMsgCount; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
		}
		g.Eventually(func() int {
			return int(msgCount.Load())
		}, 5*time.Second).Should(Equal(expectedMsgCount))
	})

	t.Run("when parse attributes is enabled", func(t *testing.T) {
		cases := []struct {
			name     string
			attr     string
			expected interface{}
		}{
			{"should convert bool", "true", true},
			{"should convert float", "10.231", 10.231},
			{"should convert integer", "10", int64(10)},
			{"should convert 0 and 1 to integers, not bools", "1", int64(1)},
			{"should convert zero to an integer, not a bool", "0", int64(0)},
			{"should keep string as is", "hello", "hello"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				g := NewGomegaWithT(t)
				setup := setupGoogleSubscriber(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

				subscriber, err := google.NewGoogleSubscriber(setup.Client,
					subscription.Name, google.WithParseAttributes(true))
				g.Expect(err).NotTo(HaveOccurred())

				var att1Val atomic.Value
				StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
					if v := msg.Metadata["att1"]; v != nil {
						att1Val.Store(v)
					}
					return nil
				})

				setup.Topic.PublishBytes(ctx, []byte("payload"), map[string]string{"att1": tc.attr})

				g.Eventually(att1Val.Load, 5*time.Second).Should(Equal(tc.expected))
			})
		}
	})

	t.Run("should receive the message ids that were published", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		var idMu sync.Mutex
		idReceived := make(map[string]bool)
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			idMu.Lock()
			defer idMu.Unlock()
			idReceived[msg.ID] = true
			return nil
		})

		expectedIds := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			id := setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
			expectedIds = append(expectedIds, id)
		}

		g.Eventually(func() []string {
			idMu.Lock()
			defer idMu.Unlock()
			keys := make([]string, 0, len(idReceived))
			for k := range idReceived {
				keys = append(keys, k)
			}
			return keys
		}, 5*time.Second).Should(ContainElements(expectedIds))
	})

	t.Run("should nack when the handler returns an error and redeliver the message", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		var mu sync.Mutex
		var deliveries []string
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			mu.Lock()
			defer mu.Unlock()
			deliveries = append(deliveries, msg.ID)
			if len(deliveries) == 1 {
				return errors.New("processing failed")
			}
			return nil
		})

		id := setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), deliveries...)
		}, 10*time.Second).Should(Equal([]string{id, id}))
	})

	t.Run("should relay the ack or nack to gcp messages", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		var nackDone atomic.Bool
		var processCount atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			processCount.Add(1)
			if nackDone.CompareAndSwap(false, true) {
				return errors.New("nack the first message")
			}
			return nil
		})

		nbMsg := 100
		for i := 0; i < nbMsg; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload1"), nil)
		}

		g.Eventually(func() int {
			return int(processCount.Load())
		}, 10*time.Second).Should(Equal(nbMsg + 1))
	})

	t.Run("should cancel the message context once the processing is done", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCtxCh := make(chan context.Context, 1)
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			select {
			case msgCtxCh <- msg.Context():
			default:
			}
			return nil
		})

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		var msgCtx context.Context
		g.Eventually(msgCtxCh, 5*time.Second).Should(Receive(&msgCtx))
		g.Eventually(msgCtx.Done(), 5*time.Second).Should(BeClosed())
	})

	t.Run("when the receive context is cancelled", func(t *testing.T) {
		t.Run("should return nil only once the in-flight handlers returned, without cancelling their context", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
			g.Expect(err).NotTo(HaveOccurred())

			receiveCtx, stopReceive := context.WithCancel(ctx)
			defer stopReceive()

			started := make(chan struct{})
			release := make(chan struct{})
			var handlerReturned atomic.Bool
			var ctxErrAtReturn atomic.Value
			errCh := StartReceive(t, receiveCtx, subscriber, func(msg *message.Message) error {
				close(started)
				<-release
				ctxErrAtReturn.Store(fmt.Sprint(msg.Context().Err()))
				handlerReturned.Store(true)
				return nil
			})

			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
			g.Eventually(started, 5*time.Second).Should(BeClosed())

			stopReceive()

			// Receive waits for the in-flight handler
			g.Consistently(errCh, 500*time.Millisecond).ShouldNot(Receive())

			close(release)

			var err2 error
			g.Eventually(errCh, 5*time.Second).Should(Receive(&err2))
			g.Expect(err2).NotTo(HaveOccurred())
			g.Expect(handlerReturned.Load()).To(BeTrue())
			g.Expect(ctxErrAtReturn.Load()).To(Equal("<nil>"))
		})

		t.Run("should ack the in-flight message, so it is not redelivered", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
			g.Expect(err).NotTo(HaveOccurred())

			receiveCtx, stopReceive := context.WithCancel(ctx)
			defer stopReceive()

			started := make(chan struct{})
			release := make(chan struct{})
			errCh := StartReceive(t, receiveCtx, subscriber, func(msg *message.Message) error {
				close(started)
				<-release
				return nil
			})

			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
			g.Eventually(started, 5*time.Second).Should(BeClosed())
			stopReceive()
			close(release)
			g.Eventually(errCh, 5*time.Second).Should(Receive(BeNil()))

			// A second receiver on the same subscription gets nothing, because the message was acked
			var redelivered atomic.Int32
			subscriber2, err := google.NewGoogleSubscriber(setup.Client, subscription.Name)
			g.Expect(err).NotTo(HaveOccurred())
			ctx2, cancel2 := context.WithCancel(ctx)
			defer cancel2()
			errCh2 := StartReceive(t, ctx2, subscriber2, func(msg *message.Message) error {
				redelivered.Add(1)
				return nil
			})
			g.Consistently(redelivered.Load, 2*time.Second).Should(BeZero())
			cancel2()
			g.Eventually(errCh2, 5*time.Second).Should(Receive(BeNil()))
		})
	})

	t.Run("when the processing timeout is reached", func(t *testing.T) {
		t.Run("should put a deadline on the message context, nack and redeliver the message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithProcessingTimeout(500*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var deliveries []string
			var hasDeadline []bool
			var firstErr error
			StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
				_, ok := msg.Context().Deadline()
				mu.Lock()
				deliveries = append(deliveries, msg.ID)
				hasDeadline = append(hasDeadline, ok)
				first := len(deliveries) == 1
				mu.Unlock()
				if first {
					<-msg.Context().Done()
					mu.Lock()
					firstErr = msg.Context().Err()
					mu.Unlock()
					return msg.Context().Err()
				}
				return nil
			})

			id := setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

			g.Eventually(func() []string {
				mu.Lock()
				defer mu.Unlock()
				return append([]string(nil), deliveries...)
			}, 10*time.Second).Should(Equal([]string{id, id}))

			mu.Lock()
			defer mu.Unlock()
			g.Expect(hasDeadline).To(Equal([]bool{true, true}))
			g.Expect(firstErr).To(MatchError(context.DeadlineExceeded))
		})

		t.Run("should not put a deadline on the message context when the timeout is 0", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGoogleSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

			subscriber, err := google.NewGoogleSubscriber(setup.Client,
				subscription.Name, google.WithProcessingTimeout(0))
			g.Expect(err).NotTo(HaveOccurred())

			hasDeadline := make(chan bool, 1)
			StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
				_, ok := msg.Context().Deadline()
				select {
				case hasDeadline <- ok:
				default:
				}
				return nil
			})

			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

			g.Eventually(hasDeadline, 5*time.Second).Should(Receive(BeFalse()))
		})
	})

	t.Run("should accept WithReceiveSettings option and process messages correctly", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		receiveSettings := pubsub.ReceiveSettings{
			NumGoroutines:          3,
			MaxOutstandingMessages: 50,
			MaxOutstandingBytes:    1024 * 1024,
		}

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithReceiveSettings(receiveSettings))
		g.Expect(err).NotTo(HaveOccurred())

		var processedCount atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			processedCount.Add(1)
			return nil
		})

		for i := 0; i < 20; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
		}

		g.Eventually(func() int {
			return int(processedCount.Load())
		}, 10*time.Second).Should(Equal(20))
	})

	t.Run("should bound the concurrency with MaxOutstandingMessages", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithReceiveSettings(pubsub.ReceiveSettings{MaxOutstandingMessages: 2}))
		g.Expect(err).NotTo(HaveOccurred())

		var inFlight, maxInFlight, processed atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			n := inFlight.Add(1)
			for {
				m := maxInFlight.Load()
				if n <= m || maxInFlight.CompareAndSwap(m, n) {
					break
				}
			}
			// Give the other messages the opportunity to be handled at the same time
			time.Sleep(50 * time.Millisecond)
			inFlight.Add(-1)
			processed.Add(1)
			return nil
		})

		for i := 0; i < 10; i++ {
			setup.Topic.PublishBytes(ctx, []byte("payload"), nil)
		}

		g.Eventually(processed.Load, 10*time.Second).Should(BeEquivalentTo(10))
		g.Expect(maxInFlight.Load()).To(BeNumerically("<=", 2))
	})

	t.Run("should accept subscriber options with various edge case values", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		receiveSettings := pubsub.ReceiveSettings{
			NumGoroutines:          -1,
			MaxOutstandingMessages: -1,
			MaxOutstandingBytes:    -1,
		}

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithReceiveSettings(receiveSettings),
			google.WithProcessingTimeout(-1*time.Second),
			google.WithParseAttributes(true))
		g.Expect(err).NotTo(HaveOccurred())

		var processedCount atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			processedCount.Add(1)
			return nil
		})

		setup.Topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() int {
			return int(processedCount.Load())
		}, 5*time.Second).Should(Equal(1))
	})

	t.Run("should process many messages with parsed attributes", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		subscription := setup.Topic.CreateTestSubscription(ctx, UniqueSubscriptionName("test-subscription"), false)

		subscriber, err := google.NewGoogleSubscriber(setup.Client,
			subscription.Name,
			google.WithParseAttributes(true))
		g.Expect(err).NotTo(HaveOccurred())

		iterations := 1000
		var processedCount, badAttributes atomic.Int32
		StartReceive(t, ctx, subscriber, func(msg *message.Message) error {
			_, iterationOk := msg.Metadata["iteration"].(int64)
			benchmark, benchmarkOk := msg.Metadata["benchmark"].(bool)
			if !iterationOk || !benchmarkOk || !benchmark {
				badAttributes.Add(1)
			}
			processedCount.Add(1)
			return nil
		})

		for i := 0; i < iterations; i++ {
			attrs := map[string]string{
				"iteration": fmt.Sprintf("%d", i),
				"benchmark": "true",
			}
			setup.Topic.PublishBytes(ctx, []byte("benchmark message"), attrs)
		}

		g.Eventually(func() int {
			return int(processedCount.Load())
		}, 30*time.Second).Should(Equal(iterations))
		g.Expect(badAttributes.Load()).To(BeZero())
	})

	t.Run("configuration validation", func(t *testing.T) {
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
				google.WithParseAttributes(true))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})
	})

}
