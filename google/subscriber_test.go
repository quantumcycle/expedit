package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
)

func asyncCountMessages(count *int, ch <-chan *message.Message, duration time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				*count++
			}
		}
	}()
}

type googleSubscriberTestSetup struct {
	client    *pubsub.Client
	emuClient *emulator.PubsubTestClient
	topic     *emulator.TestTopic
}

func setupGoogleSubscriber(t *testing.T) *googleSubscriberTestSetup {
	// Set the emulator host for Google PubSub
	t.Setenv("PUBSUB_EMULATOR_HOST", "localhost:29085")
	
	ctx := context.Background()
	emuClient := emulator.NewTestClient(ctx, "test-project")
	topic := emuClient.CreateTestTopic(ctx, "test-topic")

	client, err := pubsub.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("failed to create pubsub client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return &googleSubscriberTestSetup{
		client:    client,
		emuClient: emuClient,
		topic:     topic,
	}
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

		subscriber, err := google.NewGoogleSubscriber(setup.client, "non-existing-subscription")
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
		subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(setup.client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		msgCount := 0
		asyncCountMessages(&msgCount, msgCh, 5*time.Second)

		expectedMsgCount := 10
		for i := 0; i < expectedMsgCount; i++ {
			setup.topic.PublishBytes(ctx, []byte("payload"), nil)
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
			subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

			subscriber, err := google.NewGoogleSubscriber(setup.client,
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
			setup.topic.PublishBytes(ctx, []byte("payload"), attrs)

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
			subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

			subscriber, err := google.NewGoogleSubscriber(setup.client,
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
			setup.topic.PublishBytes(ctx, []byte("payload"), attrs)

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
			subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

			subscriber, err := google.NewGoogleSubscriber(setup.client,
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
			setup.topic.PublishBytes(ctx, []byte("payload"), attrs)

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
			subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

			subscriber, err := google.NewGoogleSubscriber(setup.client,
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
			setup.topic.PublishBytes(ctx, []byte("payload"), attrs)

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
		subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

		timeoutOccurred := false
		subscriber, err := google.NewGoogleSubscriber(setup.client,
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

		setup.topic.PublishBytes(ctx, []byte("payload"), nil)

		g.Eventually(func() bool {
			return timeoutOccurred && nackOccurred
		}, 5*time.Second).Should(Equal(true))
	})

	t.Run("should receive the message ids that were published", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGoogleSubscriber(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(setup.client, subscription.Name)
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
			id := setup.topic.PublishBytes(ctx, []byte("payload"), nil)
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
		subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(setup.client, subscription.Name)
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
			setup.topic.PublishBytes(ctx, []byte("payload1"), nil)
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
		subscription := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(setup.client, subscription.Name)
		g.Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		setup.topic.PublishBytes(ctx, []byte("payload"), nil)

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
}