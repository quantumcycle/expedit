package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	"time"
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

var _ = Describe("Google Subscriber", func() {
	var client *pubsub.Client
	var emuClient *emulator.PubsubTestClient
	var topic *emulator.TestTopic

	BeforeEach(func() {
		ctx := context.Background()
		emuClient = emulator.NewTestClient(ctx, "test-project")
		topic = emuClient.CreateTestTopic(ctx, "test-topic")

		var err error
		client, err = pubsub.NewClient(context.Background(), "test-project")
		Expect(err).NotTo(HaveOccurred())
	})

	It("should return an error if the client is missing", func() {
		_, err := google.NewGoogleSubscriber(nil,
			"test-subscription")
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("client is required"))
	})

	It("should return an error if the subscription doesnt exist", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		subscriber, err := google.NewGoogleSubscriber(client,
			"non-existing-subscription")
		Expect(err).NotTo(HaveOccurred())

		_, err = subscriber.Subscribe(ctx)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("subscription does not exist"))
	})

	It("should receives all messages sent to the subscription", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(client,
			subscription.Name)
		Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		msgCount := 0
		asyncCountMessages(&msgCount, msgCh, 5*time.Second)

		expectedMsgCount := 10
		for i := 0; i < expectedMsgCount; i++ {
			topic.PublishBytes(ctx, []byte("payload"), nil)
		}
		Eventually(func() int {
			return msgCount
		}, 3*time.Second).Should(Equal(expectedMsgCount))
	})

	It("should nack messages that are not ack/nacked after the processing timeout", func() {
		//We cannot query the emulator to see if the message was nacked, so the next best thing is to check if the
		//event handler was called.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

		timeoutOccurred := false
		subscriber, err := google.NewGoogleSubscriber(client,
			subscription.Name,
			google.WithProcessingTimeout(1*time.Second),
			google.WithProcessingTimeoutHandler(func(ctx context.Context, msg *pubsub.Message) {
				timeoutOccurred = true
			}))
		Expect(err).NotTo(HaveOccurred())

		nackOccurred := false
		msgCh, err := subscriber.Subscribe(ctx)
		defer subscriber.Close()
		Expect(err).NotTo(HaveOccurred())
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

		topic.PublishBytes(ctx, []byte("payload"), nil)

		Eventually(func() bool {
			return timeoutOccurred && nackOccurred
		}, 5*time.Second).Should(Equal(true))
	})

	It("should receive the message ids that were published", func() {
		//We cannot query the emulator to see if the message was nacked, so the next best thing is to check if the
		//event handler was called.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(client,
			subscription.Name)
		Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		defer subscriber.Close()
		Expect(err).NotTo(HaveOccurred())

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
			id := topic.PublishBytes(ctx, []byte("payload"), nil)
			expectedIds = append(expectedIds, id)
		}

		Eventually(func() []string {
			keys := make([]string, 0, len(idReceived))
			for k := range idReceived {
				keys = append(keys, k)
			}
			return keys
		}, 5*time.Second).Should(ContainElements(expectedIds))
	})

	It("should relay the ack or nack to gcp messages", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(client,
			subscription.Name)
		defer subscriber.Close()
		Expect(err).NotTo(HaveOccurred())

		//We're sending 100 messages
		//All messages will be acked, except the first one, which will be nacked and retried
		nackDone := false
		processCount := 0
		msgCh, err := subscriber.Subscribe(ctx)
		Expect(err).NotTo(HaveOccurred())
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
			topic.PublishBytes(ctx, []byte("payload1"), nil)
		}

		Eventually(func() int {
			return processCount
		}, 5*time.Second).Should(Equal(nbMsg + 1))
	})

	It("should cancel message context once the processing is done", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

		subscriber, err := google.NewGoogleSubscriber(client,
			subscription.Name)
		Expect(err).NotTo(HaveOccurred())

		msgCh, err := subscriber.Subscribe(ctx)
		Expect(err).NotTo(HaveOccurred())
		defer subscriber.Close()

		topic.PublishBytes(ctx, []byte("payload"), nil)

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
					Eventually(msgCtxDone, 3*time.Second).Should(BeClosed())
					waitCh <- true
				}
			}
		}()
		<-waitCh

		//Failsafe to make sure we asserted on the message context once
		Expect(processCount).To(Equal(1))
	})
})
