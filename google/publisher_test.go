package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	"time"
)

func ExpectNbMessages(ch <-chan *pubsub.Message, nb int, timeout time.Duration) {
	Eventually(func() int {
		return len(ch)
	}, timeout).Should(Equal(nb))
}

var _ = Describe("Publisher", Ordered, func() {
	var emuClient *emulator.PubsubTestClient
	var topic *emulator.TestTopic

	BeforeEach(func() {
		ctx := context.Background()
		emuClient = emulator.NewTestClient(ctx, "test-project")
		topic = emuClient.CreateTestTopic(ctx, "test-topic")
	})

	It("should return an error if the routing function is missing", func() {
		_, err := google.NewGooglePublisher(nil, google.PublisherOption{})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("routing function is required"))
	})

	Describe("with a valid client", func() {
		var client *pubsub.Client

		BeforeEach(func() {
			var err error
			client, err = pubsub.NewClient(context.Background(), "test-project")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should use the routing function to determine the target topic", func() {
			ctx := context.Background()

			sub1 := topic.CreateTestSubscription(ctx, "test-subscription", false)
			msgs1 := sub1.MessageChannel(context.Background(), 1)

			topic2 := emuClient.CreateTestTopic(ctx, "test-topic-2")
			sub2 := topic2.CreateTestSubscription(ctx, "test-subscription-2", false)
			msgs2 := sub2.MessageChannel(context.Background(), 1)

			pub, err := google.NewGooglePublisher(client, google.PublisherOption{
				RoutingFunc: func(msg *message.Message) (google.DestinationTopic, error) {
					if msg.ID == "id-2" {
						return topic2.Name, nil
					}
					return topic.Name, nil
				},
				OrderingKeyProvider: func(msg *message.Message) string {
					//use the same key for all messages, so they should be received in order
					return "test-key"
				},
			})

			pubEngine := publisher.NewPublishingEngine(pub)
			err = pubEngine.Publish(message.NewMessage(context.Background(), "id-1", []byte("msg1")))
			Expect(err).NotTo(HaveOccurred())
			err = pubEngine.Publish(message.NewMessage(context.Background(), "id-2", []byte("msg2")))
			Expect(err).NotTo(HaveOccurred())

			ExpectNbMessages(msgs1, 1, 1*time.Second)
			ExpectNbMessages(msgs2, 1, 1*time.Second)
		})

		When("using ordering keys", func() {
			It("should receive the messages in order", func() {
				ctx := context.Background()
				sub := topic.CreateTestSubscription(ctx, "test-subscription", true)
				msgs := sub.MessageChannel(context.Background(), 100)

				pub, err := google.NewGooglePublisher(client, google.PublisherOption{
					RoutingFunc: google.ConstantTopic(topic.Name),
					OrderingKeyProvider: func(msg *message.Message) string {
						//use the same key for all messages, so they should be received in order
						return "test-key"
					},
				})
				pubEngine := publisher.NewPublishingEngine(pub)
				var sentMsgs []interface{}
				for i := 0; i < 100; i++ {
					msg := fmt.Sprintf("message %d", i+1)
					err = pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte(msg)))
					Expect(err).NotTo(HaveOccurred())
					sentMsgs = append(sentMsgs, msg)
				}

				ExpectNbMessages(msgs, 100, 3*time.Second)

				close(msgs)
				var received []string
				for s := range msgs {
					received = append(received, string(s.Data))
				}
				Expect(received).To(HaveExactElements(sentMsgs))
			})
		})

	})

})
