package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	"time"
)

func ExpectNbMessages[T any](ch <-chan T, nb int, timeout time.Duration) {
	Eventually(func() int {
		return len(ch)
	}, timeout).Should(Equal(nb))
}

var _ = Describe("Google Publisher", Ordered, func() {
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
		_, err := google.NewGooglePublisher(
			nil,
			publisher.ConstantDestination(topic.Name))
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("client is required"))
	})

	It("should return an error if the routing function is missing", func() {
		_, err := google.NewGooglePublisher(
			client,
			nil)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("routing function is required"))
	})

	It("should return an error when trying to publish a message with an ID", func() {
		pub, err := google.NewGooglePublisher(client,
			publisher.ConstantDestination(topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				//use the same key for all messages, so they should be received in order
				return "test-key"
			}))

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		msg.ID = "something"
		err = pubEngine.Publish(msg)
		Expect(err).To(HaveOccurred())
	})

	It("should return the generated ID from pubsub in the message pointer", func() {
		pub, err := google.NewGooglePublisher(client,
			publisher.ConstantDestination(topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				//use the same key for all messages, so they should be received in order
				return "test-key"
			}))

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		err = pubEngine.Publish(msg)
		Expect(err).NotTo(HaveOccurred())
		Expect(msg.ID).NotTo(BeEmpty())
	})

	It("should use the routing function to determine the target topic", func() {
		ctx := context.Background()

		sub1 := topic.CreateTestSubscription(ctx, "test-subscription", false)
		msgs1 := sub1.MessageChannel(context.Background(), 1)

		topic2 := emuClient.CreateTestTopic(ctx, "test-topic-2")
		sub2 := topic2.CreateTestSubscription(ctx, "test-subscription-2", false)
		msgs2 := sub2.MessageChannel(context.Background(), 1)

		routingFn := func(msg *message.Message) (publisher.Destination, error) {
			if msg.Metadata["destination"] == "topic2" {
				return topic2.Name, nil
			}
			return topic.Name, nil
		}
		pub, err := google.NewGooglePublisher(client,
			routingFn,
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				//use the same key for all messages, so they should be received in order
				return "test-key"
			}))

		pubEngine := publisher.NewPublishingEngine(pub)
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg1")).WithMetadata("destination", "topic2"))
		Expect(err).NotTo(HaveOccurred())
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg2")))
		Expect(err).NotTo(HaveOccurred())

		ExpectNbMessages(msgs1, 1, 1*time.Second)
		ExpectNbMessages(msgs2, 1, 1*time.Second)
	})

	When("using attributes providers", func() {
		It("should add the attributes to the pubsub message", func() {
			ctx := context.Background()
			sub := topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageChannel(context.Background(), 1)

			pub, err := google.NewGooglePublisher(client,
				publisher.ConstantDestination(topic.Name),
				google.WithAttributesProvider(google.MetadataAsAttributes))
			pubEngine := publisher.NewPublishingEngine(pub)
			err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("message1")).WithMetadata("key1", "value1"))
			Expect(err).NotTo(HaveOccurred())

			receivedMsg := <-msgs
			Expect(receivedMsg.Attributes).To(HaveKeyWithValue("key1", "value1"))
		})
	})

	When("using ordering keys", func() {
		It("should receive the messages in order", func() {
			ctx := context.Background()
			sub := topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageDataChannel(context.Background(), 100)

			pub, err := google.NewGooglePublisher(client,
				publisher.ConstantDestination(topic.Name),
				//Same key for every message, so they should be ALL received in order
				google.WithOrderingKeyProvider(func(msg *message.Message) string {
					//use the same key for all messages, so they should be received in order
					return "test-key"
				}))
			pubEngine := publisher.NewPublishingEngine(pub)
			var sentMsgs []interface{}
			for i := 0; i < 100; i++ {
				msg := fmt.Sprintf("message %d", i+1)
				err = pubEngine.Publish(message.NewMessage(context.Background(), []byte(msg)))
				Expect(err).NotTo(HaveOccurred())
				sentMsgs = append(sentMsgs, msg)
			}

			ExpectNbMessages(msgs, 100, 10*time.Second)

			close(msgs)
			var received []string
			for s := range msgs {
				received = append(received, s)
			}
			Expect(received).To(HaveExactElements(sentMsgs))
		})
	})
})
