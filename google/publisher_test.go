package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
)

func expectNbMessages[T any](g Gomega, ch <-chan T, nb int, timeout time.Duration) {
	g.Eventually(func() int {
		return len(ch)
	}, timeout).Should(Equal(nb))
}

type googlePublisherTestSetup struct {
	client    *pubsub.Client
	emuClient *emulator.PubsubTestClient
	topic     *emulator.TestTopic
}

func setupGooglePublisher(t *testing.T) *googlePublisherTestSetup {
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

	return &googlePublisherTestSetup{
		client:    client,
		emuClient: emuClient,
		topic:     topic,
	}
}

func TestGooglePublisher(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		_, err := google.NewGooglePublisher(
			nil,
			publisher.ConstantDestination(setup.topic.Name))
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("should return an error if the routing function is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		_, err := google.NewGooglePublisher(
			setup.client,
			nil)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("routing function is required"))
	})

	t.Run("should return an error when trying to publish a message with an ID", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		pub, err := google.NewGooglePublisher(setup.client,
			publisher.ConstantDestination(setup.topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		msg.ID = "something"
		err = pubEngine.Publish(msg)
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("should return the generated ID from pubsub in the message pointer", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)

		pub, err := google.NewGooglePublisher(setup.client,
			publisher.ConstantDestination(setup.topic.Name),
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		msg := message.NewMessage(context.Background(), []byte("msg1"))
		err = pubEngine.Publish(msg)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(msg.ID).NotTo(BeEmpty())
	})

	t.Run("should use the routing function to determine the target topic", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupGooglePublisher(t)
		ctx := context.Background()

		sub1 := setup.topic.CreateTestSubscription(ctx, "test-subscription", false)
		msgs1 := sub1.MessageChannel(context.Background(), 1)

		topic2 := setup.emuClient.CreateTestTopic(ctx, "test-topic-2")
		sub2 := topic2.CreateTestSubscription(ctx, "test-subscription-2", false)
		msgs2 := sub2.MessageChannel(context.Background(), 1)

		routingFn := func(msg *message.Message) (publisher.Destination, error) {
			if msg.Metadata["destination"] == "topic2" {
				return topic2.Name, nil
			}
			return setup.topic.Name, nil
		}
		pub, err := google.NewGooglePublisher(setup.client,
			routingFn,
			google.WithOrderingKeyProvider(func(msg *message.Message) string {
				return "test-key"
			}))
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg1")).WithMetadata("destination", "topic2"))
		g.Expect(err).NotTo(HaveOccurred())
		err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("msg2")))
		g.Expect(err).NotTo(HaveOccurred())

		expectNbMessages(g, msgs1, 1, 1*time.Second)
		expectNbMessages(g, msgs2, 1, 1*time.Second)
	})

	t.Run("when using attributes providers", func(t *testing.T) {
		t.Run("should add the attributes to the pubsub message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)
			ctx := context.Background()

			sub := setup.topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageChannel(context.Background(), 1)

			pub, err := google.NewGooglePublisher(setup.client,
				publisher.ConstantDestination(setup.topic.Name),
				google.WithAttributesProvider(google.MetadataAsAttributes))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			err = pubEngine.Publish(message.NewMessage(context.Background(), []byte("message1")).WithMetadata("key1", "value1"))
			g.Expect(err).NotTo(HaveOccurred())

			receivedMsg := <-msgs
			g.Expect(receivedMsg.Attributes).To(HaveKeyWithValue("key1", "value1"))
		})
	})

	t.Run("when using ordering keys", func(t *testing.T) {
		t.Run("should receive the messages in order", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupGooglePublisher(t)
			ctx := context.Background()

			sub := setup.topic.CreateTestSubscription(ctx, "test-subscription", true)
			msgs := sub.MessageDataChannel(context.Background(), 100)

			pub, err := google.NewGooglePublisher(setup.client,
				publisher.ConstantDestination(setup.topic.Name),
				google.WithOrderingKeyProvider(func(msg *message.Message) string {
					return "test-key"
				}))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			var sentMsgs []interface{}
			for i := 0; i < 100; i++ {
				msg := fmt.Sprintf("message %d", i+1)
				err = pubEngine.Publish(message.NewMessage(context.Background(), []byte(msg)))
				g.Expect(err).NotTo(HaveOccurred())
				sentMsgs = append(sentMsgs, msg)
			}

			expectNbMessages(g, msgs, 100, 10*time.Second)

			close(msgs)
			var received []string
			for s := range msgs {
				received = append(received, s)
			}
			g.Expect(received).To(HaveExactElements(sentMsgs))
		})
	})
}