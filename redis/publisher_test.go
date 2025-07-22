package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lithammer/shortuuid/v3"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	rpub "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
)

func expectNbMessages(g Gomega, client *redis.Client, stream string, nb int64, timeout time.Duration) {
	g.Eventually(func() int64 {
		v, err := client.XLen(context.Background(), stream).Result()
		if err != nil {
			panic(err)
		}
		return v
	}, timeout).Should(Equal(nb))
}

func simpleMarshaller(msg *message.Message) map[string]interface{} {
	values := make(map[string]interface{})
	for k, v := range msg.Metadata {
		values[k] = v
	}
	if payloadMap, ok := msg.Payload.(map[string]interface{}); ok {
		for k, v := range payloadMap {
			values[k] = v
		}
		return values
	}
	panic("payload must be map[string]interface{}")
}

type redisPublisherTestSetup struct {
	client *redis.Client
}

func setupRedisPublisher(t *testing.T) *redisPublisherTestSetup {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:29379",
	})

	t.Cleanup(func() {
		client.Close()
	})

	return &redisPublisherTestSetup{
		client: client,
	}
}

func TestRedisPublisher(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		testStream := publisher.Destination("test-stream")

		_, err := rpub.NewRedisPublisher(nil,
			publisher.ConstantDestination(testStream),
			simpleMarshaller)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("should return an error if the routing function is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisPublisher(t)

		_, err := rpub.NewRedisPublisher(setup.client,
			nil,
			simpleMarshaller)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("routing function is required"))
	})

	t.Run("should return an error if the marshaller is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisPublisher(t)
		testStream := publisher.Destination("test-stream")

		_, err := rpub.NewRedisPublisher(setup.client,
			publisher.ConstantDestination(testStream),
			nil)
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("payloadMarshaller is required"))
	})

	t.Run("should use the routing function to determine the target stream", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisPublisher(t)
		stream1 := publisher.Destination("stream-1-" + shortuuid.New())
		stream2 := publisher.Destination("stream-2-" + shortuuid.New())

		routingFn := func(msg *message.Message) (publisher.Destination, error) {
			if msg.Metadata["destination"] == "stream2" {
				return stream2, nil
			}
			return stream1, nil
		}
		pub, err := rpub.NewRedisPublisher(setup.client,
			routingFn,
			simpleMarshaller)
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		err = pubEngine.Publish(message.NewMessage(context.Background(), map[string]interface{}{
			"key": "value1",
		}).WithMetadata("destination", "stream2"))
		g.Expect(err).NotTo(HaveOccurred())
		err = pubEngine.Publish(message.NewMessage(context.Background(), map[string]interface{}{
			"key": "value2",
		}))
		g.Expect(err).NotTo(HaveOccurred())

		expectNbMessages(g, setup.client, string(stream1), 1, 1*time.Second)
		expectNbMessages(g, setup.client, string(stream2), 1, 1*time.Second)
	})

	t.Run("should send all the messages", func(t *testing.T) {
		g := NewGomegaWithT(t)
		setup := setupRedisPublisher(t)
		stream := publisher.Destination("stream-" + shortuuid.New())
		routingFn := publisher.ConstantDestination(stream)
		pub, err := rpub.NewRedisPublisher(setup.client,
			routingFn,
			simpleMarshaller)
		g.Expect(err).NotTo(HaveOccurred())

		pubEngine := publisher.NewPublishingEngine(pub)
		expectedMessages := 10
		for i := 0; i < expectedMessages; i++ {
			err = pubEngine.Publish(message.NewMessage(context.Background(),
				map[string]interface{}{
					"key": "value",
				}))
			g.Expect(err).NotTo(HaveOccurred())
		}

		expectNbMessages(g, setup.client, string(stream), int64(expectedMessages), 5*time.Second)
	})

	t.Run("configuration options", func(t *testing.T) {
		t.Run("WithIDGenerator should use custom ID generation", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisPublisher(t)
			stream := publisher.Destination("test-stream-" + shortuuid.New())

			// Redis stream IDs must be in format timestamp-sequence
			customID := fmt.Sprintf("%d-0", time.Now().UnixMilli())
			idGenerator := func(msg *message.Message) (string, error) {
				return customID, nil
			}

			pub, err := rpub.NewRedisPublisher(setup.client,
				publisher.ConstantDestination(stream),
				simpleMarshaller,
				rpub.WithIDGenerator(idGenerator))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), map[string]interface{}{"test": "value"})
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			// Verify the message was published with custom ID
			messages, err := setup.client.XRead(context.Background(), &redis.XReadArgs{
				Streams: []string{string(stream), "0"},
			}).Result()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(messages).To(HaveLen(1))
			g.Expect(messages[0].Messages).To(HaveLen(1))
			g.Expect(messages[0].Messages[0].ID).To(Equal(customID))
		})

		t.Run("WithMaxlen should configure maxlen option", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisPublisher(t)
			stream := publisher.Destination("test-stream-" + shortuuid.New())

			// Test that WithMaxlen option is accepted without error
			pub, err := rpub.NewRedisPublisher(setup.client,
				publisher.ConstantDestination(stream),
				simpleMarshaller,
				rpub.WithMaxlen(3))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pub).NotTo(BeNil())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), map[string]interface{}{"test": "value"})
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("WithApprox should configure approx option", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisPublisher(t)
			stream := publisher.Destination("test-stream-" + shortuuid.New())

			// Test that WithApprox option is accepted without error
			pub, err := rpub.NewRedisPublisher(setup.client,
				publisher.ConstantDestination(stream),
				simpleMarshaller,
				rpub.WithMaxlen(2),
				rpub.WithApprox(true))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(pub).NotTo(BeNil())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), map[string]interface{}{"test": "value"})
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("WithMetadataMarshaller should transform metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisPublisher(t)
			stream := publisher.Destination("test-stream-" + shortuuid.New())

			customMarshaller := func(msg *message.Message) map[string]interface{} {
				values := make(map[string]interface{})
				for k, v := range msg.Metadata {
					values["meta_"+k] = v
				}
				return values
			}

			pub, err := rpub.NewRedisPublisher(setup.client,
				publisher.ConstantDestination(stream),
				simpleMarshaller,
				rpub.WithMetadataMarshaller(customMarshaller))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), map[string]interface{}{"test": "value"})
			msg = msg.WithMetadata("user", "john").WithMetadata("action", "login")
			
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			// Verify metadata was transformed with prefix
			messages, err := setup.client.XRead(context.Background(), &redis.XReadArgs{
				Streams: []string{string(stream), "0"},
			}).Result()
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(messages).To(HaveLen(1))
			g.Expect(messages[0].Messages).To(HaveLen(1))
			
			values := messages[0].Messages[0].Values
			g.Expect(values).To(HaveKey("meta_user"))
			g.Expect(values).To(HaveKey("meta_action"))
			g.Expect(values["meta_user"]).To(Equal("john"))
			g.Expect(values["meta_action"]).To(Equal("login"))
		})

		t.Run("should handle nil metadata marshaller gracefully", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisPublisher(t)
			stream := publisher.Destination("test-stream-" + shortuuid.New())

			pub, err := rpub.NewRedisPublisher(setup.client,
				publisher.ConstantDestination(stream),
				simpleMarshaller,
				rpub.WithMetadataMarshaller(nil))
			g.Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			msg := message.NewMessage(context.Background(), map[string]interface{}{"test": "value"})
			msg = msg.WithMetadata("user", "john")
			
			err = pubEngine.Publish(msg)
			g.Expect(err).NotTo(HaveOccurred())

			expectNbMessages(g, setup.client, string(stream), 1, 2*time.Second)
		})
	})
}
