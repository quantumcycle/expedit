package redis_test

import (
	"context"
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
}