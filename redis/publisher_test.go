package redis_test

import (
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	rpub "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
	"time"
)

func ExpectNbMessages(client *redis.Client, stream string, nb int64, timeout time.Duration) {
	Eventually(func() int64 {
		v, err := client.XLen(context.Background(), stream).Result()
		if err != nil {
			panic(err)
		}
		return v
	}, timeout).Should(Equal(nb))
}

func SimpleMarshaller(msg *message.Message) (map[string]interface{}, error) {
	values := make(map[string]interface{})
	for k, v := range msg.Metadata {
		values[k] = v
	}
	if payloadMap, ok := msg.Payload.(map[string]interface{}); ok {
		for k, v := range payloadMap {
			values[k] = v
		}
		return values, nil
	}
	panic("payload must be map[string]interface{}")
}

var testStream = publisher.Destination("test-stream")

var _ = Describe("Redis Publisher", Ordered, func() {
	var client *redis.Client

	BeforeAll(func() {
		client = redis.NewClient(&redis.Options{
			Addr: "localhost:29379",
		})
	})

	It("should return an error if the client is missing", func() {
		_, err := rpub.NewRedisPublisher(nil,
			publisher.ConstantDestination(testStream),
			SimpleMarshaller)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("client is required"))
	})

	It("should return an error if the routing function is missing", func() {
		_, err := rpub.NewRedisPublisher(client,
			nil,
			SimpleMarshaller)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("routing function is required"))
	})

	It("should return an error if the marshaller is missing", func() {
		_, err := rpub.NewRedisPublisher(client,
			publisher.ConstantDestination(testStream),
			nil)
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("marshaller is required"))
	})

	It("should use the routing function to determine the target stream", func() {
		stream1 := publisher.Destination("stream-1-" + shortuuid.New())
		stream2 := publisher.Destination("stream-2-" + shortuuid.New())

		routingFn := func(msg *message.Message) (publisher.Destination, error) {
			if msg.ID == "id-2" {
				return stream2, nil
			}
			return stream1, nil
		}
		pub, err := rpub.NewRedisPublisher(client,
			routingFn,
			SimpleMarshaller)

		pubEngine := publisher.NewPublishingEngine(pub)
		err = pubEngine.Publish(message.NewMessage(context.Background(), "id-1", map[string]interface{}{
			"key": "value1",
		}))
		Expect(err).NotTo(HaveOccurred())
		err = pubEngine.Publish(message.NewMessage(context.Background(), "id-2", map[string]interface{}{
			"key": "value2",
		}))
		Expect(err).NotTo(HaveOccurred())

		ExpectNbMessages(client, string(stream1), 1, 1*time.Second)
		ExpectNbMessages(client, string(stream2), 1, 1*time.Second)
	})

	It("should send all the messages", func() {
		stream := publisher.Destination("stream-" + shortuuid.New())
		routingFn := publisher.ConstantDestination(stream)
		pub, err := rpub.NewRedisPublisher(client,
			routingFn,
			SimpleMarshaller)

		pubEngine := publisher.NewPublishingEngine(pub)
		expectedMessages := 10
		for i := 0; i < expectedMessages; i++ {
			err = pubEngine.Publish(message.NewMessage(context.Background(),
				fmt.Sprintf("id-%d", i+1),
				map[string]interface{}{
					"key": "value",
				}))
			Expect(err).NotTo(HaveOccurred())
		}

		ExpectNbMessages(client, string(stream), int64(expectedMessages), 5*time.Second)

	})
})
