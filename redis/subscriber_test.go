package redis_test

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
	"strconv"
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
			case msg, ok := <-ch:
				if ok {
					fmt.Printf("Received message %v\n", msg.Payload)
					*count++
				}
			}
		}
	}()
}

func newStreamName() string {
	return fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
}

var _ = Describe("Redis Subscriber", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: "localhost:29379",
		})
	})

	It("should return an error if the client is missing", func() {
		_, err := subredis.NewRedisSubscriber(nil,
			newStreamName())
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("client is required"))
	})

	When("using consumer groups", func() {
		It("should return an error if the stream doesnt exist", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := fmt.Sprintf("non-existing-stream-%d", time.Now().UnixNano())

			subscriber, err := subredis.NewRedisSubscriber(client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			Expect(err).NotTo(HaveOccurred())

			_, err = subscriber.Subscribe(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(subredis.StreamDoesntExistErr))
		})

		It("should dispatch messages to different consumers", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := newStreamName()

			//Create 2 subscriber in the same consumer group. The messages should be distributed across the 2
			subscriber1, err := subredis.NewRedisSubscriber(client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
			Expect(err).NotTo(HaveOccurred())
			msgCh1, err := subscriber1.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			subscriber2, err := subredis.NewRedisSubscriber(client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			Expect(err).NotTo(HaveOccurred())
			msgCh2, err := subscriber2.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": "payload" + strconv.Itoa(i+1),
					},
				})
			}

			msg1Count := 0
			asyncCountMessages(&msg1Count, msgCh1, 3*time.Second)
			msg2Count := 0
			asyncCountMessages(&msg2Count, msgCh2, 3*time.Second)

			Eventually(func() int {
				return msg1Count + msg2Count
			}, 3*time.Second).Should(Equal(expectedMsgCount))

			//make sure at least one message was sent to each consumer
			Expect(msg1Count).To(BeNumerically(">", 0))
			Expect(msg2Count).To(BeNumerically(">", 0))
		})
	})

	When("not using consumer groups", func() {
		It("should receives all messages sent to the subscription", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": "payload" + strconv.Itoa(i+1),
					},
				})
			}

			msgCount := 0
			asyncCountMessages(&msgCount, msgCh, 3*time.Second)

			Eventually(func() int {
				return msgCount
			}, 3*time.Second).Should(Equal(expectedMsgCount))
		})

		It("should cancel message context once the processing is done", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]interface{}{
					"payload": "payload0",
				},
			})

			processCount := 0
			waitCh := make(chan bool)
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-msgCh:
						if !ok {
							return
						}
						msgCtxDone := msg.Context().Done()
						msg.Ack()
						Eventually(msgCtxDone, 3*time.Second).Should(BeClosed())
						processCount++
						waitCh <- true
					}
				}
			}()

			Eventually(func() int { return processCount }, 3*time.Second).Should(Equal(1))
		})
	})
})
