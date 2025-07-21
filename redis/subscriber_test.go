package redis_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
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

type redisSubscriberTestSetup struct {
	client *redis.Client
}

func setupRedisSubscriber(t *testing.T) *redisSubscriberTestSetup {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:29379",
	})

	t.Cleanup(func() {
		client.Close()
	})

	return &redisSubscriberTestSetup{
		client: client,
	}
}

func TestRedisSubscriber(t *testing.T) {
	t.Run("should return an error if the client is missing", func(t *testing.T) {
		g := NewGomegaWithT(t)

		_, err := subredis.NewRedisSubscriber(nil, newStreamName())
		g.Expect(err).To(HaveOccurred())
		g.Expect(err).To(MatchError("client is required"))
	})

	t.Run("when using consumer groups", func(t *testing.T) {
		t.Run("should return an error if the stream doesnt exist", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := fmt.Sprintf("non-existing-stream-%d", time.Now().UnixNano())

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			g.Expect(err).NotTo(HaveOccurred())

			_, err = subscriber.Subscribe(ctx)
			g.Expect(err).To(HaveOccurred())
			g.Expect(err).To(MatchError(subredis.StreamDoesntExistErr))
		})

		t.Run("should dispatch messages to different consumers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			stream := newStreamName()

			subscriber1, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh1, err := subscriber1.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber1.Close()

			subscriber2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			g.Expect(err).NotTo(HaveOccurred())
			msgCh2, err := subscriber2.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber2.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				setup.client.XAdd(ctx, &redis.XAddArgs{
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

			g.Eventually(func() int {
				return msg1Count + msg2Count
			}, 3*time.Second).Should(Equal(expectedMsgCount))

			g.Expect(msg1Count).To(BeNumerically(">", 0))
			g.Expect(msg2Count).To(BeNumerically(">", 0))
		})
	})

	t.Run("when not using consumer groups", func(t *testing.T) {
		t.Run("should receives all messages sent to the subscription", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				setup.client.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					Values: map[string]interface{}{
						"payload": "payload" + strconv.Itoa(i+1),
					},
				})
			}

			msgCount := 0
			asyncCountMessages(&msgCount, msgCh, 3*time.Second)

			g.Eventually(func() int {
				return msgCount
			}, 3*time.Second).Should(Equal(expectedMsgCount))
		})

		t.Run("should cancel message context once the processing is done", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())

			msgCh, err := subscriber.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			defer subscriber.Close()

			setup.client.XAdd(ctx, &redis.XAddArgs{
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
						g.Eventually(msgCtxDone, 3*time.Second).Should(BeClosed())
						processCount++
						waitCh <- true
					}
				}
			}()

			g.Eventually(func() int { return processCount }, 3*time.Second).Should(Equal(1))
		})
	})
}