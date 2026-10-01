package redis_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/loadtest"
	"github.com/quantumcycle/expedit/core/message"
	subredis "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
)

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

// receiver runs Subscriber.Receive in a goroutine. The context of Receive is cancelled, and Receive is waited for,
// when the test ends.
type receiver struct {
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startReceive(t *testing.T, sub *subredis.Subscriber, handler message.HandlerFunc) *receiver {
	ctx, cancel := context.WithCancel(context.Background())
	r := &receiver{cancel: cancel, done: make(chan struct{})}
	go func() {
		r.err = sub.Receive(ctx, handler)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-r.done
	})
	return r
}

// stop cancels the context of Receive, waits for it to return and returns its result.
func (r *receiver) stop() error {
	r.cancel()
	<-r.done
	return r.err
}

func addMessage(ctx context.Context, client *redis.Client, stream string, values map[string]interface{}) error {
	return client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err()
}

func pendingCount(ctx context.Context, client *redis.Client, stream, group string) int64 {
	pending, err := client.XPending(ctx, stream, group).Result()
	if err != nil {
		return -1
	}
	return pending.Count
}

// recordTo returns a handler that records the ID of each message under the key, and acks it.
func recordTo(r *loadtest.Recorder, key string) message.HandlerFunc {
	return func(msg *message.Message) error {
		r.Record(key, msg.ID)
		return nil
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

			stream := fmt.Sprintf("non-existing-stream-%d", time.Now().UnixNano())

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"))
			g.Expect(err).NotTo(HaveOccurred())

			r := startReceive(t, sub, recordTo(loadtest.NewRecorder(), "unused"))
			g.Eventually(r.done, 5*time.Second).Should(BeClosed())
			g.Expect(r.err).To(MatchError(subredis.StreamDoesntExistErr))
		})

		t.Run("should dispatch messages to different consumers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stream := newStreamName()
			received := loadtest.NewRecorder()

			// A consumer handles one message at a time and keeps it until the other consumer got a message too, so
			// each of them necessarily gets at least one message.
			bothReceived := make(chan struct{})
			var closeOnce sync.Once
			handlerFor := func(consumer string) message.HandlerFunc {
				return func(msg *message.Message) error {
					received.Record(consumer, msg.ID)
					if received.Count("consumer-1") > 0 && received.Count("consumer-2") > 0 {
						closeOnce.Do(func() { close(bothReceived) })
					}
					select {
					case <-bothReceived:
					case <-ctx.Done():
					}
					return nil
				}
			}

			sub1, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithMaxInFlight(1),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			startReceive(t, sub1, handlerFor("consumer-1"))

			// Same group options as sub1: the two start concurrently, and the first one to run creates the stream and
			// the group, which must then include the messages added before the other one starts reading
			sub2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithMaxInFlight(1),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			startReceive(t, sub2, handlerFor("consumer-2"))

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
					"payload": "payload" + strconv.Itoa(i+1),
				})).To(Succeed())
			}

			g.Eventually(func() int {
				return received.Count("consumer-1") + received.Count("consumer-2")
			}, 5*time.Second).Should(Equal(expectedMsgCount))

			g.Expect(received.Count("consumer-1")).To(BeNumerically(">", 0))
			g.Expect(received.Count("consumer-2")).To(BeNumerically(">", 0))
			g.Expect(loadtest.Duplicates(received.IDs("consumer-1"), received.IDs("consumer-2"))).To(BeEmpty())
		})

		t.Run("should handle a nacked message again after the pending idle timeout and not an acked one", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()
			group := "test-group"

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(300*time.Millisecond),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			var handled atomic.Int32
			startReceive(t, sub, func(msg *message.Message) error {
				// Nack the first delivery, ack the second one
				if handled.Add(1) == 1 {
					return errors.New("first delivery fails")
				}
				return nil
			})

			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "test-nack-message"})).To(Succeed())

			g.Eventually(handled.Load, 5*time.Second).Should(BeEquivalentTo(2))
			g.Eventually(func() int64 {
				return pendingCount(ctx, setup.client, stream, group)
			}, 5*time.Second).Should(BeEquivalentTo(0))
			// The acked message is not delivered anymore, even after the idle timeout
			g.Consistently(handled.Load, 1*time.Second, 100*time.Millisecond).Should(BeEquivalentTo(2))
		})

		t.Run("should leave a nacked message pending until the pending idle timeout", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()
			group := "test-group"
			idleTimeout := 2 * time.Second

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(idleTimeout),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			var mu sync.Mutex
			var deliveries []time.Time
			startReceive(t, sub, func(msg *message.Message) error {
				mu.Lock()
				defer mu.Unlock()
				deliveries = append(deliveries, time.Now())
				return errors.New("always fails")
			})
			// Redis counts the idle time from a delivery that happens after this point
			addedAt := time.Now()
			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "nack"})).To(Succeed())

			count := func() int {
				mu.Lock()
				defer mu.Unlock()
				return len(deliveries)
			}
			g.Eventually(count, 10*time.Second).Should(BeNumerically(">=", 2))
			mu.Lock()
			defer mu.Unlock()
			g.Expect(deliveries[1].Sub(addedAt)).To(BeNumerically(">=", idleTimeout))
		})

		t.Run("should claim a message nacked by another consumer", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()
			group := "test-group"
			received := loadtest.NewRecorder()

			// The first consumer nacks the message, then stops
			nackingConsumer, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(300*time.Millisecond),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			r := startReceive(t, nackingConsumer, func(msg *message.Message) error {
				received.Record("nacking", msg.ID)
				return errors.New("nack")
			})

			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "test-nack-message"})).To(Succeed())

			g.Eventually(func() int { return received.Count("nacking") }, 5*time.Second).Should(Equal(1))
			g.Expect(r.stop()).To(Succeed())
			g.Expect(pendingCount(ctx, setup.client, stream, group)).To(BeEquivalentTo(1))

			// The second consumer claims the pending message once it is idle, and acks it
			consumer2, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithPendingMessageIdleTimeout(300*time.Millisecond),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			startReceive(t, consumer2, recordTo(received, "claiming"))

			g.Eventually(func() int { return received.Count("claiming") }, 5*time.Second).Should(Equal(1))
			g.Expect(received.IDs("claiming")).To(Equal(received.IDs("nacking")))
			g.Eventually(func() int64 {
				return pendingCount(ctx, setup.client, stream, group)
			}, 5*time.Second).Should(BeEquivalentTo(0))
		})

		t.Run("should claim all the pending messages of a consumer that stopped", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			stream := newStreamName()
			group := "test-group"
			received := loadtest.NewRecorder()

			failingSubscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithPendingMessageIdleTimeout(300*time.Millisecond),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			r := startReceive(t, failingSubscriber, func(msg *message.Message) error {
				received.Record("failing", msg.ID)
				return errors.New("simulated failure")
			})

			for i := 0; i < 3; i++ {
				g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
					"payload": fmt.Sprintf("pending-test-%d", i),
				})).To(Succeed())
			}
			g.Eventually(func() int { return received.Count("failing") }, 5*time.Second).Should(Equal(3))
			g.Expect(r.stop()).To(Succeed())

			recoverySubscriber, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithPendingMessageIdleTimeout(300*time.Millisecond),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
			startReceive(t, recoverySubscriber, recordTo(received, "recovery"))

			g.Eventually(func() int { return received.Count("recovery") }, 5*time.Second).Should(Equal(3))
			g.Expect(loadtest.Missing(received.IDs("failing"), received.IDs("recovery"))).To(BeEmpty())
		})

		t.Run("should leave the message pending and call the ack error handler when acking fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stream := newStreamName()
			group := "test-group"
			setup.client.AddHook(failingCommandHook{command: "xack"})

			var ackErrors atomic.Int32
			var ackErrorMessageID atomic.Value
			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithBlockTimeout(50*time.Millisecond),
				subredis.WithAckErrorHandler(func(msg *message.Message, err error) {
					ackErrorMessageID.Store(msg.ID)
					ackErrors.Add(1)
				}))
			g.Expect(err).NotTo(HaveOccurred())

			received := loadtest.NewRecorder()
			startReceive(t, sub, recordTo(received, "consumer"))
			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "ack-fails"})).To(Succeed())

			g.Eventually(ackErrors.Load, 5*time.Second).Should(BeEquivalentTo(1))
			g.Expect(ackErrorMessageID.Load()).To(Equal(received.IDs("consumer")[0]))
			g.Expect(pendingCount(ctx, setup.client, stream, group)).To(BeEquivalentTo(1))
		})
	})

	t.Run("when not using consumer groups", func(t *testing.T) {
		t.Run("should receives all messages sent to the subscription", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			received := loadtest.NewRecorder()
			startReceive(t, sub, recordTo(received, "consumer"))

			expectedMsgCount := 10
			for i := 0; i < expectedMsgCount; i++ {
				g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
					"payload": "payload" + strconv.Itoa(i+1),
				})).To(Succeed())
			}

			g.Eventually(func() int { return received.Count("consumer") }, 3*time.Second).Should(Equal(expectedMsgCount))
		})

		t.Run("should cancel message context once the handler returned", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "payload0"})).To(Succeed())

			msgCtxCh := make(chan context.Context, 1)
			var activeDuringHandler atomic.Bool
			startReceive(t, sub, func(msg *message.Message) error {
				activeDuringHandler.Store(msg.Context().Err() == nil)
				msgCtxCh <- msg.Context()
				return nil
			})

			var msgCtx context.Context
			g.Eventually(msgCtxCh, 3*time.Second).Should(Receive(&msgCtx))
			g.Expect(activeDuringHandler.Load()).To(BeTrue())
			g.Eventually(msgCtx.Done(), 3*time.Second).Should(BeClosed())
		})

		for _, streamExists := range []bool{false, true} {
			t.Run(fmt.Sprintf("should receive every message published after Receive started with the default start ID (stream existing: %v)", streamExists), func(t *testing.T) {
				g := NewGomegaWithT(t)
				setup := setupRedisSubscriber(t)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				stream := newStreamName()
				if streamExists {
					g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"kind": "before"})).To(Succeed())
				}

				// A very short block timeout makes the consumer often not blocked in a read while messages are added
				sub, err := subredis.NewRedisSubscriber(setup.client, stream, subredis.WithBlockTimeout(2*time.Millisecond))
				g.Expect(err).NotTo(HaveOccurred())

				var mu sync.Mutex
				var seq []string
				probeSeen := make(chan struct{})
				var probeOnce sync.Once
				startReceive(t, sub, func(msg *message.Message) error {
					values := msg.Payload.(map[string]interface{})
					if values["kind"] == "probe" {
						probeOnce.Do(func() { close(probeSeen) })
						return nil
					}
					mu.Lock()
					defer mu.Unlock()
					seq = append(seq, values["kind"].(string))
					return nil
				})

				// Messages published before Receive resolved its start position are skipped by design, so publish
				// probes until one is received to know Receive is running.
				probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
				defer probeCancel()
			probing:
				for {
					g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"kind": "probe"})).To(Succeed())
					select {
					case <-probeSeen:
						break probing
					case <-probeCtx.Done():
						t.Fatal("Receive did not receive any probe message")
					case <-time.After(10 * time.Millisecond):
					}
				}

				nbMessages := 300
				expected := make([]string, nbMessages)
				for i := 0; i < nbMessages; i++ {
					expected[i] = "msg-" + strconv.Itoa(i)
					g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"kind": expected[i]})).To(Succeed())
				}

				g.Eventually(func() []string {
					mu.Lock()
					defer mu.Unlock()
					return append([]string(nil), seq...)
				}, 10*time.Second).Should(ConsistOf(expected)) // handlers run concurrently, so the order is not checked
			})
		}
	})

	t.Run("Receive", func(t *testing.T) {
		t.Run("should return nil promptly when the context is cancelled while idle", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			sub, err := subredis.NewRedisSubscriber(setup.client, newStreamName(),
				subredis.WithBlockTimeout(100*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- sub.Receive(ctx, recordTo(loadtest.NewRecorder(), "unused")) }()

			cancel()
			// Bounded by the block timeout
			g.Eventually(result, 1*time.Second).Should(Receive(BeNil()))
		})

		t.Run("should wait for the in-flight handlers on shutdown, without cancelling their context", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			stream := newStreamName()
			group := "test-group"

			sub, err := subredis.NewRedisSubscriber(setup.client, stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithBlockTimeout(100*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			started := make(chan struct{}, 1)
			release := make(chan struct{})
			var handlerCtxErr atomic.Value
			var handlerReturned atomic.Bool

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "in-flight"})).To(Succeed())

			result := make(chan error, 1)
			go func() {
				result <- sub.Receive(ctx, func(msg *message.Message) error {
					started <- struct{}{}
					<-release
					handlerCtxErr.Store(fmt.Sprint(msg.Context().Err()))
					handlerReturned.Store(true)
					return nil
				})
			}()
			releaseOnce := sync.OnceFunc(func() { close(release) })
			defer releaseOnce()

			g.Eventually(started, 5*time.Second).Should(Receive())
			cancel()

			// Receive stops receiving but does not return while the handler runs
			g.Consistently(result, 500*time.Millisecond, 50*time.Millisecond).ShouldNot(Receive())

			releaseOnce()
			g.Eventually(result, 2*time.Second).Should(Receive(BeNil()))
			g.Expect(handlerReturned.Load()).To(BeTrue())
			g.Expect(handlerCtxErr.Load()).To(Equal("<nil>"))
			// The in-flight message was acked although the context of Receive was cancelled
			g.Expect(pendingCount(context.Background(), setup.client, stream, group)).To(BeEquivalentTo(0))
		})

		t.Run("should handle at most MaxInFlight messages at the same time", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			stream := newStreamName()

			maxInFlight := 3
			nbMessages := 9
			sub, err := subredis.NewRedisSubscriber(setup.client, stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithMaxInFlight(maxInFlight),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			for i := 0; i < nbMessages; i++ {
				g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
					"payload": strconv.Itoa(i),
				})).To(Succeed())
			}

			var current, peak, completed atomic.Int32
			release := make(chan struct{})
			releaseOnce := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseOnce)

			startReceive(t, sub, func(msg *message.Message) error {
				n := current.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				<-release
				current.Add(-1)
				completed.Add(1)
				return nil
			})

			g.Eventually(current.Load, 5*time.Second).Should(BeEquivalentTo(maxInFlight))
			// The other messages wait for a free slot
			g.Consistently(current.Load, 300*time.Millisecond, 20*time.Millisecond).Should(BeEquivalentTo(maxInFlight))
			g.Expect(completed.Load()).To(BeEquivalentTo(0))

			releaseOnce()
			g.Eventually(completed.Load, 5*time.Second).Should(BeEquivalentTo(nbMessages))
			g.Expect(peak.Load()).To(BeEquivalentTo(maxInFlight))
		})

		t.Run("should return an error when a read fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			// A dedicated client, that is closed while receiving
			client := redis.NewClient(&redis.Options{Addr: "localhost:29379"})
			t.Cleanup(func() { client.Close() })

			sub, err := subredis.NewRedisSubscriber(client, newStreamName(),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			r := startReceive(t, sub, recordTo(loadtest.NewRecorder(), "unused"))
			g.Consistently(r.done, 200*time.Millisecond, 50*time.Millisecond).ShouldNot(BeClosed())

			g.Expect(client.Close()).To(Succeed())

			g.Eventually(r.done, 2*time.Second).Should(BeClosed())
			g.Expect(r.err).To(HaveOccurred())
		})
	})

	t.Run("configuration options", func(t *testing.T) {
		t.Run("WithProcessingTimeout should put a deadline on the message context", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream := newStreamName()
			timeout := 30 * time.Second

			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "timeout-test"})).To(Succeed())

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithProcessingTimeout(timeout),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			type result struct {
				deadline time.Time
				ok       bool
				start    time.Time
			}
			results := make(chan result, 1)
			startReceive(t, sub, func(msg *message.Message) error {
				start := time.Now()
				deadline, ok := msg.Context().Deadline()
				results <- result{deadline: deadline, ok: ok, start: start}
				return nil
			})

			var res result
			g.Eventually(results, 5*time.Second).Should(Receive(&res))
			g.Expect(res.ok).To(BeTrue())
			g.Expect(res.deadline.Sub(res.start)).To(BeNumerically("<=", timeout))
			g.Expect(res.deadline.Sub(res.start)).To(BeNumerically(">", 0))
		})

		t.Run("WithProcessingTimeout should leave a timed out message pending", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream := newStreamName()
			group := "test-group"

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithConsumerGroup(group),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning),
				subredis.WithProcessingTimeout(200*time.Millisecond),
				subredis.WithPendingMessageIdleTimeout(time.Minute),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{"payload": "timeout-test"})).To(Succeed())

			var timedOut atomic.Bool
			startReceive(t, sub, func(msg *message.Message) error {
				// A handler that runs for too long gives up when the deadline is reached
				select {
				case <-msg.Context().Done():
					timedOut.Store(errors.Is(msg.Context().Err(), context.DeadlineExceeded))
					return msg.Context().Err()
				case <-time.After(5 * time.Second):
					return nil
				}
			})

			g.Eventually(timedOut.Load, 3*time.Second).Should(BeTrue())
			g.Expect(pendingCount(ctx, setup.client, stream, group)).To(BeEquivalentTo(1))
		})

		t.Run("WithMetadataExtractor should extract custom metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			// Add message with metadata prefix first
			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
				"payload":   "test",
				"meta_user": "john",
				"meta_role": "admin",
				"data":      "regular_data",
			})).To(Succeed())

			// Use the existing PrefixMetadataExtractor utility but convert types
			metadataExtractor := func(wrapper subredis.MessageWrapper) map[string]interface{} {
				stringMetadata := subredis.PrefixMetadataExtractor("meta_")(wrapper)
				metadata := make(map[string]interface{})
				for k, v := range stringMetadata {
					metadata[k] = v
				}
				return metadata
			}

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithMetadataExtractor(metadataExtractor),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			metadataCh := make(chan map[string]interface{}, 1)
			startReceive(t, sub, func(msg *message.Message) error {
				metadataCh <- msg.Metadata
				return nil
			})

			var metadata map[string]interface{}
			g.Eventually(metadataCh, 3*time.Second).Should(Receive(&metadata))
			g.Expect(metadata).To(HaveKey("user"))
			g.Expect(metadata).To(HaveKey("role"))
			g.Expect(metadata["user"]).To(Equal("john"))
			g.Expect(metadata["role"]).To(Equal("admin"))
			g.Expect(metadata).NotTo(HaveKey("data")) // Should not include non-meta keys
		})

		t.Run("WithPayloadExtractor should extract custom payload", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream := newStreamName()

			// Add message with data prefix first
			g.Expect(addMessage(ctx, setup.client, stream, map[string]interface{}{
				"data_field1": "value1",
				"data_field2": "value2",
				"meta_user":   "john",
			})).To(Succeed())

			// Use the existing PrefixPayloadExtractor utility
			payloadExtractor := subredis.PrefixPayloadExtractor("data_")

			sub, err := subredis.NewRedisSubscriber(setup.client,
				stream,
				subredis.WithStartID(subredis.StartFromBeginning),
				subredis.WithPayloadExtractor(payloadExtractor),
				subredis.WithBlockTimeout(50*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())

			payloadCh := make(chan message.Payload, 1)
			startReceive(t, sub, func(msg *message.Message) error {
				payloadCh <- msg.Payload
				return nil
			})

			var received message.Payload
			g.Eventually(payloadCh, 3*time.Second).Should(Receive(&received))
			payload, ok := received.(map[string]interface{})
			g.Expect(ok).To(BeTrue())
			g.Expect(payload).To(HaveKey("field1"))
			g.Expect(payload).To(HaveKey("field2"))
			g.Expect(payload["field1"]).To(Equal("value1"))
			g.Expect(payload["field2"]).To(Equal("value2"))
			g.Expect(payload).NotTo(HaveKey("meta_user")) // Should not include non-data keys
		})

		t.Run("WithPendingMessageBatchSize should configure batch size", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithPendingMessageBatchSize option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithPendingMessageBatchSize(2))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("WithConsumerGroupStartID should configure start position", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithConsumerGroupStartID option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithConsumerGroup("test-group"),
				subredis.WithConsumerGroupCreateStreamIfMissing(true),
				subredis.WithConsumerGroupStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})

		t.Run("WithStartID should configure start position for non-consumer group", func(t *testing.T) {
			g := NewGomegaWithT(t)
			setup := setupRedisSubscriber(t)

			// Test that WithStartID option is accepted without error
			subscriber, err := subredis.NewRedisSubscriber(setup.client,
				"test-stream",
				subredis.WithStartID(subredis.StartFromBeginning))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(subscriber).NotTo(BeNil())
		})
	})
}

// failingCommandHook makes the redis command fail without sending it to the server.
type failingCommandHook struct {
	command string
}

func (failingCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h failingCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == h.command {
			err := errors.New("simulated " + h.command + " failure")
			cmd.SetErr(err)
			return err
		}
		return next(ctx, cmd)
	}
}

func (failingCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestRedisSubscriberTimeoutValidation(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	t.Run("in consumer group mode", func(t *testing.T) {
		t.Run("should accept the default processing timeout, derived from the pending idle timeout", func(t *testing.T) {
			g := NewGomegaWithT(t)
			_, err := subredis.NewRedisSubscriber(client, "stream",
				subredis.WithConsumerGroup("group"),
				subredis.WithPendingMessageIdleTimeout(100*time.Millisecond))
			g.Expect(err).NotTo(HaveOccurred())
		})

		t.Run("should reject a processing timeout that is not below the pending idle timeout", func(t *testing.T) {
			g := NewGomegaWithT(t)
			_, err := subredis.NewRedisSubscriber(client, "stream",
				subredis.WithConsumerGroup("group"),
				subredis.WithPendingMessageIdleTimeout(time.Minute),
				subredis.WithProcessingTimeout(time.Minute))
			g.Expect(err).To(MatchError(ContainSubstring("must be positive and below the pending message idle timeout")))
		})

		t.Run("should reject a processing timeout of 0", func(t *testing.T) {
			g := NewGomegaWithT(t)
			_, err := subredis.NewRedisSubscriber(client, "stream",
				subredis.WithConsumerGroup("group"),
				subredis.WithProcessingTimeout(0))
			g.Expect(err).To(MatchError(ContainSubstring("must be positive")))
		})
	})

	t.Run("without consumer group", func(t *testing.T) {
		t.Run("should accept a processing timeout of 0", func(t *testing.T) {
			g := NewGomegaWithT(t)
			_, err := subredis.NewRedisSubscriber(client, "stream", subredis.WithProcessingTimeout(0))
			g.Expect(err).NotTo(HaveOccurred())
		})
	})
}
