package subscriber_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
)

// blockingInit is an InitializeFn whose receive goroutine runs until the context is cancelled.
func blockingInit(inits *atomic.Int32) func(ctx context.Context, out chan *message.Message, done func(error)) error {
	return func(ctx context.Context, out chan *message.Message, done func(error)) error {
		inits.Add(1)
		go func() {
			<-ctx.Done()
			done(nil)
		}()
		return nil
	}
}

func TestMessageSubscriber(t *testing.T) {
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("should initialize once and return the same channel for concurrent subscribers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var inits atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{InitializeFn: blockingInit(&inits)}
			defer sub.Close()

			var wg sync.WaitGroup
			channels := make([]chan *message.Message, 20)
			for i := range channels {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ch, err := sub.Subscribe(context.Background())
					g.Expect(err).NotTo(HaveOccurred())
					channels[i] = ch
				}()
			}
			wg.Wait()

			g.Expect(inits.Load()).To(BeEquivalentTo(1))
			for _, ch := range channels {
				g.Expect(ch).To(Equal(channels[0]))
			}
		})

		t.Run("should not keep a dead channel when initialization fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var calls atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					if calls.Add(1) == 1 {
						return errors.New("cannot init")
					}
					go func() { <-ctx.Done(); done(nil) }()
					return nil
				},
			}
			defer sub.Close()

			_, err := sub.Subscribe(context.Background())
			g.Expect(err).To(MatchError("cannot init"))

			ch, err := sub.Subscribe(context.Background())
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ch).NotTo(BeNil())
			g.Expect(calls.Load()).To(BeEquivalentTo(2))
		})

		t.Run("should return ErrClosed after Close", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var inits atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{InitializeFn: blockingInit(&inits)}
			g.Expect(sub.Close()).To(Succeed())

			_, err := sub.Subscribe(context.Background())

			g.Expect(err).To(MatchError(subscriber.ErrClosed))
		})

		t.Run("should close the channel with a nil error when the context is cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var inits atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{InitializeFn: blockingInit(&inits)}
			ctx, cancel := context.WithCancel(context.Background())
			ch, _ := sub.Subscribe(ctx)

			cancel()

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Err()).NotTo(HaveOccurred())
		})

		t.Run("should close the channel and expose the error when the receive loop fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			boom := errors.New("receive loop died")
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					go done(boom)
					return nil
				},
			}
			defer sub.Close()

			ch, err := sub.Subscribe(context.Background())
			g.Expect(err).NotTo(HaveOccurred())

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Err()).To(MatchError(boom))
		})

		t.Run("should not report an error that happens because the context was cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					go func() {
						<-ctx.Done()
						done(ctx.Err())
					}()
					return nil
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			ch, _ := sub.Subscribe(ctx)

			cancel()

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Err()).NotTo(HaveOccurred())
		})

		t.Run("should close the channel on cancel even if the receive goroutine is stuck", func(t *testing.T) {
			g := NewGomegaWithT(t)
			release := make(chan struct{})
			defer close(release)
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					//a read that ignores the context, like a blocking redis XREAD
					go func() { <-release }()
					return nil
				},
			}
			ctx, cancel := context.WithCancel(context.Background())
			ch, _ := sub.Subscribe(ctx)

			cancel()

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Close()).To(Succeed())
		})

		t.Run("should relay the messages sent by the receive goroutine", func(t *testing.T) {
			g := NewGomegaWithT(t)
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					go func() {
						defer done(nil)
						newProcessor().ProcessMessage(ctx, "m1", out)
					}()
					return nil
				},
			}
			defer sub.Close()

			ch, _ := sub.Subscribe(context.Background())

			var got *message.Message
			g.Eventually(ch).Should(Receive(&got))
			g.Expect(got.Payload).To(Equal("m1"))
			g.Eventually(ch).Should(BeClosed())
		})

		t.Run("should keep the first error when done is called more than once", func(t *testing.T) {
			g := NewGomegaWithT(t)
			first := errors.New("first")
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					done(first)
					done(errors.New("second"))
					return nil
				},
			}
			defer sub.Close()

			ch, _ := sub.Subscribe(context.Background())

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Err()).To(MatchError(first))
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Run("should cancel the receive loop, close the channel and be idempotent", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var inits atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{InitializeFn: blockingInit(&inits)}
			ch, _ := sub.Subscribe(context.Background())

			g.Expect(sub.Close()).To(Succeed())
			g.Expect(sub.Close()).To(Succeed())

			g.Eventually(ch).Should(BeClosed())
			g.Expect(sub.Err()).NotTo(HaveOccurred())
		})

		t.Run("should be safe concurrently with Subscribe", func(t *testing.T) {
			var inits atomic.Int32
			sub := &subscriber.MessageSubscriber[string]{InitializeFn: blockingInit(&inits)}

			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, _ = sub.Subscribe(context.Background())
				}()
				go func() {
					defer wg.Done()
					_ = sub.Close()
				}()
			}
			wg.Wait()
		})

		t.Run("should unblock a receive loop stuck sending to a channel nobody reads", func(t *testing.T) {
			g := NewGomegaWithT(t)
			processed := make(chan struct{})
			sub := &subscriber.MessageSubscriber[string]{
				InitializeFn: func(ctx context.Context, out chan *message.Message, done func(error)) error {
					go func() {
						defer done(nil)
						newProcessor().ProcessMessage(ctx, "m", out)
						close(processed)
					}()
					return nil
				},
			}

			_, _ = sub.Subscribe(context.Background())
			time.Sleep(10 * time.Millisecond)

			g.Expect(sub.Close()).To(Succeed())
			g.Eventually(processed).Should(BeClosed())
		})
	})
}
