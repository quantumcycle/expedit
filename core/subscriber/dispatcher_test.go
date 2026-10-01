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

type ctxKey struct{}

// recordedDelivery is a delivery that records how it was acknowledged
type recordedDelivery struct {
	subscriber.Delivery
	acks   atomic.Int32
	nacks  atomic.Int32
	ackCtx chan context.Context
}

func newDelivery(ctx context.Context) *recordedDelivery {
	d := &recordedDelivery{ackCtx: make(chan context.Context, 1)}
	d.Delivery = subscriber.Delivery{
		Message: message.NewMessage(ctx, "p"),
		Ack: func(ctx context.Context) error {
			d.acks.Add(1)
			d.ackCtx <- ctx
			return nil
		},
		Nack: func(ctx context.Context) error {
			d.nacks.Add(1)
			d.ackCtx <- ctx
			return nil
		},
	}
	return d
}

func TestDispatcher(t *testing.T) {
	t.Run("Handle", func(t *testing.T) {
		t.Run("should ack when the handler returns nil", func(t *testing.T) {
			g := NewGomegaWithT(t)
			d := newDelivery(context.Background())

			subscriber.NewDispatcher(func(msg *message.Message) error { return nil }, subscriber.DispatchOptions{}).
				Handle(d.Delivery)

			g.Expect(d.acks.Load()).To(BeEquivalentTo(1))
			g.Expect(d.nacks.Load()).To(BeEquivalentTo(0))
		})

		t.Run("should nack when the handler returns an error", func(t *testing.T) {
			g := NewGomegaWithT(t)
			d := newDelivery(context.Background())

			subscriber.NewDispatcher(func(msg *message.Message) error { return errors.New("failed") },
				subscriber.DispatchOptions{}).Handle(d.Delivery)

			g.Expect(d.acks.Load()).To(BeEquivalentTo(0))
			g.Expect(d.nacks.Load()).To(BeEquivalentTo(1))
		})

		t.Run("should nack and continue the panic when the handler panics", func(t *testing.T) {
			g := NewGomegaWithT(t)
			d := newDelivery(context.Background())
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error { panic("boom") },
				subscriber.DispatchOptions{})

			g.Expect(func() { dispatcher.Handle(d.Delivery) }).To(PanicWith("boom"))
			g.Expect(d.nacks.Load()).To(BeEquivalentTo(1))
		})

		t.Run("should give the handler a context with the delivery values that is not cancelled with it", func(t *testing.T) {
			g := NewGomegaWithT(t)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "value"))
			d := newDelivery(ctx)
			cancel()
			var handlerCtx context.Context

			subscriber.NewDispatcher(func(msg *message.Message) error {
				handlerCtx = msg.Context()
				return nil
			}, subscriber.DispatchOptions{}).Handle(d.Delivery)

			g.Expect(handlerCtx.Err()).NotTo(HaveOccurred())
			g.Expect(handlerCtx.Value(ctxKey{})).To(Equal("value"))
			ackCtx := <-d.ackCtx
			g.Expect(ackCtx.Err()).NotTo(HaveOccurred())
		})

		t.Run("should set the processing timeout as the deadline of the handler context only", func(t *testing.T) {
			g := NewGomegaWithT(t)
			d := newDelivery(context.Background())

			subscriber.NewDispatcher(func(msg *message.Message) error {
				<-msg.Context().Done()
				return msg.Context().Err()
			}, subscriber.DispatchOptions{ProcessingTimeout: 10 * time.Millisecond}).Handle(d.Delivery)

			g.Expect(d.nacks.Load()).To(BeEquivalentTo(1))
			ackCtx := <-d.ackCtx
			g.Expect(ackCtx.Err()).NotTo(HaveOccurred())
		})

		t.Run("should report ack and nack errors", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var ackErr, nackErr error
			opts := subscriber.DispatchOptions{
				OnAckError:  func(msg *message.Message, err error) { ackErr = err },
				OnNackError: func(msg *message.Message, err error) { nackErr = err },
			}
			failing := func(succeed bool) subscriber.Delivery {
				return subscriber.Delivery{
					Message: message.NewMessage(context.Background(), succeed),
					Ack:     func(context.Context) error { return errors.New("ack failed") },
					Nack:    func(context.Context) error { return errors.New("nack failed") },
				}
			}
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error {
				if msg.Payload.(bool) {
					return nil
				}
				return errors.New("failed")
			}, opts)

			dispatcher.Handle(failing(true))
			dispatcher.Handle(failing(false))

			g.Expect(ackErr).To(MatchError("ack failed"))
			g.Expect(nackErr).To(MatchError("nack failed"))
		})
	})

	t.Run("Dispatch", func(t *testing.T) {
		t.Run("should handle at most MaxInFlight messages at the same time", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var inFlight, maxSeen atomic.Int32
			release := make(chan struct{})
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error {
				n := inFlight.Add(1)
				for {
					seen := maxSeen.Load()
					if n <= seen || maxSeen.CompareAndSwap(seen, n) {
						break
					}
				}
				<-release
				inFlight.Add(-1)
				return nil
			}, subscriber.DispatchOptions{MaxInFlight: 3})

			dispatched := make(chan struct{})
			go func() {
				for range 10 {
					dispatcher.Dispatch(context.Background(), newDelivery(context.Background()).Delivery)
				}
				close(dispatched)
			}()

			g.Eventually(inFlight.Load).Should(BeEquivalentTo(3))
			g.Consistently(dispatched, 50*time.Millisecond).ShouldNot(BeClosed())
			close(release)
			g.Eventually(dispatched).Should(BeClosed())
			dispatcher.Wait()
			g.Expect(maxSeen.Load()).To(BeEquivalentTo(3))
		})

		t.Run("should nack instead of waiting for a slot when ctx is done", func(t *testing.T) {
			g := NewGomegaWithT(t)
			release := make(chan struct{})
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error {
				<-release
				return nil
			}, subscriber.DispatchOptions{MaxInFlight: 1})
			first := newDelivery(context.Background())
			dispatcher.Dispatch(context.Background(), first.Delivery)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			second := newDelivery(context.Background())

			dispatcher.Dispatch(ctx, second.Delivery)

			g.Expect(second.nacks.Load()).To(BeEquivalentTo(1))
			close(release)
			dispatcher.Wait()
			g.Expect(first.acks.Load()).To(BeEquivalentTo(1))
		})

		t.Run("should wait for the dispatched messages to be acknowledged", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var handled atomic.Int32
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error {
				time.Sleep(10 * time.Millisecond)
				handled.Add(1)
				return nil
			}, subscriber.DispatchOptions{MaxInFlight: 5})
			deliveries := make([]*recordedDelivery, 5)
			for i := range deliveries {
				deliveries[i] = newDelivery(context.Background())
				dispatcher.Dispatch(context.Background(), deliveries[i].Delivery)
			}

			dispatcher.Wait()

			g.Expect(handled.Load()).To(BeEquivalentTo(5))
			for _, d := range deliveries {
				g.Expect(d.acks.Load()).To(BeEquivalentTo(1))
			}
		})

		t.Run("should be safe for concurrent use", func(t *testing.T) {
			g := NewGomegaWithT(t)
			var handled atomic.Int32
			dispatcher := subscriber.NewDispatcher(func(msg *message.Message) error {
				handled.Add(1)
				return nil
			}, subscriber.DispatchOptions{MaxInFlight: 4})
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for range 25 {
						dispatcher.Dispatch(context.Background(), newDelivery(context.Background()).Delivery)
					}
				}()
			}
			wg.Wait()
			dispatcher.Wait()

			g.Expect(handled.Load()).To(BeEquivalentTo(200))
		})
	})
}
