package subscriber_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/subscriber"
)

func newEngine(in chan *message.Message) (*subscriber.SubscriptionEngine, *subscriber.SubscriptionRouter) {
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
	return subscriber.NewSubscriptionEngine(subscriber.NewChannelSubscriber(in, 4), *router), router
}

func startEngine(ctx context.Context, e *subscriber.SubscriptionEngine) chan error {
	done := make(chan error, 1)
	go func() { done <- e.Start(ctx) }()
	return done
}

func TestSubscriptionEngine(t *testing.T) {
	t.Run("Start", func(t *testing.T) {
		t.Run("should route messages to the handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			e, router := newEngine(in)
			handled := make(chan string, 1)
			router.AddHandler("t1").Handle(func(msg *message.Message) error {
				handled <- msg.Payload.(string)
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startEngine(ctx, e)

			in <- message.NewMessage(context.Background(), "p1").WithMetadata("type", "t1")

			g.Eventually(handled).Should(Receive(Equal("p1")))
		})

		t.Run("should return nil after context cancel", func(t *testing.T) {
			g := NewGomegaWithT(t)
			e, _ := newEngine(make(chan *message.Message))
			ctx, cancel := context.WithCancel(context.Background())
			done := startEngine(ctx, e)

			cancel()

			g.Eventually(done).Should(Receive(BeNil()))
		})

		t.Run("should nack the message when the handler fails, causing a redelivery", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			e, router := newEngine(in)
			var calls atomic.Int32
			router.AddHandler("t1").Handle(func(msg *message.Message) error {
				if calls.Add(1) == 1 {
					return errors.New("first attempt fails")
				}
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startEngine(ctx, e)

			in <- message.NewMessage(context.Background(), "p1").WithMetadata("type", "t1")

			g.Eventually(calls.Load).Should(BeEquivalentTo(2))
		})

		t.Run("should apply engine middleware before the handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			e, router := newEngine(in)
			order := make(chan string, 2)
			e.AddMiddleware(func(next message.HandlerFunc) message.HandlerFunc {
				return func(msg *message.Message) error {
					order <- "middleware"
					return next(msg)
				}
			})
			router.AddHandler("t1").Handle(func(msg *message.Message) error {
				order <- "handler"
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startEngine(ctx, e)

			in <- message.NewMessage(context.Background(), "p").WithMetadata("type", "t1")

			g.Eventually(order).Should(Receive(Equal("middleware")))
			g.Eventually(order).Should(Receive(Equal("handler")))
		})

		t.Run("should panic when started twice", func(t *testing.T) {
			g := NewGomegaWithT(t)
			e, _ := newEngine(make(chan *message.Message))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startEngine(ctx, e)
			time.Sleep(20 * time.Millisecond)

			g.Expect(func() { _ = e.Start(ctx) }).To(Panic())
		})

		t.Run("should panic when adding middleware after start", func(t *testing.T) {
			g := NewGomegaWithT(t)
			e, _ := newEngine(make(chan *message.Message))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			startEngine(ctx, e)
			time.Sleep(20 * time.Millisecond)

			g.Expect(func() { e.AddMiddleware(middleware.ConvertPanicToError()) }).To(Panic())
		})
	})
}
