package subscriber_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
)

func typedMsg(routeType string) *message.Message {
	return message.NewMessage(context.Background(), "p").WithMetadata("type", routeType)
}

func TestSubscriptionRouter(t *testing.T) {
	t.Run("HandlerFunc", func(t *testing.T) {
		t.Run("should call the handler registered for the routing key", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
			var called string
			router.AddHandler("a").Handle(func(msg *message.Message) error { called = "a"; return nil })
			router.AddHandler("b").Handle(func(msg *message.Message) error { called = "b"; return errors.New("b failed") })

			g.Expect(router.HandlerFunc()(typedMsg("a"))).To(Succeed())
			g.Expect(called).To(Equal("a"))
			g.Expect(router.HandlerFunc()(typedMsg("b"))).To(MatchError("b failed"))
			g.Expect(called).To(Equal("b"))
		})

		t.Run("should apply the middleware of the route around its handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
			var order []string
			router.AddHandler("a").
				AddMiddleware(func(next message.HandlerFunc) message.HandlerFunc {
					return func(msg *message.Message) error {
						order = append(order, "middleware")
						return next(msg)
					}
				}).
				Handle(func(msg *message.Message) error { order = append(order, "handler"); return nil })

			g.Expect(router.HandlerFunc()(typedMsg("a"))).To(Succeed())

			g.Expect(order).To(Equal([]string{"middleware", "handler"}))
		})

		t.Run("should call the default handler for an unknown route", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
			called := false
			router.AddDefaultHandler(func(msg *message.Message) error { called = true; return nil })

			g.Expect(router.HandlerFunc()(typedMsg("unknown"))).To(Succeed())
			g.Expect(called).To(BeTrue())
		})

		t.Run("should ack unknown routes when WithAckOnUnknownRoute is set", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"), subscriber.WithAckOnUnknownRoute())

			g.Expect(router.HandlerFunc()(typedMsg("unknown"))).To(Succeed())
		})

		t.Run("should panic for an unknown route with no default handler", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))

			g.Expect(func() { _ = router.HandlerFunc()(typedMsg("unknown")) }).To(Panic())
		})
	})

	t.Run("AddDefaultHandler", func(t *testing.T) {
		t.Run("should panic when called twice", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
			router.AddDefaultHandler(func(msg *message.Message) error { return nil })

			g.Expect(func() { router.AddDefaultHandler(func(msg *message.Message) error { return nil }) }).To(Panic())
		})

		t.Run("should panic when combined with WithAckOnUnknownRoute", func(t *testing.T) {
			g := NewGomegaWithT(t)
			router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"), subscriber.WithAckOnUnknownRoute())

			g.Expect(func() { router.AddDefaultHandler(func(msg *message.Message) error { return nil }) }).To(Panic())
		})
	})
}

func TestBackoffRetryOnAckNackError(t *testing.T) {
	t.Run("should retry the operation and not call the fallback when a retry succeeds", func(t *testing.T) {
		g := NewGomegaWithT(t)
		attempts := 0
		fn := func(ctx context.Context, m string) error {
			attempts++
			if attempts < 3 {
				return errors.New("transient")
			}
			return nil
		}
		fallback := func(ctx context.Context, m string, ack bool, f subscriber.AckNackFn[string], err error) {
			t.Error("fallback must not be called")
		}
		handler := subscriber.BackoffRetryOnAckNackError[string](5, time.Millisecond, 5*time.Millisecond, fallback)

		handler(context.Background(), "m", true, fn, errors.New("first failure"))

		g.Expect(attempts).To(Equal(3))
	})

	t.Run("should call the fallback with the last error when retries are exhausted", func(t *testing.T) {
		g := NewGomegaWithT(t)
		boom := errors.New("permanent")
		fn := func(ctx context.Context, m string) error { return boom }
		var got error
		fallback := func(ctx context.Context, m string, ack bool, f subscriber.AckNackFn[string], err error) { got = err }
		handler := subscriber.BackoffRetryOnAckNackError[string](2, time.Millisecond, 5*time.Millisecond, fallback)

		handler(context.Background(), "m", false, fn, errors.New("first failure"))

		g.Expect(got).To(MatchError(boom))
	})
}
