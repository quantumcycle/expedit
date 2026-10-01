package middleware_test

import (
	"context"
	"errors"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
)

func newMsg() *message.Message {
	return message.NewMessage(context.Background(), "p")
}

func ok(msg *message.Message) error { return nil }

func TestChain(t *testing.T) {
	t.Run("should wrap the handler with the first added middleware outermost", func(t *testing.T) {
		g := NewGomegaWithT(t)
		var order []string
		tag := func(name string) middleware.Middleware {
			return func(next message.HandlerFunc) message.HandlerFunc {
				return func(msg *message.Message) error {
					order = append(order, name+" in")
					err := next(msg)
					order = append(order, name+" out")
					return err
				}
			}
		}
		c := middleware.NewChain()
		c.Add(tag("a"))
		c.Add(tag("b"))

		g.Expect(c.Wrap(ok)(newMsg())).To(Succeed())

		g.Expect(order).To(Equal([]string{"a in", "b in", "b out", "a out"}))
	})

	t.Run("should return the handler unchanged when empty", func(t *testing.T) {
		g := NewGomegaWithT(t)
		boom := errors.New("boom")

		g.Expect(middleware.NewChain().Wrap(func(msg *message.Message) error { return boom })(newMsg())).To(MatchError(boom))
	})
}

func TestConvertPanicToError(t *testing.T) {
	wrap := func(h message.HandlerFunc) message.HandlerFunc { return middleware.ConvertPanicToError()(h) }

	t.Run("should pass through the handler result when it does not panic", func(t *testing.T) {
		g := NewGomegaWithT(t)

		g.Expect(wrap(ok)(newMsg())).To(Succeed())
	})

	t.Run("should return a panicked error as is", func(t *testing.T) {
		g := NewGomegaWithT(t)
		boom := errors.New("boom")

		g.Expect(wrap(func(msg *message.Message) error { panic(boom) })(newMsg())).To(MatchError(boom))
	})

	t.Run("should wrap a non-error panic value in an error", func(t *testing.T) {
		g := NewGomegaWithT(t)

		err := wrap(func(msg *message.Message) error { panic("oops") })(newMsg())

		g.Expect(err).To(MatchError(ContainSubstring("oops")))
	})
}

func TestOnError(t *testing.T) {
	t.Run("should call the error handler with the handler error and return it", func(t *testing.T) {
		g := NewGomegaWithT(t)
		boom := errors.New("boom")
		var got error
		h := middleware.OnError(func(msg *message.Message, err error) { got = err })(func(msg *message.Message) error { return boom })

		g.Expect(h(newMsg())).To(MatchError(boom))
		g.Expect(got).To(MatchError(boom))
	})

	t.Run("should not call the error handler on success", func(t *testing.T) {
		g := NewGomegaWithT(t)
		called := false
		h := middleware.OnError(func(msg *message.Message, err error) { called = true })(ok)

		g.Expect(h(newMsg())).To(Succeed())
		g.Expect(called).To(BeFalse())
	})
}

func TestContextTimeout(t *testing.T) {
	t.Run("should give the handler a context with the timeout", func(t *testing.T) {
		g := NewGomegaWithT(t)
		var deadlineSet bool
		h := middleware.ContextTimeout(time.Minute)(func(msg *message.Message) error {
			_, deadlineSet = msg.Context().Deadline()
			return nil
		})

		g.Expect(h(newMsg())).To(Succeed())
		g.Expect(deadlineSet).To(BeTrue())
	})
}

func TestConditionalExecute(t *testing.T) {
	t.Run("should execute the replacement instead of the handler when the condition is true", func(t *testing.T) {
		g := NewGomegaWithT(t)
		nextCalled := false
		h := middleware.ConditionalExecute(
			func(msg *message.Message) bool { return true },
			func(msg *message.Message) error { return errors.New("diverted") },
		)(func(msg *message.Message) error { nextCalled = true; return nil })

		g.Expect(h(newMsg())).To(MatchError("diverted"))
		g.Expect(nextCalled).To(BeFalse())
	})

	t.Run("should call the handler when the condition is false", func(t *testing.T) {
		g := NewGomegaWithT(t)
		nextCalled := false
		h := middleware.ConditionalExecute(
			func(msg *message.Message) bool { return false },
			func(msg *message.Message) error { return errors.New("diverted") },
		)(func(msg *message.Message) error { nextCalled = true; return nil })

		g.Expect(h(newMsg())).To(Succeed())
		g.Expect(nextCalled).To(BeTrue())
	})
}

func TestConditionalSkip(t *testing.T) {
	t.Run("should skip the handler and succeed when the condition is true", func(t *testing.T) {
		g := NewGomegaWithT(t)
		nextCalled := false
		h := middleware.ConditionalSkip(func(msg *message.Message) bool { return true })(
			func(msg *message.Message) error { nextCalled = true; return nil })

		g.Expect(h(newMsg())).To(Succeed())
		g.Expect(nextCalled).To(BeFalse())
	})
}

func TestContextKeyToMetadata(t *testing.T) {
	type key struct{}

	t.Run("should copy a string context value to the metadata", func(t *testing.T) {
		g := NewGomegaWithT(t)
		msg := message.NewMessage(context.WithValue(context.Background(), key{}, "corr-1"), "p")
		h := middleware.ContextKeyToMetadata("correlation", key{})(ok)

		g.Expect(h(msg)).To(Succeed())
		g.Expect(msg.Metadata).To(HaveKeyWithValue("correlation", "corr-1"))
	})

	t.Run("should leave the metadata alone when the context has no such value", func(t *testing.T) {
		g := NewGomegaWithT(t)
		msg := newMsg()
		h := middleware.ContextKeyToMetadata("correlation", key{})(ok)

		g.Expect(h(msg)).To(Succeed())
		g.Expect(msg.Metadata).NotTo(HaveKey("correlation"))
	})
}

func TestThrottle(t *testing.T) {
	t.Run("should space out calls", func(t *testing.T) {
		g := NewGomegaWithT(t)
		h := middleware.Throttle(10, 100*time.Millisecond)(ok)
		start := time.Now()

		for i := 0; i < 3; i++ {
			g.Expect(h(newMsg())).To(Succeed())
		}

		g.Expect(time.Since(start)).To(BeNumerically(">=", 25*time.Millisecond))
	})
}
