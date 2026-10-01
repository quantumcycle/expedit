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

func newProcessor() subscriber.MessageProcessor[string] {
	return subscriber.MessageProcessor[string]{
		Ack:  func(ctx context.Context, m string) error { return nil },
		Nack: func(ctx context.Context, m string) error { return nil },
		MessageUnmarshall: func(ctx context.Context, m string) *message.Message {
			return message.NewMessage(ctx, m)
		},
	}
}

func process(p subscriber.MessageProcessor[string], impl string) *message.Message {
	out := make(chan *message.Message, 1)
	p.ProcessMessage(context.Background(), impl, out)
	return <-out
}

func TestMessageProcessor(t *testing.T) {
	t.Run("ProcessMessage", func(t *testing.T) {
		t.Run("should call the underlying Ack when the message is acked", func(t *testing.T) {
			g := NewGomegaWithT(t)
			acked := make(chan string, 1)
			p := newProcessor()
			p.Ack = func(ctx context.Context, m string) error { acked <- m; return nil }

			process(p, "m1").Ack()

			g.Eventually(acked).Should(Receive(Equal("m1")))
		})

		t.Run("should call OnAckError when Ack fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			boom := errors.New("ack failed")
			got := make(chan error, 1)
			p := newProcessor()
			p.Ack = func(ctx context.Context, m string) error { return boom }
			p.OnAckError = func(ctx context.Context, m string, ack bool, fn subscriber.AckNackFn[string], err error) {
				got <- err
			}

			process(p, "m1").Ack()

			g.Eventually(got).Should(Receive(MatchError(boom)))
		})

		t.Run("should call OnNackError, and not OnAckError, when Nack fails", func(t *testing.T) {
			g := NewGomegaWithT(t)
			boom := errors.New("nack failed")
			got := make(chan error, 1)
			ackErrCalled := make(chan struct{}, 1)
			p := newProcessor()
			p.Nack = func(ctx context.Context, m string) error { return boom }
			p.OnAckError = func(ctx context.Context, m string, ack bool, fn subscriber.AckNackFn[string], err error) {
				ackErrCalled <- struct{}{}
			}
			p.OnNackError = func(ctx context.Context, m string, ack bool, fn subscriber.AckNackFn[string], err error) {
				got <- err
			}

			process(p, "m1").Nack()

			g.Eventually(got).Should(Receive(MatchError(boom)))
			g.Expect(ackErrCalled).NotTo(Receive())
		})

		t.Run("should not panic when Nack fails and only OnNackError is set", func(t *testing.T) {
			g := NewGomegaWithT(t)
			called := make(chan bool, 1)
			p := newProcessor()
			p.Nack = func(ctx context.Context, m string) error { return errors.New("nack failed") }
			p.OnNackError = func(ctx context.Context, m string, ack bool, fn subscriber.AckNackFn[string], err error) {
				called <- ack
			}

			process(p, "m1").Nack()

			g.Eventually(called).Should(Receive(BeFalse()))
		})

		t.Run("should ignore Nack failures when no error handler is set", func(t *testing.T) {
			g := NewGomegaWithT(t)
			nacked := make(chan struct{})
			p := newProcessor()
			p.Nack = func(ctx context.Context, m string) error { close(nacked); return errors.New("x") }

			process(p, "m1").Nack()

			g.Eventually(nacked).Should(BeClosed())
		})

		t.Run("should nack the message when the processing timeout elapses", func(t *testing.T) {
			g := NewGomegaWithT(t)
			timedOut := make(chan string, 1)
			nacked := make(chan string, 1)
			p := newProcessor()
			p.ProcessingTimeout = 20 * time.Millisecond
			p.OnProcessingTimeout = func(ctx context.Context, m string) { timedOut <- m }
			p.Nack = func(ctx context.Context, m string) error { nacked <- m; return nil }

			msg := process(p, "m1")

			g.Eventually(timedOut).Should(Receive(Equal("m1")))
			g.Eventually(nacked).Should(Receive(Equal("m1")))
			g.Expect(msg.Ack()).To(BeFalse())
		})

		t.Run("should not block forever when the context is cancelled and nobody reads", func(t *testing.T) {
			g := NewGomegaWithT(t)
			ctx, cancel := context.WithCancel(context.Background())
			out := make(chan *message.Message) // unbuffered, nobody reads
			done := make(chan struct{})
			go func() {
				newProcessor().ProcessMessage(ctx, "m1", out)
				close(done)
			}()

			cancel()

			g.Eventually(done).Should(BeClosed())
		})
	})
}
