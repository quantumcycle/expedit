package subscriber_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
)

func TestChannelSubscriber(t *testing.T) {
	t.Run("Subscribe", func(t *testing.T) {
		t.Run("should deliver a copy of the payload and metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, err := sub.Subscribe(ctx)
			g.Expect(err).NotTo(HaveOccurred())

			in <- message.NewMessage(context.Background(), "hello").WithMetadata("k", "v")

			var got *message.Message
			g.Eventually(out).Should(Receive(&got))
			g.Expect(got.Payload).To(Equal("hello"))
			g.Expect(got.Metadata).To(HaveKeyWithValue("k", "v"))
		})

		t.Run("should close the output channel when the context is cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message)
			sub := subscriber.NewChannelSubscriber(in, 2)
			ctx, cancel := context.WithCancel(context.Background())
			out, _ := sub.Subscribe(ctx)

			cancel()

			g.Eventually(out).Should(BeClosed())
		})

		t.Run("should close the output channel when the input channel is closed", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message)
			sub := subscriber.NewChannelSubscriber(in, 2)
			out, _ := sub.Subscribe(context.Background())

			close(in)

			g.Eventually(out).Should(BeClosed())
		})

		t.Run("should redeliver a nacked message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, _ := sub.Subscribe(ctx)

			in <- message.NewMessage(context.Background(), "retry-me")

			var first, second *message.Message
			g.Eventually(out).Should(Receive(&first))
			first.Nack()
			g.Eventually(out).Should(Receive(&second))
			g.Expect(second.Payload).To(Equal("retry-me"))
		})

		t.Run("should not redeliver an acked message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, _ := sub.Subscribe(ctx)

			in <- message.NewMessage(context.Background(), "once")

			var first *message.Message
			g.Eventually(out).Should(Receive(&first))
			first.Ack()
			g.Consistently(out, 100*time.Millisecond).ShouldNot(Receive())
		})

		t.Run("should cancel the message context once it is acked", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, _ := sub.Subscribe(ctx)
			in <- message.NewMessage(context.Background(), "x")

			var got *message.Message
			g.Eventually(out).Should(Receive(&got))
			got.Ack()

			g.Eventually(got.Context().Done()).Should(BeClosed())
		})

		t.Run("should not panic nor hang when a message is nacked after Close", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, _ := sub.Subscribe(ctx)
			in <- message.NewMessage(context.Background(), "x")
			var got *message.Message
			g.Eventually(out).Should(Receive(&got))

			g.Expect(sub.Close()).To(Succeed())
			got.Nack()

			g.Eventually(out).Should(BeClosed())
		})

		t.Run("should not block on nack requeue when the input is full and the context is cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message) // unbuffered and nobody reads: requeue can't complete
			sub := subscriber.NewChannelSubscriber(in, 1)
			ctx, cancel := context.WithCancel(context.Background())
			out, _ := sub.Subscribe(ctx)
			go func() { in <- message.NewMessage(context.Background(), "x") }()
			var got *message.Message
			g.Eventually(out).Should(Receive(&got))
			got.Nack()

			cancel()

			g.Eventually(out).Should(BeClosed())
		})
	})
}
