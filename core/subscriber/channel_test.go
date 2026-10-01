package subscriber_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
)

func receive(ctx context.Context, sub subscriber.Subscriber, handler message.HandlerFunc) chan error {
	done := make(chan error, 1)
	go func() { done <- sub.Receive(ctx, handler) }()
	return done
}

func TestChannelSubscriber(t *testing.T) {
	t.Run("Receive", func(t *testing.T) {
		t.Run("should handle a copy of the payload and metadata", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			got := make(chan *message.Message, 1)
			receive(ctx, subscriber.NewChannelSubscriber(in, 1), func(msg *message.Message) error {
				got <- msg
				return nil
			})
			sent := message.NewMessage(context.Background(), "hello").WithMetadata("k", "v")

			in <- sent

			var msg *message.Message
			g.Eventually(got).Should(Receive(&msg))
			g.Expect(msg).NotTo(BeIdenticalTo(sent))
			g.Expect(msg.Payload).To(Equal("hello"))
			g.Expect(msg.Metadata).To(HaveKeyWithValue("k", "v"))
		})

		t.Run("should handle a nacked message again until it is acked", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var attempts atomic.Int32
			receive(ctx, subscriber.NewChannelSubscriber(in, 1), func(msg *message.Message) error {
				if attempts.Add(1) < 3 {
					return errors.New("not yet")
				}
				return nil
			})

			in <- message.NewMessage(context.Background(), "p")

			g.Eventually(attempts.Load).Should(BeEquivalentTo(3))
			g.Consistently(attempts.Load, 50*time.Millisecond).Should(BeEquivalentTo(3))
		})

		t.Run("should return nil when ctx is done, after the in-flight handlers", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 1)
			ctx, cancel := context.WithCancel(context.Background())
			started := make(chan struct{})
			release := make(chan struct{})
			var finished atomic.Bool
			done := receive(ctx, subscriber.NewChannelSubscriber(in, 1), func(msg *message.Message) error {
				close(started)
				<-release
				finished.Store(true)
				return nil
			})
			in <- message.NewMessage(context.Background(), "p")
			<-started

			cancel()

			g.Consistently(done, 50*time.Millisecond).ShouldNot(Receive())
			close(release)
			g.Eventually(done).Should(Receive(BeNil()))
			g.Expect(finished.Load()).To(BeTrue())
		})

		t.Run("should return nil once the input is closed and every message is handled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 3)
			var acked, attempts atomic.Int32
			for range 3 {
				in <- message.NewMessage(context.Background(), "p")
			}
			close(in)

			err := subscriber.NewChannelSubscriber(in, 2).Receive(context.Background(), func(msg *message.Message) error {
				// Nack the first attempt of each message
				if attempts.Add(1)%2 == 1 {
					return errors.New("nack")
				}
				acked.Add(1)
				return nil
			})

			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(acked.Load()).To(BeEquivalentTo(3))
		})

		t.Run("should not handle more than maxInFlight messages at the same time", func(t *testing.T) {
			g := NewGomegaWithT(t)
			in := make(chan *message.Message, 10)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var inFlight atomic.Int32
			release := make(chan struct{})
			receive(ctx, subscriber.NewChannelSubscriber(in, 2), func(msg *message.Message) error {
				inFlight.Add(1)
				<-release
				return nil
			})

			for range 10 {
				in <- message.NewMessage(context.Background(), "p")
			}

			g.Eventually(inFlight.Load).Should(BeEquivalentTo(2))
			g.Consistently(inFlight.Load, 50*time.Millisecond).Should(BeEquivalentTo(2))
			close(release)
		})
	})
}
