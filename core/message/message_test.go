package message_test

import (
	"context"
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
)

func TestMessage(t *testing.T) {
	t.Run("Ack and Nack", func(t *testing.T) {
		t.Run("should start in Processing state", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")

			g.Expect(msg.State()).To(Equal(message.Processing))
		})

		t.Run("should be idempotent for the same transition", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")

			g.Expect(msg.Ack()).To(BeTrue())
			g.Expect(msg.Ack()).To(BeTrue())
			g.Expect(msg.State()).To(Equal(message.Ack))
		})

		t.Run("should refuse to Nack an acked message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			msg.Ack()

			g.Expect(msg.Nack()).To(BeFalse())
			g.Expect(msg.State()).To(Equal(message.Ack))
		})

		t.Run("should refuse to Ack a nacked message", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			msg.Nack()

			g.Expect(msg.Ack()).To(BeFalse())
			g.Expect(msg.State()).To(Equal(message.Nack))
		})

		t.Run("should settle on exactly one state under concurrent Ack and Nack", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			stateCh := msg.StateChange()

			var wg sync.WaitGroup
			results := make(chan bool, 100)
			for i := 0; i < 50; i++ {
				wg.Add(2)
				go func() { defer wg.Done(); results <- msg.Ack() }()
				go func() { defer wg.Done(); results <- msg.Nack() }()
			}
			wg.Wait()
			close(results)

			state := msg.State()
			g.Expect(state).To(BeElementOf(message.Ack, message.Nack))
			g.Expect(stateCh).To(Receive(Equal(state)))
			g.Expect(stateCh).NotTo(Receive())
			wins := 0
			for ok := range results {
				if ok {
					wins++
				}
			}
			g.Expect(wins).To(Equal(50))
		})
	})

	t.Run("StateChange", func(t *testing.T) {
		t.Run("should notify every listener of the new state", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			first, second := msg.StateChange(), msg.StateChange()

			msg.Nack()

			g.Expect(first).To(Receive(Equal(message.Nack)))
			g.Expect(second).To(Receive(Equal(message.Nack)))
		})

		t.Run("should deliver the current state to a listener subscribing after the change", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			msg.Ack()

			g.Expect(msg.StateChange()).To(Receive(Equal(message.Ack)))
		})

		t.Run("should close the listener channels on Destroy", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p")
			ch := msg.StateChange()

			msg.Destroy()

			g.Expect(ch).To(BeClosed())
			g.Expect(msg.Ack()).To(BeTrue())
		})
	})

	t.Run("Copy", func(t *testing.T) {
		t.Run("should copy payload and metadata but reset the state", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p").WithMetadata("k", "v")
			msg.Ack()

			cp := msg.Copy()
			cp.Metadata["k"] = "changed"

			g.Expect(cp.Payload).To(Equal("p"))
			g.Expect(cp.State()).To(Equal(message.Processing))
			g.Expect(msg.Metadata["k"]).To(Equal("v"))
		})
	})

	t.Run("Equals", func(t *testing.T) {
		t.Run("should compare ID, metadata and payload", func(t *testing.T) {
			g := NewGomegaWithT(t)
			a := message.NewMessage(context.Background(), "p").WithMetadata("k", "v")
			b := message.NewMessage(context.Background(), "p").WithMetadata("k", "v")
			c := message.NewMessage(context.Background(), "other").WithMetadata("k", "v")
			d := message.NewMessage(context.Background(), "p").WithMetadata("k", "different")

			g.Expect(a.Equals(b)).To(BeTrue())
			g.Expect(a.Equals(c)).To(BeFalse())
			g.Expect(a.Equals(d)).To(BeFalse())
		})
	})
}

func TestHandleWithPayload(t *testing.T) {
	t.Run("should pass the typed payload to the handler", func(t *testing.T) {
		g := NewGomegaWithT(t)
		var got string
		handler := message.HandleWithPayload(func(msg *message.Message, payload string) error {
			got = payload
			return nil
		})

		g.Expect(handler(message.NewMessage(context.Background(), "typed"))).To(Succeed())
		g.Expect(got).To(Equal("typed"))
	})

	t.Run("should return an error when the payload has another type", func(t *testing.T) {
		g := NewGomegaWithT(t)
		handler := message.HandleWithPayload(func(msg *message.Message, payload string) error {
			return nil
		})

		g.Expect(handler(message.NewMessage(context.Background(), 42))).To(MatchError(ContainSubstring("does not match")))
	})
}
