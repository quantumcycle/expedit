package message_test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
)

func TestMessage(t *testing.T) {
	t.Run("Copy", func(t *testing.T) {
		t.Run("should copy ID, payload and metadata, with its own metadata map", func(t *testing.T) {
			g := NewGomegaWithT(t)
			msg := message.NewMessage(context.Background(), "p").WithMetadata("k", "v")
			msg.ID = "id"

			cp := msg.Copy()
			cp.Metadata["k"] = "changed"

			g.Expect(cp.ID).To(Equal("id"))
			g.Expect(cp.Payload).To(Equal("p"))
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

		t.Run("should compare metadata values that are not comparable with ==", func(t *testing.T) {
			g := NewGomegaWithT(t)
			a := message.NewMessage(context.Background(), "p").WithMetadata("k", []string{"v"})
			b := message.NewMessage(context.Background(), "p").WithMetadata("k", []string{"v"})
			c := message.NewMessage(context.Background(), "p").WithMetadata("other", []string{"v"})

			g.Expect(a.Equals(b)).To(BeTrue())
			g.Expect(a.Equals(c)).To(BeFalse())
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
