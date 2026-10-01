package publisher_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
)

func TestPublishingEngine(t *testing.T) {
	t.Run("Publish", func(t *testing.T) {
		t.Run("should run middleware in the order they were added, then publish", func(t *testing.T) {
			g := NewGomegaWithT(t)
			ch := make(chan *message.Message, 1)
			e := publisher.NewPublishingEngine(publisher.NewChannelPublisher(ch))
			var order []string
			tag := func(name string) middleware.Middleware {
				return func(next message.HandlerFunc) message.HandlerFunc {
					return func(msg *message.Message) error {
						order = append(order, name)
						return next(msg)
					}
				}
			}
			e.AddMiddleware(tag("first")).AddMiddleware(tag("second"))

			g.Expect(e.Publish(message.NewMessage(context.Background(), "p"))).To(Succeed())

			g.Expect(order).To(Equal([]string{"first", "second"}))
			g.Expect(ch).To(Receive())
		})

		t.Run("should build the handler once under concurrent publishes", func(t *testing.T) {
			g := NewGomegaWithT(t)
			ch := make(chan *message.Message, 100)
			e := publisher.NewPublishingEngine(publisher.NewChannelPublisher(ch))
			var built atomic.Int32
			e.AddMiddleware(func(next message.HandlerFunc) message.HandlerFunc {
				built.Add(1)
				return next
			})

			var wg sync.WaitGroup
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = e.Publish(message.NewMessage(context.Background(), "p"))
				}()
			}
			wg.Wait()

			g.Expect(built.Load()).To(BeEquivalentTo(1))
			g.Expect(ch).To(HaveLen(50))
		})

		t.Run("should panic when adding middleware after publishing has started", func(t *testing.T) {
			g := NewGomegaWithT(t)
			ch := make(chan *message.Message, 1)
			e := publisher.NewPublishingEngine(publisher.NewChannelPublisher(ch))
			g.Expect(e.Publish(message.NewMessage(context.Background(), "p"))).To(Succeed())

			g.Expect(func() { e.AddMiddleware(middleware.ConvertPanicToError()) }).To(Panic())
		})
	})
}

func TestChannelPublisher(t *testing.T) {
	t.Run("should publish to the channel and close it on Close", func(t *testing.T) {
		g := NewGomegaWithT(t)
		ch := make(chan *message.Message, 1)
		p := publisher.NewChannelPublisher(ch)
		msg := message.NewMessage(context.Background(), "p")
		msg.ID = "id1"

		g.Expect(p.Publish(msg)).To(Succeed())
		g.Expect(p.GetMessageID(msg)).To(Equal("id1"))
		g.Expect(p.GetMessageID(nil)).To(BeEmpty())
		g.Expect(ch).To(Receive(Equal(msg)))

		g.Expect(p.Close()).To(Succeed())
		g.Expect(ch).To(BeClosed())
	})
}

func TestConstantRoutingFunctions(t *testing.T) {
	g := NewGomegaWithT(t)
	msg := message.NewMessage(context.Background(), "p")

	dest, err := publisher.ConstantDestination("d")(msg)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(dest).To(Equal(publisher.Destination("d")))

	b, _ := publisher.ConstantBoolMsgFn(true)(msg)
	s, _ := publisher.ConstantStringMsgFn("s")(msg)
	i, _ := publisher.ConstantIntMsgFn(7)(msg)
	g.Expect(b).To(BeTrue())
	g.Expect(s).To(Equal("s"))
	g.Expect(i).To(Equal(7))
}
