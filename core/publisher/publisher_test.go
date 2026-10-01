package publisher_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
)

type fakeDestPublisher struct {
	dest      publisher.Destination
	published atomic.Int32
	closed    atomic.Int32
	closeErr  error
	publishFn func() error
}

func (f *fakeDestPublisher) Publish(ctx context.Context, msg string) error {
	f.published.Add(1)
	if f.publishFn != nil {
		return f.publishFn()
	}
	return nil
}

func (f *fakeDestPublisher) GetMessageID(msg string) string { return "id-" + msg }

func (f *fakeDestPublisher) Close() error {
	f.closed.Add(1)
	return f.closeErr
}

type fakeFactory struct {
	lock    sync.Mutex
	created map[publisher.Destination]*fakeDestPublisher
	count   int
	failFor map[publisher.Destination]error
	closeFn func(d publisher.Destination) error
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{created: map[publisher.Destination]*fakeDestPublisher{}}
}

func (f *fakeFactory) get(d publisher.Destination) (publisher.MessagesPublisherImpl[string], error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if err := f.failFor[d]; err != nil {
		return nil, err
	}
	f.count++
	p := &fakeDestPublisher{dest: d}
	if f.closeFn != nil {
		p.closeErr = f.closeFn(d)
	}
	f.created[d] = p
	return p, nil
}

func (f *fakeFactory) clearFailures() {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.failFor = nil
}

func routeByMetadata(msg *message.Message) (publisher.Destination, error) {
	return publisher.Destination(msg.Metadata["dest"].(string)), nil
}

func newPublisher(f *fakeFactory) *publisher.MessagePublisher[string] {
	return &publisher.MessagePublisher[string]{
		RoutingFunc: routeByMetadata,
		MessageMarshaller: func(msg *message.Message) (string, error) {
			return msg.Payload.(string), nil
		},
		GetDestinationPublisher: f.get,
	}
}

func msgFor(dest string, payload string) *message.Message {
	return message.NewMessage(context.Background(), payload).WithMetadata("dest", dest)
}

func TestMessagePublisher(t *testing.T) {
	t.Run("Publish", func(t *testing.T) {
		t.Run("should create one destination publisher per destination and reuse it", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			p := newPublisher(f)

			for i := 0; i < 100; i++ {
				g.Expect(p.Publish(msgFor("a", "x"))).To(Succeed())
			}
			g.Expect(p.Publish(msgFor("b", "x"))).To(Succeed())

			g.Expect(f.count).To(Equal(2))
			g.Expect(f.created["a"].published.Load()).To(BeEquivalentTo(100))
		})

		t.Run("should set the message ID from the destination publisher", func(t *testing.T) {
			g := NewGomegaWithT(t)
			p := newPublisher(newFakeFactory())
			msg := msgFor("a", "x")

			g.Expect(p.Publish(msg)).To(Succeed())

			g.Expect(msg.ID).To(Equal("id-x"))
		})

		t.Run("should create a single destination publisher under concurrent publishes", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			p := newPublisher(f)

			var wg sync.WaitGroup
			for i := 0; i < 50; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = p.Publish(msgFor("a", "x"))
				}()
			}
			wg.Wait()

			g.Expect(f.count).To(Equal(1))
		})

		t.Run("should not cache a destination publisher that failed to be created", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			f.failFor = map[publisher.Destination]error{"a": errors.New("boom")}
			p := newPublisher(f)

			g.Expect(p.Publish(msgFor("a", "x"))).To(MatchError("boom"))

			f.clearFailures()
			g.Expect(p.Publish(msgFor("a", "x"))).To(Succeed())
		})

		t.Run("should return routing errors", func(t *testing.T) {
			g := NewGomegaWithT(t)
			p := newPublisher(newFakeFactory())
			p.RoutingFunc = func(msg *message.Message) (publisher.Destination, error) {
				return "", errors.New("no route")
			}

			g.Expect(p.Publish(msgFor("a", "x"))).To(MatchError("no route"))
		})

		t.Run("should return marshalling errors", func(t *testing.T) {
			g := NewGomegaWithT(t)
			p := newPublisher(newFakeFactory())
			p.MessageMarshaller = func(msg *message.Message) (string, error) {
				return "", errors.New("bad payload")
			}

			g.Expect(p.Publish(msgFor("a", "x"))).To(MatchError("bad payload"))
		})

		t.Run("should return the destination publisher error and not set the ID", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			p := newPublisher(f)
			g.Expect(p.Publish(msgFor("a", "x"))).To(Succeed())
			f.created["a"].publishFn = func() error { return errors.New("send failed") }
			msg := msgFor("a", "y")

			g.Expect(p.Publish(msg)).To(MatchError("send failed"))
			g.Expect(msg.ID).To(BeEmpty())
		})

		t.Run("should return ErrClosed after Close", func(t *testing.T) {
			g := NewGomegaWithT(t)
			p := newPublisher(newFakeFactory())
			g.Expect(p.Close()).To(Succeed())

			g.Expect(p.Publish(msgFor("a", "x"))).To(MatchError(publisher.ErrClosed))
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Run("should close every cached destination publisher exactly once", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			p := newPublisher(f)
			g.Expect(p.Publish(msgFor("a", "x"))).To(Succeed())
			g.Expect(p.Publish(msgFor("b", "x"))).To(Succeed())

			g.Expect(p.Close()).To(Succeed())
			g.Expect(p.Close()).To(Succeed())

			g.Expect(f.created["a"].closed.Load()).To(BeEquivalentTo(1))
			g.Expect(f.created["b"].closed.Load()).To(BeEquivalentTo(1))
		})

		t.Run("should keep closing after an error and return all errors joined", func(t *testing.T) {
			g := NewGomegaWithT(t)
			errA := errors.New("close a")
			errB := errors.New("close b")
			f := newFakeFactory()
			f.closeFn = func(d publisher.Destination) error {
				switch d {
				case "a":
					return errA
				case "b":
					return errB
				}
				return nil
			}
			p := newPublisher(f)
			for _, d := range []string{"a", "b", "c"} {
				g.Expect(p.Publish(msgFor(d, "x"))).To(Succeed())
			}

			err := p.Close()

			g.Expect(err).To(MatchError(errA))
			g.Expect(err).To(MatchError(errB))
			g.Expect(f.created["c"].closed.Load()).To(BeEquivalentTo(1))
		})

		t.Run("should be safe concurrently with Publish", func(t *testing.T) {
			g := NewGomegaWithT(t)
			f := newFakeFactory()
			p := newPublisher(f)

			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(2)
				go func() {
					defer wg.Done()
					_ = p.Publish(msgFor("a", "x"))
				}()
				go func() {
					defer wg.Done()
					_ = p.Close()
				}()
			}
			wg.Wait()

			g.Expect(p.Publish(msgFor("a", "x"))).To(MatchError(publisher.ErrClosed))
			// every destination publisher that was created has been closed
			for _, d := range f.created {
				g.Expect(d.closed.Load()).To(BeEquivalentTo(1))
			}
		})
	})
}
