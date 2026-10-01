package subscriber_test

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
)

type fakeSubscriber struct {
	ch           chan *message.Message
	err          error
	subscribeErr error
}

func (f *fakeSubscriber) Subscribe(ctx context.Context) (<-chan *message.Message, error) {
	return f.ch, f.subscribeErr
}
func (f *fakeSubscriber) Close() error { return nil }
func (f *fakeSubscriber) Err() error   { return f.err }

func TestSubscriptionEngineSubscriberErrors(t *testing.T) {
	newRouter := func() subscriber.SubscriptionRouter {
		return *subscriber.NewRouter(subscriber.RouteFromMetadataKey("type"))
	}

	t.Run("Start", func(t *testing.T) {
		t.Run("should return the terminal error of the subscriber once its channel closes", func(t *testing.T) {
			g := NewGomegaWithT(t)
			boom := errors.New("receive loop died")
			sub := &fakeSubscriber{ch: make(chan *message.Message), err: boom}
			e := subscriber.NewSubscriptionEngine(sub, newRouter())
			done := startEngine(context.Background(), e)

			close(sub.ch)

			g.Eventually(done).Should(Receive(MatchError(boom)))
		})

		t.Run("should return the Subscribe error", func(t *testing.T) {
			g := NewGomegaWithT(t)
			sub := &fakeSubscriber{subscribeErr: errors.New("subscribe failed")}
			e := subscriber.NewSubscriptionEngine(sub, newRouter())

			g.Expect(e.Start(context.Background())).To(MatchError("subscribe failed"))
		})
	})
}
