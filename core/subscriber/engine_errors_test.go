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
	err error
}

func (f *fakeSubscriber) Receive(ctx context.Context, handler message.HandlerFunc) error {
	return f.err
}

func TestSubscriptionEngineSubscriberErrors(t *testing.T) {
	t.Run("Start", func(t *testing.T) {
		t.Run("should return the error of the subscriber", func(t *testing.T) {
			g := NewGomegaWithT(t)
			boom := errors.New("receive failed")
			e := subscriber.NewSubscriptionEngine(&fakeSubscriber{err: boom},
				*subscriber.NewRouter(subscriber.RouteFromMetadataKey("type")))

			g.Expect(e.Start(context.Background())).To(MatchError(boom))
		})
	})
}
