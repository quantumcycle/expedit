package subscriber

import (
	"context"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"sync"
)

type SubscriptionEngine struct {
	sub    Subscriber
	router SubscriptionRouter
	mc     *middleware.Chain

	lock      sync.Mutex
	handlerFn message.HandlerFunc
}

func NewSubscriptionEngine(sub Subscriber, router SubscriptionRouter) *SubscriptionEngine {
	engine := &SubscriptionEngine{
		sub:       sub,
		router:    router,
		mc:        middleware.NewChain(),
		lock:      sync.Mutex{},
		handlerFn: nil,
	}
	return engine
}

func (p *SubscriptionEngine) AddMiddleware(m middleware.Middleware) *SubscriptionEngine {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.handlerFn != nil {
		panic("cannot add middleware after subscription has started")
	}
	p.mc.Add(m)
	return p
}

// Start receives and handles messages until ctx is done or receiving fails, see Subscriber.Receive.
func (e *SubscriptionEngine) Start(ctx context.Context) error {
	e.lock.Lock()
	if e.handlerFn != nil {
		e.lock.Unlock()
		panic("cannot start subscription engine twice")
	}
	handlerFn := e.mc.Wrap(e.router.HandlerFunc())
	e.handlerFn = handlerFn
	e.lock.Unlock()

	return e.sub.Receive(ctx, handlerFn)
}
