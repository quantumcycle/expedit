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

// Start subscribes and handles messages until the message channel of the subscriber is closed. It returns the
// terminal error reported by the subscriber (see Subscriber.Err), or nil if the subscription ended without error,
// for example because ctx was cancelled.
func (e *SubscriptionEngine) Start(ctx context.Context) error {
	e.lock.Lock()
	if e.handlerFn != nil {
		e.lock.Unlock()
		panic("cannot start subscription engine twice")
	}
	handlerFn := e.mc.Wrap(e.router.HandlerFunc())
	e.handlerFn = handlerFn
	e.lock.Unlock()

	msgChannel, err := e.sub.Subscribe(ctx)
	if err != nil {
		return err
	}
	for msg := range msgChannel {
		//Avoid golang loop variable issue
		loopMsg := msg
		go func() {
			handleMessage(loopMsg, handlerFn)
		}()
	}
	return e.sub.Err()
}

func handleMessage(
	msg *message.Message,
	handler message.HandlerFunc) {
	defer func() {
		//intercept panic to nack the message, and then resume the panic
		if recovered := recover(); recovered != nil {
			msg.Nack()
			//continue the panic
			//if you don't want a panic, add a middleware to recover from panics
			panic(recovered)
		}
	}()
	err := handler(msg)
	if err != nil {
		msg.Nack()
		return
	}
	msg.Ack()
	return
}
