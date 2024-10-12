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
	if p.handlerFn != nil {
		panic("cannot add middleware after subscription has started")
	}
	p.mc.Add(m)
	return p
}

func (e *SubscriptionEngine) Start(ctx context.Context) error {
	if e.handlerFn == nil {
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.handlerFn == nil {
			e.handlerFn = e.mc.Wrap(e.router.HandlerFunc())
		}
	} else {
		panic("cannot start subscription engine twice")
	}
	msgChannel, err := e.sub.Subscribe(ctx)
	if err != nil {
		return err
	}
	for msg := range msgChannel {
		//Avoid golang loop variable issue
		loopMsg := msg
		go func() {
			handleMessage(loopMsg, e.handlerFn)
		}()
	}
	return nil
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
