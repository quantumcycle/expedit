package subscriber

import (
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"sync"
)

type SubscriptionEngine struct {
	sub     Subscriber
	router  SubscriptionRouter
	mc      *middleware.Chain
	onPanic OnPanicListener
	onError OnErrorListener

	lock      sync.Mutex
	handlerFn message.HandlerFunc
}

type OnPanicListener = func(msg *message.Message, r any)
type OnErrorListener = func(msg *message.Message, err error)

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

func (p *SubscriptionEngine) SetOnPanicListener(listener OnPanicListener) *SubscriptionEngine {
	if p.handlerFn != nil {
		panic("cannot alter engine after subscription has started")
	}
	p.onPanic = listener
	return p
}

func (p *SubscriptionEngine) SetOnErrorListener(listener OnErrorListener) *SubscriptionEngine {
	if p.handlerFn != nil {
		panic("cannot alter engine after subscription has started")
	}
	p.onError = listener
	return p
}

func (p *SubscriptionEngine) AddMiddleware(m middleware.Middleware) *SubscriptionEngine {
	if p.handlerFn != nil {
		panic("cannot add middleware after subscription has started")
	}
	p.mc.Add(m)
	return p
}

func (e *SubscriptionEngine) Start() error {
	if e.handlerFn == nil {
		e.lock.Lock()
		defer e.lock.Unlock()
		if e.handlerFn == nil {
			e.handlerFn = e.mc.Wrap(e.router.HandlerFunc())
		}
	} else {
		panic("cannot start subscription engine twice")
	}
	msgChannel, err := e.sub.Subscribe()
	if err != nil {
		return err
	}
	for msg := range msgChannel {
		//Avoid golang loop variable issue
		loopMsg := msg
		go func() {
			handleMessage(loopMsg, e.handlerFn, e.onError, e.onPanic)
		}()
	}
	return nil
}

func handleMessage(
	msg *message.Message,
	handler message.HandlerFunc,
	onError OnErrorListener,
	onPanic OnPanicListener) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if onPanic != nil {
				onPanic(msg, recovered)
			}
			msg.Nack()
		}
	}()
	err := handler(msg)
	if err != nil {
		if onError != nil {
			onError(msg, err)
		}
		msg.Nack()
		return
	}
	msg.Ack()
	return
}
