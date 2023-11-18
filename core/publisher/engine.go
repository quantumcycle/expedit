package publisher

import (
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"sync"
)

type PublishingEngine struct {
	pub       Publisher
	mc        *middleware.Chain
	lock      sync.Mutex
	handlerFn message.HandlerFunc
}

func NewPublishingEngine(pub Publisher) *PublishingEngine {
	return &PublishingEngine{
		pub:       pub,
		mc:        middleware.NewChain(),
		lock:      sync.Mutex{},
		handlerFn: nil,
	}
}

func (p *PublishingEngine) AddMiddleware(m middleware.Middleware) *PublishingEngine {
	if p.handlerFn != nil {
		panic("cannot add middleware after publishing has started")
	}
	p.mc.Add(m)
	return p
}

// Publish publishes the messages to the destination topic calculated by the routing function.
func (p *PublishingEngine) Publish(msg *message.Message) error {
	if p.handlerFn == nil {
		p.lock.Lock()
		defer p.lock.Unlock()
		if p.handlerFn == nil {
			p.handlerFn = p.mc.Wrap(func(msg *message.Message[any]) error {
				return p.pub.Publish(msg)
			})
		}
	}

	return p.handlerFn(msg)
}
