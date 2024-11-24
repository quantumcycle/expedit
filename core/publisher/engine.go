package publisher

import (
	"github.com/quantumcycle/expedit/core/log"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"sync"
)

type PublishingEngine struct {
	pub       Publisher
	logger    log.Logger
	mc        *middleware.Chain
	lock      sync.Mutex
	handlerFn message.HandlerFunc
}

func NewPublishingEngine(pub Publisher) *PublishingEngine {
	return NewLoggedPublishingEngine(pub, nil)
}

func NewLoggedPublishingEngine(pub Publisher, l log.Logger) *PublishingEngine {
	return &PublishingEngine{
		pub:       pub,
		logger:    l,
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
			p.handlerFn = p.mc.Wrap(func(msg *message.Message) error {
				err := p.pub.Publish(msg)
				if err != nil && p.logger != nil {
					p.logger.Errorf(msg.Context(), "error publishing message: %v", err)
				}
				return err
			})
		}
	}

	return p.handlerFn(msg)
}
