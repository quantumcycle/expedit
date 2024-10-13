package subscriber

import (
	"context"
	"errors"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
)

type TypedMessageHandler[T any] interface {
	OnMessage(ctx context.Context, event *T) error
}

type SubscriptionRouter struct {
	generator    RoutingKeyGenerator
	routesByType map[string]*HandlerBuilder
	defaultRoute message.HandlerFunc
}

type RoutingKey string
type RoutingKeyGenerator = func(msg *message.Message) RoutingKey

func RouteFromMetadataKey(metadataKey string) RoutingKeyGenerator {
	return func(msg *message.Message) RoutingKey {
		val := msg.Metadata[metadataKey]
		return RoutingKey(val)
	}
}

func NewRouter(generator RoutingKeyGenerator) *SubscriptionRouter {
	return &SubscriptionRouter{
		generator:    generator,
		routesByType: map[string]*HandlerBuilder{},
	}
}

type HandlerBuilder struct {
	handler message.HandlerFunc
	mc      *middleware.Chain
}

func (hb *HandlerBuilder) AddMiddleware(m middleware.Middleware) *HandlerBuilder {
	hb.mc.Add(m)
	return hb
}

func (hb *HandlerBuilder) Handle(handler message.HandlerFunc) {
	hb.handler = handler
}

func (d *SubscriptionRouter) AddHandler(value string) *HandlerBuilder {
	builder := &HandlerBuilder{
		handler: nil,
		mc:      middleware.NewChain(),
	}
	d.routesByType[value] = builder
	return builder
}

func (d *SubscriptionRouter) AddDefaultHandler(handler message.HandlerFunc) {
	if d.defaultRoute != nil {
		//programming error, adding a default handler twice
		panic("default handler already set")
	}
	d.defaultRoute = handler
}

func (d *SubscriptionRouter) HandlerFunc() message.HandlerFunc {
	return func(msg *message.Message) error {
		dispatchType := d.generator(msg)
		handlerBuilder := d.routesByType[string(dispatchType)]
		if handlerBuilder != nil && handlerBuilder.handler != nil {
			handlerFn := handlerBuilder.mc.Wrap(handlerBuilder.handler)
			return handlerFn(msg)
		}
		if d.defaultRoute != nil {
			return d.defaultRoute(msg)
		}
		panic("no handler found for dispatch type")
	}
}

func NewTypedMessageHandler[T any](handler func(ctx context.Context, msgID string, metadata map[string]string, event T) error) message.HandlerFunc {
	handlerFn := func(msg *message.Message) error {
		pl := msg.Payload
		if typed, ok := pl.(T); ok {
			return handler(msg.Context(), msg.ID, msg.Metadata, typed)
		}
		return errors.New("payload type does not match handler type")
	}
	return handlerFn
}
