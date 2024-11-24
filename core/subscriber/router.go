package subscriber

import (
	"context"
	"fmt"
	"github.com/quantumcycle/expedit/core/log"
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
	opts         SubscriptionRouterOptions
}

type RoutingKey string
type RoutingKeyGenerator = func(msg *message.Message) RoutingKey

func RouteFromMetadataKey(metadataKey string) RoutingKeyGenerator {
	return func(msg *message.Message) RoutingKey {
		val := msg.Metadata[metadataKey]
		return RoutingKey(val)
	}
}

type SubscriptionRouterOptions struct {
	ackOnUnknownRoute bool
	logger            log.Logger
}

type Option func(*SubscriptionRouterOptions)

// WithAckOnUnknownRoute will ack messages that do not have a route handler defined, if set to true.
// This is mutually exclusive with the default route handler. If you have a default route handler, this
// option has no effect.
func WithAckOnUnknownRoute() Option {
	return func(opts *SubscriptionRouterOptions) {
		opts.ackOnUnknownRoute = true
	}
}

// WithLogger sets the logger for the subscription router.
func WithLogger(l log.Logger) Option {
	return func(opts *SubscriptionRouterOptions) {
		opts.logger = l
	}
}

func NewRouter(generator RoutingKeyGenerator, opts ...Option) *SubscriptionRouter {
	options := SubscriptionRouterOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	return &SubscriptionRouter{
		generator:    generator,
		routesByType: map[string]*HandlerBuilder{},
		opts:         options,
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
	if d.opts.ackOnUnknownRoute {
		//programming error, setting both AckOnUnknownRoute and a default handler
		panic("cannot set both AckOnUnknownRoute and a default handler")
	}
	d.defaultRoute = handler
}

func (d *SubscriptionRouter) HandlerFunc() message.HandlerFunc {
	return func(msg *message.Message) error {
		routeKey := d.generator(msg)
		handlerBuilder := d.routesByType[string(routeKey)]
		if handlerBuilder != nil && handlerBuilder.handler != nil {
			if d.opts.logger != nil {
				d.opts.logger.Infof(msg.Context(), "router handling message %s with route [%s]", msg.ID, routeKey)
			}
			handlerFn := handlerBuilder.mc.Wrap(handlerBuilder.handler)
			return handlerFn(msg)
		}
		if d.defaultRoute != nil {
			if d.opts.logger != nil {
				d.opts.logger.Infof(msg.Context(), "router handling message %s with default route", msg.ID)
			}
			return d.defaultRoute(msg)
		}
		if d.opts.ackOnUnknownRoute {
			if d.opts.logger != nil {
				d.opts.logger.Infof(msg.Context(), "router acked unknown message %s. route was %s", msg.ID, routeKey)
			}
			return nil
		}
		panic(fmt.Sprintf("no handler found for route [%s], and no default route defined.", routeKey))
	}
}
