package subscriber

import (
	"context"
	"encoding/json"
	"github.com/quantumcycle/expedit/core/message"
)

type TypedMessageHandler[T any] interface {
	OnMessage(ctx context.Context, event *T) error
}

type SubscriptionRouter struct {
	generator    RoutingKeyGenerator
	routesByType map[string]message.HandlerFunc
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
		routesByType: map[string]message.HandlerFunc{},
	}
}

func (d *SubscriptionRouter) AddHandler(value string, handler message.HandlerFunc) {
	d.routesByType[value] = handler
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
		handler := d.routesByType[string(dispatchType)]
		if handler != nil {
			return handler(msg)
		}
		if d.defaultRoute != nil {
			return d.defaultRoute(msg)
		}
		panic("no handler found for dispatch type")
	}
}

func NewJSONMessageTypedHandler[T any](handler func(ctx context.Context, msgID string, metadata map[string]string, event *T) error) message.HandlerFunc {
	handlerFn := func(msg *message.Message) error {
		event := new(T)
		err := json.Unmarshal(msg.Payload, event)
		if err != nil {
			return err
		}
		return handler(msg.Context(), msg.ID, msg.Metadata, event)
	}
	return handlerFn
}
