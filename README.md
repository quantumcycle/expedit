# Expedit

**NOT READY FOR PRODUCTION USE**

This is a golang library for interacting with message publish/subscribe brokers. It provides building blocks for creating 
messages producers and consumers.

## Getting started

### Install
```bash
go get -u github.com/quantumcycle/expedit/core

#then choose your implementation
# Google Pubsub
go get -u github.com/quantumcycle/expedit/google

# Redis
go get -u github.com/quantumcycle/expedit/redis
```

### Example

Check the code in the [example](example/main.go) folder for a working example.

## Supported implementations

| Impl              |  Status  | Notes                            |
|-------------------|:--------:|----------------------------------|
| Golang channel    |   Beta   | Publisher and subscriber written |
| Google GCP Pubsub |   Beta   | Publisher and subscriber written |
| Redis stream      |  Alpha   | Only publisher written           |
| Kafka             | Planned  |                                  |

## Definitions

* Middleware		: a function that takes a message and performs some action before and after the message being processed
* Publishing components
  * Publisher 		: the entrypoint for publishing messages to a broker of a pubsub implementation
  * PublishingEngine  : the combination of a Publisher implementation and a set of middlewares
* Subscription components
  * Subscriber		: the endpoint of a pipeline receiving messages from a pubsub implementation
  * Handler           : a function that takes a message and performs some action with it
  * Router            : a component that takes care of sending a message to a message handler
  * SubscriptionEngine: the combination of a Subscriber implementation, a set of middlewares and a message router

## How to use

### Middlewares

The middlewares are used in both the publishing and subscription engines. They are used to perform actions before and after
the message is processed. For example, you can use a middleware to log the message before it's processed and then log it again
after it's processed. 

You can also do useful stuff, like add a correlation id to the message before it's processed, gather metrics on your
message processing via Prometheus, or even catch errors, panic, and retry the message with exponential backoff if your
underlying implementation doesn't support it.

Here is a list of the provided middlewares, but of course, you can build your own.
* PanicRecovered
* OnError
* ContextTimeout
* Throttle
* PrometheusMetricsCount
* PrometheusMetricsCountVec
* PrometheusMetricsDuration
* PrometheusMetricsDurationVec
* CorrelationIDFromContext

### Publishing

The publishing part is more simple than the subscription part. You just need to create a Publisher and then use it to 
publish messages. You must create a `PublishingEngine`, which starts by providing a `Publisher` implementation and then,
optionally, you can add middlewares to the engine. The middlewares will be executed in the order they are added to the engine.

```go
    channel := make(chan *message.Message, 100)
	channelPub := publisher.NewChannelPublisher(channel)
	pubEngine := publisher.NewPublishingEngine(channelPub)
	pubEngine.AddMiddleware(middleware.Throttle(1, time.Second))
	pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte("test")))
```


### Subscribing

The subscription part is a bit more complex. You need to create a `SubscriptionEngine`, which starts by providing a 
`Subscriber` implementation and then a `Router` to send messages to the right handler. You can also add middlewares to the engine.

Finally, you can set some error and panic listeners via the `SetErrorListener` and `SetPanicListener` methods.

The last step is to start the engine for it to start processing messages.

```go
    channel := make(chan *message.Message, 100)
	subs := subscriber.NewChannelSubscriber(channel, 10)
	router := subscriber.NewRouter(nil)
	router.AddDefaultHandler(func(msg *message.Message) error {
		return nil
	})

	subEngine := subscriber.NewSubscriptionEngine(subs, *router)
	go func() {
		err := subEngine.Start()
		if err != nil {
			panic(err)
		}
	}()
```

### Router

The router component is very similar to an HTTP router. The big difference is that instead of using a HTTP Verb and URL
to do the routing, it will use something present in the messages. 

The simplest case would be if you only want a single handler to process all messages. In this case, you can use the 
`AddDefaultHandler` method of the router and all messages will be sent to that handler.

```go
    router := router.NewRouter()
    router.AddDefaultHandler(handler.NewHandler(func(ctx context.Context, msg *message.Message) error {
        fmt.Println("Received message:", string(msg.Payload))
        return nil
    }))
```

If you want to route messages to different handlers based on some property of the message, you will first need to provide
a `RoutingKeyGenerator`. This is just a function that will return a string to identify the "route" for a specific message.
For example, you could use the `RouteFromMetadataKey` function to get the routing key from a metadata key of the message.

```go
    router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
    router.AddHandler("type1", func(msg *message.Message) error {
        return nil
    })
    router.AddHandler("type2", func(msg *message.Message) error {
        return nil
    })
```

You could of course write your own `RoutingKeyGenerator`.

For each message, the `RoutingKeyGenerator` will generate a value, and the matching handler will be called. If no handlers
are found for the generated value, the message will be sent to the default handler, if one is provided. **If no handlers are
found and no default handler is provided, the router will panic**.

### Message

The `Message` struct is the heart of this library. It's the struct that will be passed around between the different components.

A message has:
* An ID: Just a simple string to identify the message. The value of this ID is gonna depend on the implementation 
you're using. You don't need to provide an ID when creating new messages, but your publisher implementation might require it.
* Metadata: Just a simple map of strings to strings. You can use this to add any metadata you want to the message. 
For example, you could add a `type` key to the metadata to be used by the router to route the message to the right handler. 
Your publisher implementation is responsible for adding the metadata to the underlying structure if supported.
* Payload: The actual message payload. This is of type `any`. Your `PublishingEngine` will use a marshaller to transform 
this into a byte slice to be sent, and your `SubscriptionEngine` will use an unmarshaller to transform the byte slice into 
something expected by your handlers.
* Context: The context of the message. This is a `context.Context` struct. 

Then, you have some methods on the message itself. The most important ones are `Ack` and `Nack` to acknowledge or reject the message.

## Why

I created this library after trying to use [Watermill](https://watermill.io/) in a work project. I found Watermill to be a great library,
but it's designed for more complex use cases where intricate routing is required between multiple Publishers and Subscribers. 
My use cases were a bit simpler, but still, I liked some of the concepts in Watermill, like the use of middleware and the 
ability to integrate with multiple message brokers. So I decided to create this library to provide a simpler interface for 
interacting with message brokers.

Also, Expedit takes the opposite approach to Watermill when it comes to abstractions. Watermill tries to abstract all implementation
to a single interface. Expedit has a very simplement interface, but you keep the ability to use the underlying implementation features 
and particularities. One example of this is that with GCP PubSub, if you want to subscribe to messages, you don't actually have
to know from which topic the messages are coming from. With Watermill, the library has that Topic/Subscription abstraction baked
in the abstraction, so even though you don't need to know the topic, you still need to provide it when creating the subscriber. With
Expedit, you don't because the abstraction is so simple that it doesn't care about the topic in the first place.

A big thanks to Three dot labs for creating Watermill and also for their [Event driven Go](https://threedots.tech/event-driven/) hands-on training, which I did and greatly recommend.

## License

[MIT License](LICENSE)
```