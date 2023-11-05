# Expedit

**NOT READY FOR PRODUCTION USE**

This is a golang library for interacting with message publish/subscribe brokers. It provides building blocks for creating messages producers and consumers.

## Getting started

### Install
```bash
go get -u github.com/quantumcycle/expedit/core

#then choose your implementation, for example Google Pubsub
go get -u github.com/quantumcycle/expedit/google
```

### Example

Check the code in the [example](example/main.go) folder for a working example.

## Definitions

* Middleware		: a function that takes a message and performs some action before and after the message being processed
* Publisher 		: the entrypoint for publishing messages to a broker of a pubsub implementation
* PublishingEngine  : the combination of a Publisher implementation and a set of middlewares
* Subscriber		: the endpoint of a pipeline receiving messages from a pubsub implementation
* Handler           : a function that takes a message and performs some action with it
* Router            : a component that takes care of sending a message to a message handler
* SubscriptionEngine: the combination of a Subscriber implementation, a set of middlewares and a message router

## Why

I created this library after trying to use [Watermill](https://watermill.io/) in a work project. I found Watermill to be a great library,
but it's designed for more complex use cases where intricate routing is required between multiple Publishers and Subscribers. My use cases were a bit simpler,
but still, I liked some of the concepts in Watermill, like the use of middleware and the ability to integrate with multiple message brokers. So I decided
to create this library to provide a simpler interface for interacting with message brokers.

A big thanks to Three dot labs for creating Watermill and also for their [Event driven Go](https://threedots.tech/event-driven/) hands-on training, which I did and greatly recommend.


## License

[MIT License](LICENSE)
```