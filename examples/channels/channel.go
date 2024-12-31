package main

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	examples "github.com/quantumcycle/expedit/example"
	promware "github.com/quantumcycle/expedit/prometheus/middleware"
	"math/rand"
	"time"
)

func main() {

	//****************** Producer **********************

	var err error
	channel := make(chan *message.Message, 100)

	channelPub := publisher.NewChannelPublisher(channel)
	pubEngine := publisher.NewPublishingEngine(channelPub)
	pubEngine.AddMiddleware(middleware.Throttle(1, time.Second))
	publisherLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"publisher": "my_publisher"}
	}
	pubEngine.AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromOutgoingCount([]string{"publisher"}), publisherLabelsProducer))
	pubEngine.AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromOutgoingDuration([]string{"publisher"}), publisherLabelsProducer))
	pubEngine.AddMiddleware(middleware.ConvertPanicToError())

	err = pubEngine.Publish(message.NewMessage(context.Background(), examples.DummyEvent1{Prop1: "value1"}).
		WithMetadata("event_type", "DummyEvent1"))
	if err != nil {
		panic(err)
	}
	err = pubEngine.Publish(message.NewMessage(context.Background(), examples.DummyEvent2{Prop2: "value2"}).
		WithMetadata("event_type", "DummyEvent2"))
	if err != nil {
		panic(err)
	}

	//***************** Consumer **********************

	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
	router.AddHandler("DummyEvent1").
		Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent1 examples.DummyEvent1) error {
			sec := rand.Intn(5) + 2
			start := time.Now()
			time.Sleep(time.Duration(sec) * time.Second)
			fmt.Printf("Dummy event handler 1 handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
			return nil
		}))
	router.AddHandler("DummyEvent2").
		Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent1 examples.DummyEvent2) error {
			fmt.Printf("Dummy event handler 2 handler received message %s at %s\n", msg.ID, time.Now().Format(time.StampMilli))
			return nil
		}))
	router.AddDefaultHandler(func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Default handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})
	subs := subscriber.NewChannelSubscriber(channel, 10)
	subEngine := subscriber.NewSubscriptionEngine(subs, *router)
	subEngine.AddMiddleware(middleware.ContextTimeout(30 * time.Second))
	subEngine.AddMiddleware(middleware.Throttle(1, 500*time.Millisecond))
	subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"subscriber": "my_subscriber", "success": fmt.Sprintf("%t", err == nil)}
	}
	subEngine.AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromIncomingCount([]string{"subscriber", "success"}), subscriberLabelsProducer))
	subEngine.AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromIncomingDuration([]string{"subscriber", "success"}), subscriberLabelsProducer))
	subEngine.AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
		fmt.Printf("Error in handler for message %s [%s]: %s\n", msg.ID, msg.Metadata, err.Error())
	}))
	subEngine.AddMiddleware(middleware.ConvertPanicToError())

	go func() {
		err := subEngine.Start(context.TODO())
		if err != nil {
			panic(err)
		}
	}()

	//****************** Main loop **********************
	waitCh := make(chan bool, 1)
	examples.CleanupOnInterrupt("main_wait", func() {
		waitCh <- true
	})
	<-waitCh
}
