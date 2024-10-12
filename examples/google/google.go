package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/json"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	"github.com/quantumcycle/expedit/core/unmarshaller"
	examples "github.com/quantumcycle/expedit/example"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	promware "github.com/quantumcycle/expedit/prometheus/middleware"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"time"
)

func JSONPayloadMarshaller(msg *message.Message) ([]byte, error) {
	return json.Marshal(msg.Payload)
}

var payloadUnmarshaller = unmarshaller.JSONUnmarshaller{}

func JSONByTypeUnmarshaller(msg *pubsub.Message) (message.Payload, error) {
	name := msg.Attributes["event_type"]
	return payloadUnmarshaller.CreateBytesUnmarshaller()(name, msg.Data)
}

func getTypeName(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func main() {
	//****************** Producer **********************

	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:29085")
	var err error
	var client *pubsub.Client
	var emuClient *emulator.PubsubTestClient
	var topic *emulator.TestTopic

	ctx := context.Background()
	emuClient = emulator.NewTestClient(ctx, "test-project")
	topic = emuClient.CreateTestTopic(ctx, "test-topic")
	subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

	client, err = pubsub.NewClient(ctx, "test-project")
	if err != nil {
		panic(err)
	}

	// no routing. send everything to our single topic
	routingFn := func(msg *message.Message) (publisher.Destination, error) {
		return topic.Name, nil
	}
	pub, err := google.NewGooglePublisher(client,
		routingFn,
		JSONPayloadMarshaller,
		google.PublisherOption{})
	pubEngine := publisher.NewPublishingEngine(pub)
	pubEngine.AddMiddleware(middleware.Throttle(1, time.Second))

	publisherLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"publisher": "my_publisher", "error": strconv.FormatBool(err != nil)}
	}
	pubEngine.AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromOutgoingCount(), publisherLabelsProducer))
	pubEngine.AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromOutgoingDuration(), publisherLabelsProducer))
	pubEngine.AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
		fmt.Printf("Error in publisher for message %s [%s]: %s\n", msg.ID, msg.Metadata, err.Error())
	}))
	pubEngine.AddMiddleware(middleware.ConvertPanicToError())

	err = pubEngine.Publish(message.NewMessage(ctx, "id-1", examples.DummyEvent1{Prop1: "value1"}))

	//***************** Consumer **********************
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
	router.AddHandler("DummyEvent1", func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Dummy event handler 1 handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})
	router.AddDefaultHandler(func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Default handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})

	payloadUnmarshaller.AddType("DummyEvent1", examples.DummyEvent1{})

	googleSub, err := google.NewGoogleSubscriber(client,
		subscription.Name,
		google.SubscriberOption{})
	subEngine := subscriber.NewSubscriptionEngine(googleSub, *router)
	subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"subscriber": "my_subscriber", "error": strconv.FormatBool(err != nil)}
	}
	subEngine.AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromIncomingCount(), subscriberLabelsProducer))
	subEngine.AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromIncomingDuration(), subscriberLabelsProducer))
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
