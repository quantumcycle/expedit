package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	examples "github.com/quantumcycle/expedit/example"
	exmw "github.com/quantumcycle/expedit/example/middleware"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	promware "github.com/quantumcycle/expedit/prometheus/middleware"
	"github.com/sony/gobreaker/v2"
	"log/slog"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"time"
)

func getTypeName(myvar interface{}) string {
	if t := reflect.TypeOf(myvar); t.Kind() == reflect.Ptr {
		return "*" + t.Elem().Name()
	} else {
		return t.Name()
	}
}

func createPublisher(client *pubsub.Client, topic publisher.Destination) (*publisher.PublishingEngine, error) {
	// no routing. send everything to our single topic
	routingFn := func(msg *message.Message) (publisher.Destination, error) {
		return topic, nil
	}
	pub, err := google.NewGooglePublisher(client,
		routingFn)
	if err != nil {
		return nil, err
	}
	publisherLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"publisher": "my_publisher", "error": strconv.FormatBool(err != nil)}
	}
	pubEngine := publisher.NewPublishingEngine(pub)
	pubEngine.
		AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromOutgoingCount([]string{"publisher", "error"}), publisherLabelsProducer)).
		AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromOutgoingDuration([]string{"publisher", "error"}), publisherLabelsProducer)).
		AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
			fmt.Printf("Error in publisher for message %s [%s]: %s\n", msg.ID, msg.Metadata, err.Error())
		})).
		AddMiddleware(middleware.ConvertPanicToError()).
		//need to be before the Marshall step so we can have the original struct name
		AddMiddleware(AddStructNameToMetadata()).
		AddMiddleware(google.MarshallPayloadToJson())

	return pubEngine, nil
}

func createSubscriber(client *pubsub.Client, subscription string, router *subscriber.SubscriptionRouter) (*subscriber.SubscriptionEngine, error) {
	googleSub, err := google.NewGoogleSubscriber(client,
		subscription)
	if err != nil {
		return nil, err
	}
	subEngine := subscriber.NewSubscriptionEngine(googleSub, *router)
	subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"subscriber": "my_subscriber", "error": strconv.FormatBool(err != nil)}
	}
	testBreaker := gobreaker.NewCircuitBreaker[*message.Message](gobreaker.Settings{})
	subEngine.
		AddMiddleware(exmw.CircuitBreaker(testBreaker)).
		AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromIncomingCount([]string{"subscriber", "error"}), subscriberLabelsProducer)).
		AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromIncomingDuration([]string{"subscriber", "error"}), subscriberLabelsProducer)).
		AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
			fmt.Printf("Error in handler for message %s [%s]: %s\n", msg.ID, msg.Metadata, err.Error())
		})).
		AddMiddleware(middleware.ConvertPanicToError())
	return subEngine, nil
}

func AddStructNameToMetadata() middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			msg.Metadata["event_type"] = getTypeName(msg.Payload)
			return next(msg)
		}
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	logger.Debug("Debug message")
	logger.Info("Info message")
	logger.Warn("Warning message")
	logger.Error("Error message", "time", time.Now().Add(1*time.Hour))

	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:29085")
	var err error
	var client *pubsub.Client
	var emuClient *emulator.PubsubTestClient
	var topic *emulator.TestTopic

	ctx := context.Background()
	emuClient = emulator.NewTestClient(ctx, "test-project")
	topic = emuClient.CreateTestTopic(ctx, "test-topic")
	subscription := topic.CreateTestSubscription(ctx, "test-subscription", false)

	//****************** Producer **********************

	client, err = pubsub.NewClient(ctx, "test-project")
	if err != nil {
		panic(err)
	}

	pubEngine, err := createPublisher(client, topic.Name)
	if err != nil {
		panic(err)
	}
	msg := message.NewMessage(ctx, "id-1", examples.DummyEvent1{Prop1: "value1"})
	err = pubEngine.Publish(msg)
	if err != nil {
		panic(err)
	}

	//***************** Consumer **********************
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
	router.
		AddHandler("DummyEvent1").
		AddMiddleware(google.UnmarshallPayloadFromJson(examples.DummyEvent1{})).
		Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent1 examples.DummyEvent1) error {
			sec := rand.Intn(5) + 2
			start := time.Now()
			fmt.Println("Dummy event handler 1: Sleeping for", sec, "seconds")
			time.Sleep(time.Duration(sec) * time.Second)
			fmt.Printf("Dummy event handler 1: handler received message %s with prop1=[%s] at %s finished at %s\n",
				msg.ID, dummyEvent1.Prop1, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
			return nil
		}))

	router.AddDefaultHandler(func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		fmt.Println("Default handler: Sleeping for", sec, "seconds")
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Default handler received message %s at %s finished at %s\n",
			msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})

	subEngine, err := createSubscriber(client, subscription.Name, router)
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
