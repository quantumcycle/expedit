package main

import (
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	examples "github.com/quantumcycle/expedit/example"
	promware "github.com/quantumcycle/expedit/prometheus/middleware"
	exred "github.com/quantumcycle/expedit/redis"
	"github.com/redis/go-redis/v9"
	"math/rand"
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

func AddStructNameToMetadata() middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) error {
			msg.Metadata["event_type"] = getTypeName(msg.Payload)
			return next(msg)
		}
	}
}

func createPublisher(client *redis.Client, stream publisher.Destination) (*publisher.PublishingEngine, error) {
	pub, err := exred.NewRedisPublisher(client,
		publisher.ConstantDestination(stream),
		exred.MarshallPayloadToJsonMap("json"),
		exred.WithMetadataMarshaller(exred.MetadataWithPrefix("metadata_")))
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
		AddMiddleware(AddStructNameToMetadata())

	return pubEngine, nil
}

func createSubscriber(client *redis.Client, stream string, router *subscriber.SubscriptionRouter) (*subscriber.SubscriptionEngine, error) {
	redisSub, err := exred.NewRedisSubscriber(client,
		stream,
		exred.WithStartID(exred.StartFromBeginning),
		//Match the prefixes used by the marshaller
		exred.WithMetadataExtractor(exred.PrefixMetadataExtractor("metadata_")),
		exred.WithPayloadExtractor(exred.NonPrefixPayloadExtractor("metadata_")))
	if err != nil {
		return nil, err
	}

	subEngine := subscriber.NewSubscriptionEngine(redisSub, *router)
	subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"subscriber": "my_subscriber", "error": strconv.FormatBool(err != nil)}
	}
	subEngine.
		AddMiddleware(promware.PrometheusMetricsCountVec(examples.CreatePromIncomingCount([]string{"subscriber", "error"}), subscriberLabelsProducer)).
		AddMiddleware(promware.PrometheusMetricsDurationVec(examples.CreatePromIncomingDuration([]string{"subscriber", "error"}), subscriberLabelsProducer)).
		AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
			fmt.Printf("Error in handler for message %s [%s]: %s\n", msg.ID, msg.Metadata, err.Error())
		})).
		AddMiddleware(middleware.ConvertPanicToError())
	return subEngine, nil
}

func main() {
	ctx := context.Background()
	stream := publisher.Destination("demo-stream-" + shortuuid.New())
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:29379",
	})

	//****************** Producer **********************

	pubEngine, err := createPublisher(client, stream)
	if err != nil {
		panic(err)
	}

	msg := message.NewMessage(ctx, "id-1", examples.DummyEvent1{Prop1: "value1"})
	err = pubEngine.Publish(msg)
	if err != nil {
		panic(err)
	}
	msg = message.NewMessage(ctx, "id-2", examples.DummyEvent2{Prop2: "values2"})
	err = pubEngine.Publish(msg)
	if err != nil {
		panic(err)
	}

	//***************** Consumer **********************
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
	router.
		AddHandler("DummyEvent1").
		AddMiddleware(exred.UnmarshallMapPayloadFromJson("json", examples.DummyEvent1{})).
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

	subEngine, err := createSubscriber(client, string(stream), router)
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
