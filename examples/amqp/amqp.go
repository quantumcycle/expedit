package main

import (
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	examples "github.com/quantumcycle/expedit/example"
	promware "github.com/quantumcycle/expedit/prometheus/middleware"
	amqpgo "github.com/rabbitmq/amqp091-go"
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

func createPublisher(channel *amqp.ReconnectingChannel, exchange string) (*publisher.PublishingEngine, error) {
	// Create routing function for different event types
	routingKeyFn := func(msg *message.Message) (string, error) {
		eventType, exists := msg.Metadata["event_type"]
		if !exists {
			return "default", nil
		}
		return fmt.Sprintf("%v", eventType), nil
	}

	pub, err := amqp.NewAMQPPublisher(channel,
		publisher.ConstantDestination(publisher.Destination(exchange)),
		routingKeyFn,
		amqp.DefaultMessageOptions{
			ContentType:  "application/json",
			DeliveryMode: amqpgo.Persistent,
		})
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
		// Add struct name to metadata before marshalling
		AddMiddleware(AddStructNameToMetadata()).
		AddMiddleware(amqp.MarshallPayloadToJson())

	return pubEngine, nil
}

func createSubscriber(channel *amqp.ReconnectingChannel, queueName string, router *subscriber.SubscriptionRouter) (*subscriber.SubscriptionEngine, error) {
	amqpSub, err := amqp.NewAMQPSubscriber(channel,
		queueName,
		amqp.WithAutoAck())
	if err != nil {
		return nil, err
	}

	subEngine := subscriber.NewSubscriptionEngine(amqpSub, *router)
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

func setupInfrastructure(conn *amqp.ReconnectingConnection, exchangeName, queueName string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Declare exchange
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return err
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return err
	}

	// Bind a queue to the exchange
	err = ch.QueueBind(
		queueName,    // queue name
		"",           // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	ctx := context.Background()
	randomID := shortuuid.New()
	exchangeName := fmt.Sprintf("demo-exchange-%s", randomID)
	queueName := fmt.Sprintf("demo-queue-%s", randomID)

	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to RabbitMQ: %v", err))
	}
	defer conn.Close()

	// Setup infrastructure (exchange, queue, bindings)
	err = setupInfrastructure(conn, exchangeName, queueName)
	if err != nil {
		panic(fmt.Sprintf("Failed to setup infrastructure: %v", err))
	}

	channel, err := conn.Channel()

	//****************** Producer **********************

	pubEngine, err := createPublisher(channel, exchangeName)
	if err != nil {
		panic(err)
	}

	//***************** Consumer **********************
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))

	// Handler for DummyEvent1
	router.
		AddHandler("DummyEvent1").
		AddMiddleware(amqp.UnmarshallPayloadFromJson(examples.DummyEvent1{})).
		Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent1 examples.DummyEvent1) error {
			sec := rand.Intn(3) + 1
			start := time.Now()
			fmt.Println("Dummy event handler 1: Processing for", sec, "seconds")
			time.Sleep(time.Duration(sec) * time.Second)
			fmt.Printf("Dummy event handler 1: received message %s with prop1=[%s] at %s finished at %s\n",
				msg.ID, dummyEvent1.Prop1, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
			return nil
		}))

	// Handler for DummyEvent2
	router.
		AddHandler("DummyEvent2").
		AddMiddleware(amqp.UnmarshallPayloadFromJson(examples.DummyEvent2{})).
		Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent2 examples.DummyEvent2) error {
			sec := rand.Intn(3) + 1
			start := time.Now()
			fmt.Println("Dummy event handler 2: Processing for", sec, "seconds")
			time.Sleep(time.Duration(sec) * time.Second)
			fmt.Printf("Dummy event handler 2: received message %s with prop2=[%s] at %s finished at %s\n",
				msg.ID, dummyEvent2.Prop2, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
			return nil
		}))

	// Default handler for unmatched messages
	router.AddDefaultHandler(func(msg *message.Message) error {
		sec := rand.Intn(3) + 1
		start := time.Now()
		fmt.Println("Default handler: Processing for", sec, "seconds")
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Default handler received message %s at %s finished at %s\n",
			msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})

	subEngine, err := createSubscriber(channel, queueName, router)
	if err != nil {
		panic(err)
	}

	// Start subscriber in goroutine
	go func() {
		err := subEngine.Start(ctx)
		if err != nil {
			panic(err)
		}
	}()

	//****************** Main loop **********************
	// Publish different types of messages
	msg1 := message.NewMessage(ctx, examples.DummyEvent1{Prop1: "value1"})
	err = pubEngine.Publish(msg1)
	if err != nil {
		panic(err)
	}

	msg2 := message.NewMessage(ctx, examples.DummyEvent2{Prop2: "value2"})
	err = pubEngine.Publish(msg2)
	if err != nil {
		panic(err)
	}

	msg3 := message.NewMessage(ctx, examples.DummyEvent3{Prop3: "value3"})
	err = pubEngine.Publish(msg3)
	if err != nil {
		panic(err)
	}

	fmt.Println("AMQP example running. Press Ctrl+C to stop...")
	waitCh := make(chan bool, 1)
	examples.CleanupOnInterrupt("amqp_example", func() {
		fmt.Println("Shutting down...")
		waitCh <- true
	})
	<-waitCh
}
