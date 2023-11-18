package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/core/subscriber"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

func main() {

	//****************** Producer **********************

	//ctx := context.Background()
	var err error
	channel := make(chan *message.Message, 100)

	channelPub := publisher.NewChannelPublisher(channel)

	outgoingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_counter",
	}, []string{"publisher"})
	err = prometheus.DefaultRegisterer.Register(outgoingMsgCount)
	if err != nil {
		panic(err)
	}

	outgoingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_duration",
	}, []string{"publisher"})
	err = prometheus.DefaultRegisterer.Register(outgoingMsgDuration)
	if err != nil {
		panic(err)
	}

	pubEngine := publisher.NewPublishingEngine(channelPub)
	pubEngine.AddMiddleware(middleware.Throttle(1, time.Second))
	publisherLabelsProducer := func(msg *message.Message[any], err error) prometheus.Labels {
		return prometheus.Labels{"publisher": "my_publisher"}
	}
	pubEngine.AddMiddleware(middleware.PrometheusMetricsCountVec(outgoingMsgCount, publisherLabelsProducer))
	pubEngine.AddMiddleware(middleware.PrometheusMetricsDurationVec(outgoingMsgDuration, publisherLabelsProducer))

	err = pubEngine.Publish(message.NewMessage[[]byte](context.Background(), uuid.New().String(), []byte("{}")))
	if err != nil {
		panic(err)
	}
	err = pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte("{}")))
	if err != nil {
		panic(err)
	}
	err = pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte("{}")))
	if err != nil {
		panic(err)
	}
	err = pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte("{\"prop1\":\"value1\"}")).
		WithMetadata("event_type", "DummyEvent1"))
	if err != nil {
		panic(err)
	}
	err = pubEngine.Publish(message.NewMessage(context.Background(), uuid.New().String(), []byte("{\"prop2\":\"value2\"}")).
		WithMetadata("event_type", "DummyEvent2"))
	if err != nil {
		panic(err)
	}

	//***************** Consumer **********************

	incomingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_counter",
	}, []string{"subscriber", "success"})
	err = prometheus.DefaultRegisterer.Register(incomingMsgCount)
	if err != nil {
		panic(err)
	}

	incomingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_duration",
	}, []string{"subscriber", "success"})
	err = prometheus.DefaultRegisterer.Register(incomingMsgDuration)
	if err != nil {
		panic(err)
	}

	subs := subscriber.NewChannelSubscriber(channel, 10)
	router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
	router.AddHandler("DummyEvent1", func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Dummy event handler 1 handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})
	router.AddHandler("DummyEvent2",
		subscriber.NewJSONMessageTypedHandler[DummyEvent2](func(ctx context.Context, msgID string, metadata map[string]string, event *DummyEvent2) error {
			fmt.Printf("Dummy event handler 2 handler received message %s at %s\n", msgID, time.Now().Format(time.StampMilli))
			return nil
		}))
	router.AddDefaultHandler(func(msg *message.Message) error {
		sec := rand.Intn(5) + 2
		start := time.Now()
		time.Sleep(time.Duration(sec) * time.Second)
		fmt.Printf("Default handler received message %s at %s finished at %s\n", msg.ID, start.Format(time.StampMilli), time.Now().Format(time.StampMilli))
		return nil
	})
	subEngine := subscriber.NewSubscriptionEngine(subs, *router)
	subEngine.SetOnPanicListener(func(msg *message.Message, err interface{}) {
		fmt.Printf("Panic in handler for message %s: %s\n", msg.ID, err)
	})
	subEngine.AddMiddleware(middleware.ContextTimeout(30 * time.Second))
	subEngine.AddMiddleware(middleware.Throttle(1, 500*time.Millisecond))
	subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
		return prometheus.Labels{"subscriber": "my_subscriber", "success": fmt.Sprintf("%t", err == nil)}
	}
	subEngine.AddMiddleware(middleware.PrometheusMetricsCountVec(incomingMsgCount, subscriberLabelsProducer))
	subEngine.AddMiddleware(middleware.PrometheusMetricsDurationVec(incomingMsgDuration, subscriberLabelsProducer))
	subEngine.AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
		fmt.Printf("Error in handler for message %s: %s\n", msg.ID, err.Error())
	}))
	subEngine.AddMiddleware(middleware.PanicRecoverer())

	go func() {
		err := subEngine.Start()
		if err != nil {
			panic(err)
		}
	}()

	//****************** Main loop **********************
	waitCh := make(chan bool, 1)
	CleanupOnInterrupt("main_wait", func() {
		waitCh <- true
	})
	<-waitCh
}

type DummyEvent1 struct {
	Prop1 string `json:"prop1"`
}

type DummyEvent2 struct {
	Prop2 string `json:"prop2"`
}

func CleanupOnInterrupt(name string, fn func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		fn()
		fmt.Printf("Cleanup done for %s\n", name)
	}()
}

type channelCloser struct {
	ch chan bool
}

func (c channelCloser) Close() error {
	close(c.ch)
	return nil
}
