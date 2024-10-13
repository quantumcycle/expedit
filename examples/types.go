package examples

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"os/signal"
)

type DummyEvent1 struct {
	Prop1 string `json:"prop1"`
}

type DummyEvent2 struct {
	Prop2 string `json:"prop2"`
}

func CreatePromOutgoingCount(labels []string) *prometheus.CounterVec {
	outgoingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_counter",
	}, labels)
	err := prometheus.DefaultRegisterer.Register(outgoingMsgCount)
	if err != nil {
		panic(err)
	}
	return outgoingMsgCount
}

func CreatePromOutgoingDuration(labels []string) *prometheus.HistogramVec {
	outgoingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_duration",
	}, labels)
	err := prometheus.DefaultRegisterer.Register(outgoingMsgDuration)
	if err != nil {
		panic(err)
	}
	return outgoingMsgDuration
}

func CreatePromIncomingCount(labels []string) *prometheus.CounterVec {
	incomingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_counter",
	}, labels)
	err := prometheus.DefaultRegisterer.Register(incomingMsgCount)
	if err != nil {
		panic(err)
	}
	return incomingMsgCount
}

func CreatePromIncomingDuration(labels []string) *prometheus.HistogramVec {
	incomingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_duration",
	}, labels)
	err := prometheus.DefaultRegisterer.Register(incomingMsgDuration)
	if err != nil {
		panic(err)
	}
	return incomingMsgDuration
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
