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

func CreatePromOutgoingCount() *prometheus.CounterVec {
	outgoingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_counter",
	}, []string{"publisher"})
	err := prometheus.DefaultRegisterer.Register(outgoingMsgCount)
	if err != nil {
		panic(err)
	}
	return outgoingMsgCount
}

func CreatePromOutgoingDuration() *prometheus.HistogramVec {
	outgoingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "outgoing_test_duration",
	}, []string{"publisher"})
	err := prometheus.DefaultRegisterer.Register(outgoingMsgDuration)
	if err != nil {
		panic(err)
	}
	return outgoingMsgDuration
}

func CreatePromIncomingCount() *prometheus.CounterVec {
	incomingMsgCount := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_counter",
	}, []string{"subscriber", "success"})
	err := prometheus.DefaultRegisterer.Register(incomingMsgCount)
	if err != nil {
		panic(err)
	}
	return incomingMsgCount
}

func CreatePromIncomingDuration() *prometheus.HistogramVec {
	incomingMsgDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test",
		Subsystem: "test",
		Name:      "incoming_test_duration",
	}, []string{"subscriber", "success"})
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
