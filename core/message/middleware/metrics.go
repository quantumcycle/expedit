package middleware

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"time"
)

type PrometheusLabelsProducer func(msg *message.Message[any], err error) prometheus.Labels

// PrometheusMetricsCount counts the number of messages passing through
// Make sure the counter is registered in your prometheus registry
func PrometheusMetricsCount(counter prometheus.Counter) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			defer func() {
				counter.Inc()
			}()
			err = next(msg)
			return
		}
	}
}

// PrometheusMetricsCount counts the number of messages passing through
// Make sure the counter is registered in your prometheus registry
func PrometheusMetricsCountVec(counter *prometheus.CounterVec, labelsProducer PrometheusLabelsProducer) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			defer func() {
				counter.With(labelsProducer(msg, err)).Inc()
			}()
			err = next(msg)
			return
		}
	}
}

// PrometheusMetricsDuration records the duration of the message processing time
// Make sure the counter is registered in your prometheus registry
func PrometheusMetricsDuration(histogram prometheus.Histogram) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			start := time.Now()
			defer func() {
				histogram.Observe(time.Since(start).Seconds())
			}()
			err = next(msg)
			return
		}
	}
}

// PrometheusMetricsDuration records the duration of the message processing time
// Make sure the counter is registered in your prometheus registry
func PrometheusMetricsDurationVec(histogram *prometheus.HistogramVec, labelsProducer PrometheusLabelsProducer) Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message[any]) (err error) {
			start := time.Now()
			defer func() {
				histogram.With(labelsProducer(msg, err)).Observe(time.Since(start).Seconds())
			}()
			err = next(msg)
			return
		}
	}
}
