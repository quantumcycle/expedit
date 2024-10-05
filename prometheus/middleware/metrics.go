package middleware

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/message/middleware"
	"time"
)

type PrometheusLabelsProducer func(msg *message.Message, err error) prometheus.Labels

// PrometheusMetricsCount counts the number of messages passing through
// Make sure the counter is registered in your prometheus registry
func PrometheusMetricsCount(counter prometheus.Counter) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (err error) {
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
func PrometheusMetricsCountVec(counter *prometheus.CounterVec, labelsProducer PrometheusLabelsProducer) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (err error) {
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
func PrometheusMetricsDuration(histogram prometheus.Histogram) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (err error) {
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
func PrometheusMetricsDurationVec(histogram *prometheus.HistogramVec, labelsProducer PrometheusLabelsProducer) middleware.Middleware {
	return func(next message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (err error) {
			start := time.Now()
			defer func() {
				histogram.With(labelsProducer(msg, err)).Observe(time.Since(start).Seconds())
			}()
			err = next(msg)
			return
		}
	}
}
