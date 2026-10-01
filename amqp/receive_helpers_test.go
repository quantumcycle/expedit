package amqp_test

import (
	"context"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/amqp/testrabbit"
	"github.com/quantumcycle/expedit/core/message"
)

// newTestChannel connects to RabbitMQ and returns a connection and a channel, closed when the test ends.
func newTestChannel(t *testing.T) (*amqp.ReconnectingConnection, *amqp.ReconnectingChannel) {
	t.Helper()
	conn, channel, err := createTestConnection()
	if err != nil {
		t.Fatalf("failed to connect to RabbitMQ: %v", err)
	}
	t.Cleanup(func() {
		_ = channel.Close()
		_ = conn.Close()
	})
	return conn, channel
}

// deleteQueueOnTestEnd deletes the queue on a new channel, so it works even if the test closed its own channel.
func deleteQueueOnTestEnd(t *testing.T, conn *amqp.ReconnectingConnection, queueName string) {
	t.Helper()
	t.Cleanup(func() {
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		defer ch.Close()
		_, _ = ch.QueueDelete(queueName, false, false, false)
	})
}

// newTestQueue declares a queue that is deleted when the test ends.
func newTestQueue(t *testing.T, conn *amqp.ReconnectingConnection, channel *amqp.ReconnectingChannel, name string) testrabbit.DirectQueue {
	t.Helper()
	queue := testrabbit.CreateDirectExchangeQueue(channel, name)
	deleteQueueOnTestEnd(t, conn, queue.QueueName)
	return queue
}

// queueState returns the number of ready messages, and of consumers, of the queue. It uses its own channel, so it
// never runs a RPC concurrently with the ones of the channel under test.
func queueState(g Gomega, conn *amqp.ReconnectingConnection, queueName string) (ready, consumers int) {
	ch, err := conn.Channel()
	g.Expect(err).NotTo(HaveOccurred())
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queueName, false, false, false, false, nil)
	g.Expect(err).NotTo(HaveOccurred())
	return q.Messages, q.Consumers
}

// receiving is a running Subscriber.Receive.
type receiving struct {
	t        *testing.T
	cancel   context.CancelFunc
	finished chan struct{}
	err      error
	observed bool
	lock     sync.Mutex
}

// startReceive runs sub.Receive(ctx, handler) in a goroutine and returns once the broker has the new consumer. Call
// it one at a time for subscribers that share a channel. The subscriber is stopped when the test ends, and Receive
// must then return nil unless the test observed its result with Stop or Wait.
func startReceive(t *testing.T, conn *amqp.ReconnectingConnection, sub *amqp.Subscriber, queueName string, handler message.HandlerFunc) *receiving {
	t.Helper()
	return startReceiveFunc(t, conn, queueName, func(ctx context.Context) error {
		return sub.Receive(ctx, handler)
	})
}

// startReceiveFunc is startReceive for anything that blocks like Subscriber.Receive, such as a subscription engine.
func startReceiveFunc(t *testing.T, conn *amqp.ReconnectingConnection, queueName string, receive func(ctx context.Context) error) *receiving {
	t.Helper()
	g := NewGomegaWithT(t)
	_, consumersBefore := queueState(g, conn, queueName)

	ctx, cancel := context.WithCancel(context.Background())
	r := &receiving{t: t, cancel: cancel, finished: make(chan struct{})}
	go func() {
		defer close(r.finished)
		r.err = receive(ctx)
	}()
	t.Cleanup(func() {
		err := r.Stop()
		r.lock.Lock()
		observed := r.observed
		r.lock.Unlock()
		if !observed {
			g.Expect(err).NotTo(HaveOccurred())
		}
	})

	g.Eventually(func() bool {
		select {
		case <-r.finished:
			return true // Receive failed before the consumer was registered
		default:
		}
		_, consumers := queueState(g, conn, queueName)
		return consumers == consumersBefore+1
	}, 5*time.Second, 20*time.Millisecond).Should(BeTrue())
	select {
	case <-r.finished:
		t.Fatalf("Receive returned before consuming: %v", r.err)
	default:
	}
	return r
}

// Stop cancels the context of Receive, waits for it to return and returns its result.
func (r *receiving) Stop() error {
	r.cancel()
	return r.Wait()
}

// Wait waits for Receive to return, without cancelling it, and returns its result.
func (r *receiving) Wait() error {
	r.lock.Lock()
	r.observed = true
	r.lock.Unlock()
	NewGomegaWithT(r.t).Eventually(r.finished, 15*time.Second).Should(BeClosed(), "Receive did not return")
	return r.err
}

// Finished is closed when Receive has returned.
func (r *receiving) Finished() <-chan struct{} {
	return r.finished
}

// stringSet is a concurrent set of strings.
type stringSet struct {
	lock  sync.Mutex
	items map[string]int
}

func newStringSet() *stringSet {
	return &stringSet{items: map[string]int{}}
}

func (s *stringSet) Add(item string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.items[item]++
}

// Len is the number of distinct items.
func (s *stringSet) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return len(s.items)
}

// Count is the number of times the item was added.
func (s *stringSet) Count(item string) int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.items[item]
}
