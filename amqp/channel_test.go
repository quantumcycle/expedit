package amqp_test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/amqp"
	"github.com/quantumcycle/expedit/amqp/testrabbit"
	amqpgo "github.com/rabbitmq/amqp091-go"
)

// deleteQueueOnNewChannel cleans up a queue after the channel that created it was closed by the test.
func deleteQueueOnNewChannel(g Gomega, conn *amqp.ReconnectingConnection, queueName string) {
	ch, err := conn.Channel()
	g.Expect(err).NotTo(HaveOccurred())
	defer ch.Close()
	_, err = ch.QueueDelete(queueName, false, false, false)
	g.Expect(err).NotTo(HaveOccurred())
}

func TestReconnectingChannelConsume(t *testing.T) {
	t.Run("ConsumeWithContext", func(t *testing.T) {
		t.Run("should deliver the messages of the queue", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel, err := createTestConnection()
			g.Expect(err).NotTo(HaveOccurred())
			defer conn.Close()
			defer channel.Close()
			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-consume-ctx-queue")
			defer queue.Delete()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			deliveries, err := channel.ConsumeWithContext(ctx, queue.QueueName, "", true, false, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())
			queue.PublishBytes([]byte("hello"), nil)

			var delivery amqpgo.Delivery
			g.Eventually(deliveries, 3*time.Second).Should(Receive(&delivery))
			g.Expect(string(delivery.Body)).To(Equal("hello"))
		})

		t.Run("should close the deliveries channel when the context is cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel, err := createTestConnection()
			g.Expect(err).NotTo(HaveOccurred())
			defer conn.Close()
			defer channel.Close()
			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-consume-cancel-queue")
			defer queue.Delete()
			ctx, cancel := context.WithCancel(context.Background())

			deliveries, err := channel.ConsumeWithContext(ctx, queue.QueueName, "", false, false, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())
			cancel()

			g.Eventually(deliveries, 3*time.Second).Should(BeClosed())
		})

		t.Run("should stop consuming the queue when the context is cancelled", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel, err := createTestConnection()
			g.Expect(err).NotTo(HaveOccurred())
			defer conn.Close()
			defer channel.Close()
			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-consume-stop-queue")
			defer queue.Delete()
			ctx, cancel := context.WithCancel(context.Background())

			deliveries, err := channel.ConsumeWithContext(ctx, queue.QueueName, "", false, false, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())
			cancel()
			g.Eventually(deliveries, 3*time.Second).Should(BeClosed())

			// the broker no longer has a consumer on the queue
			g.Eventually(func() int {
				q, err := channel.QueueDeclarePassive(queue.QueueName, false, false, false, false, nil)
				g.Expect(err).NotTo(HaveOccurred())
				return q.Consumers
			}, 3*time.Second, 50*time.Millisecond).Should(Equal(0))
		})

		t.Run("should close the deliveries channel when the channel is closed by the developer", func(t *testing.T) {
			g := NewGomegaWithT(t)
			conn, channel, err := createTestConnection()
			g.Expect(err).NotTo(HaveOccurred())
			defer conn.Close()
			queue := testrabbit.CreateDirectExchangeQueue(channel, "test-consume-closed-queue")
			defer deleteQueueOnNewChannel(g, conn, queue.QueueName)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			deliveries, err := channel.ConsumeWithContext(ctx, queue.QueueName, "", false, false, false, false, nil)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(channel.Close()).To(Succeed())

			g.Eventually(deliveries, 5*time.Second).Should(BeClosed())
		})
	})
}

func TestAMQPSubscriberShutdown(t *testing.T) {
	t.Run("should close the message channel when the context is cancelled", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		defer channel.Close()
		queue := testrabbit.CreateDirectExchangeQueue(channel, "test-subscriber-shutdown-queue")
		defer queue.Delete()
		sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
		g.Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())

		msgCh, err := sub.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())
		cancel()

		g.Eventually(msgCh, 3*time.Second).Should(BeClosed())
		g.Expect(sub.Err()).NotTo(HaveOccurred())
	})

	t.Run("should report an error when the channel is closed by the developer", func(t *testing.T) {
		g := NewGomegaWithT(t)
		conn, channel, err := createTestConnection()
		g.Expect(err).NotTo(HaveOccurred())
		defer conn.Close()
		queue := testrabbit.CreateDirectExchangeQueue(channel, "test-subscriber-chan-closed-queue")
		defer deleteQueueOnNewChannel(g, conn, queue.QueueName)
		sub, err := amqp.NewAMQPSubscriber(channel, queue.QueueName)
		g.Expect(err).NotTo(HaveOccurred())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		msgCh, err := sub.Subscribe(ctx)
		g.Expect(err).NotTo(HaveOccurred())

		g.Expect(channel.Close()).To(Succeed())

		g.Eventually(msgCh, 5*time.Second).Should(BeClosed())
		g.Expect(sub.Err()).To(HaveOccurred())
	})
}
