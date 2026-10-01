package emulator

import (
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/quantumcycle/expedit/core/publisher"
)

type PubsubTestClient struct {
	client *pubsub.Client
}

func NewTestClient(ctx context.Context, gcpProject string) *PubsubTestClient {
	client, err := pubsub.NewClient(ctx, gcpProject)
	if err != nil {
		panic(err)
	}
	testClient := PubsubTestClient{
		client: client,
	}
	return &testClient
}

func (c PubsubTestClient) Close() {
	err := c.client.Close()
	if err != nil {
		panic(err)
	}
}

type TestTopic struct {
	client     *pubsub.Client
	Prefix     string
	Identifier string
	Name       publisher.Destination
}

func (c PubsubTestClient) CreateTestTopic(ctx context.Context, identifier string) *TestTopic {
	//topic name must start with a letter, so we use ID, but make all of the test topics start with T
	prefix := fmt.Sprintf("T%s", shortuuid.New())
	topicName := fmt.Sprintf("%s%s", prefix, identifier)
	_, err := c.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{
		Name: topicPath(c.client, topicName),
	})
	if err != nil {
		panic(err)
	}
	return &TestTopic{
		client:     c.client,
		Prefix:     prefix,
		Identifier: identifier,
		Name:       publisher.Destination(topicName),
	}
}

func (tt TestTopic) Delete(ctx context.Context) {
	err := tt.client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{
		Topic: topicPath(tt.client, string(tt.Name)),
	})
	if err != nil {
		panic(err)
	}
}

type TestSubscription struct {
	client     *pubsub.Client
	Prefix     string
	Identifier string
	Name       string
}

func (tt TestTopic) CreateTestSubscription(ctx context.Context, identifier string, ordered bool) *TestSubscription {
	subsName := fmt.Sprintf("%s%s", tt.Prefix, identifier)
	_, err := tt.client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:                  subscriptionPath(tt.client, subsName),
		Topic:                 topicPath(tt.client, string(tt.Name)),
		EnableMessageOrdering: ordered,
	})
	if err != nil {
		panic(err)
	}
	return &TestSubscription{
		client:     tt.client,
		Name:       subsName,
		Prefix:     tt.Prefix,
		Identifier: identifier,
	}
}

func (tt TestTopic) PublishBytes(ctx context.Context, bytes []byte, attrs map[string]string) string {
	topicPublisher := tt.client.Publisher(string(tt.Name))
	defer topicPublisher.Stop()
	r := topicPublisher.Publish(ctx, &pubsub.Message{
		Attributes: attrs,
		Data:       bytes,
	})
	id, err := r.Get(ctx)
	if err != nil {
		panic(err)
	}
	return id
}

func (ts TestSubscription) Delete(ctx context.Context) {
	err := ts.client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{
		Subscription: subscriptionPath(ts.client, ts.Name),
	})
	if err != nil {
		panic(err)
	}
}

func (ts TestSubscription) MessageChannel(ctx context.Context, size int) chan *pubsub.Message {
	ch := make(chan *pubsub.Message, size)
	go func() {
		err := ts.client.Subscriber(ts.Name).Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			m.Ack()
			ch <- m
		})
		if err != nil {
			panic(err)
		}
	}()
	return ch
}

func (ts TestSubscription) MessageDataChannel(ctx context.Context, size int) chan string {
	ch := make(chan string, size)
	go func() {
		err := ts.client.Subscriber(ts.Name).Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			m.Ack()
			ch <- string(m.Data)
		})
		if err != nil {
			panic(err)
		}
	}()
	return ch
}

func topicPath(client *pubsub.Client, topic string) string {
	return fmt.Sprintf("projects/%s/topics/%s", client.Project(), topic)
}

func subscriptionPath(client *pubsub.Client, subscription string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", client.Project(), subscription)
}
