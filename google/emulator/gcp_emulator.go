package emulator

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/quantumcycle/expedit/google"
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
	Name       google.DestinationTopic
}

func (c PubsubTestClient) CreateTestTopic(ctx context.Context, identifier string) *TestTopic {
	//topic name must start with a letter, so we use ID, but make all of the test topics start with T
	prefix := fmt.Sprintf("T%s", shortuuid.New())
	topicName := fmt.Sprintf("%s%s", prefix, identifier)
	_, err := c.client.CreateTopic(ctx, topicName)
	if err != nil {
		panic(err)
	}
	return &TestTopic{
		client:     c.client,
		Prefix:     prefix,
		Identifier: identifier,
		Name:       google.DestinationTopic(topicName),
	}
}

func (tt TestTopic) Delete(ctx context.Context) {
	err := tt.client.Topic(string(tt.Name)).Delete(ctx)
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
	topic := tt.client.Topic(string(tt.Name))
	_, err := tt.client.CreateSubscription(ctx, subsName, pubsub.SubscriptionConfig{
		Topic:                 topic,
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

func (ts TestSubscription) Delete(ctx context.Context) {
	err := ts.client.Subscription(ts.Name).Delete(ctx)
	if err != nil {
		panic(err)
	}
}

func (ts TestSubscription) MessageChannel(ctx context.Context, size int) chan *pubsub.Message {
	ch := make(chan *pubsub.Message, size)
	go func() {
		err := ts.client.Subscription(ts.Name).Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			ch <- m
		})
		if err != nil {
			panic(err)
		}
	}()
	return ch
}
