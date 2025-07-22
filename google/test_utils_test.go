// test_utils_test.go - shared test utilities for Google module tests
package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/google/emulator"
)

// GoogleTestSetup provides a unified structure for Google PubSub test setup
type GoogleTestSetup struct {
	Client    *pubsub.Client
	EmuClient *emulator.PubsubTestClient
	Topic     *emulator.TestTopic
}

// SetupGoogleTest creates the basic Google PubSub test environment with emulator
func SetupGoogleTest(t *testing.T) (*pubsub.Client, *emulator.PubsubTestClient) {
	// Set the emulator host for Google PubSub
	t.Setenv("PUBSUB_EMULATOR_HOST", "localhost:29085")

	ctx := context.Background()
	emuClient := emulator.NewTestClient(ctx, "test-project")

	client, err := pubsub.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("failed to create pubsub client: %v", err)
	}

	t.Cleanup(func() {
		client.Close()
	})

	return client, emuClient
}

// CreateTestTopic creates a test topic with a unique name
func CreateTestTopic(t *testing.T, emuClient *emulator.PubsubTestClient, namePrefix string) *emulator.TestTopic {
	uniqueID := shortuuid.New()
	topicName := fmt.Sprintf("%s-%s", namePrefix, uniqueID)
	return emuClient.CreateTestTopic(context.Background(), topicName)
}

// NewGoogleTestSetup creates a complete Google PubSub test setup with topic
func NewGoogleTestSetup(t *testing.T, topicPrefix string) *GoogleTestSetup {
	client, emuClient := SetupGoogleTest(t)
	topic := CreateTestTopic(t, emuClient, topicPrefix)

	return &GoogleTestSetup{
		Client:    client,
		EmuClient: emuClient,
		Topic:     topic,
	}
}

// ExpectMessageCount waits for a specific number of messages in a channel
func ExpectMessageCount[T any](g Gomega, ch <-chan T, expectedCount int, timeout time.Duration) {
	g.Eventually(func() int {
		return len(ch)
	}, timeout).Should(Equal(expectedCount))
}

// AsyncCountMessages counts messages in a channel for a specified duration
func AsyncCountMessages(count *int, ch <-chan *message.Message, duration time.Duration) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				*count++
			}
		}
	}()
}

// LoadTestSetup provides setup for load testing scenarios
type LoadTestSetup struct {
	Client    *pubsub.Client
	EmuClient *emulator.PubsubTestClient
	Topics    []*emulator.TestTopic
	Subs      map[string]*emulator.TestSubscription
}

// NewLoadTestSetup creates a setup for load testing with multiple topics and subscriptions
func NewLoadTestSetup(t *testing.T, topicCount, subsPerTopic int) *LoadTestSetup {
	client, emuClient := SetupGoogleTest(t)

	// Create unique test identifier for this test run
	testID := shortuuid.New()

	topics := make([]*emulator.TestTopic, 0, topicCount)
	subs := make(map[string]*emulator.TestSubscription)
	ctx := context.Background()

	// Create topics and subscriptions
	for i := 0; i < topicCount; i++ {
		topicName := fmt.Sprintf("test-topic-%s-%d", testID, i+1)
		topic := emuClient.CreateTestTopic(ctx, topicName)
		topics = append(topics, topic)

		// For each topic, create the specified number of subscriptions
		for j := 0; j < subsPerTopic; j++ {
			subName := fmt.Sprintf("test-topic-%s-%d-subscription-%d", testID, i+1, j+1)
			sub := topic.CreateTestSubscription(ctx, subName, true)
			subs[sub.Name] = sub
		}
	}

	return &LoadTestSetup{
		Client:    client,
		EmuClient: emuClient,
		Topics:    topics,
		Subs:      subs,
	}
}

// FindMissingMessages compares sent and received message lists and returns missing messages
func FindMissingMessages(sentMsgs []string, receivedMsgs []string) []string {
	missing := []string{}
	for _, sentMsg := range sentMsgs {
		found := false
		for _, receivedMsg := range receivedMsgs {
			if sentMsg == receivedMsg {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, sentMsg)
		}
	}
	return missing
}

// UniqueSubscriptionName generates a unique subscription name for tests
func UniqueSubscriptionName(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, shortuuid.New())
}
