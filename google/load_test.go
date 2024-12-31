package google_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/publisher"
	"github.com/quantumcycle/expedit/google"
	"github.com/quantumcycle/expedit/google/emulator"
	"golang.org/x/exp/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Load test scenario

var _ = Describe("Google Pubsub load test", func() {
	var client *pubsub.Client
	var emuClient *emulator.PubsubTestClient
	var topics []*emulator.TestTopic
	var subs map[string]*emulator.TestSubscription

	BeforeEach(func() {
		topics = []*emulator.TestTopic{}
		subs = make(map[string]*emulator.TestSubscription)
		ctx := context.Background()
		emuClient = emulator.NewTestClient(ctx, "test-project")

		// Create topics and subscriptions
		for i := 0; i < 3; i++ {
			topic := emuClient.CreateTestTopic(ctx, fmt.Sprintf("test-topic-%d", i+1))
			topics = append(topics, topic)

			//for each topic, create 3 subscriptions
			for j := 0; j < 3; j++ {
				sub := topic.CreateTestSubscription(ctx, fmt.Sprintf("test-topic-%d-subscription-%d", i+1, j+1), true)
				subs[sub.Name] = sub
			}
		}

		var err error
		client, err = pubsub.NewClient(context.Background(), "test-project")
		Expect(err).NotTo(HaveOccurred())
	})

	It("should process all the messages at scale", func() {
		ctx := context.Background()
		var totalSentCount int64
		var totalProcessedCount int64
		var nackCount int64
		var publisherMu sync.Mutex
		var consumerMu sync.Mutex
		var publisherCounts map[string][]string
		var consumerCounts map[string][]string

		publisherCounts = make(map[string][]string)
		consumerCounts = make(map[string][]string)

		nbMessagesToSendPerTopic := 1000

		//Create subscribers
		for subName, sub := range subs {
			//Create 2 consumer for each subscription, to emulate multiple processes on the same GCP subscription
			//We expect about half of the messages to be processed by each subscriber
			for j := 0; j < 2; j++ {
				subscriber, err := google.NewGoogleSubscriber(client, sub.Name)
				Expect(err).NotTo(HaveOccurred())

				msgCh, err := subscriber.Subscribe(ctx)
				Expect(err).NotTo(HaveOccurred())
				go func(consumerIndex int, subscriptionName string) {
					consumerID := fmt.Sprintf("%s-consumer-%d", subscriptionName, consumerIndex)
					for msg := range msgCh {
						//simulate some random processing error and make sure we still process all messages
						//1% of messages will fail
						if randomInt(1, 100) == 1 {
							atomic.AddInt64(&nackCount, 1)
							msg.Nack()
							continue
						}

						atomic.AddInt64(&totalProcessedCount, 1)
						consumerMu.Lock()
						if _, exists := consumerCounts[consumerID]; !exists {
							consumerCounts[consumerID] = []string{}
						}
						consumerCounts[consumerID] = append(consumerCounts[consumerID], msg.ID)
						consumerMu.Unlock()
						msg.Ack()
					}
				}(j+1, subName)
			}
		}

		// Create publishers
		for _, topic := range topics {
			pub, err := google.NewGooglePublisher(client, publisher.ConstantDestination(topic.Name))
			Expect(err).NotTo(HaveOccurred())

			pubEngine := publisher.NewPublishingEngine(pub)
			go func(publisherName string) {
				for j := 0; j < nbMessagesToSendPerTopic; j++ {
					payload := fmt.Sprintf("message %d", j+1)
					msg := message.NewMessage(context.Background(), []byte(payload))
					err = pubEngine.Publish(msg)
					Expect(err).NotTo(HaveOccurred())
					atomic.AddInt64(&totalSentCount, 1)
					publisherMu.Lock()
					if _, exists := publisherCounts[publisherName]; !exists {
						publisherCounts[publisherName] = []string{}
					}
					publisherCounts[publisherName] = append(publisherCounts[publisherName], msg.ID)
					publisherMu.Unlock()
				}
			}(string(topic.Name))
		}

		// We have 3 topics, each with 3 subscriptions, so the total is the number of messages times 9
		Eventually(func() int64 {
			return atomic.LoadInt64(&totalProcessedCount)
		}, "30s").Should(BeNumerically("==", nbMessagesToSendPerTopic*9))

		//Should be at least one random nack
		Expect(nackCount).To(BeNumerically(">", 0))

		//each subscription has 2 consumers, so total for both should be nbMessagesToSendPerTopic
		for subName, _ := range subs {
			consumer1ID := fmt.Sprintf("%s-consumer-%d", subName, 1)
			consumer1Msgs := consumerCounts[consumer1ID]

			consumer2ID := fmt.Sprintf("%s-consumer-%d", subName, 2)
			consumer2Msgs := consumerCounts[consumer2ID]

			receivedMsgs := append(consumer1Msgs, consumer2Msgs...)
			Expect(len(receivedMsgs)).To(Equal(nbMessagesToSendPerTopic))

			var sentMsgs []string
			if strings.Index(subName, "test-topic-1") > 0 {
				sentMsgs = publisherCounts[string(topics[0].Name)]
			} else if strings.Index(subName, "test-topic-2") > 0 {
				sentMsgs = publisherCounts[string(topics[1].Name)]
			} else {
				sentMsgs = publisherCounts[string(topics[2].Name)]
			}

			delta := findMissingMessages(sentMsgs, receivedMsgs)
			Expect(delta).To(BeEmpty(), "Missing messages never received: %v", delta)
		}
	})
})

func findMissingMessages(sentMsgs []string, receivedMsgs []string) interface{} {
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

func randomInt(lower int, higher int) int {
	rand.Seed(uint64(time.Now().UnixNano()))
	return rand.Intn(higher-lower+1) + lower
}
