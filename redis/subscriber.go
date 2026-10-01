package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lithammer/shortuuid/v3"
	"github.com/quantumcycle/expedit/core/message"
	"github.com/quantumcycle/expedit/core/subscriber"
	"github.com/redis/go-redis/v9"
)

var StreamDoesntExistErr = errors.New("stream does not exist")

type MessageUnmarshaller func(ctx context.Context, msg *redis.XMessage) (*message.Message, error)

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	consumerGroup                      string
	consumerGroupCreateStreamIfMissing bool
	consumerGroupStartID               StartPosition
	startID                            StartPosition
	processingTimeout                  *time.Duration
	metadataExtractor                  func(wrapper MessageWrapper) map[string]interface{}
	payloadExtractor                   func(wrapper MessageWrapper) map[string]interface{}
	pendingMessageIdleTimeout          time.Duration
	pendingMessageBatchSize            int
	maxInFlight                        int
	blockTimeout                       time.Duration
	onAckError                         func(msg *message.Message, err error)
}

// WithConsumerGroup identifies the consumer group to which the subscriber belongs.
func WithConsumerGroup(group string) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroup = group
	}
}

// WithConsumerGroupCreateStreamIfMissing will create the stream if it does not exist in consumer group mode.
func WithConsumerGroupCreateStreamIfMissing(create bool) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroupCreateStreamIfMissing = create
	}
}

// WithConsumerGroupStartID is the position the consumer of a consumer group should start from
// Using ConsumerGroupStartFromBeginning "0" means the consumer group will consume from the very first message.
// Using ConsumerGroupStartFromLatest "$" means the consumer group will consume from the latest message.
func WithConsumerGroupStartID(startID StartPosition) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.consumerGroupStartID = startID
	}
}

// WithStartID is the ID of the last message that was processed by the subscriber. This is used only when not using
// consumer groups. The default is "$" which means the subscriber will start after the latest message at the time
// Receive is called.
func WithStartID(startID StartPosition) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.startID = startID
	}
}

// WithProcessingTimeout is the deadline of the context of each message, see subscriber.DispatchOptions.
// 0 means no deadline, which is only allowed without a consumer group. In consumer group mode it must be below the
// pending message idle timeout, otherwise another consumer could claim a message that is still being processed.
// Default is 80% of the pending message idle timeout, so 4 minutes with the default idle timeout.
func WithProcessingTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.processingTimeout = &timeout
	}
}

// WithMetadataExtractor is a function that extracts metadata from a redis message.
// If not provided, no metadata will be extracted. The extractor usually needs to be aligned with the
// marshaller used by the publisher.
func WithMetadataExtractor(extractor func(wrapper MessageWrapper) map[string]interface{}) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.metadataExtractor = extractor
	}
}

// WithPayloadExtractor is a function that extracts payload from a redis message.
// If not provided, all the values in the redis message are converted into a map as payload.
func WithPayloadExtractor(extractor func(wrapper MessageWrapper) map[string]interface{}) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.payloadExtractor = extractor
	}
}

// WithPendingMessageIdleTimeout sets how long a message must be idle before it can be claimed by another consumer.
// It must be above the processing timeout. Default is 5 minutes. Only applies to consumer group mode.
func WithPendingMessageIdleTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.pendingMessageIdleTimeout = timeout
	}
}

// WithPendingMessageBatchSize sets the maximum number of pending messages to check and claim per cycle.
// Default is 10. Only applies to consumer group mode.
func WithPendingMessageBatchSize(batchSize int) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.pendingMessageBatchSize = batchSize
	}
}

// WithMaxInFlight is the maximum number of messages handled at the same time, see subscriber.DispatchOptions.
// Default is subscriber.DefaultMaxInFlight. It is also the maximum number of messages read at once.
func WithMaxInFlight(maxInFlight int) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.maxInFlight = maxInFlight
	}
}

// WithBlockTimeout is how long a read waits for new messages. Receive notices that its ctx is done between reads,
// so it bounds how long a shutdown waits. Default is 2 seconds.
func WithBlockTimeout(timeout time.Duration) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.blockTimeout = timeout
	}
}

// WithAckErrorHandler is called when acking a message fails. The message stays pending and is claimed again after
// the pending message idle timeout. If not provided, the error is ignored.
func WithAckErrorHandler(handler func(msg *message.Message, err error)) SubscriberOption {
	return func(opts *SubscriberOptions) {
		opts.onAckError = handler
	}
}

type Subscriber struct {
	client            *redis.Client
	stream            string
	options           SubscriberOptions
	processingTimeout time.Duration
}

type MessageWrapper struct {
	stream        string
	consumerGroup string
	msg           *redis.XMessage
}

type StartPosition string

const (
	StartFromBeginning StartPosition = "0"
	StartFromLatest    StartPosition = "$"
)

func NewRedisSubscriber(
	c *redis.Client,
	stream string,
	opts ...SubscriberOption) (*Subscriber, error) {

	if c == nil {
		return nil, errors.New("client is required")
	}

	options := SubscriberOptions{
		consumerGroupStartID:      StartFromLatest,
		startID:                   StartFromLatest,
		pendingMessageIdleTimeout: 5 * time.Minute,
		pendingMessageBatchSize:   10,
		maxInFlight:               subscriber.DefaultMaxInFlight,
		blockTimeout:              2 * time.Second,
	}

	for _, opt := range opts {
		opt(&options)
	}
	processingTimeout := options.pendingMessageIdleTimeout * 4 / 5
	if options.processingTimeout != nil {
		processingTimeout = *options.processingTimeout
	}
	if options.consumerGroup != "" && (processingTimeout <= 0 || processingTimeout >= options.pendingMessageIdleTimeout) {
		return nil, fmt.Errorf("processing timeout (%s) must be positive and below the pending message idle timeout (%s) "+
			"in consumer group mode, otherwise another consumer could claim a message that is still being processed",
			processingTimeout, options.pendingMessageIdleTimeout)
	}

	return &Subscriber{
		client:            c,
		stream:            stream,
		options:           options,
		processingTimeout: processingTimeout,
	}, nil
}

// Receive implements subscriber.Subscriber.
//
// In consumer group mode, a message is acked with XACK. A nacked message stays pending, and any consumer of the
// group claims it once it is idle for the pending message idle timeout. Without a consumer group, acking and nacking
// do nothing.
func (s *Subscriber) Receive(ctx context.Context, handler message.HandlerFunc) error {
	if err := s.createConsumerGroup(ctx); err != nil {
		return err
	}
	d := subscriber.NewDispatcher(handler, subscriber.DispatchOptions{
		MaxInFlight:       s.options.maxInFlight,
		ProcessingTimeout: s.processingTimeout,
		OnAckError:        s.options.onAckError,
	})
	err := s.receive(ctx, d)
	d.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (s *Subscriber) createConsumerGroup(ctx context.Context) error {
	group := s.options.consumerGroup
	if group == "" {
		return nil
	}
	var err error
	if s.options.consumerGroupCreateStreamIfMissing {
		err = s.client.XGroupCreateMkStream(ctx, s.stream, group, string(s.options.consumerGroupStartID)).Err()
	} else {
		err = s.client.XGroupCreate(ctx, s.stream, group, string(s.options.consumerGroupStartID)).Err()
	}
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "The XGROUP subcommand requires the key to exist") {
		return StreamDoesntExistErr
	}
	//Getting this error when the consumer group already exists is fine
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

// receive reads and dispatches messages until ctx is done or a read fails.
func (s *Subscriber) receive(ctx context.Context, d *subscriber.Dispatcher) error {
	group := s.options.consumerGroup
	consumer := shortuuid.New()
	startID, err := s.resolveStartID(ctx)
	if err != nil {
		return err
	}
	claimStart := "0-0"

	for ctx.Err() == nil {
		var streams []redis.XStream
		if group != "" {
			// Claim the messages left pending by other consumers, for example because they crashed or nacked them
			var claimed []redis.XMessage
			claimed, claimStart, err = s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
				Stream:   s.stream,
				Group:    group,
				Consumer: consumer,
				MinIdle:  s.options.pendingMessageIdleTimeout,
				Start:    claimStart,
				Count:    int64(s.options.pendingMessageBatchSize),
			}).Result()
			if err != nil {
				return err
			}
			for _, msg := range claimed {
				d.Dispatch(ctx, s.delivery(ctx, msg))
			}

			streams, err = s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: consumer,
				Streams:  []string{s.stream, ">"},
				Count:    int64(s.options.maxInFlight),
				Block:    s.options.blockTimeout,
			}).Result()
		} else {
			streams, err = s.client.XRead(ctx, &redis.XReadArgs{
				Streams: []string{s.stream, startID},
				Count:   int64(s.options.maxInFlight),
				Block:   s.options.blockTimeout,
			}).Result()
		}
		if errors.Is(err, redis.Nil) {
			// No new message before the block timeout
			continue
		}
		if err != nil {
			return err
		}
		for _, xs := range streams {
			for _, msg := range xs.Messages {
				d.Dispatch(ctx, s.delivery(ctx, msg))
				startID = msg.ID
			}
		}
	}
	return nil
}

// resolveStartID replaces "$" by the ID of the latest message when not using a consumer group, so the messages
// added between two reads are not skipped.
func (s *Subscriber) resolveStartID(ctx context.Context) (string, error) {
	startID := string(s.options.startID)
	if s.options.consumerGroup != "" || s.options.startID != StartFromLatest {
		return startID, nil
	}
	info, err := s.client.XInfoStream(ctx, s.stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			// The stream does not exist yet, every message added to it is new
			return "0-0", nil
		}
		return "", err
	}
	return info.LastGeneratedID, nil
}

func (s *Subscriber) delivery(ctx context.Context, msg redis.XMessage) subscriber.Delivery {
	wrapper := MessageWrapper{
		stream:        s.stream,
		consumerGroup: s.options.consumerGroup,
		msg:           &msg,
	}
	return subscriber.Delivery{
		Message: s.toMessage(ctx, wrapper),
		Ack: func(ctx context.Context) error {
			if wrapper.consumerGroup == "" {
				return nil
			}
			return s.client.XAck(ctx, wrapper.stream, wrapper.consumerGroup, msg.ID).Err()
		},
		Nack: func(ctx context.Context) error {
			// The message stays pending until it is claimed, see Receive
			return nil
		},
	}
}

func (s *Subscriber) toMessage(ctx context.Context, wrapper MessageWrapper) *message.Message {
	var metadata map[string]interface{}
	if s.options.metadataExtractor != nil {
		metadata = s.options.metadataExtractor(wrapper)
	}
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	payload := wrapper.msg.Values
	if s.options.payloadExtractor != nil {
		payload = s.options.payloadExtractor(wrapper)
	}
	msg := message.NewMessage(ctx, payload)
	msg.ID = wrapper.msg.ID
	msg.Metadata = metadata
	return msg
}
