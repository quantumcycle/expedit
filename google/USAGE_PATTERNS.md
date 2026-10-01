# Google Pub/Sub Usage Patterns

This document provides comprehensive examples of Google Pub/Sub-specific usage patterns for the Expedit library. These patterns leverage Google Pub/Sub's unique features like ordering keys, attributes, and the emulator.

> **📋 Note**: For general messaging patterns that work across all Expedit implementations (Redis, AMQP, Channels), see [MESSAGING_PATTERNS.md](../MESSAGING_PATTERNS.md) in the project root.

## Overview

This guide covers Google Pub/Sub-specific features and demonstrates best practices for implementing reliable, scalable message processing systems using Google Pub/Sub with the Expedit library. The examples are based on real working code from the test suite and production examples.

The implementation is built on the Pub/Sub Go SDK v2 (`cloud.google.com/go/pubsub/v2`), which requires Go 1.25. `NewGooglePublisher` and `NewGoogleSubscriber` take a v2 `*pubsub.Client`. Topics and subscriptions are managed with the admin clients of the v2 client (`client.TopicAdminClient`, `client.SubscriptionAdminClient`), see `google/emulator` for an example.

**Google Pub/Sub Specific Features Covered:**
- Ordering keys for message sequencing
- Attributes and metadata conversion
- GCP emulator setup and testing
- Processing timeouts and graceful shutdown
- Receive settings optimization
- Google-specific error handling

## Usage Patterns

### 1. Basic Publish-Subscribe Pattern

**Use Case**: Simple event notifications with minimal configuration  
**Test Reference**: See `publisher_test.go` and `subscriber_test.go`

**Key Features**:
- Simple one-to-one messaging
- Basic publisher configuration
- Acknowledgement driven by the handler result
- Ideal for getting started or simple notifications

**Example Scenario**: Basic system notifications, simple event logging

```go
// From publisher_test.go - Basic publisher setup
func setupBasicPublisher(client *pubsub.Client, topicName string) (*publisher.PublishingEngine, error) {
    pub, err := google.NewGooglePublisher(client, 
        publisher.ConstantDestination(topicName))
    if err != nil {
        return nil, err
    }
    return publisher.NewPublishingEngine(pub), nil
}

// From subscriber_test.go - Basic subscriber setup
func setupBasicSubscriber(client *pubsub.Client, subscriptionName string) (*google.Subscriber, error) {
    return google.NewGoogleSubscriber(client, subscriptionName)
}

// Basic message publishing
msg := message.NewMessage(context.Background(), []byte("Hello World"))
err = pubEngine.Publish(msg)

// Basic message consumption. Receive blocks until ctx is done or receiving fails.
// The message is acked when the handler returns nil, and nacked when it returns an error.
err = subscriber.Receive(ctx, func(msg *message.Message) error {
    log.Printf("Received: %s", string(msg.Payload.([]byte)))
    return nil
})
```

`Receive` returns nil once `ctx` is done and the in-flight handlers have returned. The context of the
messages is not cancelled by the shutdown, so the in-flight handlers can finish and be acknowledged. If
receiving fails, `Receive` waits for the in-flight handlers and returns the error. A subscription that does
not exist makes `Receive` return an error matching `google.ErrSubscriptionNotFound` (use `errors.Is`).

### 2. Ordered Message Processing Pattern

**Use Case**: User activity streams, financial transactions, state changes  
**Test Reference**: See `publisher_test.go` - "when using ordering keys" test

**Key Features**:
- Message ordering per entity (user, account, etc.)
- Ordering key configuration
- Guaranteed sequential processing within ordering groups
- Parallel processing across different ordering keys

**Example Scenario**: User activity tracking where events must be processed in order per user

```go
// From publisher_test.go - Publisher with ordering keys
pub, err := google.NewGooglePublisher(setup.Client,
    publisher.ConstantDestination(setup.Topic.Name),
    google.WithOrderingKeyProvider(func(msg *message.Message) string {
        return "test-key" // All messages use same ordering key for guaranteed order
    }))

pubEngine := publisher.NewPublishingEngine(pub)

// Publishing ordered messages
for i := 0; i < 100; i++ {
    msg := fmt.Sprintf("message %d", i+1)
    err = pubEngine.Publish(message.NewMessage(context.Background(), []byte(msg)))
    // Messages will be received in the exact order sent
}

// Advanced ordering by user ID
google.WithOrderingKeyProvider(func(msg *message.Message) string {
    if userID, ok := msg.Metadata["user_id"].(string); ok {
        return userID // Messages from same user will be ordered
    }
    if category, ok := msg.Metadata["category"].(string); ok {
        return "category:" + category
    }
    return "default"
})
```

### 3. Content-Based Routing Pattern

**Use Case**: Event distribution, microservice communication, data pipelines  
**Test Reference**: See `publisher_test.go` - "should use the routing function" test

**Key Features**:
- Smart routing based on message content
- Multiple destination topics
- Event type classification
- Attribute-based message filtering

**Example Scenario**: E-commerce system routing order events, user events, and product events to different processing pipelines

```go
// From publisher_test.go - Dynamic routing based on message metadata
routingFn := func(msg *message.Message) (publisher.Destination, error) {
    if msg.Metadata["destination"] == "topic2" {
        return topic2.Name, nil
    }
    return setup.Topic.Name, nil
}

pub, err := google.NewGooglePublisher(setup.Client, routingFn,
    google.WithOrderingKeyProvider(func(msg *message.Message) string {
        return "test-key"
    }))

// Publishing with routing metadata
pubEngine := publisher.NewPublishingEngine(pub)
err = pubEngine.Publish(
    message.NewMessage(context.Background(), []byte("msg1")).
        WithMetadata("destination", "topic2"))
err = pubEngine.Publish(
    message.NewMessage(context.Background(), []byte("msg2")))

// Advanced routing example
func smartRoutingFunction(msg *message.Message) (publisher.Destination, error) {
    eventType, ok := msg.Metadata["event_type"].(string)
    if !ok {
        return defaultTopic, nil
    }
    
    switch eventType {
    case "order_created", "order_updated", "order_cancelled":
        return orderTopic, nil
    case "user_registered", "user_updated", "user_deleted":
        return userTopic, nil
    case "product_created", "inventory_changed":
        return productTopic, nil
    default:
        return defaultTopic, nil
    }
}
```

### 4. Attributes and Metadata Pattern

**Use Case**: Message enrichment, tracing, filtering  
**Test Reference**: See `publisher_test.go` - "when using attributes providers" test

**Key Features**:
- Convert metadata to Google Pub/Sub attributes
- Custom attribute providers
- Message enrichment and filtering
- Integration with Google Pub/Sub features

**Example Scenario**: Adding tracing information and business context to messages

```go
// From publisher_test.go - Using metadata as attributes
pub, err := google.NewGooglePublisher(setup.Client,
    publisher.ConstantDestination(setup.Topic.Name),
    google.WithAttributesProvider(google.MetadataAsAttributes))

pubEngine := publisher.NewPublishingEngine(pub)
err = pubEngine.Publish(
    message.NewMessage(context.Background(), []byte("message1")).
        WithMetadata("key1", "value1"))

// The metadata will be converted to Pub/Sub attributes automatically

// From publisher_test.go - Handling complex metadata types
msg := message.NewMessage(context.Background(), []byte("test"))
msg.Metadata[""] = "empty-key"
msg.Metadata["unicode-key-🔑"] = "unicode-value-🎯"
msg.Metadata["null-value"] = ""
msg.Metadata["number"] = 42
msg.Metadata["bool"] = true

// MetadataAsAttributes converts all types to strings safely
err = pubEngine.Publish(msg)

// Custom attributes provider example
customAttributesProvider := func(msg *message.Message) map[string]string {
    attrs := make(map[string]string)
    
    // Add system attributes
    attrs["timestamp"] = time.Now().Format(time.RFC3339)
    attrs["service"] = "my-service"
    attrs["version"] = "1.0.0"
    
    // Add selective metadata
    if userID, ok := msg.Metadata["user_id"].(string); ok {
        attrs["user"] = userID
    }
    if priority, ok := msg.Metadata["priority"].(int); ok {
        attrs["prio"] = fmt.Sprintf("%d", priority)
    }
    
    return attrs
}
```

### 5. Error Handling and Timeout Pattern

**Use Case**: Resilient message processing, fault tolerance  
**Test Reference**: See `subscriber_test.go` - "when the processing timeout is reached" and "should nack when the handler returns an error and redeliver the message" tests

**Key Features**:
- The handler result decides the acknowledgement: nil acks, an error nacks
- A panicking handler nacks the message, then the panic continues (use `middleware.ConvertPanicToError` to nack without crashing)
- Configurable processing timeout, set as the deadline of `msg.Context()`
- Nacked messages are redelivered by Pub/Sub

**Example Scenario**: Payment processing system with timeout protection

```go
// From subscriber_test.go - Subscriber with processing timeout
subscriber, err := google.NewGoogleSubscriber(setup.Client,
    subscription.Name,
    google.WithProcessingTimeout(1*time.Second))

err = subscriber.Receive(ctx, func(msg *message.Message) error {
    // The processing timeout is the deadline of the message context. Pass the context to
    // everything the handler calls, and return its error: the message is nacked and redelivered.
    if err := chargeCustomer(msg.Context(), msg); err != nil {
        return err
    }
    return nil
})

// Error handling with a router
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("message_type"))
router.AddDefaultHandler(func(msg *message.Message) error {
    // Simulate processing that might fail
    if shouldFail(msg) {
        return fmt.Errorf("processing failed: %v", msg.ID) // nack, the message is redelivered
    }
    // Successful processing
    return nil
})
```

### 6. Complete Production Example

**Use Case**: Real-world application with middleware, metrics, and error handling  
**Example Reference**: See `examples/google/google.go`

**Key Features**:
- Complete publisher and subscriber setup
- Middleware chains for cross-cutting concerns
- Prometheus metrics integration
- Circuit breaker pattern
- JSON serialization
- Error handling and logging

**Example Scenario**: Production microservice with comprehensive monitoring and resilience

```go
// From examples/google/google.go - Complete publisher setup
func createPublisher(client *pubsub.Client, topic publisher.Destination) (*publisher.PublishingEngine, error) {
    routingFn := func(msg *message.Message) (publisher.Destination, error) {
        return topic, nil
    }
    
    pub, err := google.NewGooglePublisher(client, routingFn)
    if err != nil {
        return nil, err
    }
    
    publisherLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
        return prometheus.Labels{
            "publisher": "my_publisher", 
            "error": strconv.FormatBool(err != nil),
        }
    }
    
    pubEngine := publisher.NewPublishingEngine(pub)
    pubEngine.
        AddMiddleware(promware.PrometheusMetricsCountVec(
            examples.CreatePromOutgoingCount([]string{"publisher", "error"}), 
            publisherLabelsProducer)).
        AddMiddleware(promware.PrometheusMetricsDurationVec(
            examples.CreatePromOutgoingDuration([]string{"publisher", "error"}), 
            publisherLabelsProducer)).
        AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
            fmt.Printf("Error in publisher for message %s [%s]: %s\n", 
                msg.ID, msg.Metadata, err.Error())
        })).
        AddMiddleware(middleware.ConvertPanicToError()).
        AddMiddleware(AddStructNameToMetadata()).
        AddMiddleware(google.MarshallPayloadToJson())

    return pubEngine, nil
}

// Complete subscriber setup with middleware chain
func createSubscriber(client *pubsub.Client, subscription string, 
                     router *subscriber.SubscriptionRouter) (*subscriber.SubscriptionEngine, error) {
    googleSub, err := google.NewGoogleSubscriber(client, subscription)
    if err != nil {
        return nil, err
    }
    
    subEngine := subscriber.NewSubscriptionEngine(googleSub, *router)
    subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
        return prometheus.Labels{
            "subscriber": "my_subscriber", 
            "error": strconv.FormatBool(err != nil),
        }
    }
    
    testBreaker := gobreaker.NewCircuitBreaker[*message.Message](gobreaker.Settings{})
    subEngine.
        AddMiddleware(exmw.CircuitBreaker(testBreaker)).
        AddMiddleware(promware.PrometheusMetricsCountVec(
            examples.CreatePromIncomingCount([]string{"subscriber", "error"}), 
            subscriberLabelsProducer)).
        AddMiddleware(promware.PrometheusMetricsDurationVec(
            examples.CreatePromIncomingDuration([]string{"subscriber", "error"}), 
            subscriberLabelsProducer)).
        AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
            fmt.Printf("Error in handler for message %s [%s]: %s\n", 
                msg.ID, msg.Metadata, err.Error())
        })).
        AddMiddleware(middleware.ConvertPanicToError())
        
    return subEngine, nil
}

// Start blocks until ctx is done or receiving fails. When ctx is done, it stops receiving, waits for the
// in-flight handlers and returns nil.
runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
defer stop()
if err := subEngine.Start(runCtx); err != nil {
    panic(err)
}

// Message routing with typed handlers
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
router.
    AddHandler("DummyEvent1").
    AddMiddleware(google.UnmarshallPayloadFromJson(examples.DummyEvent1{})).
    Handle(message.HandleWithPayload(func(msg *message.Message, dummyEvent1 examples.DummyEvent1) error {
        fmt.Printf("Dummy event handler received message %s with prop1=[%s]\n",
            msg.ID, dummyEvent1.Prop1)
        return nil
    }))

router.AddDefaultHandler(func(msg *message.Message) error {
    fmt.Printf("Default handler received message %s\n", msg.ID)
    return nil
})
```

### 7. Attribute Parsing and Type Conversion

**Use Case**: Automatic type conversion of Google Pub/Sub attributes to Go types  
**Test Reference**: See `subscriber_test.go` - "when parse attributes is enabled" tests

**Key Features**:
- Automatic type conversion of string attributes to Go types
- Support for bool, int, float types
- Safe fallback to string for unknown types
- Seamless integration with message metadata

**Example Scenario**: Receiving structured data through Pub/Sub attributes

```go
// From subscriber_test.go - Enable attribute parsing
subscriber, err := google.NewGoogleSubscriber(setup.Client,
    subscription.Name, 
    google.WithParseAttributes(true)) // Enable automatic parsing

err = subscriber.Receive(ctx, func(msg *message.Message) error {
    // Attributes are automatically converted to appropriate Go types

    // Boolean conversion
    if boolVal, ok := msg.Metadata["is_active"].(bool); ok {
        fmt.Printf("Boolean value: %v\n", boolVal)
    }

    // Integer conversion (including "0" and "1")
    if intVal, ok := msg.Metadata["count"].(int64); ok {
        fmt.Printf("Integer value: %d\n", intVal)
    }

    // Float conversion
    if floatVal, ok := msg.Metadata["price"].(float64); ok {
        fmt.Printf("Float value: %f\n", floatVal)
    }

    // String values remain as strings
    if strVal, ok := msg.Metadata["name"].(string); ok {
        fmt.Printf("String value: %s\n", strVal)
    }

    return nil
})

// Publishing with attributes that will be parsed
attrs := map[string]string{
    "is_active": "true",     // Will become bool
    "count": "42",           // Will become int64  
    "price": "19.99",        // Will become float64
    "name": "product",       // Remains string
}
publishMessageWithAttributes(topic, payload, attrs)
```

### 8. Subscription Configuration and Optimization

**Use Case**: Fine-tuning subscriber performance and behavior  
**Test Reference**: See `subscriber_test.go` - configuration tests

**Key Features**:
- Custom receive settings for performance tuning
- Processing timeout configuration
- Concurrency control: `Receive` maps to `pubsub.Subscriber.Receive` (Pub/Sub Go SDK v2), so the number of handlers running at the same time is bounded by `MaxOutstandingMessages` and `MaxOutstandingBytes`. `NumGoroutines` is the number of StreamingPull streams and does not limit concurrency
- Leave `ShutdownOptions` nil, so `Receive` keeps waiting for the in-flight handlers on shutdown
- Memory management

**Example Scenario**: High-throughput system requiring optimized message processing

```go
// From subscriber_test.go - Advanced subscriber configuration
receiveSettings := pubsub.ReceiveSettings{
    MaxOutstandingMessages: 1000,            // Max messages handled at the same time
    MaxOutstandingBytes:    1024 * 1024,     // Memory limit
}

subscriber, err := google.NewGoogleSubscriber(setup.Client,
    subscription.Name,
    google.WithReceiveSettings(receiveSettings),
    google.WithProcessingTimeout(30*time.Second),
    google.WithParseAttributes(true))

// Negative values disable the flow control limits: the number of messages handled at the same time is unbounded
edgeCaseSettings := pubsub.ReceiveSettings{
    MaxOutstandingMessages: -1,    // No limit on the number of messages
    MaxOutstandingBytes:    -1,    // No limit on the size of the messages
}

subscriber, err := google.NewGoogleSubscriber(setup.Client,
    subscription.Name,
    google.WithReceiveSettings(edgeCaseSettings),
    google.WithProcessingTimeout(-1*time.Second)) // Disables timeout (0 does too)
```

## Best Practices

### 1. Error Handling
- Always implement proper error handling in message handlers
- Use middleware for consistent error logging and monitoring
- Pass `msg.Context()` to long-running operations, so they stop at the processing timeout
- Test error scenarios with message nacking (return an error from the handler)

```go
// From examples/google/google.go - Error handling middleware
pubEngine.AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
    fmt.Printf("Error in publisher for message %s [%s]: %s\n", 
        msg.ID, msg.Metadata, err.Error())
}))

subEngine.AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
    fmt.Printf("Error in handler for message %s [%s]: %s\n", 
        msg.ID, msg.Metadata, err.Error())
}))
```

### 2. Ordering and Concurrency  
- Use ordering keys when message order is important
- Design ordering keys to balance ordering guarantees with throughput
- Test ordering behavior under load
- Consider the impact of ordering on processing parallelism

```go
// From publisher_test.go - Multiple ordering key strategies
google.WithOrderingKeyProvider(func(msg *message.Message) string {
    if userID, ok := msg.Metadata["user_id"].(string); ok {
        return userID // Per-user ordering
    }
    if category, ok := msg.Metadata["category"].(string); ok {
        return "category:" + category // Per-category ordering
    }
    return "default" // Default ordering group
})
```

### 3. Configuration and Performance
- Configure receive settings based on your throughput requirements
- Use appropriate timeouts for your processing needs
- Monitor memory usage with MaxOutstandingBytes
- Test with realistic workloads

```go
// Optimized settings for high-throughput scenarios
receiveSettings := pubsub.ReceiveSettings{
    NumGoroutines:          2,                // More StreamingPull streams, for very high throughput
    MaxOutstandingMessages: 2000,             // More messages handled at the same time
    MaxOutstandingBytes:    10 * 1024 * 1024, // 10MB memory limit
}
```

### 4. Testing with Emulator
- Always use the GCP Pub/Sub emulator for tests
- Use the provided test utilities for consistent setup
- Test with unique topic/subscription names to enable parallel execution
- Validate message content and metadata

```go
// From test_utils_test.go - Proper test setup
func NewGoogleTestSetup(t *testing.T, topicPrefix string) *GoogleTestSetup {
    client, emuClient := SetupGoogleTest(t)
    topic := CreateTestTopic(t, emuClient, topicPrefix)
    return &GoogleTestSetup{
        Client:    client,
        EmuClient: emuClient,
        Topic:     topic,
    }
}
```

### 5. Resource Management
- Use context cancellation for graceful shutdowns: cancelling the context passed to `Receive` stops receiving, waits for the in-flight handlers and returns nil
- Wait for `Receive` to return before the process exits
- Clean up test resources in tests

```go
// Context-based cancellation
ctx, cancel := context.WithCancel(context.Background())

errCh := make(chan error, 1)
go func() { errCh <- subscriber.Receive(ctx, handler) }()

// ... later, on shutdown
cancel()
err := <-errCh // nil once the in-flight handlers have returned
```

### 6. Production Considerations
- Use middleware for metrics and monitoring
- Implement circuit breakers for resilience
- Add structured logging with correlation IDs
- Monitor message processing latencies

```go
// From examples/google/google.go - Production middleware stack
subEngine.
    AddMiddleware(exmw.CircuitBreaker(testBreaker)).
    AddMiddleware(promware.PrometheusMetricsCountVec(...)).
    AddMiddleware(promware.PrometheusMetricsDurationVec(...)).
    AddMiddleware(middleware.OnError(...)).
    AddMiddleware(middleware.ConvertPanicToError())
```

## Running the Tests and Examples

### Running Tests
The Google Pub/Sub tests demonstrate these usage patterns in action:

```bash
# Run all Google tests (requires Docker)
task google:test

# Run specific test files
go test -v ./google -run TestGooglePublisher
go test -v ./google -run TestGoogleSubscriber

# Run individual test cases
go test -v ./google -run "TestGooglePublisher/should_use_the_routing_function"
go test -v ./google -run "TestGoogleSubscriber/when_parse_attributes_is_enabled"
```

### Running Examples
The production example demonstrates real-world usage:

```bash
# Start dependencies
task google:du

# Run the complete example
cd examples/google && go run google.go

# Stop dependencies when done
task google:dd
```

### Setting Up Development Environment

```bash
# Install Task runner (if not already installed)
go install github.com/go-task/task/v3/cmd/task@latest

# Start all dependencies for development
task du

# Run tests across all modules
task test

# Stop all dependencies
task dd
```

## Key Files Reference

- **Core Implementation**: `google/publisher.go`, `google/subscriber.go`
- **Test Examples**: `google/publisher_test.go`, `google/subscriber_test.go`
- **Production Example**: `examples/google/google.go`
- **Test Utilities**: `google/test_utils_test.go`
- **Documentation**: `google/USAGE_PATTERNS.md` (this file)

## Dependencies

- **Docker**: Required for running the GCP Pub/Sub emulator
- **Go 1.19+**: For generics support in circuit breakers and other features
- **Task**: For running development commands (`go install github.com/go-task/task/v3/cmd/task@latest`)
- **Testing**: Gomega for BDD-style testing

## Contributing

When working with Google Pub/Sub patterns:

1. **Follow the Test-Driven Development approach**: Write tests first using the existing patterns
2. **Use the test utilities**: Leverage `NewGoogleTestSetup()` and other utilities for consistent testing
3. **Test with the emulator**: Always use the Docker-based GCP emulator for integration tests
4. **Document patterns**: Update this guide when adding new usage patterns
5. **Consider performance**: Test with realistic message volumes and concurrency

### Adding New Patterns
1. Study existing test cases to understand the testing patterns
2. Add new test cases following the BDD style (Given/When/Then)
3. Use unique topic/subscription names to enable parallel test execution
4. Document the new pattern in this guide with real code examples

## Additional Resources

- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [Expedit Core Documentation](../core/)
- [Examples](../examples/google/)
- [Task Configuration](../Taskfile.yml)
- [Docker Compose](docker-compose.yml)