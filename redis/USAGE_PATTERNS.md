# Redis Streams Usage Patterns

This document provides comprehensive examples of Redis Streams-specific usage patterns for the Expedit library. These patterns leverage Redis Streams' unique features like consumer groups, pending message recovery, metadata/payload separation, and stream size management.

> **📋 Note**: For general messaging patterns that work across all Expedit implementations (Google Pub/Sub, AMQP, Channels), see [MESSAGING_PATTERNS.md](../MESSAGING_PATTERNS.md) in the project root.

## Overview

This guide covers Redis Streams-specific features and demonstrates best practices for implementing reliable, scalable message processing systems using Redis Streams with the Expedit library. The examples are based on real working code from the test suite and production examples.

**Redis Streams Specific Features Covered:**
- Consumer groups for load balancing and fault tolerance  
- Pending message recovery with XCLAIM
- Metadata and payload separation with prefixes
- Stream size management with MAXLEN and APPROX
- Custom ID generation for message ordering
- JSON serialization utilities
- Processing timeouts and error handling

## Usage Patterns

### 1. Basic Publish-Subscribe Pattern

**Use Case**: Simple event notifications with minimal configuration  
**Test Reference**: See `publisher_test.go` and `subscriber_test.go`

**Key Features**:
- Simple one-to-one messaging
- Basic publisher configuration  
- Straightforward message acknowledgment
- Ideal for getting started or simple notifications

**Example Scenario**: Basic system notifications, simple event logging

```go
// From publisher_test.go - Basic publisher setup
func setupBasicPublisher(client *redis.Client, stream string) (*publisher.PublishingEngine, error) {
    pub, err := redis.NewRedisPublisher(client,
        publisher.ConstantDestination(stream),
        simpleMarshaller) // Simple marshaller that flattens metadata and payload
    if err != nil {
        return nil, err
    }
    return publisher.NewPublishingEngine(pub), nil
}

// From subscriber_test.go - Basic subscriber setup (non-consumer group)
func setupBasicSubscriber(client *redis.Client, stream string) (*redis.Subscriber, error) {
    return redis.NewRedisSubscriber(client, stream,
        redis.WithStartID(redis.StartFromBeginning)) // Start from beginning of stream
}

// Basic message publishing
msg := message.NewMessage(context.Background(), map[string]interface{}{
    "action": "user_login",
    "details": "User john.doe logged in",
})
err = pubEngine.Publish(msg)

// Basic message consumption  
msgCh, err := subscriber.Subscribe(ctx)
defer subscriber.Close()

go func() {
    for msg := range msgCh {
        // Process the message
        log.Printf("Received: %v", msg.Payload)
        msg.Ack() // Acknowledge successful processing
    }
}()
```

### 2. Consumer Groups for Load Balancing

**Use Case**: High-throughput systems requiring horizontal scaling  
**Test Reference**: See `subscriber_test.go` - "when using consumer groups" tests

**Key Features**:
- Load balancing across multiple consumers
- Message distribution within consumer groups
- Fault tolerance with pending message recovery
- Automatic consumer identification

**Example Scenario**: Order processing system with multiple worker instances

```go
// From subscriber_test.go - Consumer group setup
func setupConsumerGroup(client *redis.Client, stream string, groupName string) (*redis.Subscriber, error) {
    return redis.NewRedisSubscriber(client, stream,
        redis.WithConsumerGroup(groupName),
        redis.WithConsumerGroupCreateStreamIfMissing(true),
        redis.WithConsumerGroupStartID(redis.StartFromBeginning))
}

// Multiple consumers in the same group
consumer1, err := setupConsumerGroup(client, "orders-stream", "order-processors")
consumer2, err := setupConsumerGroup(client, "orders-stream", "order-processors")

// Start both consumers - messages will be distributed between them
msgCh1, err := consumer1.Subscribe(ctx)
msgCh2, err := consumer2.Subscribe(ctx)

// Consumer 1 processing
go func() {
    for msg := range msgCh1 {
        fmt.Printf("Consumer 1 processing: %v\n", msg.Payload)
        // Process order...
        msg.Ack()
    }
}()

// Consumer 2 processing  
go func() {
    for msg := range msgCh2 {
        fmt.Printf("Consumer 2 processing: %v\n", msg.Payload)
        // Process order...
        msg.Ack()
    }
}()

// Publishing messages - they'll be distributed across consumers
for i := 0; i < 100; i++ {
    order := map[string]interface{}{
        "order_id": fmt.Sprintf("order-%d", i),
        "amount":   rand.Float64() * 100,
    }
    pubEngine.Publish(message.NewMessage(ctx, order))
}
```

### 3. Fault Tolerance with Pending Message Recovery

**Use Case**: Critical systems requiring guaranteed message processing  
**Test Reference**: See `subscriber_test.go` - XCLAIM and pending message tests

**Key Features**:
- Automatic pending message detection
- XCLAIM for message recovery
- Configurable idle timeouts
- Consumer failure handling

**Example Scenario**: Payment processing system with failure recovery

```go
// From subscriber_test.go - Fault-tolerant consumer setup
func setupFaultTolerantConsumer(client *redis.Client, stream string) (*redis.Subscriber, error) {
    return redis.NewRedisSubscriber(client, stream,
        redis.WithConsumerGroup("payment-processors"),
        redis.WithConsumerGroupCreateStreamIfMissing(true),
        redis.WithPendingMessageIdleTimeout(30*time.Second), // Claim messages idle for 30s
        redis.WithPendingMessageBatchSize(10))               // Process up to 10 pending messages per cycle
}

// Consumer that might fail
consumer, err := setupFaultTolerantConsumer(client, "payments-stream")
msgCh, err := consumer.Subscribe(ctx)

go func() {
    for msg := range msgCh {
        // Simulate processing that might fail
        if shouldFail(msg) {
            fmt.Printf("Processing failed for message %s, will be nacked\n", msg.ID)
            msg.Nack() // Message becomes pending for another consumer to claim
            continue
        }
        
        // Successful processing
        processPayment(msg.Payload)
        msg.Ack()
    }
}()

// Recovery consumer that claims abandoned messages
recoveryConsumer, err := setupFaultTolerantConsumer(client, "payments-stream")
recoveryMsgCh, err := recoveryConsumer.Subscribe(ctx)

go func() {
    for msg := range recoveryMsgCh {
        fmt.Printf("Recovery consumer claimed message %s\n", msg.ID)
        // More robust processing for recovered messages
        processPaymentRobustly(msg.Payload)
        msg.Ack()
    }
}()
```

### 4. Metadata and Payload Separation Pattern

**Use Case**: Structured data with routing information and business logic separation  
**Test Reference**: See `publisher_test.go` - metadata marshaller tests and `subscriber_test.go` - extractor tests

**Key Features**:
- Prefix-based metadata separation
- Custom metadata marshalling
- Selective payload extraction
- Clean data organization in Redis

**Example Scenario**: Event-driven architecture with routing metadata and business data

```go
// From examples/redis/redis.go - Publisher with metadata prefixes
func createPublisher(client *redis.Client, stream string) (*publisher.PublishingEngine, error) {
    pub, err := redis.NewRedisPublisher(client,
        publisher.ConstantDestination(stream),
        redis.MarshallPayloadToJsonMap("json"), // Payload as JSON in "json" field
        redis.WithMetadataMarshaller(redis.MetadataWithPrefix("metadata_"))) // Metadata with prefix
    
    pubEngine := publisher.NewPublishingEngine(pub)
    return pubEngine, nil
}

// From examples/redis/redis.go - Subscriber with matching extractors
func createSubscriber(client *redis.Client, stream string) (*subscriber.SubscriptionEngine, error) {
    subscriber, err := redis.NewRedisSubscriber(client, stream,
        redis.WithMetadataExtractor(redis.PrefixMetadataExtractor("metadata_")), // Extract metadata_* fields
        redis.WithPayloadExtractor(redis.NonPrefixPayloadExtractor("metadata_"))) // Extract non-metadata_* fields
    
    return subscriber, nil
}

// Publishing with metadata and payload separation
msg := message.NewMessage(ctx, OrderEvent{
    OrderID: "order-123",
    Amount:  99.99,
    Items:   []string{"item1", "item2"},
})
msg = msg.WithMetadata("event_type", "order_created")
msg = msg.WithMetadata("user_id", "user-456")
msg = msg.WithMetadata("priority", "high")

err = pubEngine.Publish(msg)

// Redis will store:
// metadata_event_type: "order_created"
// metadata_user_id: "user-456"  
// metadata_priority: "high"
// json: "{\"OrderID\":\"order-123\",\"Amount\":99.99,\"Items\":[\"item1\",\"item2\"]}"

// On consumption, metadata and payload are cleanly separated:
// msg.Metadata["event_type"] = "order_created"
// msg.Metadata["user_id"] = "user-456"
// msg.Payload contains the JSON data
```

### 5. Content-Based Routing Pattern

**Use Case**: Event distribution, microservice communication, data pipelines  
**Test Reference**: See `publisher_test.go` - routing function tests

**Key Features**:
- Smart routing based on message content
- Multiple destination streams
- Event type classification
- Dynamic stream selection

**Example Scenario**: E-commerce system routing different event types to specialized streams

```go
// From publisher_test.go - Dynamic routing based on message metadata
func createSmartRouter() publisher.RoutingFunc {
    return func(msg *message.Message) (publisher.Destination, error) {
        eventType, ok := msg.Metadata["event_type"].(string)
        if !ok {
            return "default-stream", nil
        }
        
        switch eventType {
        case "order_created", "order_updated", "order_cancelled":
            return "orders-stream", nil
        case "user_registered", "user_updated", "user_deleted":
            return "users-stream", nil
        case "inventory_updated", "product_created":
            return "inventory-stream", nil
        case "payment_processed", "payment_failed":
            return "payments-stream", nil
        default:
            return "default-stream", nil
        }
    }
}

pub, err := redis.NewRedisPublisher(client,
    createSmartRouter(), // Use smart routing
    redis.MarshallPayloadToJsonMap("data"))

pubEngine := publisher.NewPublishingEngine(pub)

// Messages automatically routed to appropriate streams
orderMsg := message.NewMessage(ctx, map[string]interface{}{
    "order_id": "12345",
    "total": 99.99,
}).WithMetadata("event_type", "order_created")

userMsg := message.NewMessage(ctx, map[string]interface{}{
    "user_id": "user-789",  
    "email": "user@example.com",
}).WithMetadata("event_type", "user_registered")

pubEngine.Publish(orderMsg)  // -> orders-stream
pubEngine.Publish(userMsg)   // -> users-stream
```

### 6. Stream Size Management Pattern

**Use Case**: Long-running systems requiring memory management  
**Test Reference**: See `publisher_test.go` - WithMaxlen and WithApprox tests

**Key Features**:
- Stream size limiting with MAXLEN
- Performance optimization with APPROX
- Memory usage control
- Automatic old message eviction

**Example Scenario**: Real-time analytics system with sliding window data

```go
// From publisher_test.go - Stream with size management
func createManagedStreamPublisher(client *redis.Client, stream string) (*publisher.PublishingEngine, error) {
    pub, err := redis.NewRedisPublisher(client,
        publisher.ConstantDestination(stream),
        simpleMarshaller,
        redis.WithMaxlen(1000),    // Keep only last 1000 messages
        redis.WithApprox(true))    // Use approximate trimming for performance
        
    return publisher.NewPublishingEngine(pub), nil
}

// Real-time metrics publishing
pubEngine, err := createManagedStreamPublisher(client, "metrics-stream")

// Continuously publish metrics - old ones are automatically evicted
ticker := time.NewTicker(1 * time.Second)
go func() {
    for range ticker.C {
        metrics := map[string]interface{}{
            "timestamp": time.Now().Unix(),
            "cpu_usage": getCurrentCPUUsage(),
            "memory_usage": getCurrentMemoryUsage(),
            "active_connections": getActiveConnections(),
        }
        
        msg := message.NewMessage(ctx, metrics)
        pubEngine.Publish(msg)
        // Stream automatically maintains size ~1000 messages
    }
}()
```

### 7. Custom ID Generation and Ordering

**Use Case**: Message ordering, deduplication, custom sequencing  
**Test Reference**: See `publisher_test.go` - WithIDGenerator tests

**Key Features**:
- Custom message ID generation
- Deterministic ordering
- Deduplication support
- Timestamp-based sequencing

**Example Scenario**: Financial transaction system requiring strict ordering

```go
// From publisher_test.go - Custom ID generation for ordering
func createOrderedPublisher(client *redis.Client, stream string) (*publisher.PublishingEngine, error) {
    // Custom ID generator for deterministic ordering
    idGenerator := func(msg *message.Message) (string, error) {
        timestamp := time.Now().UnixMilli()
        if customTime, ok := msg.Metadata["timestamp"].(int64); ok {
            timestamp = customTime
        }
        
        // Redis stream ID format: timestamp-sequence
        return fmt.Sprintf("%d-0", timestamp), nil
    }
    
    pub, err := redis.NewRedisPublisher(client,
        publisher.ConstantDestination(stream),
        simpleMarshaller,
        redis.WithIDGenerator(idGenerator))
        
    return publisher.NewPublishingEngine(pub), nil
}

// Publishing transactions with custom ordering
pubEngine, err := createOrderedPublisher(client, "transactions-stream")

// Messages will be stored with deterministic IDs based on timestamp
transactions := []Transaction{
    {ID: "tx-1", Amount: 100.00, Timestamp: 1640995200000},
    {ID: "tx-2", Amount: 200.00, Timestamp: 1640995201000},
    {ID: "tx-3", Amount: 150.00, Timestamp: 1640995202000},
}

for _, tx := range transactions {
    msg := message.NewMessage(ctx, tx)
    msg = msg.WithMetadata("timestamp", tx.Timestamp)
    pubEngine.Publish(msg)
    // Messages stored with IDs: 1640995200000-0, 1640995201000-0, 1640995202000-0
}
```

### 8. JSON Serialization Pattern

**Use Case**: Structured data serialization and type safety  
**Test Reference**: See `json_test.go` - JSON utilities tests

**Key Features**:
- Automatic JSON marshalling/unmarshalling
- Type-safe message processing
- Middleware integration
- Error handling for malformed data

**Example Scenario**: API event processing with structured payloads

```go
// From examples/redis/redis.go - JSON-based message processing
type OrderEvent struct {
    OrderID   string    `json:"order_id"`
    UserID    string    `json:"user_id"`
    Amount    float64   `json:"amount"`
    Items     []string  `json:"items"`
    CreatedAt time.Time `json:"created_at"`
}

// Publisher with JSON marshalling
pub, err := redis.NewRedisPublisher(client,
    publisher.ConstantDestination("orders-stream"),
    redis.MarshallPayloadToJsonMap("payload")) // Serialize payload as JSON

pubEngine := publisher.NewPublishingEngine(pub)

// Publishing structured events
orderEvent := OrderEvent{
    OrderID:   "order-12345",
    UserID:    "user-789",
    Amount:    99.99,
    Items:     []string{"laptop", "mouse"},
    CreatedAt: time.Now(),
}

msg := message.NewMessage(ctx, orderEvent)
pubEngine.Publish(msg)

// Subscriber with JSON unmarshalling
subscriber, err := redis.NewRedisSubscriber(client, "orders-stream")

router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
router.
    AddHandler("OrderEvent").
    AddMiddleware(redis.UnmarshallMapPayloadFromJson("payload", OrderEvent{})).
    Handle(message.HandleWithPayload(func(msg *message.Message, order OrderEvent) error {
        fmt.Printf("Processing order %s for user %s, amount: $%.2f\n",
            order.OrderID, order.UserID, order.Amount)
        
        // Type-safe processing
        return processOrder(order)
    }))

router.AddDefaultHandler(func(msg *message.Message) error {
    fmt.Printf("Unhandled message: %v\n", msg.Payload)
    return nil
})
```

### 9. Error Handling and Timeout Pattern

**Use Case**: Resilient message processing, fault tolerance  
**Test Reference**: See `subscriber_test.go` - processing timeout tests

**Key Features**:
- Configurable processing timeouts
- Custom timeout handlers  
- Automatic message nacking
- Graceful failure handling

**Example Scenario**: Image processing service with timeout protection

```go
// From subscriber_test.go - Subscriber with processing timeout
func createResilientSubscriber(client *redis.Client, stream string) (*redis.Subscriber, error) {
    return redis.NewRedisSubscriber(client, stream,
        redis.WithConsumerGroup("image-processors"),
        redis.WithProcessingTimeout(30*time.Second), // 30-second timeout
        redis.WithProcessingTimeoutHandler(func(ctx context.Context, wrapper redis.MessageWrapper) {
            log.Printf("Image processing timed out for message %s", wrapper.msg.ID)
            // Could move to dead letter queue, send alert, etc.
        }))
}

subscriber, err := createResilientSubscriber(client, "images-stream")
msgCh, err := subscriber.Subscribe(ctx)

go func() {
    for msg := range msgCh {
        // Processing with timeout protection
        err := processImage(msg.Payload)
        if err != nil {
            log.Printf("Failed to process image %s: %v", msg.ID, err)
            msg.Nack() // Will be retried or claimed by another consumer
            continue
        }
        
        msg.Ack() // Successful processing
    }
}()

func processImage(payload interface{}) error {
    // Simulate image processing that might take too long
    imageData, ok := payload.(map[string]interface{})
    if !ok {
        return fmt.Errorf("invalid image data format")
    }
    
    // Heavy processing...
    time.Sleep(45 * time.Second) // This will trigger timeout handler
    
    return nil
}
```

### 10. Complete Production Example

**Use Case**: Real-world application with middleware, metrics, and error handling  
**Example Reference**: See `examples/redis/redis.go`

**Key Features**:
- Complete publisher and subscriber setup
- Middleware chains for cross-cutting concerns
- Prometheus metrics integration
- Circuit breaker pattern
- JSON serialization
- Error handling and logging

**Example Scenario**: Production microservice with comprehensive monitoring and resilience

```go
// From examples/redis/redis.go - Complete publisher setup
func createPublisher(client *redis.Client, stream publisher.Destination) (*publisher.PublishingEngine, error) {
    pub, err := redis.NewRedisPublisher(client,
        publisher.ConstantDestination(stream),
        redis.MarshallPayloadToJsonMap("json"),
        redis.WithMetadataMarshaller(redis.MetadataWithPrefix("metadata_")))
    if err != nil {
        return nil, err
    }

    publisherLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
        return prometheus.Labels{
            "publisher": "my_publisher",
            "error":     strconv.FormatBool(err != nil),
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
        AddMiddleware(AddStructNameToMetadata())

    return pubEngine, nil
}

// Complete subscriber setup with middleware chain
func createSubscriber(client *redis.Client, stream string, router *subscriber.SubscriptionRouter) (*subscriber.SubscriptionEngine, error) {
    redisSub, err := redis.NewRedisSubscriber(client, stream,
        redis.WithStartID(redis.StartFromBeginning),
        redis.WithMetadataExtractor(redis.PrefixMetadataExtractor("metadata_")),
        redis.WithPayloadExtractor(redis.NonPrefixPayloadExtractor("metadata_")))
    if err != nil {
        return nil, err
    }

    subEngine := subscriber.NewSubscriptionEngine(redisSub, *router)
    subscriberLabelsProducer := func(msg *message.Message, err error) prometheus.Labels {
        return prometheus.Labels{
            "subscriber": "my_subscriber",
            "error":      strconv.FormatBool(err != nil),
        }
    }

    subEngine.
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

// Message routing with typed handlers
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
router.
    AddHandler("DummyEvent1").
    AddMiddleware(redis.UnmarshallMapPayloadFromJson("json", examples.DummyEvent1{})).
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

## Best Practices

### 1. Error Handling and Recovery
- Always implement proper error handling in message handlers
- Use consumer groups for automatic message recovery
- Configure appropriate pending message timeouts
- Implement timeout handlers for long-running operations

```go
// Robust error handling setup
subscriber, err := redis.NewRedisSubscriber(client, stream,
    redis.WithConsumerGroup("processors"),
    redis.WithPendingMessageIdleTimeout(60*time.Second),
    redis.WithProcessingTimeout(30*time.Second),
    redis.WithProcessingTimeoutHandler(func(ctx context.Context, wrapper redis.MessageWrapper) {
        // Log timeout, send to dead letter queue, etc.
        log.Printf("Message %s timed out", wrapper.msg.ID)
    }))
```

### 2. Consumer Groups and Scaling
- Use consumer groups for horizontal scaling
- Configure appropriate batch sizes for pending message recovery
- Design consumer IDs to be meaningful for debugging
- Test consumer failure scenarios

```go
// Scalable consumer group configuration
redis.WithConsumerGroup("order-processors"),
redis.WithPendingMessageIdleTimeout(30*time.Second),
redis.WithPendingMessageBatchSize(20), // Process more pending messages per cycle
```

### 3. Metadata and Payload Organization
- Use consistent prefix schemes for metadata
- Align publisher marshallers with subscriber extractors
- Keep metadata small and payload-focused
- Document your prefix conventions

```go
// Consistent prefix usage
publisher: redis.WithMetadataMarshaller(redis.MetadataWithPrefix("meta_"))
subscriber: redis.WithMetadataExtractor(redis.PrefixMetadataExtractor("meta_"))
```

### 4. Stream Management
- Use MAXLEN for long-running streams to prevent unbounded growth
- Enable APPROX for better performance in high-throughput scenarios
- Monitor stream sizes in production
- Consider stream partitioning for very high volumes

```go
// Production stream management
redis.WithMaxlen(10000),    // Keep last 10k messages
redis.WithApprox(true),     // Use approximate trimming for performance
```

### 5. Testing and Development
- Use Docker for local Redis development and testing
- Create unique stream names for parallel test execution
- Test consumer group scenarios and failure recovery
- Validate message content and metadata extraction

```go
// Test-friendly stream naming
streamName := fmt.Sprintf("test-stream-%d", time.Now().UnixNano())
```

### 6. Production Considerations
- Use middleware for metrics and monitoring
- Implement circuit breakers for resilience
- Add structured logging with correlation IDs
- Monitor consumer lag and pending message counts

```go
// Production middleware stack
subEngine.
    AddMiddleware(promware.PrometheusMetricsCountVec(...)).
    AddMiddleware(promware.PrometheusMetricsDurationVec(...)).
    AddMiddleware(middleware.OnError(...)).
    AddMiddleware(middleware.ConvertPanicToError())
```

## Running the Tests and Examples

### Running Tests
The Redis Streams tests demonstrate these usage patterns in action:

```bash
# Run all Redis tests (requires Docker)
task redis:test

# Run specific test files
go test -v ./redis -run TestRedisPublisher
go test -v ./redis -run TestRedisSubscriber

# Run individual test cases
go test -v ./redis -run "TestRedisPublisher/should_use_the_routing_function"
go test -v ./redis -run "TestRedisSubscriber/when_using_consumer_groups"
```

### Running Examples
The production example demonstrates real-world usage:

```bash
# Start dependencies
task redis:du

# Run the complete example
cd examples/redis && go run redis.go

# Stop dependencies when done
task redis:dd
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

- **Core Implementation**: `redis/publisher.go`, `redis/subscriber.go`
- **Utilities**: `redis/values.go`, `redis/json.go`
- **Test Examples**: `redis/publisher_test.go`, `redis/subscriber_test.go`
- **Load Tests**: `redis/load_test.go`
- **Production Example**: `examples/redis/redis.go`
- **Documentation**: `redis/USAGE_PATTERNS.md` (this file)

## Dependencies

- **Docker**: Required for running Redis in development and testing
- **Go 1.19+**: For generics support and modern Go features
- **Task**: For running development commands (`go install github.com/go-task/task/v3/cmd/task@latest`)
- **Testing**: Ginkgo/Gomega for BDD-style testing
- **Redis**: Version 6.2+ for full Redis Streams support

## Contributing

When working with Redis Streams patterns:

1. **Follow the Test-Driven Development approach**: Write tests first using the existing patterns
2. **Use unique stream names**: Enable parallel test execution with timestamped stream names
3. **Test with Docker**: Always use Docker-based Redis for integration tests
4. **Document patterns**: Update this guide when adding new usage patterns
5. **Consider performance**: Test with realistic message volumes and consumer groups

### Adding New Patterns
1. Study existing test cases to understand the testing patterns
2. Add new test cases following the BDD style (Given/When/Then)
3. Use unique stream names to enable parallel test execution
4. Document the new pattern in this guide with real code examples
5. Test both consumer group and non-consumer group scenarios

## Redis-Specific Considerations

### Stream IDs and Ordering
- Redis stream IDs are automatically ordered by timestamp
- Custom ID generation must follow Redis format: `timestamp-sequence`
- Use consistent timestamp sources for ordering across services

### Consumer Groups vs Individual Consumers
- **Consumer Groups**: Use for load balancing and fault tolerance
- **Individual Consumers**: Use for simple scenarios or when you need all messages

### Memory Management
- Redis Streams can grow indefinitely without MAXLEN
- Use MAXLEN with APPROX for production workloads
- Monitor Redis memory usage and stream sizes

### Pending Messages
- Pending messages are automatically tracked in consumer groups
- Use XCLAIM to recover messages from failed consumers
- Configure appropriate idle timeouts based on your processing time

## Additional Resources

- [Redis Streams Documentation](https://redis.io/docs/data-types/streams/)
- [Expedit Core Documentation](../core/)
- [Examples](../examples/redis/)
- [Task Configuration](../Taskfile.yml)
- [Docker Compose](docker-compose.yml)