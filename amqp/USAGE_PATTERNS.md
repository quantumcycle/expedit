# AMQP Usage Patterns

This document provides comprehensive examples of AMQP-specific usage patterns for the Expedit library. These patterns leverage AMQP/RabbitMQ's unique features like exchanges, routing keys, queues, and reliable message delivery.

> **📋 Note**: For general messaging patterns that work across all Expedit implementations (Google PubSub, Redis, Channels), see [MESSAGING_PATTERNS.md](../MESSAGING_PATTERNS.md) in the project root.

## Overview

This guide covers AMQP-specific features and demonstrates best practices for implementing reliable, scalable message processing systems using AMQP/RabbitMQ with the Expedit library. The examples are based on real working code from the test suite and production patterns.

**AMQP Specific Features Covered:**
- Exchange types (Direct, Fanout, Topic, Headers)
- Routing keys and dynamic routing
- Headers and metadata handling
- Connection reliability and auto-reconnection
- Message acknowledgment and delivery guarantees
- Queue management and bindings
- JSON marshalling/unmarshalling middleware

## Exchange Types and Queue Binding Fundamentals

Before diving into usage patterns, it's crucial to understand how AMQP exchanges route messages to queues:

### Exchange Types and Routing Logic

1. **Direct Exchange**: Routes messages to queues based on exact routing key match
   - Publisher sets routing key → Exchange routes to queues bound with that exact key
   - One-to-one or one-to-many routing based on routing key

2. **Fanout Exchange**: Broadcasts messages to ALL bound queues
   - Routing key is ignored → All bound queues receive every message
   - One-to-many broadcasting

3. **Topic Exchange**: Routes messages based on routing key patterns
   - Uses wildcards: `*` (exactly one word), `#` (zero or more words)
   - Publisher sets routing key → Exchange matches against queue binding patterns

4. **Headers Exchange**: Routes messages based on header key-value pairs
   - Routing key is ignored → Routing based on message headers
   - Supports "all" (match all headers) or "any" (match any header) logic

### Queue Binding Requirements

For messages to reach subscribers, queues must be properly bound to exchanges:

```go
// Direct: Queue bound with specific routing key
channel.QueueBind(queueName, "email_sending", exchangeName, false, nil)

// Fanout: Queue bound with empty routing key (key ignored)
channel.QueueBind(queueName, "", exchangeName, false, nil)  

// Topic: Queue bound with pattern
channel.QueueBind(queueName, "logs.*.error", exchangeName, false, nil)

// Headers: Queue bound with header matching arguments
args := amqp.Table{"x-match": "all", "priority": "urgent"}
channel.QueueBind(queueName, "", exchangeName, false, args)
```

**Important**: The subscriber must consume from a queue that is bound to the exchange with the appropriate routing key/pattern/headers that match what the publisher sends.

## Usage Patterns

### 1. Basic Direct Exchange Pattern

**Use Case**: Point-to-point messaging with exact routing  
**Test Reference**: See `publisher_test.go` and `subscriber_test.go`

**Key Features**:
- Direct message routing with exact routing key matching
- Simple one-to-one or one-to-many messaging
- Reliable message delivery with acknowledgments
- Ideal for task queues and command processing

**Example Scenario**: Task distribution system where specific workers handle specific task types

```go
// From publisher_test.go - Basic direct exchange publisher
routingKeyFn := func(msg *message.Message) (string, error) {
    return fmt.Sprintf("%v", msg.Metadata["task_type"]), nil
}

pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("test-direct-exchange"),
    routingKeyFn,
    amqp.DefaultMessageOptions{
        ContentType:  "text/plain",
        Priority:     0,
        DeliveryMode: amqpgo.Persistent,
    })

pubEngine := publisher.NewPublishingEngine(pub)

// Publishing with routing key
msg := message.NewMessage(context.Background(), []byte("Process this task")).
    WithMetadata("task_type", "email_sending")
err = pubEngine.Publish(msg)

// Queue setup for direct exchange - routing key must match
// For direct exchange, create a queue bound to the exchange with specific routing key
// Channel setup (this would typically be done during infrastructure setup)
channel.QueueBind(queueName, "email_sending", exchangeName, false, nil)

// Subscribe to the queue
subscriber, err := amqp.NewAMQPSubscriber(channel, queueName)
msgCh, err := subscriber.Subscribe(ctx)

go func() {
    for msg := range msgCh {
        // Process the task - this queue only receives "email_sending" tasks
        log.Printf("Processing email task: %s", string(msg.Payload.([]byte)))
        msg.Ack() // Acknowledge successful processing
    }
}()
```

### 2. Fanout Broadcasting Pattern

**Use Case**: Event broadcasting, notifications, cache invalidation  
**Test Reference**: See `publisher_test.go` - fanout exchange tests

**Key Features**:
- Broadcast messages to all bound queues
- Routing key is ignored (can be empty)
- Multiple consumers receive the same message
- Ideal for event notifications and system-wide updates

**Example Scenario**: User registration event broadcasted to multiple services (email, analytics, audit)

```go
// From publisher_test.go - Fanout exchange setup
routingKeyFn := amqp.ConstantRoutingKey("") // Empty routing key for fanout

pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("user-events-fanout"),
    routingKeyFn,
    amqp.DefaultMessageOptions{
        ContentType:  "application/json",
        DeliveryMode: amqpgo.Persistent,
    })

pubEngine := publisher.NewPublishingEngine(pub).
    AddMiddleware(amqp.MarshallPayloadToJson())

// Publishing user event - all services will receive this
userEvent := UserRegisteredEvent{
    UserID:    "user-123",
    Email:     "user@example.com",
    Timestamp: time.Now(),
}

msg := message.NewMessage(context.Background(), userEvent).
    WithMetadata("event_type", "user_registered")
err = pubEngine.Publish(msg)

// Fanout exchange setup - routing key is ignored
// For fanout exchange, create queues bound to the exchange (no routing key needed)
channel.QueueBind("email-service", "", exchangeName, false, nil)
channel.QueueBind("analytics-service", "", exchangeName, false, nil)

// Email service subscriber
emailSubscriber, err := amqp.NewAMQPSubscriber(channel, "email-service")
emailMsgCh, err := emailSubscriber.Subscribe(ctx)

// Analytics service subscriber  
analyticsSubscriber, err := amqp.NewAMQPSubscriber(channel, "analytics-service")
analyticsMsgCh, err := analyticsSubscriber.Subscribe(ctx)

// All subscribers receive the same messages
go func() {
    for msg := range emailMsgCh {
        log.Printf("Email service processing: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()

go func() {
    for msg := range analyticsMsgCh {
        log.Printf("Analytics service processing: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()
```

### 3. Topic Pattern Matching Pattern

**Use Case**: Hierarchical routing, log aggregation, event filtering  
**Test Reference**: See `publisher_test.go` - topic exchange tests

**Key Features**:
- Pattern-based routing with wildcards (`*` and `#`)
- `*` matches exactly one word
- `#` matches zero or more words
- Flexible message filtering and routing

**Example Scenario**: Log aggregation system with different log levels and components

```go
// From publisher_test.go - Topic exchange with pattern routing
routingKeyFn := func(msg *message.Message) (string, error) {
    component := msg.Metadata["component"].(string)
    level := msg.Metadata["level"].(string)
    return fmt.Sprintf("logs.%s.%s", component, level), nil
}

pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("logs-topic-exchange"),
    routingKeyFn,
    defaultOptions)

pubEngine := publisher.NewPublishingEngine(pub)

// Publishing with hierarchical routing keys
msg1 := message.NewMessage(context.Background(), []byte("Database connection error")).
    WithMetadata("component", "database").
    WithMetadata("level", "error")

msg2 := message.NewMessage(context.Background(), []byte("User login")).
    WithMetadata("component", "auth").
    WithMetadata("level", "info")

msg3 := message.NewMessage(context.Background(), []byte("Payment processed")).
    WithMetadata("component", "payment").
    WithMetadata("level", "info")

// Topic exchange setup - pattern-based routing
// Create queues bound with different patterns
// queue binding required here

// Subscriber for error logs only
errorSubscriber, err := amqp.NewAMQPSubscriber(channel, errorLogsQueue)
errorMsgCh, err := errorSubscriber.Subscribe(ctx)

// Subscriber for all database logs
databaseSubscriber, err := amqp.NewAMQPSubscriber(channel, databaseLogsQueue)
databaseMsgCh, err := databaseSubscriber.Subscribe(ctx)

go func() {
    for msg := range errorMsgCh {
        // Only receives error logs: "logs.auth.error", "logs.payment.error", etc.
        log.Printf("Error log: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()

go func() {
    for msg := range databaseMsgCh {
        // Receives all database logs: "logs.database.info", "logs.database.error.connection", etc.
        log.Printf("Database log: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()
```

### 4. Headers Exchange Pattern

**Use Case**: Complex message routing based on multiple criteria  
**Test Reference**: See `publisher_test.go` - headers exchange tests

**Key Features**:
- Route messages based on header key-value pairs
- Support for "all" (match all headers) or "any" (match any header) logic
- More flexible than routing key-based systems
- Ideal for complex business rule routing

**Example Scenario**: Order processing system routing based on multiple order attributes

```go
// From publisher_test.go - Headers exchange with metadata routing
pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("order-headers-exchange"),
    amqp.ConstantRoutingKey(""), // Routing key ignored for headers exchange
    defaultOptions,
    amqp.WithHeadersProvider(amqp.MetadataAsHeaders),
)

pubEngine := publisher.NewPublishingEngine(pub)

// Publishing with multiple header criteria
urgentOrder := message.NewMessage(context.Background(), []byte("Order data")).
    WithMetadata("priority", "urgent").
    WithMetadata("amount", "1000").
    WithMetadata("region", "US")

regularOrder := message.NewMessage(context.Background(), []byte("Order data")).
    WithMetadata("priority", "normal").
    WithMetadata("amount", "100").
    WithMetadata("region", "EU")

//queue binding required here

// Subscriber for urgent orders
urgentSubscriber, err := amqp.NewAMQPSubscriber(channel, urgentOrdersQueue)
urgentMsgCh, err := urgentSubscriber.Subscribe(ctx)

// Subscriber for high-value orders
highValueSubscriber, err := amqp.NewAMQPSubscriber(channel, highValueQueue)
highValueMsgCh, err := highValueSubscriber.Subscribe(ctx)

go func() {
    for msg := range urgentMsgCh {
        // Only receives messages with priority=urgent header
        log.Printf("Urgent order: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()

go func() {
    for msg := range highValueMsgCh {
        // Receives messages with amount=1000 header (or any matching header if "any" match)
        log.Printf("High-value order: %s", string(msg.Payload.([]byte)))
        msg.Ack()
    }
}()
```

### 5. Reliable Message Delivery Pattern

**Use Case**: Critical message processing requiring delivery guarantees  
**Test Reference**: See `publisher_test.go` - mandatory flag tests

**Key Features**:
- Mandatory flag ensures messages are routable
- Persistent delivery mode for durability
- Publisher confirms for delivery acknowledgment
- Message rejection handling

**Example Scenario**: Financial transaction processing requiring guaranteed delivery

```go
// From publisher_test.go - Reliable delivery configuration  
routingKeyFn := func(msg *message.Message) (string, error) {
    if txnType, ok := msg.Metadata["transaction_type"].(string); ok {
        return txnType, nil
    }
    return "default", nil
}

pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("financial-exchange"),
    routingKeyFn,
    amqp.DefaultMessageOptions{
        ContentType:  "application/json",
        Priority:     9, // Highest priority
        DeliveryMode: amqpgo.Persistent, // Survive broker restarts
    },
    amqp.WithMandatoryMsgFn(func(msg *message.Message) (bool, error) {
        // Mark critical messages as mandatory
        if critical, ok := msg.Metadata["critical"].(bool); ok && critical {
            return true, nil
        }
        return false, nil
    }),
    amqp.WithHeadersProvider(amqp.MetadataAsHeaders),
)

pubEngine := publisher.NewPublishingEngine(pub).
    AddMiddleware(amqp.MarshallPayloadToJson())

// Publishing critical transaction
transaction := PaymentTransaction{
    ID:     "txn-123",
    Amount: 1000.00,
    Status: "pending",
}

msg := message.NewMessage(context.Background(), transaction).
    WithMetadata("critical", true).
    WithMetadata("transaction_type", "payment")

err = pubEngine.Publish(msg) // Will fail if not routable due to mandatory flag
```

### 6. Advanced Subscriber Configuration Pattern

**Use Case**: High-performance message consumption with custom behavior  
**Test Reference**: See `subscriber_test.go` - configuration tests

**Key Features**:
- Processing timeouts with custom handlers
- Exclusive consumer access
- Manual acknowledgment control
- No requeue on failure options

**Example Scenario**: High-throughput order processing with timeout protection

```go
// From subscriber_test.go - Advanced subscriber configuration
subscriber, err := amqp.NewAMQPSubscriber(
    channel,
    "order-processing-queue",
    amqp.WithProcessingTimeout(30*time.Second),
    amqp.WithProcessingTimeoutHandler(func(ctx context.Context, msg *amqpgo.Delivery) {
        log.Printf("Order processing timed out: %s", msg.MessageId)
        // Could send to dead letter queue, alert monitoring, etc.
    }),
    amqp.WithExclusive(), // Only this consumer can access the queue
    amqp.WithNoRequeueOnNack(), // Failed messages go to DLQ instead of requeue
)

msgCh, err := subscriber.Subscribe(ctx)

//queue binding required here

msgCh, err := subscriber.Subscribe(ctx)

go func() {
    for msg := range msgCh {
        // Simulate complex order processing
        if err := processOrder(msg); err != nil {
            log.Printf("Order processing failed: %v", err)
            msg.Nack() // Will not requeue due to WithNoRequeueOnNack
        } else {
            msg.Ack()
        }
    }
}()

func processOrder(msg *message.Message) error {
    // Complex processing that might timeout
    time.Sleep(25 * time.Second) // Simulated processing
    return nil
}
```

### 7. JSON Middleware Pattern

**Use Case**: Structured data serialization and type-safe deserialization  
**Test Reference**: See `json_test.go`

**Key Features**:
- Automatic JSON marshalling/unmarshalling
- Type-safe message handling
- Support for complex nested structures
- Round-trip data integrity

**Example Scenario**: Microservice communication with structured events

```go
// From json_test.go - JSON marshalling and unmarshalling
type OrderEvent struct {
    OrderID   string    `json:"order_id"`
    UserID    string    `json:"user_id"`
    Amount    float64   `json:"amount"`
    Items     []Item    `json:"items"`
    Timestamp time.Time `json:"timestamp"`
}

type Item struct {
    SKU      string  `json:"sku"`
    Quantity int     `json:"quantity"`
    Price    float64 `json:"price"`
}

// Publisher with JSON marshalling
pubEngine := publisher.NewPublishingEngine(pub).
    AddMiddleware(amqp.MarshallPayloadToJson())

// Publishing structured data
order := OrderEvent{
    OrderID:   "order-456",
    UserID:    "user-789",
    Amount:    299.99,
    Items:     []Item{{SKU: "WIDGET-001", Quantity: 2, Price: 149.99}},
    Timestamp: time.Now(),
}

msg := message.NewMessage(context.Background(), order).
    WithMetadata("event_type", "order_created")
err = pubEngine.Publish(msg)

// Subscriber with type-safe unmarshalling
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
router.
    AddHandler("order_created").
    AddMiddleware(amqp.UnmarshallPayloadFromJson(OrderEvent{})).
    Handle(message.HandleWithPayload(func(msg *message.Message, order OrderEvent) error {
        // Type-safe access to order data
        log.Printf("Processing order %s for user %s, amount: $%.2f", 
            order.OrderID, order.UserID, order.Amount)
        
        for _, item := range order.Items {
            log.Printf("  Item: %s, Qty: %d, Price: $%.2f", 
                item.SKU, item.Quantity, item.Price)
        }
        
        return nil
    }))

subEngine := subscriber.NewSubscriptionEngine(amqpSubscriber, *router)
```

### 8. Connection Resilience Pattern

**Use Case**: Robust production deployment with network fault tolerance  
**Test Reference**: See connection and channel auto-reconnection features

**Key Features**:
- Automatic connection and channel reconnection
- Configurable retry strategies
- Graceful handling of network interruptions
- Connection health monitoring

**Example Scenario**: Production service that must handle network disruptions gracefully

```go
// Connection with auto-reconnection
conn, err := amqp.NewConnection("amqp://localhost:5672",
    amqp.WithReconnectionRetry(func(attempt int) time.Duration {
        // Exponential backoff with jitter
        base := time.Duration(attempt) * time.Second
        jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
        return base + jitter
    }))

if err != nil {
    return fmt.Errorf("failed to create connection: %w", err)
}
defer conn.Close()

// Channel with auto-reconnection
channel, err := amqp.NewChannel(conn)
if err != nil {
    return fmt.Errorf("failed to create channel: %w", err)
}
defer channel.Close()

// The publisher and subscriber will automatically handle
// connection/channel reconnections transparently
subscriber, err := amqp.NewAMQPSubscriber(channel, queueName)
msgCh, err := subscriber.Subscribe(ctx)

// Message processing continues even through network disruptions
go func() {
    for msg := range msgCh {
        // Processing continues after reconnection
        if err := processMessage(msg); err != nil {
            log.Printf("Processing error: %v", err)
            msg.Nack()
        } else {
            msg.Ack()
        }
    }
}()
```

### 9. Header Filtering and Management Pattern

**Use Case**: Selective metadata propagation and header transformation  
**Test Reference**: See `headers_test.go`

**Key Features**:
- Multiple header provider strategies
- Prefix-based filtering
- Custom header transformation
- Metadata key exclusion/inclusion

**Example Scenario**: Microservice communication with controlled metadata propagation

```go
// From headers_test.go - Advanced header management
routingKeyFn := amqp.ConstantRoutingKey("service-data")
defaultOptions := amqp.DefaultMessageOptions{
    ContentType:  "application/json",
    Priority:     0,
    DeliveryMode: amqpgo.Persistent,
}

pub, err := amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("service-exchange"),
    routingKeyFn,
    defaultOptions,
    // Exclude internal metadata from headers
    amqp.WithHeadersProvider(amqp.ExcludePrefixHeadersProvider("internal_")),
)

// Alternative header strategies:

// 1. Only include specific prefixed metadata
pub, err = amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("service-exchange"),
    routingKeyFn,
    defaultOptions,
    amqp.WithHeadersProvider(amqp.OnlyPrefixHeadersProvider("public_")),
)

// 2. Add prefix to all metadata keys
pub, err = amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("service-exchange"),
    routingKeyFn,
    defaultOptions,
    amqp.WithHeadersProvider(amqp.PrefixedHeadersProvider("app_")),
)

// 3. Custom header filtering
customFilter := func(key string, value interface{}) bool {
    // Only include specific keys
    allowedKeys := []string{"user_id", "request_id", "trace_id"}
    for _, allowed := range allowedKeys {
        if key == allowed {
            return true
        }
    }
    return false
}

pub, err = amqp.NewAMQPPublisher(
    channel,
    publisher.ConstantDestination("service-exchange"),
    routingKeyFn,
    defaultOptions,
    amqp.WithHeadersProvider(amqp.FilteredHeadersProvider(customFilter)),
)

// Publishing with controlled header propagation
msg := message.NewMessage(context.Background(), []byte("data")).
    WithMetadata("user_id", "user-123").        // Will be included
    WithMetadata("trace_id", "trace-456").       // Will be included  
    WithMetadata("internal_secret", "hidden").   // Will be excluded
    WithMetadata("debug_info", "verbose")        // Will be excluded

err = pubEngine.Publish(msg)
```

### 10. Complete Production Example

**Use Case**: Real-world application with comprehensive middleware and monitoring  
**Example Reference**: Production-ready setup with all features

**Key Features**:
- Complete publisher and subscriber setup
- JSON serialization middleware
- Error handling and logging
- Connection resilience
- Message routing and type safety

**Example Scenario**: E-commerce order processing service

```go
// Complete production setup
func createOrderProcessingService() error {
    // Resilient connection
    conn, err := amqp.NewConnection("amqp://rabbitmq:5672",
        amqp.WithReconnectionRetry(func(attempt int) time.Duration {
            return time.Duration(attempt) * time.Second
        }))
    if err != nil {
        return err
    }
    defer conn.Close()

    channel, err := amqp.NewChannel(conn)
    if err != nil {
        return err
    }
    defer channel.Close()

    // Publisher for order events with dynamic exchange routing
    exchangeRoutingFn := func(msg *message.Message) (publisher.Destination, error) {
        eventType := msg.Metadata["event_type"].(string)
        switch eventType {
        case "order_created", "order_updated", "order_cancelled":
            return "orders-exchange", nil
        case "payment_processed", "payment_failed":
            return "payments-exchange", nil
        default:
            return "events-exchange", nil
        }
    }

    routingKeyFn := func(msg *message.Message) (string, error) {
        eventType := msg.Metadata["event_type"].(string)
        return eventType, nil
    }

    pub, err := amqp.NewAMQPPublisher(
        channel,
        exchangeRoutingFn,
        routingKeyFn,
        amqp.DefaultMessageOptions{
            ContentType:  "application/json",
            DeliveryMode: amqpgo.Persistent,
        },
        amqp.WithHeadersProvider(amqp.ExcludePrefixHeadersProvider("internal_")),
        amqp.WithMandatoryMsgFn(func(msg *message.Message) (bool, error) {
            // Critical events must be routable
            return msg.Metadata["critical"] == true, nil
        }),
    )
    if err != nil {
        return err
    }

    pubEngine := publisher.NewPublishingEngine(pub).
        AddMiddleware(amqp.MarshallPayloadToJson()).
        AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
            log.Printf("Publishing error for message %s: %v", msg.ID, err)
        })).
        AddMiddleware(middleware.ConvertPanicToError())

    // Subscriber for order processing
    subscriber, err := amqp.NewAMQPSubscriber(
        channel,
        "order-processing-queue",
        amqp.WithProcessingTimeout(30*time.Second),
        amqp.WithProcessingTimeoutHandler(func(ctx context.Context, msg *amqpgo.Delivery) {
            log.Printf("Order processing timeout: %s", msg.MessageId)
        }),
    )
    if err != nil {
        return err
    }

    // Message routing with type safety
    router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
    
    router.
        AddHandler("order_created").
        AddMiddleware(amqp.UnmarshallPayloadFromJson(OrderCreatedEvent{})).
        Handle(message.HandleWithPayload(func(msg *message.Message, event OrderCreatedEvent) error {
            log.Printf("Processing new order: %s", event.OrderID)
            return processNewOrder(event)
        }))

    router.
        AddHandler("payment_processed").
        AddMiddleware(amqp.UnmarshallPayloadFromJson(PaymentEvent{})).
        Handle(message.HandleWithPayload(func(msg *message.Message, event PaymentEvent) error {
            log.Printf("Processing payment: %s", event.TransactionID)
            return processPayment(event)
        }))

    router.AddDefaultHandler(func(msg *message.Message) error {
        log.Printf("Unhandled event type: %v", msg.Metadata["event_type"])
        return nil
    })

    subEngine := subscriber.NewSubscriptionEngine(subscriber, *router).
        AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
            log.Printf("Processing error for message %s: %v", msg.ID, err)
        })).
        AddMiddleware(middleware.ConvertPanicToError())

    // Start processing
    ctx := context.Background()
    return subEngine.Start(ctx)
}

// Event types
type OrderCreatedEvent struct {
    OrderID   string    `json:"order_id"`
    UserID    string    `json:"user_id"`
    Amount    float64   `json:"amount"`
    Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
    TransactionID string    `json:"transaction_id"`
    OrderID       string    `json:"order_id"`
    Amount        float64   `json:"amount"`
    Status        string    `json:"status"`
    Timestamp     time.Time `json:"timestamp"`
}
```

## Best Practices

### 1. Exchange Type Selection
- **Direct**: Use for task queues and point-to-point messaging
- **Fanout**: Use for broadcasting events and notifications
- **Topic**: Use for hierarchical routing and log aggregation
- **Headers**: Use for complex routing based on multiple criteria

```go
// Choose exchange type based on routing needs
// Direct: Exact routing key matching
routingKeyFn := func(msg *message.Message) (string, error) {
    return msg.Metadata["task_type"].(string), nil
}

// Topic: Pattern-based routing
routingKeyFn := func(msg *message.Message) (string, error) {
    return fmt.Sprintf("logs.%s.%s", 
        msg.Metadata["component"], msg.Metadata["level"]), nil
}
```

### 2. Connection Management
- Always use auto-reconnecting connections in production
- Implement proper retry strategies with exponential backoff
- Handle connection events gracefully
- Monitor connection health

```go
// Production connection with retry strategy
conn, err := amqp.NewConnection(connectionURL,
    amqp.WithReconnectionRetry(func(attempt int) time.Duration {
        base := time.Duration(math.Min(float64(attempt), 10)) * time.Second
        jitter := time.Duration(rand.Intn(1000)) * time.Millisecond
        return base + jitter
    }))
```

### 3. Message Durability and Reliability
- Use persistent delivery mode for critical messages
- Enable mandatory flag for messages that must be routable
- Implement proper error handling and dead letter queues
- Use acknowledgments appropriately

```go
// Reliable message configuration
amqp.DefaultMessageOptions{
    ContentType:  "application/json",
    Priority:     5,
    DeliveryMode: amqpgo.Persistent, // Survive broker restarts
}

// Mandatory routing for critical messages
amqp.WithMandatoryMsgFn(func(msg *message.Message) (bool, error) {
    return msg.Metadata["critical"] == true, nil
})
```

### 4. Performance Optimization
- Use appropriate queue prefetch settings
- Configure processing timeouts based on workload
- Monitor queue depths and consumer lag
- Use exclusive consumers when appropriate

```go
// Optimized subscriber configuration
subscriber, err := amqp.NewAMQPSubscriber(
    channel,
    queueName,
    amqp.WithProcessingTimeout(30*time.Second),
    amqp.WithExclusive(), // For single consumer scenarios
)
```

### 5. Header and Metadata Management
- Filter sensitive information from headers
- Use consistent metadata naming conventions
- Consider header size limits
- Implement header validation

```go
// Secure header filtering
amqp.WithHeadersProvider(amqp.ExcludePrefixHeadersProvider("internal_"))

// Or use custom filtering
customFilter := func(key string, value interface{}) bool {
    sensitiveKeys := []string{"password", "token", "secret"}
    for _, sensitive := range sensitiveKeys {
        if strings.Contains(strings.ToLower(key), sensitive) {
            return false
        }
    }
    return true
}
```

### 6. Testing with Docker
- Always use Docker for integration tests
- Use unique queue names to enable parallel test execution
- Clean up test resources properly
- Test connection failures and recovery

```go
// Test setup with proper cleanup
func TestAMQPIntegration(t *testing.T) {
    // Start dependencies
    // task du (handled by test infrastructure)
    
    // Use unique names for parallel execution
    queueName := fmt.Sprintf("test-queue-%d", time.Now().UnixNano())
    
    // Test implementation
    // ...
    
    // Cleanup handled by deferred functions and test teardown
}
```

## Running Tests and Examples

### Running Tests
The AMQP tests demonstrate these usage patterns in action:

```bash
# Run all AMQP tests (requires Docker)
task amqp:test

# Run specific test files
go test -v ./amqp -run TestAMQPPublisher
go test -v ./amqp -run TestAMQPSubscriber

# Run load tests
go test -v ./amqp -run TestLoad
```

### Running Examples
```bash
# Start dependencies
task amqp:du

# Run tests with Docker dependencies
cd amqp && go test -v ./...

# Stop dependencies when done  
task amqp:dd
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

- **Core Implementation**: `amqp/publisher.go`, `amqp/subscriber.go`
- **Connection Management**: `amqp/connection.go`, `amqp/channel.go`
- **Middleware**: `amqp/json.go`, `amqp/headers.go`
- **Test Examples**: `amqp/publisher_test.go`, `amqp/subscriber_test.go`
- **Load Testing**: `amqp/load_test.go`
- **Documentation**: `amqp/USAGE_PATTERNS.md` (this file)

## Dependencies

- **Docker**: Required for running RabbitMQ broker
- **Go 1.19+**: For generics support and modern Go features
- **Task**: For running development commands
- **RabbitMQ**: Message broker (provided via Docker)
- **Testing**: Standard Go testing with comprehensive integration tests

## Contributing

When working with AMQP patterns:

1. **Follow Test-Driven Development**: Write tests first using existing patterns
2. **Use Docker for Integration Tests**: Always test against real RabbitMQ instance
3. **Test Connection Resilience**: Include network failure scenarios
4. **Document New Patterns**: Update this guide when adding new usage patterns
5. **Consider AMQP Semantics**: Understand exchange types and routing behavior

### Adding New Patterns
1. Study existing test cases in `publisher_test.go` and `subscriber_test.go`
2. Add new test cases following the BDD style
3. Use unique queue/exchange names for parallel test execution
4. Document the new pattern in this guide with real code examples
5. Test with multiple exchange types if applicable

## Additional Resources

- [AMQP 0.9.1 Specification](https://www.amqp.org/specification/0-9-1/amqp-org-download)
- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [Expedit Core Documentation](../core/)
- [Task Configuration](../Taskfile.yml)
- [Docker Compose](docker-compose.yml)