# Expedit Messaging Patterns

This document provides comprehensive examples of messaging patterns that apply across all Expedit implementations (Google Pub/Sub, Redis, AMQP, and Go channels). These patterns demonstrate common architectural approaches for building reliable, scalable message-driven systems.

## Overview

Expedit supports multiple messaging implementations while providing consistent patterns and abstractions. This guide covers universal messaging patterns that work across all implementations, helping you build robust message-driven architectures regardless of your underlying message broker.

## Universal Messaging Patterns

### 1. Middleware Chain Pattern

**Use Case**: Cross-cutting concerns like logging, metrics, authentication, validation  
**Applies To**: All implementations (Google, Redis, AMQP, Channels)

**Key Features**:
- Consistent middleware across publisher and subscriber
- Execution order control
- Error handling and panic recovery
- Metrics and observability integration

**Example Scenario**: Production microservice with comprehensive monitoring and business logic

```go
// Publisher middleware chain - works with any implementation
pubEngine := publisher.NewPublishingEngine(anyPublisher)
pubEngine.
    AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
        log.Printf("Publish error for %s: %v", msg.ID, err)
    })).
    AddMiddleware(middleware.ConvertPanicToError()).
    AddMiddleware(addCorrelationIDMiddleware).
    AddMiddleware(addTimestampMiddleware)

// Subscriber middleware chain - works with any implementation  
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
subEngine := subscriber.NewSubscriptionEngine(anySubscriber, router)
subEngine.
    AddMiddleware(authenticationMiddleware).
    AddMiddleware(rateLimitingMiddleware).
    AddMiddleware(middleware.OnError(func(msg *message.Message, err error) {
        log.Printf("Processing error for %s: %v", msg.ID, err)
    })).
    AddMiddleware(middleware.ConvertPanicToError())

// Custom middleware example
func addCorrelationIDMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        if msg.Metadata["correlation_id"] == nil {
            msg.Metadata["correlation_id"] = generateCorrelationID()
        }
        return next(msg)
    }
}

func addTimestampMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        msg.Metadata["processed_at"] = time.Now().Unix()
        return next(msg)
    }
}
```

### 2. Message Routing Pattern

**Use Case**: Directing messages to different handlers based on content  
**Applies To**: All implementations

**Key Features**:
- Content-based routing using metadata
- Type-safe message handlers
- Default handler fallback
- Dynamic routing decisions

**Example Scenario**: Event-driven microservice routing different event types

```go
// Universal routing - works with any implementation
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))

// Handler for specific event types
router.AddHandler("user_created").Handle(func(msg *message.Message) error {
    log.Printf("Processing user creation: %s", string(msg.Payload.([]byte)))
    // Business logic for user creation
    return nil
})

router.AddHandler("order_placed").Handle(func(msg *message.Message) error {
    log.Printf("Processing order: %s", string(msg.Payload.([]byte)))
    // Business logic for order processing
    return nil
})

// Typed handler with automatic JSON unmarshalling
router.AddHandler("product_updated").
    AddMiddleware(jsonUnmarshallMiddleware(Product{})).
    Handle(message.HandleWithPayload(func(msg *message.Message, product Product) error {
        log.Printf("Product updated: %s - %s", product.ID, product.Name)
        return updateProductCache(product)
    }))

// Default handler for unknown event types
router.AddDefaultHandler(func(msg *message.Message) error {
    log.Printf("Unknown event type: %v", msg.Metadata["event_type"])
    // Could log to monitoring, send to dead letter queue, etc.
    return nil
})

// Advanced routing with multiple criteria
router := subscriber.NewRouter(func(msg *message.Message) subscriber.RoutingKey {
    eventType := msg.Metadata["event_type"]
    priority := msg.Metadata["priority"]
    
    // Route based on both event type and priority
    return subscriber.RoutingKey(fmt.Sprintf("%s:%s", eventType, priority))
})
```

### 3. Error Handling and Retry Pattern

**Use Case**: Resilient message processing with configurable retry logic  
**Applies To**: All implementations (with implementation-specific retry mechanisms)

**Key Features**:
- Configurable retry strategies
- Error classification and handling
- Dead letter queue simulation
- Circuit breaker integration

**Example Scenario**: Payment processing with retry logic for transient failures

```go
// Universal error handling middleware
func retryMiddleware(maxRetries int) middleware.Middleware {
    return func(next message.HandlerFunc) message.HandlerFunc {
        return func(msg *message.Message) error {
            var lastErr error
            
            for attempt := 0; attempt <= maxRetries; attempt++ {
                err := next(msg)
                if err == nil {
                    return nil // Success
                }
                
                lastErr = err
                
                // Check if error is retryable
                if !isRetryableError(err) {
                    log.Printf("Non-retryable error for %s: %v", msg.ID, err)
                    return err
                }
                
                if attempt < maxRetries {
                    backoff := time.Duration(attempt+1) * time.Second
                    log.Printf("Retry %d/%d for %s after %v: %v", 
                        attempt+1, maxRetries, msg.ID, backoff, err)
                    time.Sleep(backoff)
                }
            }
            
            log.Printf("Max retries exceeded for %s: %v", msg.ID, lastErr)
            return fmt.Errorf("max retries exceeded: %w", lastErr)
        }
    }
}

func isRetryableError(err error) bool {
    // Classify errors as retryable or not
    switch {
    case errors.Is(err, context.DeadlineExceeded):
        return true
    case errors.Is(err, syscall.ECONNREFUSED):
        return true
    case strings.Contains(err.Error(), "temporary"):
        return true
    case strings.Contains(err.Error(), "timeout"):
        return true
    default:
        return false
    }
}

// Usage with any implementation
subEngine.AddMiddleware(retryMiddleware(3))
```

### 4. Message Correlation and Tracing Pattern

**Use Case**: Request tracing and correlation across distributed systems  
**Applies To**: All implementations

**Key Features**:
- Automatic correlation ID generation
- Trace context propagation
- Request-response correlation
- Distributed tracing integration

**Example Scenario**: Microservice communication with full request tracing

```go
// Correlation middleware for publishers
func correlationPublisherMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        // Generate correlation ID if not present
        if msg.Metadata["correlation_id"] == nil {
            msg.Metadata["correlation_id"] = generateCorrelationID()
        }
        
        // Add trace context
        if msg.Metadata["trace_id"] == nil {
            msg.Metadata["trace_id"] = generateTraceID()
        }
        
        // Add service context
        msg.Metadata["source_service"] = "my-service"
        msg.Metadata["published_at"] = time.Now().Format(time.RFC3339)
        
        return next(msg)
    }
}

// Correlation middleware for subscribers
func correlationSubscriberMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        // Extract correlation context
        correlationID := msg.Metadata["correlation_id"]
        traceID := msg.Metadata["trace_id"]
        
        // Set up logging context
        logger := log.WithFields(log.Fields{
            "correlation_id": correlationID,
            "trace_id":       traceID,
            "message_id":     msg.ID,
        })
        
        // Add to message context for downstream use
        ctx := context.WithValue(msg.Context(), "logger", logger)
        ctx = context.WithValue(ctx, "correlation_id", correlationID)
        
        // Process with enriched context
        return next(msg.WithContext(ctx))
    }
}

// Request-Response correlation helper
type PendingRequests struct {
    requests map[string]chan *message.Message
    mutex    sync.RWMutex
}

func (pr *PendingRequests) WaitForResponse(correlationID string, timeout time.Duration) (*message.Message, error) {
    responseChan := make(chan *message.Message, 1)
    
    pr.mutex.Lock()
    pr.requests[correlationID] = responseChan
    pr.mutex.Unlock()
    
    defer func() {
        pr.mutex.Lock()
        delete(pr.requests, correlationID)
        pr.mutex.Unlock()
        close(responseChan)
    }()
    
    select {
    case response := <-responseChan:
        return response, nil
    case <-time.After(timeout):
        return nil, fmt.Errorf("timeout waiting for response")
    }
}

func (pr *PendingRequests) HandleResponse(msg *message.Message) {
    correlationID, ok := msg.Metadata["correlation_id"].(string)
    if !ok {
        return
    }
    
    pr.mutex.RLock()
    responseChan, exists := pr.requests[correlationID]
    pr.mutex.RUnlock()
    
    if exists {
        select {
        case responseChan <- msg:
        default:
            // Channel full or closed
        }
    }
}
```

### 5. Message Transformation Pattern

**Use Case**: Converting messages between different formats and schemas  
**Applies To**: All implementations

**Key Features**:
- Payload transformation middleware
- Schema evolution support
- Format conversion (JSON, Protobuf, etc.)
- Backward compatibility

**Example Scenario**: API gateway transforming between external and internal message formats

```go
// JSON marshalling middleware - universal
func jsonMarshallMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        // Only transform if payload is not already []byte
        if _, ok := msg.Payload.([]byte); ok {
            return next(msg)
        }
        
        jsonBytes, err := json.Marshal(msg.Payload)
        if err != nil {
            return fmt.Errorf("failed to marshal payload: %w", err)
        }
        
        // Store original type for potential unmarshalling
        msg.Metadata["original_type"] = fmt.Sprintf("%T", msg.Payload)
        msg.Payload = jsonBytes
        
        return next(msg)
    }
}

// JSON unmarshalling middleware - universal
func jsonUnmarshallMiddleware(targetType interface{}) middleware.Middleware {
    return func(next message.HandlerFunc) message.HandlerFunc {
        return func(msg *message.Message) error {
            payloadBytes, ok := msg.Payload.([]byte)
            if !ok {
                return next(msg) // Already unmarshalled
            }
            
            // Create new instance of target type
            targetValue := reflect.New(reflect.TypeOf(targetType))
            target := targetValue.Interface()
            
            err := json.Unmarshal(payloadBytes, target)
            if err != nil {
                return fmt.Errorf("failed to unmarshal payload: %w", err)
            }
            
            // Replace payload with unmarshalled object
            msg.Payload = reflect.ValueOf(target).Elem().Interface()
            
            return next(msg)
        }
    }
}

// Schema migration middleware
func schemaMigrationMiddleware(next message.HandlerFunc) message.HandlerFunc {
    return func(msg *message.Message) error {
        version, ok := msg.Metadata["schema_version"].(string)
        if !ok {
            version = "v1" // Default version
        }
        
        switch version {
        case "v1":
            return migrateV1ToV2(msg, next)
        case "v2":
            return next(msg) // Current version
        default:
            return fmt.Errorf("unsupported schema version: %s", version)
        }
    }
}

func migrateV1ToV2(msg *message.Message, next message.HandlerFunc) error {
    // Example: migrate old user structure to new format
    var v1User struct {
        Name  string `json:"name"`
        Email string `json:"email"`
    }
    
    if err := json.Unmarshal(msg.Payload.([]byte), &v1User); err == nil {
        // Convert to v2 format
        v2User := struct {
            FullName    string `json:"full_name"`
            EmailAddress string `json:"email_address"`
            Version     string `json:"version"`
        }{
            FullName:    v1User.Name,
            EmailAddress: v1User.Email,
            Version:     "v2",
        }
        
        v2Bytes, _ := json.Marshal(v2User)
        msg.Payload = v2Bytes
        msg.Metadata["schema_version"] = "v2"
    }
    
    return next(msg)
}
```

### 6. Message Batching Pattern

**Use Case**: Efficient processing of multiple messages together  
**Applies To**: All implementations

**Key Features**:
- Configurable batch sizes and timeouts
- Memory-efficient processing
- Parallel batch processing
- Error handling for partial batch failures

**Example Scenario**: Database bulk operations, external API calls with rate limits

```go
// Universal batching processor
type BatchProcessor struct {
    batchSize    int
    timeout      time.Duration
    processor    func([]message.Message) error
    currentBatch []message.Message
    mutex        sync.Mutex
    timer        *time.Timer
}

func NewBatchProcessor(batchSize int, timeout time.Duration, 
                      processor func([]message.Message) error) *BatchProcessor {
    return &BatchProcessor{
        batchSize: batchSize,
        timeout:   timeout,
        processor: processor,
    }
}

func (bp *BatchProcessor) ProcessMessage(msg *message.Message) error {
    bp.mutex.Lock()
    defer bp.mutex.Unlock()
    
    bp.currentBatch = append(bp.currentBatch, *msg)
    
    // Process if batch is full
    if len(bp.currentBatch) >= bp.batchSize {
        return bp.processBatch()
    }
    
    // Set up timer for timeout-based processing
    if bp.timer == nil {
        bp.timer = time.AfterFunc(bp.timeout, func() {
            bp.mutex.Lock()
            defer bp.mutex.Unlock()
            if len(bp.currentBatch) > 0 {
                bp.processBatch()
            }
        })
    }
    
    return nil
}

func (bp *BatchProcessor) processBatch() error {
    if len(bp.currentBatch) == 0 {
        return nil
    }
    
    batch := make([]message.Message, len(bp.currentBatch))
    copy(batch, bp.currentBatch)
    bp.currentBatch = bp.currentBatch[:0] // Clear batch
    
    if bp.timer != nil {
        bp.timer.Stop()
        bp.timer = nil
    }
    
    return bp.processor(batch)
}

// Batching middleware
func batchingMiddleware(batchSize int, timeout time.Duration) middleware.Middleware {
    processor := NewBatchProcessor(batchSize, timeout, func(batch []message.Message) error {
        log.Printf("Processing batch of %d messages", len(batch))
        
        // Process batch - example: bulk database insert
        var records []DatabaseRecord
        for _, msg := range batch {
            record := DatabaseRecord{
                ID:        msg.ID,
                Data:      string(msg.Payload.([]byte)),
                Timestamp: time.Now(),
            }
            records = append(records, record)
        }
        
        return database.BulkInsert(records)
    })
    
    return func(next message.HandlerFunc) message.HandlerFunc {
        return func(msg *message.Message) error {
            // Add to batch instead of processing immediately
            return processor.ProcessMessage(msg)
        }
    }
}
```

### 7. Circuit Breaker Pattern

**Use Case**: Preventing cascade failures and providing graceful degradation  
**Applies To**: All implementations

**Key Features**:
- Automatic failure detection
- Configurable thresholds and timeouts
- Graceful degradation strategies
- Recovery monitoring

**Example Scenario**: External service integration with circuit breaker protection

```go
// Circuit breaker middleware using sony/gobreaker
func circuitBreakerMiddleware(name string, settings gobreaker.Settings) middleware.Middleware {
    cb := gobreaker.NewCircuitBreaker[*message.Message](settings)
    
    return func(next message.HandlerFunc) message.HandlerFunc {
        return func(msg *message.Message) error {
            result, err := cb.Execute(func() (*message.Message, error) {
                err := next(msg)
                return msg, err
            })
            
            if err != nil {
                switch err {
                case gobreaker.ErrOpenState:
                    log.Printf("Circuit breaker %s is OPEN, failing fast for message %s", 
                        name, msg.ID)
                    return handleCircuitBreakerOpen(msg)
                case gobreaker.ErrTooManyRequests:
                    log.Printf("Circuit breaker %s has too many requests, rejecting message %s", 
                        name, msg.ID)
                    return handleCircuitBreakerOverload(msg)
                default:
                    return err
                }
            }
            
            return nil
        }
    }
}

func handleCircuitBreakerOpen(msg *message.Message) error {
    // Graceful degradation strategies:
    
    // 1. Cache fallback
    if cachedResult := getFromCache(msg.ID); cachedResult != nil {
        log.Printf("Using cached result for %s", msg.ID)
        return nil
    }
    
    // 2. Default response
    log.Printf("Providing default response for %s", msg.ID)
    return nil // Accept message but provide default handling
    
    // 3. Defer processing
    // return sendToDelayQueue(msg)
    
    // 4. Fail fast
    // return fmt.Errorf("service unavailable")
}

func handleCircuitBreakerOverload(msg *message.Message) error {
    // Handle overload scenario
    log.Printf("Service overloaded, deferring message %s", msg.ID)
    return sendToDelayQueue(msg)
}

// Usage with configuration
settings := gobreaker.Settings{
    Name:        "external-service",
    MaxRequests: 3,                    // Max requests in half-open state
    Interval:    time.Minute,          // When to reset failure count
    Timeout:     30 * time.Second,     // How long to stay open
    ReadyToTrip: func(counts gobreaker.Counts) bool {
        failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
        return counts.Requests >= 3 && failureRatio >= 0.6
    },
    OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
        log.Printf("Circuit breaker %s changed from %s to %s", name, from, to)
    },
}

subEngine.AddMiddleware(circuitBreakerMiddleware("payment-service", settings))
```

## Implementation-Specific Setup Examples

### Google Pub/Sub
```go
import "github.com/quantumcycle/expedit/google"

// Publisher setup
pub, err := google.NewGooglePublisher(client, routingFunc)
pubEngine := publisher.NewPublishingEngine(pub)

// Subscriber setup  
sub, err := google.NewGoogleSubscriber(client, subscriptionName)
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
subEngine := subscriber.NewSubscriptionEngine(sub, router)
```

### Redis Streams
```go
import "github.com/quantumcycle/expedit/redis"

// Publisher setup
pub, err := redis.NewRedisPublisher(client, routingFunc)
pubEngine := publisher.NewPublishingEngine(pub)

// Subscriber setup
sub, err := redis.NewRedisSubscriber(client, consumerGroup, streams)
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
subEngine := subscriber.NewSubscriptionEngine(sub, router)
```

### AMQP (RabbitMQ)
```go
import "github.com/quantumcycle/expedit/amqp"

// Publisher setup
pub, err := amqp.NewAMQPPublisher(connection, routingFunc)
pubEngine := publisher.NewPublishingEngine(pub)

// Subscriber setup
sub, err := amqp.NewAMQPSubscriber(connection, queueName)
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
subEngine := subscriber.NewSubscriptionEngine(sub, router)
```

### Go Channels
```go
import "github.com/quantumcycle/expedit/core/publisher"
import "github.com/quantumcycle/expedit/core/subscriber"

// Channel-based setup
msgChan := make(chan *message.Message, 100)
pub := publisher.NewChannelPublisher(msgChan)
pubEngine := publisher.NewPublishingEngine(pub)

sub := subscriber.NewChannelSubscriber(msgChan)
router := subscriber.NewRouter(subscriber.RouteFromMetadataKey("event_type"))
subEngine := subscriber.NewSubscriptionEngine(sub, router)
```

## Best Practices

### 1. Middleware Design
- Keep middleware focused on single responsibilities
- Design middleware to be composable and reusable
- Consider execution order carefully
- Handle errors gracefully within middleware

### 2. Message Design
- Use consistent metadata schemas across your application
- Include correlation IDs for tracing
- Add timestamps for debugging and monitoring
- Keep payloads focused and coherent

### 3. Error Handling
- Classify errors as retryable vs non-retryable
- Implement circuit breakers for external dependencies
- Use dead letter queues for permanently failed messages
- Log errors with sufficient context

### 4. Performance
- Use batching for high-throughput scenarios
- Configure appropriate timeouts
- Monitor message processing latencies
- Consider memory usage with large message volumes

### 5. Testing
- Test middleware chains thoroughly
- Use dependency injection for testability
- Mock external dependencies in tests
- Test error scenarios and edge cases

## Contributing

When adding new messaging patterns:
1. Ensure the pattern works across all implementations
2. Provide clear use cases and examples
3. Include error handling and edge cases
4. Add performance considerations
5. Update this documentation

## Implementation-Specific Pattern Guides

Each implementation has additional patterns that leverage implementation-specific features:

- **[Google Pub/Sub Patterns](google/USAGE_PATTERNS.md)** - Ordering keys, attributes, emulator testing
- **Redis Patterns** - Redis Streams, consumer groups, blocking reads  
- **AMQP Patterns** - Exchanges, routing keys, dead letter queues
- **Channel Patterns** - In-memory messaging, testing patterns

## Additional Resources

- [Core Message Documentation](core/message/)
- [Publisher Documentation](core/publisher/)
- [Subscriber Documentation](core/subscriber/)
- [Middleware Documentation](core/message/middleware/)
- [Examples](examples/)
- [Google Test Plan](google/test-plan.md)