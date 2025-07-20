package testrabbit

import (
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	amqp "github.com/quantumcycle/expedit/amqp"
	amqpgo "github.com/rabbitmq/amqp091-go"
	"time"
)

// waitForQueueReady waits for a queue to be fully available with retry logic
// This replaces hard-coded sleeps with proper health checks
func waitForQueueReady(channel *amqp.ReconnectingChannel, queueName string, maxWait time.Duration) error {
	start := time.Now()
	for time.Since(start) < maxWait {
		_, err := channel.QueueDeclarePassive(queueName, false, false, false, false, nil)
		if err == nil {
			return nil // Queue is ready
		}
		time.Sleep(25 * time.Millisecond) // Poll interval
	}
	return fmt.Errorf("queue %s not ready within %v", queueName, maxWait)
}

// retryOperation performs a queue/exchange operation with retry logic
func retryOperation(operation func() error, maxRetries int, description string) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := operation()
		if err == nil {
			return nil
		}
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond) // Exponential backoff
		}
	}
	return fmt.Errorf("%s failed after %d retries: %w", description, maxRetries, lastErr)
}

type DirectQueue struct {
	channel   *amqp.ReconnectingChannel
	QueueName string
}

func (d DirectQueue) Delete() {
	if d.QueueName == "" {
		return // Already deleted or not initialized
	}
	_, err := d.channel.QueueDelete(d.QueueName, false, false, false)
	if err != nil {
		fmt.Printf("Warning: Failed to delete queue %s: %v\n", d.QueueName, err)
	}
}

func (d DirectQueue) PublishBytes(bytes []byte, attrs map[string]interface{}) {
	publishing := amqpgo.Publishing{
		Body:         bytes,
		Headers:      attrs,
		ContentType:  "text/plain",
		Priority:     0,
		DeliveryMode: amqpgo.Persistent,
	}
	err := d.channel.Publish("", d.QueueName, false, false, publishing)
	if err != nil {
		// During toxiproxy testing, channels can become invalid due to network simulation
		// Instead of panicking, log the error and continue
		fmt.Printf("Warning: Failed to publish to queue %s: %v\n", d.QueueName, err)
		return
	}
}

func (d DirectQueue) Consume() <-chan amqpgo.Delivery {
	ch, err := d.channel.Consume(d.QueueName, "", false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	return ch
}

// Use the built in direct exchange to route message to a specific queue
func CreateDirectExchangeQueue(channel *amqp.ReconnectingChannel, queueName string) DirectQueue {
	randomPart := shortuuid.New()
	fullQueueName := fmt.Sprintf("%s_%s", queueName, randomPart)
	queue, err := channel.QueueDeclare(
		fullQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		panic(err)
	}

	// Wait for queue to be ready with proper health check
	if err := waitForQueueReady(channel, queue.Name, 2*time.Second); err != nil {
		fmt.Printf("Warning: %v\n", err)
	}

	return DirectQueue{
		channel:   channel,
		QueueName: queue.Name,
	}
}

type DirectRoutingExchange struct {
	channel        *amqp.ReconnectingChannel
	ExchangeName   string
	RoutingToQueue map[string]string
}

func (d DirectRoutingExchange) PublishBytes(bytes []byte, attrs map[string]interface{}, route string) {
	publishing := amqpgo.Publishing{
		Body:         bytes,
		Headers:      attrs,
		ContentType:  "text/plain",
		Priority:     0,
		DeliveryMode: amqpgo.Persistent,
	}
	err := d.channel.Publish(d.ExchangeName, route, false, false, publishing)
	if err != nil {
		panic(err)
	}
}

func (d DirectRoutingExchange) Delete() {
	if d.ExchangeName == "" {
		return // Already deleted or not initialized
	}

	// Delete queues first
	for route, queueName := range d.RoutingToQueue {
		if _, err := d.channel.QueueDelete(queueName, false, false, false); err != nil {
			fmt.Printf("Warning: Failed to delete queue %s for route %s: %v\n", queueName, route, err)
		}
	}

	// Then delete exchange
	if err := d.channel.ExchangeDelete(d.ExchangeName, false, false); err != nil {
		fmt.Printf("Warning: Failed to delete exchange %s: %v\n", d.ExchangeName, err)
	}
}

func (d DirectRoutingExchange) Consume(route string) <-chan amqpgo.Delivery {
	queueName, ok := d.RoutingToQueue[route]
	if !ok {
		panic(fmt.Errorf("no queue for route %s", route))
	}
	ch, err := d.channel.Consume(queueName, "", false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	return ch
}

// Create an direct exchange and a routing key binding for a bunch of queues
func CreateDirectRoutingExchange(channel *amqp.ReconnectingChannel, exchangeName string, routingKeys ...string) DirectRoutingExchange {
	randomPart := shortuuid.New()
	fullExchangeName := fmt.Sprintf("%s_%s", exchangeName, randomPart)

	if err := channel.ExchangeDeclare(
		fullExchangeName,
		amqpgo.ExchangeDirect,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	mapping := make(map[string]string)
	for _, key := range routingKeys {
		fullQueueName := fmt.Sprintf("%s_%s_%s", exchangeName, key, randomPart)
		queue, err := channel.QueueDeclare(
			fullQueueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			panic(err)
		}

		err = retryOperation(func() error {
			return channel.QueueBind(queue.Name, key, fullExchangeName, false, nil)
		}, 3, fmt.Sprintf("binding queue %s to exchange %s with key %s", queue.Name, fullExchangeName, key))
		if err != nil {
			panic(err)
		}

		// Verify queue is accessible with a passive declare
		_, err = channel.QueueDeclarePassive(queue.Name, false, false, false, false, nil)
		if err != nil {
			panic(fmt.Errorf("failed to verify queue %s: %w", queue.Name, err))
		}

		mapping[key] = queue.Name
	}

	// Wait for all queues to be ready with proper health checks
	for _, queueName := range mapping {
		if err := waitForQueueReady(channel, queueName, 2*time.Second); err != nil {
			fmt.Printf("Warning: %v\n", err)
		}
	}

	return DirectRoutingExchange{
		channel:        channel,
		ExchangeName:   fullExchangeName,
		RoutingToQueue: mapping,
	}
}

type FanoutExchange struct {
	channel         *amqp.ReconnectingChannel
	ExchangeName    string
	Queues          []string
	LogicalToActual map[string]string
}

func (d FanoutExchange) PublishBytes(bytes []byte, attrs map[string]interface{}) {
	publishing := amqpgo.Publishing{
		Body:         bytes,
		Headers:      attrs,
		ContentType:  "text/plain",
		Priority:     0,
		DeliveryMode: amqpgo.Persistent,
	}
	err := d.channel.Publish(d.ExchangeName, "", false, false, publishing)
	if err != nil {
		panic(err)
	}
}

func (d FanoutExchange) Delete() {
	if d.ExchangeName == "" {
		return // Already deleted or not initialized
	}

	// Delete queues first
	for _, queueName := range d.Queues {
		if _, err := d.channel.QueueDelete(queueName, false, false, false); err != nil {
			fmt.Printf("Warning: Failed to delete queue %s: %v\n", queueName, err)
		}
	}

	// Then delete exchange
	if err := d.channel.ExchangeDelete(d.ExchangeName, false, false); err != nil {
		fmt.Printf("Warning: Failed to delete exchange %s: %v\n", d.ExchangeName, err)
	}
}

func (d FanoutExchange) Consume(logicalQueueName string) <-chan amqpgo.Delivery {
	actualQueueName, ok := d.LogicalToActual[logicalQueueName]
	if !ok {
		panic(fmt.Errorf("no queue for logical name %s", logicalQueueName))
	}
	ch, err := d.channel.Consume(actualQueueName, "", false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	return ch
}

// Create an exchange, a queue, and a binding for a specific routing key between the exchange and queue
func CreateFanoutExchange(channel *amqp.ReconnectingChannel, exchangeName string, queues ...string) FanoutExchange {
	randomPart := shortuuid.New()
	fullExchangeName := fmt.Sprintf("%s_%s", exchangeName, randomPart)

	if err := channel.ExchangeDeclare(
		fullExchangeName,
		amqpgo.ExchangeFanout,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	actualQueues := make([]string, 0, len(queues))
	logicalToActual := make(map[string]string)
	for _, queueLogicalName := range queues {
		fullQueueName := fmt.Sprintf("%s_%s_%s", exchangeName, queueLogicalName, randomPart)
		queue, err := channel.QueueDeclare(
			fullQueueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			panic(err)
		}

		err = retryOperation(func() error {
			return channel.QueueBind(queue.Name, "", fullExchangeName, false, nil)
		}, 3, fmt.Sprintf("binding queue %s to fanout exchange %s", queue.Name, fullExchangeName))
		if err != nil {
			panic(err)
		}

		// Verify queue is accessible with a passive declare
		_, err = channel.QueueDeclarePassive(queue.Name, false, false, false, false, nil)
		if err != nil {
			panic(fmt.Errorf("failed to verify queue %s: %w", queue.Name, err))
		}

		actualQueues = append(actualQueues, queue.Name)
		logicalToActual[queueLogicalName] = queue.Name
	}

	// Wait for all queues to be ready with proper health checks
	for _, queueName := range logicalToActual {
		if err := waitForQueueReady(channel, queueName, 2*time.Second); err != nil {
			fmt.Printf("Warning: %v\n", err)
		}
	}

	return FanoutExchange{
		channel:         channel,
		ExchangeName:    fullExchangeName,
		Queues:          actualQueues,
		LogicalToActual: logicalToActual,
	}
}

type TopicExchange struct {
	channel        *amqp.ReconnectingChannel
	ExchangeName   string
	PatternToQueue map[string]string
}

func (d TopicExchange) PublishBytes(bytes []byte, attrs map[string]interface{}, routingKey string) {
	publishing := amqpgo.Publishing{
		Body:         bytes,
		Headers:      attrs,
		ContentType:  "text/plain",
		Priority:     0,
		DeliveryMode: amqpgo.Persistent,
	}
	err := d.channel.Publish(d.ExchangeName, routingKey, false, false, publishing)
	if err != nil {
		panic(err)
	}
}

func (d TopicExchange) Delete() {
	if d.ExchangeName == "" {
		return // Already deleted or not initialized
	}

	// Delete queues first
	for pattern, queueName := range d.PatternToQueue {
		if _, err := d.channel.QueueDelete(queueName, false, false, false); err != nil {
			fmt.Printf("Warning: Failed to delete queue %s for pattern %s: %v\n", queueName, pattern, err)
		}
	}

	// Then delete exchange
	if err := d.channel.ExchangeDelete(d.ExchangeName, false, false); err != nil {
		fmt.Printf("Warning: Failed to delete exchange %s: %v\n", d.ExchangeName, err)
	}
}

func (d TopicExchange) Consume(pattern string) <-chan amqpgo.Delivery {
	queueName, ok := d.PatternToQueue[pattern]
	if !ok {
		panic(fmt.Errorf("no queue for pattern %s", pattern))
	}
	ch, err := d.channel.Consume(queueName, "", false, true, false, false, nil)
	if err != nil {
		panic(fmt.Errorf("cannot consume queue pattern %s [%s]: %w", pattern, d.ExchangeName, err))
	}
	return ch
}

// Create a topic exchange and bind queues with specific routing patterns
func CreateTopicExchange(channel *amqp.ReconnectingChannel, exchangeName string, patterns ...string) TopicExchange {
	randomPart := shortuuid.New()
	fullExchangeName := fmt.Sprintf("%s_%s", exchangeName, randomPart)

	if err := channel.ExchangeDeclare(
		fullExchangeName,
		amqpgo.ExchangeTopic,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	mapping := make(map[string]string)
	for i, pattern := range patterns {
		// Create a unique queue name for each pattern using index to avoid special chars
		fullQueueName := fmt.Sprintf("%s_pattern%d_%s", exchangeName, i, randomPart)
		queue, err := channel.QueueDeclare(
			fullQueueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			panic(err)
		}

		err = retryOperation(func() error {
			return channel.QueueBind(queue.Name, pattern, fullExchangeName, false, nil)
		}, 3, fmt.Sprintf("binding queue %s to topic exchange %s with pattern %s", queue.Name, fullExchangeName, pattern))
		if err != nil {
			panic(err)
		}

		// Verify queue is accessible with a passive declare
		_, err = channel.QueueDeclarePassive(queue.Name, false, false, false, false, nil)
		if err != nil {
			panic(fmt.Errorf("failed to verify queue %s: %w", queue.Name, err))
		}

		mapping[pattern] = queue.Name
	}

	// Wait for all queues to be ready with proper health checks
	for _, queueName := range mapping {
		if err := waitForQueueReady(channel, queueName, 2*time.Second); err != nil {
			fmt.Printf("Warning: %v\n", err)
		}
	}

	return TopicExchange{
		channel:        channel,
		ExchangeName:   fullExchangeName,
		PatternToQueue: mapping,
	}
}

type HeadersExchange struct {
	channel        *amqp.ReconnectingChannel
	ExchangeName   string
	HeadersToQueue map[string]string
	HeaderBindings map[string]amqpgo.Table
}

func (d HeadersExchange) PublishBytes(bytes []byte, headers map[string]interface{}) {
	publishing := amqpgo.Publishing{
		Body:         bytes,
		Headers:      headers,
		ContentType:  "text/plain",
		Priority:     0,
		DeliveryMode: amqpgo.Persistent,
	}
	err := d.channel.Publish(d.ExchangeName, "", false, false, publishing)
	if err != nil {
		panic(err)
	}
}

func (d HeadersExchange) Delete() {
	if d.ExchangeName == "" {
		return // Already deleted or not initialized
	}

	// Delete queues first
	for bindingKey, queueName := range d.HeadersToQueue {
		if _, err := d.channel.QueueDelete(queueName, false, false, false); err != nil {
			fmt.Printf("Warning: Failed to delete queue %s for binding %s: %v\n", queueName, bindingKey, err)
		}
	}

	// Then delete exchange
	if err := d.channel.ExchangeDelete(d.ExchangeName, false, false); err != nil {
		fmt.Printf("Warning: Failed to delete exchange %s: %v\n", d.ExchangeName, err)
	}
}

func (d HeadersExchange) Consume(bindingKey string) <-chan amqpgo.Delivery {
	queueName, ok := d.HeadersToQueue[bindingKey]
	if !ok {
		panic(fmt.Errorf("no queue for binding key %s", bindingKey))
	}
	ch, err := d.channel.Consume(queueName, "", false, true, false, false, nil)
	if err != nil {
		panic(err)
	}
	return ch
}

// HeaderBinding represents a header binding configuration
type HeaderBinding struct {
	BindingKey string
	Headers    amqpgo.Table
	MatchType  string // "all" or "any"
}

// Create a headers exchange and bind queues with specific header patterns
func CreateHeadersExchange(channel *amqp.ReconnectingChannel, exchangeName string, bindings ...HeaderBinding) HeadersExchange {
	randomPart := shortuuid.New()
	fullExchangeName := fmt.Sprintf("%s_%s", exchangeName, randomPart)

	if err := channel.ExchangeDeclare(
		fullExchangeName,
		amqpgo.ExchangeHeaders,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		panic(err)
	}

	headersToQueue := make(map[string]string)
	headerBindings := make(map[string]amqpgo.Table)

	for _, binding := range bindings {
		// Create a unique queue name for each binding
		fullQueueName := fmt.Sprintf("%s_%s_%s", exchangeName, binding.BindingKey, randomPart)
		queue, err := channel.QueueDeclare(
			fullQueueName,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			panic(err)
		}

		// Prepare binding arguments with x-match
		bindingArgs := make(amqpgo.Table)
		bindingArgs["x-match"] = binding.MatchType
		for k, v := range binding.Headers {
			bindingArgs[k] = v
		}
		

		err = retryOperation(func() error {
			return channel.QueueBind(queue.Name, "", fullExchangeName, false, bindingArgs)
		}, 3, fmt.Sprintf("binding queue %s to headers exchange %s with binding %s", queue.Name, fullExchangeName, binding.BindingKey))
		if err != nil {
			panic(err)
		}

		// Verify queue is accessible with a passive declare
		_, err = channel.QueueDeclarePassive(queue.Name, false, false, false, false, nil)
		if err != nil {
			panic(fmt.Errorf("failed to verify queue %s: %w", queue.Name, err))
		}

		headersToQueue[binding.BindingKey] = queue.Name
		headerBindings[binding.BindingKey] = bindingArgs
	}

	// Wait for all queues to be ready with proper health checks
	for _, queueName := range headersToQueue {
		if err := waitForQueueReady(channel, queueName, 2*time.Second); err != nil {
			fmt.Printf("Warning: %v\n", err)
		}
	}

	return HeadersExchange{
		channel:        channel,
		ExchangeName:   fullExchangeName,
		HeadersToQueue: headersToQueue,
		HeaderBindings: headerBindings,
	}
}
