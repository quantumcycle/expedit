# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Expedit is a Go library for message publish/subscribe brokers that provides building blocks for creating message producers and consumers. It supports multiple implementations including Google PubSub, Redis streams, AMQP (RabbitMQ), and Go channels.

**Status: NOT READY FOR PRODUCTION USE**

## Architecture

This is a **Go workspace** project with multiple modules:

- `core/` - Core abstractions and interfaces for publishers, subscribers, middleware, and message handling
- `google/` - Google Cloud Pub/Sub implementation  
- `redis/` - Redis streams implementation
- `amqp/` - AMQP/RabbitMQ implementation (Beta)
- `prometheus/` - Prometheus metrics middleware
- `examples/` - Usage examples and sample code

### Key Components

- **PublishingEngine**: Combines a Publisher implementation with middleware chain
- **SubscriptionEngine**: Combines a Subscriber implementation with middleware and message router
- **Router**: Routes messages to handlers based on configurable routing keys (similar to HTTP routers)
- **Middleware**: Functions that execute before/after message processing (logging, metrics, error handling, etc.)
- **Message**: Core data structure containing ID, metadata map, payload, and context

## Development Commands

### Testing
```bash
# Run all tests across all modules
task test

# Test specific modules
task core:test
task google:test  
task redis:test
task amqp:test

# Or use go directly in each module
cd core && go test ./...
```

### Dependencies
```bash
# Start all Docker dependencies (Redis, GCP emulator, RabbitMQ)
task du

# Stop all Docker dependencies  
task dd

# Or manage specific implementations
task google:du / task google:dd
task redis:du / task redis:dd
task amqp:du / task amqp:dd
```

### Linting & Formatting
```bash
# Format code
gofmt -w .

# Lint (golangci-lint is available)
golangci-lint run
```

## Module Structure

Each implementation module follows this pattern:
- `publisher.go` - Publisher implementation
- `subscriber.go` - Subscriber implementation  
- `*_test.go` - Tests requiring Docker dependencies
- `docker-compose.yml` - Test dependencies
- `task.yml` - Module-specific tasks

## Testing Requirements

- Google and Redis modules require Docker containers for integration tests
- Core module has no external dependencies
- Use BDD style tests as per project conventions
- Tests automatically start required Docker dependencies via Task

## Key Implementation Notes

- Each implementation maintains its own characteristics rather than forcing a common abstraction
- Middleware is executed in the order added to engines
- Router panics if no handler is found and no default handler is provided
- Message acknowledgment (Ack/Nack) is handled by the underlying implementation