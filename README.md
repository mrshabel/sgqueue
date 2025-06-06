# SGQueue

A lightweight, reliable postgres-based message queue with exactly-once-delivery guarantees. Built for applications that need a reliable message queue without running additional services.

## Architecture

SGQueue leverages PostgreSQL to store and manage messages using an SQL table for storing messages and a dead-letter table for storing failed messages. The client library handles enqueueing, dequeuing, and processing, with a scheduler managing delayed messages and stuck messages.

![Architecture](./architecture.png)

## Features

-   **Channel-Based Consumption**: Simple RabbitMQ-like consumer API
-   **Exactly-Once Delivery**: Messages are delivered reliably using SQL ACID transactions
-   **Priority Queues**: Support for message prioritization
-   **Message Retries**: Automatic retry handling with configurable max retries
-   **Dead Letter Queue (DLQ)**: Failed messages are moved to DLQ after max retries
-   **Batch Processing**: Support for processing multiple messages at once
-   **Idempotency**: Prevent duplicate messages using idempotency keys
-   **Flexible Payloads**: Store any JSON-serializable payload
-   **Visibility Windows**: Prevent message processing hangs
-   **PostgreSQL Backend**: Leverage your existing Postgres infrastructure

## Installation

```bash
go get github.com/mrshabel/sgqueue
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/mrshabel/sgqueue"
)

type Order struct {
    ID     string    `json:"id"`
    Amount float64   `json:"amount"`
    Time   time.Time `json:"time"`
}

func main() {
    // initialize queue
    q, err := queue.NewQueue(queue.SGQueueConfig{
        Name:              "orders",
        DatabaseURL:       "postgres://user:pass@localhost/dbname",
        MessageMaxRetries: 3,
        VisibilityWindow: 5 * time.Minute,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer q.Shutdown(context.Background())

    // create consumer channel
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    messages, err := q.Consume(ctx, 100)
    if err != nil {
        log.Fatal(err)
    }

    // process messages
    for msg := range messages {
        var order Order
        if err := msg.DecodeJSONPayload(&order); err != nil {
            log.Printf("Failed to decode: %v", err)
            msg.Nack()
            continue
        }

        // process the order
        log.Printf("Processing order: %s", order.ID)

        if err := processOrder(order); err != nil {
			// will retry up to MaxRetries
            msg.Retry()
            continue
        }

		// success
        msg.Ack()
    }
}
```

## Configuration

```go
type SGQueueConfig struct {
    Name              string        // Queue name
    DatabaseURL       string        // Postgres connection string
    MessageMaxRetries int          // Maximum retry attempts
    VisibilityWindow time.Duration // Max processing time
    ProcessingTimeout time.Duration // Operation timeout
}
```

## Advanced Usage

### Batch Processing

```go
messages, err := queue.ConsumeBatch(10)
if err != nil {
    log.Fatal(err)
}

for _, msg := range messages {
    // Process batch message
    msg.Ack()
}
```

### Priority Messages

```go
// Higher priority messages are processed first
err := queue.Produce(payload, 10, "order-123")
```

## Database Setup

SGQueue requires a PostgreSQL database. The tables are automatically created when you initialize the queue.

Required Postgres version: 12 or higher

## Development

### Running Tests

SGQueue uses `TestContainers` to spin up a test instance of a postgres database.
To run the tests, ensure that docker client is running on your machine. You can optionally replace the test-container section with another database connection string if you want to opt-in for a non-docker based test.
Run the tests using:

```bash
make test
```

Run the benchmarks with:

```bash
make bench
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## TODO

-   Refactor consumer polling logic to a reactive mechanism with Listen/Notify.
-   Monitoring Integration: Export queue stats to Prometheus/Grafana.
