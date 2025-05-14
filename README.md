# SGQueue

A lightweight, reliable postgres message queue with exactly-once-delivery. Publish and Consume messages with no need for an additional service.

## Architecture

SGQueue leverages PostgreSQL to store and manage messages using an SQL table for storing messages and a dead-letter table for storing failed messages. The client library handles enqueueing, dequeuing, and processing, with a scheduler managing delayed messages and stuck messages.

## Features

-   Exactly-Once Delivery: Ensures messages are delivered reliably using SQL ACID transactions and row-locks.
-   Priority Queues: Prioritize messages with a priority field (defaults to no priority).
-   Retries and Dead Letter Queue (DLQ): Automatically retry failed messages up to the specified maximum retries, then move to DLQ.
-   Flexible Payloads: Store payloads in JSON format.
-   Batch Processing:Optionally Dequeue multiple messages at once for high throughput.
-   Visibility Timeout: Prevent double-processing and hanging clients with configurable timeouts.

## Setup

## Installation

## Usage

## Running Tests

SGQueue includes tests to verify all features stated above:

```bash
make test
```

## TODO

-   Monitoring Integration: Export queue stats to Prometheus/Grafana.
