package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// table based unit tests
func TestQueue(t *testing.T) {
	// setup db
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("sgqueue_test"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	// get connection string
	dbURL, err := postgresContainer.ConnectionString(ctx)
	require.NoError(t, err)

	// instantiate queue
	sgq, err := NewQueue(SGQueueConfig{Name: "test", DatabaseURL: dbURL, MessageMaxRetries: 2, VisibilityWindow: 5 * time.Second, ProcessingTimeout: 5 * time.Second})
	require.NoError(t, err)

	cases := map[string]func(*testing.T, *SGQueue){
		"test produce":       testProduce,
		"test consume":       testConsume,
		"test consume batch": testConsumeBatch,
		// TODO: add DLQ, visibility window, retry tests
		// "test consume failed": testConsumeFailed,
	}

	for name, fn := range cases {
		// run test case
		t.Run(name, func(t *testing.T) {
			fn(t, sgq)
		})
	}

	// cleanup
	err = sgq.Shutdown(context.Background())
	require.NoError(t, err)
}

func testProduce(t *testing.T, sgq *SGQueue) {
	// add a new message with unique key
	payload := "this is a test message from @mrshabel"
	priority := 0
	idempotencyKey := "new random key"
	err := sgq.Produce(payload, priority, idempotencyKey)
	require.NoError(t, err)

	// insert message with duplicate idempotency key
	err = sgq.Produce(payload, priority, idempotencyKey)
	require.Error(t, err)

	// update idempotency key and add message again
	idempotencyKey = "updated random queue"
	err = sgq.Produce(payload, priority, idempotencyKey)
	require.NoError(t, err)

	// consume produced messages (2)
	messages, err := sgq.Consume(context.Background(), 2)
	require.NoError(t, err)
	for range 2 {
		msg := <-messages
		require.NotNil(t, msg)
		// acknowledge message
		err := msg.Ack()
		require.NoError(t, err)
	}
}

func testConsume(t *testing.T, sgq *SGQueue) {
	t.Helper()
	// create consumer channel
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messages, err := sgq.Consume(ctx, 10)
	require.NoError(t, err)

	// produce test messages
	samplePayloads := []string{"message 1", "message 2", "message 3"}
	for i, payload := range samplePayloads {
		err = sgq.Produce(payload, i, fmt.Sprintf("key-%v", i))
		require.NoError(t, err)
	}

	// consume and verify messages
	consumed := 0
	for range len(samplePayloads) {
		select {
		case msg := <-messages:
			if msg == nil {
				continue
			}
			payload, ok := msg.Payload.(string)
			require.True(t, ok, "expected string output")
			require.Contains(t, samplePayloads, payload)

			// acknowledge message
			err := msg.Ack()
			require.NoError(t, err)
			consumed++

		case <-ctx.Done():
			t.Fatalf("timeout waiting for messages, consumed: %d, expected: %d", consumed, len(samplePayloads))
		}
	}

	// test retry message
	err = sgq.Produce("this is to test retry", 0, "retry-key")
	require.NoError(t, err)

	// consume message
	msg := <-messages
	require.NotNil(t, msg)
	err = msg.Retry()
	require.NoError(t, err)

	// verify that only one retry is present and ack message
	retryMsg := <-messages
	require.Equal(t, 1, retryMsg.Message.Retries)
	err = retryMsg.Ack()
	require.NoError(t, err)

	// test nack
	err = sgq.Produce("this is to test nack", 0, "nack-key")
	require.NoError(t, err)

	// consume message
	msg = <-messages
	require.NotNil(t, msg)
	err = msg.Nack()
	require.NoError(t, err)

	// verify that same message was retrieved
	nackMsg := <-messages
	require.NotNil(t, nackMsg)
	require.Equal(t, nackMsg.Message.ID, msg.Message.ID)
	err = nackMsg.Ack()
	require.NoError(t, err)
}

func testConsumeBatch(t *testing.T, sgq *SGQueue) {
	// produce 5 messages
	for i := range 5 {
		payload := map[string]interface{}{
			"message": fmt.Sprintf("batch message %d", i),
		}
		err := sgq.Produce(payload, i, fmt.Sprintf("random batch key %v", i))
		require.NoError(t, err)
	}

	// consume 3 messages in a single batch
	messages, err := sgq.ConsumeBatch(3)
	require.NoError(t, err)
	require.Len(t, messages, 3)

	for _, msg := range messages {
		require.NotNil(t, msg)
		payload, ok := msg.Payload.(map[string]interface{})
		if !ok {
			t.Fatalf("invalid message payload: %v\n", payload)
		}

		require.Contains(t, payload["message"], "batch message")

		// acknowledge message
		err := msg.Ack()
		require.NoError(t, err)
	}

	// attempt to consume 3 messages
	messages, err = sgq.ConsumeBatch(3)
	require.NoError(t, err)
	// expect only 2 messages. ie: 5 - 3 consumed = 2
	require.Len(t, messages, 2)

	for _, msg := range messages {
		require.NotNil(t, msg)
		payload, ok := msg.Payload.(map[string]interface{})
		if !ok {
			t.Fatalf("invalid message payload: %v\n", payload)
		}

		require.Contains(t, payload["message"], "batch message")

		// acknowledge message
		err := msg.Ack()
		require.NoError(t, err)
	}

	// ensure no messages are left
	messages, err = sgq.ConsumeBatch(1)
	require.NoError(t, err)
	require.Empty(t, messages)
}

// benchmarks
func BenchmarkQueue(b *testing.B) {
	// setup db
	ctx := context.Background()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("sgqueue_bench"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		b.Fatalf("failed to start container: %s\n", err)
	}

	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			b.Fatalf("failed to terminate container: %s\n", err)
		}
	}()

	// get connection string
	dbURL, err := postgresContainer.ConnectionString(ctx)
	require.NoError(b, err)

	// instantiate queue
	sgq, err := NewQueue(SGQueueConfig{Name: "test", DatabaseURL: dbURL, MessageMaxRetries: 2, VisibilityWindow: 5 * time.Second, ProcessingTimeout: 5 * time.Second})
	require.NoError(b, err)

	cases := map[string]func(*testing.B, *SGQueue){
		"bench produce":       benchmarkProduce,
		"bench consume":       benchmarkConsume,
		"bench consume batch": benchmarkConsumeBatch,
	}

	for name, fn := range cases {
		// run test case
		b.Run(name, func(b *testing.B) {
			fn(b, sgq)
		})
	}

	// cleanup
	err = sgq.Shutdown(context.Background())
	require.NoError(b, err)
}

func benchmarkProduce(b *testing.B, sgq *SGQueue) {
	payload := "benchmark test message"
	priority := 0

	for range b.N {
		err := sgq.Produce(payload, priority, uuid.NewString())
		if err != nil {
			b.Fatalf("failed to produce message: %v", err)
		}
	}
}

func benchmarkConsume(b *testing.B, sgq *SGQueue) {
	// seed messages
	for i := range b.N {
		payload := fmt.Sprintf("message-%d", i)
		err := sgq.Produce(payload, 0, uuid.NewString())
		if err != nil {
			b.Fatalf("failed to produce message: %v", err)
		}
	}

	b.ResetTimer()

	// consume messages up to benchmark size
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messages, err := sgq.Consume(ctx, b.N)
	if err != nil {
		b.Fatalf("failed to register consumer: %v", err)
	}

	consumed := 0
	for msg := range messages {
		if msg == nil {
			b.Fatalf("no message to consume")
		}
		err = msg.Ack()
		if err != nil {
			b.Fatalf("failed to acknowledge message: %v", err)
		}
		consumed++
		if consumed >= b.N {
			break
		}

	}
}

func benchmarkConsumeBatch(b *testing.B, sgq *SGQueue) {
	// seed messages
	for i := range b.N * 10 {
		payload := fmt.Sprintf("batch message %d", i)
		err := sgq.Produce(payload, 0, uuid.NewString())
		if err != nil {
			b.Fatalf("failed to produce message: %v", err)
		}
	}

	b.ResetTimer()

	for range b.N {
		// consume in batches of 10
		messages, err := sgq.ConsumeBatch(10)
		if err != nil {
			b.Fatalf("failed to consume batch: %v", err)
		}
		if len(messages) == 0 {
			b.Fatalf("no messages to consume in batch")
		}

		// acknowledge messages
		for _, msg := range messages {
			err := msg.Ack()
			if err != nil {
				b.Fatalf("failed to acknowledge message: %v", err)
			}
		}
	}
}
