package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	queue "github.com/mrshabel/sgqueue"
)

func main() {
	// load configs
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, using environment variables")
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost/sgqueue"
		log.Println("no DATABASE_URL found, using default:", dbURL)
	}
	queueName := os.Getenv("QUEUE_NAME")
	if queueName == "" {
		queueName = "default"
		log.Println("no QUEUE_NAME found, using default:", dbURL)
	}

	// set up context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// initialize queue
	sgq, err := queue.NewQueue(queue.SGQueueConfig{
		Name:              queueName,
		DatabaseURL:       dbURL,
		MessageMaxRetries: 5,
		VisibilityWindow:  5 * time.Minute,
		ProcessingTimeout: 3 * time.Minute,
	})
	if err != nil {
		log.Fatalf("queue initialization error: %v", err)
	}

	// handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("shutdown signal received. Stopping all operations now...")
		cancel()

		if err := sgq.Shutdown(ctx); err != nil {
			log.Printf("error shutting down queue: %v", err)
		}
		os.Exit(0)
	}()

	// cleanup function
	defer func() {
		if err := sgq.Shutdown(ctx); err != nil {
			log.Printf("error shutting down queue: %v", err)
		}
	}()

	// example 1: basic message production and consumption

	// run producer in the background
	go produceMessages(ctx, sgq, 1*time.Second)

	// consume messages
	messages, err := sgq.Consume(ctx, 100)
	if err != nil {
		log.Fatalf("failed to register consumer: %v\n", err)
	}
	for msg := range messages {
		processMessage(msg)
	}

	log.Println("examples completed successfully")
}

// // demoBasicUsage demonstrates basic produce/consume operations
// func demoBasicUsage(sgq *queue.SGQueue) error {
// 	log.Println("------ Basic Usage Demo ------")

// 	// create message
// 	msg := map[string]interface{}{
// 		"greeting":  "Hello, World!",
// 		"timestamp": time.Now().Format(time.RFC3339),
// 	}

// 	// generate unique idempotency key
// 	idempotencyKey := uuid.New().String()

// 	log.Printf("Producing message with key: %s", idempotencyKey)
// 	if err := sgq.Produce(msg, 0, idempotencyKey); err != nil {
// 		return fmt.Errorf("failed to produce message: %w", err)
// 	}

// 	// consume the message
// 	log.Println("Consuming message...")
// 	cmsg, err := sgq.Consume()
// 	if err != nil {
// 		return fmt.Errorf("failed to consume message: %w", err)
// 	}

// 	// process the message
// 	payload, ok := cmsg.Payload.(map[string]interface{})
// 	if !ok {
// 		// mark as failed
// 		cmsg.Nack()
// 		return fmt.Errorf("invalid payload type: expected map[string]interface{}, got %T", cmsg.Payload)
// 	}

// 	// display message content
// 	greeting, _ := payload["greeting"].(string)
// 	log.Printf("Received message: %s", greeting)

// 	// acknowledge the message
// 	if err := cmsg.Ack(); err != nil {
// 		return fmt.Errorf("failed to acknowledge message: %w", err)
// 	}
// 	log.Println("message processed successfully")

// 	return nil
// }

// // demoBatchProcessing demonstrates batch processing
// func demoBatchProcessing(sgq *queue.SGQueue) error {
// 	log.Println("------ Batch Processing Demo ------")

// 	// produce multiple messages
// 	for i := range 5 {
// 		msg := map[string]interface{}{
// 			"index":     i,
// 			"message":   fmt.Sprintf("batch message %d", i),
// 			"timestamp": time.Now().Format(time.RFC3339),
// 		}

// 		idempotencyKey := fmt.Sprintf("batch-%s-%d", uuid.New().String(), i)

// 		// dynamic priorities
// 		if err := sgq.Produce(msg, i, idempotencyKey); err != nil {
// 			return fmt.Errorf("failed to produce batch message %d: %w", i, err)
// 		}
// 	}

// 	// consume messages in batch
// 	log.Println("consuming batch messages...")
// 	messages, err := sgq.ConsumeBatch(5)
// 	if err != nil {
// 		return fmt.Errorf("failed to consume batch messages: %w", err)
// 	}

// 	log.Printf("received %d messages in batch", len(messages))

// 	// Process batch messages
// 	for i, msg := range messages {
// 		payload, ok := msg.Payload.(map[string]interface{})
// 		if !ok {
// 			msg.Nack()
// 			log.Printf("invalid payload type for message %d", i)
// 			continue
// 		}

// 		// Display message info
// 		index, _ := payload["index"].(float64)
// 		message, _ := payload["message"].(string)
// 		log.Printf("batch message %d: %s (Priority: %d)", int(index), message, msg.Message.Priority)

// 		// Acknowledge message
// 		if err := msg.Ack(); err != nil {
// 			log.Printf("failed to acknowledge message %d: %v", i, err)
// 		}
// 	}

// 	return nil
// }

// // demoErrorHandling demonstrates error handling and message retries
// func demoErrorHandling(sgq *queue.SGQueue) error {
// 	log.Println("------ Error Handling Demo ------")

// 	// create problematic message
// 	msg := map[string]interface{}{
// 		"type":       "problematic",
// 		"shouldFail": true,
// 		"timestamp":  time.Now().Format(time.RFC3339),
// 	}

// 	idempotencyKey := "error-" + uuid.New().String()
// 	if err := sgq.Produce(msg, 10, idempotencyKey); err != nil {
// 		return fmt.Errorf("failed to produce error test message: %w", err)
// 	}

// 	// consume the message
// 	cmsg, err := sgq.Consume()
// 	if err != nil {
// 		return fmt.Errorf("failed to consume message: %w", err)
// 	}

// 	payload, ok := cmsg.Payload.(map[string]interface{})
// 	if !ok {
// 		// send nack
// 		cmsg.Nack()
// 		return fmt.Errorf("invalid payload type")
// 	}

// 	// Check if this is our "problematic" message
// 	msgType, _ := payload["type"].(string)
// 	shouldFail, _ := payload["shouldFail"].(bool)

// 	if msgType == "problematic" && shouldFail {
// 		log.Println("Demonstrating retry for problematic message")

// 		// Retry the message
// 		if err := cmsg.Retry(); err != nil {
// 			return fmt.Errorf("failed to retry message: %w", err)
// 		}
// 		log.Println("message marked for retry")

// 		// Consume it again
// 		log.Println("attempting to consume the retried message...")
// 		retriedMsg, err := sgq.Consume()
// 		if err != nil {
// 			return fmt.Errorf("failed to consume retried message: %w", err)
// 		}

// 		// This time the message will be processed successfully
// 		log.Println("processing previously retried message")
// 		if err := retriedMsg.Ack(); err != nil {
// 			return fmt.Errorf("failed to acknowledge retried message: %w", err)
// 		}
// 		log.Println("retried message processed successfully")
// 	}

// 	// demonstrate failed message handling
// 	log.Println("Demonstrating failed message handling...")
// 	if err := sgq.Produce(map[string]interface{}{"willFail": true}, 5, "will-fail-"+uuid.New().String()); err != nil {
// 		return fmt.Errorf("failed to produce message: %w", err)
// 	}

// 	failedMsg, err := sgq.Consume()
// 	if err != nil {
// 		return fmt.Errorf("failed to consume message: %w", err)
// 	}

// 	// mark as failed
// 	log.Println("marking message as failed")
// 	if err := failedMsg.Nack(); err != nil {
// 		return fmt.Errorf("failed to nack message: %w", err)
// 	}

// 	// Check failed queue
// 	log.Println("checking failed messages queue...")
// 	failedMsgs, err := sgq.ConsumeFailed()
// 	if err != nil {
// 		return fmt.Errorf("failed to consume failed messages: %w", err)
// 	}

// 	log.Printf("Found %d failed messages", len(failedMsgs))

// 	return nil
// }
