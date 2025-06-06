package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
	queue "github.com/mrshabel/sgqueue"
)

// Order represents a sample order message
type Order struct {
	ID        string    `json:"id"`
	Items     []string  `json:"items"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}

// produceMessages simulates creating Order messages in the specified interval. The process is stopped once the context is cancelled
func produceMessages(ctx context.Context, q *queue.SGQueue, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			order := Order{
				ID:        uuid.New().String(),
				Items:     []string{},
				Amount:    float64(100 * rand.Intn(10)),
				CreatedAt: time.Now().UTC(),
			}
			idempotencyKey := "order-" + order.ID

			if err := q.Produce(order, 0, idempotencyKey); err != nil {
				// skip producing when an error occurs
				log.Printf("failed to produce message: %v\n", err)
				continue
			}
			log.Printf("Produced order: %v", order.ID)
		}
	}
}

func processMessage(msg *queue.SGMessage) {
	var order Order
	if err := msg.DecodeJSONPayload(&order); err != nil {
		log.Printf("Failed to decode message: %v", err)
		msg.Nack()
		return
	}

	log.Printf("Processing order: %s, amount: %.2f", order.ID, order.Amount)

	// simulate processing time
	time.Sleep(100 * time.Millisecond)

	// acknowledge message
	if err := msg.Ack(); err != nil {
		log.Printf("Failed to acknowledge message: %v", err)
	}
}
