package queue

import (
	"database/sql"
	"encoding/json"
	"time"
)

type Message struct {
	ID             int
	QueueName      string
	Payload        any
	Status         MessageStatus
	IdempotencyKey string
	Priority       int
	Retries        int
	MaxRetries     int
	CreatedAt      time.Time
	StartedAt      sql.NullTime
}

type CreateMessage struct {
	QueueName      string
	Payload        json.RawMessage
	Status         MessageStatus
	IdempotencyKey string
	Priority       int
	Retries        int
	MaxRetries     int
}

type MessageStatus string

// SGMessage is returned after a given message is consumed from the queue. The caller is supposed to manually send an acknowledgement by calling the ACK() method
type SGMessage struct {
	Message Message
	Payload any
	// internal queue for ack and nack operations
	queue *SGQueue
}
