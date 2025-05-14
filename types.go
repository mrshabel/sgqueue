package main

import "time"

type Message struct {
	ID             int
	QueueName      string
	Status         MessageStatus
	IdempotencyKey string
	Priority       int
	Retries        int
	MaxRetries     int
	CreatedAt      time.Time
	StartedAt      time.Time
}

type CreateMessage struct {
	QueueName      string
	Status         MessageStatus
	IdempotencyKey string
	Priority       int
	Retries        int
	MaxRetries     int
}

type MessageStatus string
