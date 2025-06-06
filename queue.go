package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

type Queue interface {
	// Produce adds a new message to the underlying queue. A non-nil error is returned if the idempotency key is duplicated.
	Produce(payload any, priority int, idempotencyKey string) error
	// Consume dequeues a message from the queue in order of their priority and time the record was created
	Consume(ctx context.Context, bufferSize int) (<-chan *SGMessage, error)
	// ConsumeBatch dequeues bulk messages with a batch size from the queue in order of their priority and time the record was created
	ConsumeBatch(int) ([]*SGMessage, error)
	// ConsumeFailed consumes all failed messages from the dead-letter queue
	ConsumeFailed() ([]*SGMessage, error)
}

type SGQueueConfig struct {
	Name              string
	DatabaseURL       string
	MessageMaxRetries int
	// the maximum time a message can remaining processed by a consumer in minutes
	VisibilityWindow time.Duration
	// the maximum time required to complete each given task
	ProcessingTimeout time.Duration
}

type SGQueue struct {
	name              string
	messageMaxRetries int
	visibilityWindow  time.Duration
	processingTimeout time.Duration
	db                *DB
}

// NewQueue initializes a new message queue. A non-nil error is returned if a problem occurred while connecting to the database
func NewQueue(cfg SGQueueConfig) (*SGQueue, error) {
	// connect to db
	db, err := NewDB(cfg.DatabaseURL, 16, cfg.Name)
	if err != nil {
		return nil, err
	}
	if cfg.VisibilityWindow == 0 {
		cfg.VisibilityWindow = DefaultVisibilityWindow
	}
	// validate processing timeout
	if cfg.ProcessingTimeout == 0 || cfg.ProcessingTimeout > cfg.VisibilityWindow {
		cfg.ProcessingTimeout = 3 * time.Minute
	}

	return &SGQueue{db: db, name: cfg.Name, messageMaxRetries: cfg.MessageMaxRetries, visibilityWindow: cfg.VisibilityWindow, processingTimeout: cfg.ProcessingTimeout}, nil
}

// Produce adds a new message to the underlying queue. A non-nil error is returned if the idempotency key is duplicated.
func (q *SGQueue) Produce(payload any, priority int, idempotencyKey string) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx := context.Background()
	// validate message
	msg := CreateMessage{QueueName: q.name, Payload: payloadBytes, Priority: priority, Status: MessagePending, IdempotencyKey: idempotencyKey}
	if q.messageMaxRetries > 0 {
		msg.MaxRetries = q.messageMaxRetries
	}

	// add message to db
	return q.db.AddMessage(ctx, msg)
}

// Consume dequeues a message from the queue in order of their priority and time the record was created
// Consume returns a channel that continuously receives messages from the queue.
// The caller must provide a context for cancellation and a buffer size for the channel.
// The channel will be closed when the context is cancelled or an error occurs.
// It is the caller's responsibility to call either the Ack method after a successful implementation or Nack after a failure occurs.
func (q *SGQueue) Consume(ctx context.Context, bufferSize int) (<-chan *SGMessage, error) {
	// default to 20 as buffer size
	if bufferSize < 1 {
		bufferSize = 20
	}

	messages := make(chan *SGMessage, bufferSize)

	// TODO: use listeners for signaling when a new message arrives
	// poll messages from the queue
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		// cleanup resources
		defer close(messages)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m, err := q.db.GetMessage(ctx)
				if err != nil {
					// skip if no message is available
					if err == sql.ErrNoRows {
						continue
					}
					log.Printf("error consuming message in channel: %v\n", err)
					return
				}
				// skip if no message was found
				if m == nil {
					continue
				}

				msg, err := q.composeMessage(m)
				// invalid message
				if err != nil {
					log.Printf("error composing retrieved message from storage: %v\n", err)
					return
				}
				// add message to channel
				messages <- msg
			}
		}
	}()
	return messages, nil
}

// ConsumeBatch dequeues bulk messages with a batch size from the queue in order of their priority and time the record was created
func (q *SGQueue) ConsumeBatch(batchSize int) ([]*SGMessage, error) {
	ctx := context.Background()

	msgs, err := q.db.GetBatchMessages(ctx, batchSize)
	if err != nil {
		return nil, err
	}

	// convert messages to appropriate output messages with standard methods
	messages := make([]*SGMessage, 0, len(msgs))
	for _, msg := range msgs {
		// convert message payload
		var payload any
		payloadBytes, ok := msg.Payload.([]byte)

		// skip message if payload is invalid
		if !ok {
			continue
		}

		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			log.Println("failed to unmarshal message payload:", err)
			continue
		}

		messages = append(messages, &SGMessage{Message: *msg, Payload: payload, queue: q})
	}

	// consume old message from db
	return messages, nil
}

// ConsumeFailed consumes all failed messages from the dead-letter queue
func (q *SGQueue) ConsumeFailed() ([]*SGMessage, error) {
	ctx := context.Background()

	// parse time window to sql interval
	interval := fmt.Sprintf("%v minutes", q.visibilityWindow.Minutes())

	// consume old message from db
	msgs, err := q.db.GetHangingMessages(ctx, interval)
	if err != nil {
		return nil, err
	}

	// convert messages to appropriate output messages with standard methods
	messages := make([]*SGMessage, 0, len(msgs))
	for _, msg := range msgs {
		// convert message payload
		var payload any
		payloadBytes, ok := msg.Payload.([]byte)

		// skip message if payload is invalid
		if !ok {
			continue
		}
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			continue
		}

		messages = append(messages, &SGMessage{Message: *msg, Payload: payload, queue: q})
	}

	// consume old message from db
	return messages, nil
}

// Shutdown gracefully shutdowns the queue and releases all resources
func (q *SGQueue) Shutdown(ctx context.Context) error {
	return q.db.Close()
}

// consumed message callbacks

// Ack acknowledges successful processing of a message
func (m *SGMessage) Ack() error {
	return m.queue.ack(m.Message.ID)
}

// Nack marks a message as failed and requeues it
func (m *SGMessage) Nack() error {
	return m.queue.nack(m.Message.ID)
}

// Retry increments the retry count and returns the message to the pending state
func (m *SGMessage) Retry() error {
	return m.queue.retry(m.Message.ID)
}

// helper methods

// composeMessage parses the message from the underlying storage into a broker-compatible message
func (q *SGQueue) composeMessage(msg *Message) (*SGMessage, error) {
	// decode json payload
	var payload any
	payloadBytes, ok := msg.Payload.([]byte)

	if !ok {
		return nil, fmt.Errorf("unexpected payload: %v,  expected []byte", msg.Payload)
	}

	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message payload: %v", err)
	}

	return &SGMessage{Message: *msg, Payload: payload, queue: q}, nil
}

func (q *SGQueue) ack(id int) error {
	ctx, cancel := context.WithTimeout(context.Background(), q.processingTimeout)
	defer cancel()

	return q.db.UpdateStatus(ctx, id, MessageCompleted)
}

func (q *SGQueue) nack(id int) error {
	ctx, cancel := context.WithTimeout(context.Background(), q.processingTimeout)
	defer cancel()

	return q.db.UpdateStatus(ctx, id, MessagePending)
}

func (q *SGQueue) retry(id int) error {
	ctx, cancel := context.WithTimeout(context.Background(), q.processingTimeout)
	defer cancel()

	return q.db.UpdateRetries(ctx, id, MessagePending)
}

// DecodeJSONPayload unmarshal the retrieved message into the struct provided
func (msg *SGMessage) DecodeJSONPayload(out any) error {
	// marshal payload into bytes and unmarshal it into input json struct
	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}
	if err := json.Unmarshal(payloadBytes, out); err != nil {
		return fmt.Errorf("failed to unmarshal into target JSON: %w", err)
	}
	return nil
}
