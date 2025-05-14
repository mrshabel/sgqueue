package main

type Queue interface {
	// Produce adds a new message to the underlying queue. A non-nil error is returned if the idempotency key is duplicated.
	Produce(CreateMessage) error
	// Consume dequeues a message from the queue in order of their priority and time the record was created
	Consume() (*Message, error)
	// ConsumeBatch dequeues bulk messages with a batch size from the queue in order of their priority and time the record was created
	ConsumeBatch(int) ([]*Message, error)
	// ConsumeFailed consumes all failed messages from the dead-letter queue
	ConsumeFailed() ([]*Message, error)
}

type SGQueue struct {
	Name string
}
