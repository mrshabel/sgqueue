package main

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v5"
)

const (
	MessagePending    = "pending"
	MessageInProgress = "in_progress"
	MessageCompleted  = "completed"
	MessageFailed     = "failed"
)

// DB is a representation of the underlying database
type DB struct {
	*sql.DB
}

// New instantiates a new database connection and migrations
func New(connStr string, maxConns int) (*DB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// verify that database connection is still alive
	if err = db.Ping(); err != nil {
		return nil, err
	}

	// connection pool setup
	db.SetMaxOpenConns(maxConns)

	// migrations setup
	migration_queries := `
		-- messages--
		CREATE TABLE IF NOT EXISTS messages (
			id SERIAL PRIMARY KEY,
			queue_name VARCHAR(255) DEFAULT 'sgqueue',
			payload JSONB NOT NULL,
			status VARCHAR(50) DEFAULT 'pending',
			idempotency_key UNIQUE VARCHAR(255),
			priority INTEGER DEFAULT 0,
			retries INTEGER DEFAULT 0,
			max_retries INTEGER DEFAULT 5,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			started_at TIMESTAMPTZ
		);

		-- dead_letter_messages --
		CREATE TABLE IF NOT EXISTS dead_letter_messages (
			id SERIAL PRIMARY KEY,
			queue_name VARCHAR(255) NOT NULL,
			message_id INTEGER REFERENCES messages(id),
			failure_reason VARCHAR(1000),
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		);

		-- index for messages --
		CREATE INDEX IF NOT EXISTS idx_messages_status_priority_created_at ON messages (status, priority, created_at)
	`

	if _, err := db.Exec(migration_queries); err != nil {
		return nil, err
	}

	return &DB{db}, nil
}

// AddMessage adds a new message into the messages table
func (db *DB) AddMessage(ctx context.Context, message CreateMessage) error {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}

	query :=
		`
		INSERT INTO messages (queue_name, status, idempotency_key, priority, retries, max_retries)
		VALUES
		($1, $2, $3, $4, $5, $6)
		-- RETURNING id, queue_name, status, idempotency_key, priority, retries, max_retries, created_at, started_at --
	`

	if _, err := tx.ExecContext(ctx, query, &message.QueueName, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries); err != nil {
		tx.Rollback()
		return err
	}

	// commit transaction
	return tx.Commit()
}

// GetMessage retrieves a single message from the pending messages in order of priority and time the message was created
func (db *DB) GetMessage(ctx context.Context) (*Message, error) {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	// skip locked messages to ensure that only one worker can process a given message
	query :=
		`
		-- query for pending messages while locking row --
		WITH cte AS (
			SELECT id
			FROM messages
			ORDER BY priority, created_at DESC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		-- update message status --
		UPDATE tasks
		SET status = $1, started_at = NOW()
		FROM cte
		WHERE messages.id = cte.id
		RETURNING id, queue_name, status, idempotency_key, priority, retries, max_retries, created_at, started_at
	`

	var message Message
	if err := tx.QueryRowContext(ctx, query, MessageInProgress).Scan(&message.ID, &message.QueueName, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
		// rollback transaction
		tx.Rollback()
		return nil, err
	}

	// commit transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &message, nil
}

// GetBatchMessages retrieves a batch of messages from the pending messages in order of priority and time the message was created
func (db *DB) GetBatchMessages(ctx context.Context, batchSize int) ([]*Message, error) {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	// skip locked messages to ensure that only one worker can process the message
	query :=
		`
		-- query for pending messages while locking row --
		WITH cte AS (
			SELECT id
			FROM messages
			ORDER BY priority, created_at DESC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		-- update message status --
		UPDATE tasks
		SET status = $2, started_at = NOW()
		FROM cte
		WHERE messages.id = cte.id
		RETURNING id, queue_name, status, idempotency_key, priority, retries, max_retries, created_at, started_at
	`

	var messages []*Message
	rows, err := tx.QueryContext(ctx, query, batchSize, MessageInProgress)
	if err != nil {
		// rollback transaction
		tx.Rollback()
		return nil, err
	}

	for rows.Next() {
		var message Message

		if err := rows.Scan(&message.ID, &message.QueueName, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
			// rollback transaction
			tx.Rollback()
			return nil, err
		}

		messages = append(messages, &message)
	}

	// rollback incase an error was returned in the iteration
	if rows.Err() != nil {
		tx.Rollback()
		return nil, rows.Err()
	}

	// commit transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return messages, nil
}

// MarkAsComplete updates the given task as completed
func (db *DB) MarkAsComplete(ctx context.Context, id int) error {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}

	query :=
		`
		UPDATE tasks
		SET status = $1
		WHERE id = $2
		-- RETURNING id, queue_name, status, idempotency_key, priority, retries, max_retries, created_at, started_at --
	`

	if _, err := tx.ExecContext(ctx, query, MessageCompleted, id); err != nil {
		tx.Rollback()
		return err
	}

	// commit transaction
	return tx.Commit()
}

// GetHangingMessages retrieves all hanging messages within a given interval
func (db *DB) GetHangingMessages(ctx context.Context, interval string) ([]*Message, error) {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}

	// skip locked messages to ensure that only one worker can process the message
	query :=
		`
		SELECT id, queue_name, status, idempotency_key, priority, retries, max_retries, created_at, started_at
		FROM messages
		WHERE status = $1
		AND started_at < NOW() INTERVAL $2
	`

	var messages []*Message
	rows, err := tx.QueryContext(ctx, query, MessagePending, interval)
	if err != nil {
		// rollback transaction
		tx.Rollback()
		return nil, err
	}

	for rows.Next() {
		var message Message

		if err := rows.Scan(&message.ID, &message.QueueName, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
			// rollback transaction
			tx.Rollback()
			return nil, err
		}

		messages = append(messages, &message)
	}

	// rollback incase an error was returned in the iteration
	if rows.Err() != nil {
		tx.Rollback()
		return nil, rows.Err()
	}

	// commit transaction
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return messages, nil
}
