package queue

import (
	"context"
	"database/sql"
	"errors"

	_ "github.com/jackc/pgx/v5/stdlib"
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
	queueName string
}

// New instantiates a new database connection and migrations
func NewDB(connStr string, maxConns int, queueName string) (*DB, error) {
	db, err := sql.Open("pgx", connStr)
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
			idempotency_key VARCHAR(255) UNIQUE,
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
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		);

		-- index for messages --
		CREATE INDEX IF NOT EXISTS idx_messages_queue_status_priority_created_at ON messages (queue_name, status, priority, created_at);
	`

	if _, err := db.Exec(migration_queries); err != nil {
		return nil, err
	}

	return &DB{db, queueName}, nil
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
		INSERT INTO messages (queue_name, payload, status, idempotency_key, priority, retries, max_retries)
		VALUES
		($1, $2, $3, $4, $5, $6, $7)
		-- RETURNING id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at --
	`

	if _, err := tx.ExecContext(ctx, query, &message.QueueName, &message.Payload, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries); err != nil {
		tx.Rollback()
		return err
	}

	// commit transaction
	return tx.Commit()
}

// GetMessage retrieves a single message from the pending messages of a named-queue in order of priority and time the message was created
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
			WHERE queue_name = $1 AND status = $2
			ORDER BY priority DESC, created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		-- update message status --
		UPDATE messages
		SET status = $3, started_at = NOW()
		FROM cte
		WHERE messages.id = cte.id
		RETURNING messages.id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at
	`

	var message Message
	var payload []byte

	if err := tx.QueryRowContext(ctx, query, &db.queueName, MessagePending, MessageInProgress).Scan(&message.ID, &message.QueueName, &payload, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
		// rollback transaction
		tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	// save payload as byte-slice
	message.Payload = payload

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
			WHERE queue_name = $1 AND status = $2
			ORDER BY priority DESC, created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		-- update message status --
		UPDATE messages
		SET status = $4, started_at = NOW()
		FROM cte
		WHERE messages.id = cte.id
		RETURNING messages.id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at
	`

	var messages []*Message
	rows, err := tx.QueryContext(ctx, query, db.queueName, MessagePending, batchSize, MessageInProgress)
	if err != nil {
		// rollback transaction
		tx.Rollback()
		return nil, err
	}

	for rows.Next() {
		var message Message
		var payload []byte

		if err := rows.Scan(&message.ID, &message.QueueName, &payload, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
			// rollback transaction
			tx.Rollback()
			return nil, err
		}

		message.Payload = payload
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

// UpdateStatus updates the status of given task to either completed or failed
func (db *DB) UpdateStatus(ctx context.Context, id int, status MessageStatus) error {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}

	query :=
		`
		UPDATE messages
		SET status = $1
		WHERE id = $2
		-- RETURNING id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at --
	`

	if _, err := tx.ExecContext(ctx, query, status, id); err != nil {
		tx.Rollback()
		return err
	}

	// commit transaction
	return tx.Commit()
}

// UpdateRetries updates the the status and retry count of a given task
func (db *DB) UpdateRetries(ctx context.Context, id int, status MessageStatus) error {
	// begin transaction. read only committed messages
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}

	query :=
		`
		UPDATE messages
		SET status = $1, retries = retries + 1
		WHERE id = $2 AND retries < max_retries
		-- RETURNING id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at --
	`

	result, err := tx.ExecContext(ctx, query, status, id)
	if err != nil {
		tx.Rollback()
		return err
	}

	// check if message was not updated as retry count was exceeded and move into dead letter queue
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return err
	}
	if rowsAffected == 0 {
		query := `
			INSERT INTO dead_letter_messages (queue_name, message_id, failure_reason)
			VALUES ($1, $2, $3);
		`
		if _, err := tx.ExecContext(ctx, query, db.queueName, id, "Max retries exceeded"); err != nil {
			tx.Rollback()
			return err
		}

		// set status to failed
		query = `UPDATE messages SET status = $1 WHERE id = $2;`
		if _, err := tx.ExecContext(ctx, query, MessageFailed, id); err != nil {
			tx.Rollback()
			return err
		}

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

	// message count
	// query :=
	// 	`
	// 	SELECT COUNT(*)
	// 	FROM messages
	// 	WHERE status = $1
	// 	AND started_at < NOW() INTERVAL $2
	// `

	// count := tx.ExecContext(ctx, query)

	// messages query
	query :=
		`
		SELECT id, queue_name, payload, status, idempotency_key, priority, retries, max_retries, created_at, started_at
		FROM messages
		WHERE status = $1 AND queue_name = $2
			AND started_at < NOW() - $3::INTERVAL
		ORDER BY priority DESC, created_at ASC
	`
	var messages []*Message
	rows, err := tx.QueryContext(ctx, query, MessagePending, db.queueName, interval)
	if err != nil {
		// rollback transaction
		tx.Rollback()
		return nil, err
	}

	for rows.Next() {
		var message Message
		var payload []byte

		if err := rows.Scan(&message.ID, &message.QueueName, &payload, &message.Status, &message.IdempotencyKey, &message.Priority, &message.Retries, &message.MaxRetries, &message.CreatedAt, &message.StartedAt); err != nil {
			// rollback transaction
			tx.Rollback()
			return nil, err
		}

		message.Payload = payload
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
