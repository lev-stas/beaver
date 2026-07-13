package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lev-stas/beaver/internal/event"
)

// Client upserts the latest known state per wiki page into PostgreSQL.
//
// The producer does not key Kafka records by page, so records for the same
// page are not guaranteed to arrive in event order. UpsertBatch therefore
// applies a plain last-write-wins upsert rather than guarding on timestamps.
// edit_count only increments when the incoming event's id differs from the
// row's latest_event_id, so retrying or redelivering the same event (a
// consumer restart before offsets were committed, for example) does not
// inflate the counter.
type Client struct {
	pool  *pgxpool.Pool
	table string
}

func New(ctx context.Context, dsn, table string) (*Client, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("opening postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging postgres: %w", err)
	}

	c := &Client{pool: pool, table: table}

	if err := c.ensureTable(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) ensureTable(ctx context.Context) error {
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			wiki TEXT NOT NULL,
			namespace INTEGER NOT NULL,
			title TEXT NOT NULL,
			latest_event_id BIGINT,
			latest_revision BIGINT,
			latest_user TEXT,
			latest_comment TEXT,
			latest_timestamp TIMESTAMPTZ NOT NULL,
			edit_count BIGINT NOT NULL DEFAULT 1,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (wiki, namespace, title)
		)
	`, c.table)

	if _, err := c.pool.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("creating postgres table %q: %w", c.table, err)
	}

	return nil
}

// UpsertBatch applies each event's page state in a single transaction.
func (c *Client) UpsertBatch(ctx context.Context, events []*event.RecentChange) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning postgres transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := fmt.Sprintf(`
		INSERT INTO %[1]s (wiki, namespace, title, latest_event_id, latest_revision, latest_user, latest_comment, latest_timestamp, edit_count, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1, now())
		ON CONFLICT (wiki, namespace, title) DO UPDATE SET
			latest_event_id = EXCLUDED.latest_event_id,
			latest_revision = EXCLUDED.latest_revision,
			latest_user = EXCLUDED.latest_user,
			latest_comment = EXCLUDED.latest_comment,
			latest_timestamp = EXCLUDED.latest_timestamp,
			edit_count = CASE
				WHEN %[1]s.latest_event_id IS DISTINCT FROM EXCLUDED.latest_event_id
				THEN %[1]s.edit_count + 1
				ELSE %[1]s.edit_count
			END,
			updated_at = now()
	`, c.table)

	for _, e := range events {
		if _, err := tx.Exec(ctx, query, e.Wiki, e.Namespace, e.Title, e.ID, e.Revision.New, e.User, e.Comment, time.Unix(e.Timestamp, 0)); err != nil {
			return fmt.Errorf("upserting page state: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing postgres transaction: %w", err)
	}

	return nil
}

func (c *Client) Close() {
	c.pool.Close()
}
