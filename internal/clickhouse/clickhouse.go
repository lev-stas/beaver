package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/lev-stas/beaver/internal/event"
)

// Client writes raw recentchange events into ClickHouse.
type Client struct {
	conn  driver.Conn
	table string
}

func New(ctx context.Context, address, database, table string) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{address},
		Auth: clickhouse.Auth{Database: database},
	})
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging clickhouse: %w", err)
	}

	c := &Client{conn: conn, table: table}

	if err := c.ensureTable(ctx); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Client) ensureTable(ctx context.Context) error {
	// ReplacingMergeTree collapses rows with an identical (wiki, ts, id) key
	// during merges, so a retried or redelivered insert of the same event
	// does not leave duplicate rows behind. Queries that need exact counts
	// before merges have run should use FINAL or an equivalent dedup.
	ddl := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id UInt64,
			type String,
			wiki String,
			title String,
			ts DateTime,
			raw String
		) ENGINE = ReplacingMergeTree
		ORDER BY (wiki, ts, id)
	`, c.table)

	if err := c.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("creating clickhouse table %q: %w", c.table, err)
	}

	return nil
}

// WriteBatch inserts events into the raw events table as a single batch insert.
func (c *Client) WriteBatch(ctx context.Context, events []*event.RecentChange) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, type, wiki, title, ts, raw)", c.table))
	if err != nil {
		return fmt.Errorf("preparing clickhouse batch: %w", err)
	}
	defer func() {
		if !batch.IsSent() {
			_ = batch.Abort()
		}
	}()

	for _, e := range events {
		if err := batch.Append(uint64(e.ID), e.Type, e.Wiki, e.Title, time.Unix(e.Timestamp, 0), string(e.Raw)); err != nil {
			return fmt.Errorf("appending to clickhouse batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending clickhouse batch: %w", err)
	}

	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}
