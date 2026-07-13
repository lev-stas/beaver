package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/lev-stas/beaver/internal/clickhouse"
	"github.com/lev-stas/beaver/internal/config"
	"github.com/lev-stas/beaver/internal/event"
	"github.com/lev-stas/beaver/internal/postgres"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EventWriter persists raw events for archival/analytics.
type EventWriter interface {
	WriteBatch(ctx context.Context, events []*event.RecentChange) error
}

// StateWriter upserts the latest derived state per event.
type StateWriter interface {
	UpsertBatch(ctx context.Context, events []*event.RecentChange) error
}

func Run(ctx context.Context, cfg *config.ConsumerConfig) error {
	chClient, err := clickhouse.New(context.Background(), cfg.ClickHouse.Address, cfg.ClickHouse.Database, cfg.ClickHouse.Table)
	if err != nil {
		return fmt.Errorf("connecting to clickhouse: %w", err)
	}
	defer chClient.Close()

	pgClient, err := postgres.New(context.Background(), cfg.Postgres.DSN, cfg.Postgres.Table)
	if err != nil {
		return fmt.Errorf("connecting to postgres: %w", err)
	}
	defer pgClient.Close()

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumeTopics(cfg.Kafka.Topic),
		kgo.ConsumerGroup(cfg.Consumer.GroupName),
		kgo.DisableAutoCommit(),
		// Blocks a group rebalance from reassigning our partitions until
		// AllowRebalance is called below, once the in-flight batch is
		// safely written and committed. Without this, a rebalance mid-batch
		// could hand our partitions to another member while we still
		// process and commit them, causing duplicate processing.
		kgo.BlockRebalanceOnPoll(),
	}

	kclient, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating kafka client: %w", err)
	}
	defer kclient.Close()

	for {
		fetches := kclient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					log.Println("Shutdown signal received, stopping consumer")
					return nil
				}
			}
			return fmt.Errorf("polling fetches: %v", errs)
		}

		records := fetches.Records()
		if len(records) == 0 {
			kclient.AllowRebalance()
			continue
		}

		events := make([]*event.RecentChange, 0, len(records))
		for _, record := range records {
			rc, err := event.Parse(record.Value)
			if err != nil {
				log.Printf("Skipping malformed event at %s[%d]@%d: %v", record.Topic, record.Partition, record.Offset, err)
				continue
			}
			events = append(events, rc)
		}

		if err := processAndCommit(kclient, chClient, pgClient, records, events); err != nil {
			return err
		}

		kclient.AllowRebalance()
	}
}

// writeTimeout bounds how long a batch write/commit can run so that a stuck
// (not merely erroring) ClickHouse or PostgreSQL connection cannot hang
// shutdown indefinitely. It is deliberately not tied to the shutdown signal
// context, so a signal mid-write doesn't abort an otherwise-healthy write.
const writeTimeout = 30 * time.Second

func processAndCommit(kclient *kgo.Client, ch EventWriter, pg StateWriter, records []*kgo.Record, events []*event.RecentChange) error {
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()

	if err := processBatchWithRetry(ctx, ch, pg, events); err != nil {
		return fmt.Errorf("processing batch: %w", err)
	}

	if err := kclient.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}

	return nil
}

const maxProcessAttempts = 3

// processBatchWithRetry writes to ClickHouse and PostgreSQL, retrying each
// step independently on transient failure. Once a step succeeds it is not
// repeated, so a PostgreSQL failure after a successful ClickHouse write does
// not re-insert the same rows into ClickHouse. Offsets are only committed by
// the caller once this returns without error, so a persistent failure here
// leaves the whole batch uncommitted and safe to reprocess after a restart.
func processBatchWithRetry(ctx context.Context, ch EventWriter, pg StateWriter, events []*event.RecentChange) error {
	if err := withRetry(func() error { return ch.WriteBatch(ctx, events) }); err != nil {
		return fmt.Errorf("writing to clickhouse after %d attempts: %w", maxProcessAttempts, err)
	}

	if err := withRetry(func() error { return pg.UpsertBatch(ctx, events) }); err != nil {
		return fmt.Errorf("upserting to postgres after %d attempts: %w", maxProcessAttempts, err)
	}

	return nil
}

func withRetry(fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxProcessAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}

		log.Printf("attempt %d/%d failed: %v", attempt, maxProcessAttempts, err)

		if attempt < maxProcessAttempts {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	return err
}
