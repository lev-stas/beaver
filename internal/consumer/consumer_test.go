package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/lev-stas/beaver/internal/event"
	"github.com/lev-stas/beaver/internal/metrics"
)

func testMetrics() *Metrics {
	return NewMetrics(metrics.NewRegistry())
}

type fakeCH struct {
	failUntil int
	calls     int
}

func (f *fakeCH) WriteBatch(ctx context.Context, events []*event.RecentChange) error {
	f.calls++
	if f.calls <= f.failUntil {
		return errors.New("clickhouse unavailable")
	}
	return nil
}

type fakePG struct {
	failUntil int
	calls     int
}

func (f *fakePG) UpsertBatch(ctx context.Context, events []*event.RecentChange) error {
	f.calls++
	if f.calls <= f.failUntil {
		return errors.New("postgres unavailable")
	}
	return nil
}

func TestProcessBatchWithRetry_Success(t *testing.T) {
	ch := &fakeCH{}
	pg := &fakePG{}
	events := []*event.RecentChange{{Title: "Foo"}}

	if err := processBatchWithRetry(context.Background(), ch, pg, events, testMetrics()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.calls != 1 {
		t.Errorf("clickhouse WriteBatch calls = %d, want 1", ch.calls)
	}
	if pg.calls != 1 {
		t.Errorf("postgres UpsertBatch calls = %d, want 1", pg.calls)
	}
}

func TestProcessBatchWithRetry_ClickHouseRecovers(t *testing.T) {
	ch := &fakeCH{failUntil: 1}
	pg := &fakePG{}
	events := []*event.RecentChange{{Title: "Foo"}}

	if err := processBatchWithRetry(context.Background(), ch, pg, events, testMetrics()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.calls != 2 {
		t.Errorf("clickhouse WriteBatch calls = %d, want 2", ch.calls)
	}
	if pg.calls != 1 {
		t.Errorf("postgres UpsertBatch calls = %d, want 1 (only reached once clickhouse succeeded)", pg.calls)
	}
}

func TestProcessBatchWithRetry_ClickHouseExhaustsRetries(t *testing.T) {
	ch := &fakeCH{failUntil: maxProcessAttempts}
	pg := &fakePG{}
	events := []*event.RecentChange{{Title: "Foo"}}

	if err := processBatchWithRetry(context.Background(), ch, pg, events, testMetrics()); err == nil {
		t.Fatal("expected error, got nil")
	}
	if ch.calls != maxProcessAttempts {
		t.Errorf("clickhouse WriteBatch calls = %d, want %d", ch.calls, maxProcessAttempts)
	}
	if pg.calls != 0 {
		t.Errorf("postgres UpsertBatch calls = %d, want 0 (clickhouse never succeeded)", pg.calls)
	}
}

func TestProcessBatchWithRetry_PostgresFailureDoesNotReinsertClickHouse(t *testing.T) {
	ch := &fakeCH{}
	pg := &fakePG{failUntil: 1}
	events := []*event.RecentChange{{Title: "Foo"}}

	if err := processBatchWithRetry(context.Background(), ch, pg, events, testMetrics()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.calls != 1 {
		t.Errorf("clickhouse WriteBatch calls = %d, want 1 (must not retry after it already succeeded)", ch.calls)
	}
	if pg.calls != 2 {
		t.Errorf("postgres UpsertBatch calls = %d, want 2", pg.calls)
	}
}
