package consumer

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the application-level (as opposed to process/resource)
// metrics for the consumer: Kafka intake, per-sink write throughput/latency
// and retries, and offset commits.
type Metrics struct {
	recordsPolled  prometheus.Counter
	bytesPolled    prometheus.Counter
	parseErrors    prometheus.Counter
	batchRecords   prometheus.Histogram
	writeDuration  *prometheus.HistogramVec
	eventsWritten  *prometheus.CounterVec
	writeRetries   *prometheus.CounterVec
	writeErrors    *prometheus.CounterVec
	commitDuration prometheus.Histogram
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		recordsPolled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_consumer_records_polled_total",
			Help: "Kafka records polled from the topic.",
		}),
		bytesPolled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_consumer_bytes_polled_total",
			Help: "Bytes of record value polled from Kafka.",
		}),
		parseErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_consumer_parse_errors_total",
			Help: "Records skipped for failing to parse as a recentchange event.",
		}),
		batchRecords: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "beaver_consumer_batch_records",
			Help:    "Number of records in each polled Kafka fetch batch.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1..512
		}),
		writeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "beaver_consumer_write_duration_seconds",
			Help:    "Time to write one batch to a sink, including retries.",
			Buckets: prometheus.DefBuckets,
		}, []string{"sink"}),
		eventsWritten: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "beaver_consumer_events_written_total",
			Help: "Events successfully written to a sink.",
		}, []string{"sink"}),
		writeRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "beaver_consumer_write_retries_total",
			Help: "Retry attempts issued after a sink write failed.",
		}, []string{"sink"}),
		writeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "beaver_consumer_write_errors_total",
			Help: "Batch writes to a sink that failed even after all retries.",
		}, []string{"sink"}),
		commitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "beaver_consumer_commit_duration_seconds",
			Help:    "Time to commit Kafka offsets for one batch.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	reg.MustRegister(
		m.recordsPolled, m.bytesPolled, m.parseErrors, m.batchRecords,
		m.writeDuration, m.eventsWritten, m.writeRetries, m.writeErrors,
		m.commitDuration,
	)

	return m
}
