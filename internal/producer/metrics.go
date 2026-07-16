package producer

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the application-level (as opposed to process/resource)
// metrics for the producer: SSE intake, Kafka produce throughput/latency,
// and batching behavior.
type Metrics struct {
	eventsReceived prometheus.Counter
	eventsProduced prometheus.Counter
	produceErrors  prometheus.Counter
	bytesProduced  prometheus.Counter
	bufferedEvents prometheus.Gauge
	batchRecords   prometheus.Histogram
	flushDuration  prometheus.Histogram
	flushesTotal   *prometheus.CounterVec
}

func NewMetrics(reg *prometheus.Registry) *Metrics {
	m := &Metrics{
		eventsReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_producer_events_received_total",
			Help: "SSE events received from the Wikimedia recentchange stream.",
		}),
		eventsProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_producer_events_produced_total",
			Help: "Events successfully produced to Kafka.",
		}),
		produceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_producer_produce_errors_total",
			Help: "Batch produce calls to Kafka that returned an error.",
		}),
		bytesProduced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "beaver_producer_bytes_produced_total",
			Help: "Bytes of event payload successfully produced to Kafka.",
		}),
		bufferedEvents: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "beaver_producer_buffered_events",
			Help: "Events currently queued in the SSE-to-Kafka handoff channel (backpressure signal).",
		}),
		batchRecords: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "beaver_producer_batch_records",
			Help:    "Number of records in each batch sent to Kafka.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1..512
		}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "beaver_producer_flush_duration_seconds",
			Help:    "Time spent producing one batch to Kafka (ProduceSync).",
			Buckets: prometheus.DefBuckets,
		}),
		flushesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "beaver_producer_flushes_total",
			Help: "Batch flushes, labeled by what triggered them.",
		}, []string{"trigger"}),
	}

	reg.MustRegister(
		m.eventsReceived, m.eventsProduced, m.produceErrors, m.bytesProduced,
		m.bufferedEvents, m.batchRecords, m.flushDuration, m.flushesTotal,
	)

	return m
}
