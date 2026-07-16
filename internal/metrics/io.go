package metrics

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// ioCollector exposes process-level disk I/O byte counters from
// /proc/self/io. Linux has no meaningful per-process "iowait" counter
// (iowait is a system-wide CPU state, not attributable to one process
// without kernel delay accounting, which isn't reliably available in
// containers); bytes actually read from / written to storage is the
// practical per-process proxy for "is this app I/O heavy". On platforms
// without /proc (e.g. running `go run` on macOS), Collect just emits
// nothing rather than failing.
type ioCollector struct {
	readBytes  *prometheus.Desc
	writeBytes *prometheus.Desc
}

func newIOCollector() *ioCollector {
	return &ioCollector{
		readBytes: prometheus.NewDesc(
			"beaver_process_io_read_bytes_total",
			"Bytes actually read from storage by this process (/proc/self/io read_bytes).",
			nil, nil,
		),
		writeBytes: prometheus.NewDesc(
			"beaver_process_io_write_bytes_total",
			"Bytes actually written to storage by this process (/proc/self/io write_bytes).",
			nil, nil,
		),
	}
}

func (c *ioCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.readBytes
	ch <- c.writeBytes
}

func (c *ioCollector) Collect(ch chan<- prometheus.Metric) {
	f, err := os.Open("/proc/self/io")
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}

		v, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil {
			continue
		}

		switch strings.TrimSpace(key) {
		case "read_bytes":
			ch <- prometheus.MustNewConstMetric(c.readBytes, prometheus.CounterValue, v)
		case "write_bytes":
			ch <- prometheus.MustNewConstMetric(c.writeBytes, prometheus.CounterValue, v)
		}
	}
}
