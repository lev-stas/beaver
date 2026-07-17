# STEP 01: Observability from the Start

STEP 01 starts with the working single-node pipeline from STEP 00. The producer
and consumer now expose Prometheus-format application and process metrics, but
there is no monitoring system collecting them and no visibility into Kafka,
PostgreSQL, or ClickHouse.

The purpose of this STEP is to make monitoring part of the infrastructure from
the beginning rather than an improvement postponed until after the first
incident.

## Initial State

The stand contains:

- one Go producer reading the Wikimedia SSE stream;
- one Kafka broker/controller with three topic partitions;
- one Go consumer writing each valid event to both databases;
- one ClickHouse server storing raw events;
- one PostgreSQL server storing current page state.

Both applications expose `/metrics` on port `9100` inside the Compose network.
The endpoints are available to monitoring services by the `producer:9100` and
`consumer:9100` addresses and are not published on the Docker host.

The application metrics already describe:

- producer input and Kafka output throughput;
- producer batching, buffering, errors, and flush latency;
- consumer Kafka input throughput and parse errors;
- consumer writes, retries, errors, and latency for each database;
- Go runtime and process resource usage.

These endpoints are useful, but inspecting them manually with `curl` provides
only a momentary snapshot. There is no historical data, correlation, dashboard,
or infrastructure-level context.

## Problems to Address

Without monitoring, an operator sees only the final symptom of a problem. A
slow consumer, for example, may be caused by Kafka lag, PostgreSQL pressure,
ClickHouse writes, application retries, or host resource exhaustion. Logs can
help after the fact, but they do not show trends or provide a baseline for
comparison.

This becomes more important as the stand evolves. Every later topology change,
load test, failure, or recovery should be observable from the moment it starts.
Otherwise there is no reliable way to compare behavior before, during, and
after an experiment.

## Goals

In this STEP, try to achieve the following outcomes:

- expose Kafka topic, partition, offset, and consumer-group metrics;
- expose PostgreSQL activity and database metrics;
- enable the native Prometheus endpoint provided by ClickHouse;
- collect infrastructure and application metrics in Prometheus;
- keep monitoring data across normal container restarts;
- connect Grafana to Prometheus;
- build a small dashboard covering availability, throughput, lag, errors,
  latency, and basic database activity;
- establish a normal-operation baseline before introducing failures.

The first dashboard should remain intentionally small. The objective is not to
visualize every exported series, but to choose a few signals that answer basic
operational questions.

## Questions the Dashboard Should Answer

- Are all monitoring targets reachable?
- Is the producer receiving and publishing events?
- Is the consumer keeping up with Kafka?
- Is consumer lag growing or recovering?
- Are writes to PostgreSQL or ClickHouse failing or slowing down?
- How many PostgreSQL connections are active?
- Is ClickHouse processing queries, merges, or inserted rows?
- Did application CPU or memory usage change during an experiment?

## Definition of Done

STEP 01 is complete when:

- a Kafka exporter is running and exposes Kafka and consumer-group metrics;
- a PostgreSQL exporter is running and connects to the BEAVER database;
- the ClickHouse native Prometheus endpoint is enabled;
- Prometheus is running with persistent storage;
- Prometheus has healthy targets for Kafka, PostgreSQL, ClickHouse, producer,
  consumer, and Prometheus itself;
- Grafana is running with Prometheus configured as a data source;
- a BEAVER dashboard contains a focused set of basic infrastructure and
  application graphs;
- live events produce visible changes in throughput, lag, database, and process
  metrics;
- the monitoring stack starts together with the stand instead of being added
  manually only when troubleshooting begins.

## Why This STEP Matters

This STEP creates the feedback loop required by every later STEP. When a broker
is stopped, a consumer is scaled, a database is constrained, or a batch setting
is changed, the effect should be visible as a time series rather than inferred
from a few log lines.

The resulting monitoring is deliberately basic. Kafka JVM internals, advanced
PostgreSQL collectors, ClickHouse query analysis, alerting, and long-term metric
retention can be introduced when an experiment needs them. Starting with a
small useful dashboard is better than delaying all observability while trying
to design a complete one.

One possible implementation is provided in [SOLUTION.md](SOLUTION.md).
