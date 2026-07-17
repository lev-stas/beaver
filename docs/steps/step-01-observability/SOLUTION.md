# STEP 01 Solution: Build the Monitoring Stack

This document presents one possible monitoring setup for STEP 01. It favors a
small, understandable configuration over complete production coverage. Image
versions, exporters, dashboards, and queries can all be replaced as part of
your own experiments.

The resulting data flow is:

```text
producer /metrics -----------+
consumer /metrics -----------+
kafka-exporter /metrics -----+
postgres-exporter /metrics --+--> Prometheus --> Grafana
ClickHouse /metrics ---------+
Prometheus /metrics ---------+
```

Run all commands from the repository root unless a step explicitly changes the
working directory.

## 1. Prepare the STEP 01 Configuration

After the STEP 01 release has been published, check it out and create a branch
for experiments:

```bash
git fetch --tags
git switch -c my-step-01-observability step-01
```

Create local configuration files when starting from a clean checkout:

```bash
cp cmd/producer/config.example.yml cmd/producer/config.yml
cp cmd/consumer/config.example.yml cmd/consumer/config.yml
cp deploy/.env.example deploy/.env
```

If the local `config.yml` files were retained from STEP 00, add the following
section to both files:

```yaml
metrics:
  listen-address: ":9100"
```

The applications expose metrics inside their containers on port `9100`.
Prometheus reaches them by service name over the Compose network, so those
ports do not need to be published on the Docker host.

Add dedicated PostgreSQL monitoring credentials to `deploy/.env`. Choose a
different password for any stand that is reachable by other users:

```dotenv
POSTGRES_EXPORTER_USER=beaver_monitor
POSTGRES_EXPORTER_PASSWORD=beaver_monitor
```

The `.env` file is ignored by Git. Do not put the monitoring password in the
version-controlled `.env.example` file unless it is an explicitly documented
throwaway value.

## 2. Create the PostgreSQL Monitoring Role

Start PostgreSQL before creating the exporter role:

```bash
docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d postgres
```

Open a PostgreSQL session as the administrative user created during STEP 00:

```bash
docker compose --env-file deploy/.env -f deploy/docker-compose.yml exec postgres \
  sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Create the role with the same password that was placed in `deploy/.env`:

```sql
CREATE ROLE beaver_monitor
    WITH LOGIN
    PASSWORD 'beaver_monitor';

GRANT CONNECT ON DATABASE beaver TO beaver_monitor;
GRANT pg_monitor TO beaver_monitor;
```

Verify the membership and leave `psql`:

```sql
SELECT pg_has_role('beaver_monitor', 'pg_monitor', 'member');
\du beaver_monitor
\q
```

`pg_monitor` is a predefined PostgreSQL role intended for monitoring systems.
It allows the exporter to read server statistics and monitoring views without
making it a superuser. Do not grant this role `INSERT`, `UPDATE`, `DELETE`, or
ownership privileges on the application table `page_state`.

For a larger shared PostgreSQL installation, `pg_monitor` may still expose more
cluster-wide information than desired. In that case, replace it with explicit
grants for only the views and functions used by enabled exporter collectors.
The predefined role is a reasonable minimum for the default exporter setup in
this isolated stand.

## 3. Enable ClickHouse Metrics

ClickHouse can expose Prometheus metrics itself, so a separate exporter is not
required for the initial dashboard.

Create `deploy/clickhouse/config.d/prometheus.xml`:

```xml
<clickhouse>
    <prometheus>
        <endpoint>/metrics</endpoint>
        <port>9363</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
</clickhouse>
```

Add the configuration file to the existing `clickhouse` service in
`deploy/docker-compose.yml`:

```yaml
  clickhouse:
    # Keep the existing image, hostname, environment and other settings.
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - ./data/clickhouse:/var/lib/clickhouse
      - ./clickhouse/config.xml:/etc/clickhouse-server/config.xml:ro
      - ./clickhouse/users.xml:/etc/clickhouse-server/users.xml:ro
      - ./clickhouse/config.d/listen.xml:/etc/clickhouse-server/config.d/listen.xml:ro
      - ./clickhouse/config.d/prometheus.xml:/etc/clickhouse-server/config.d/prometheus.xml:ro
```

ClickHouse listens on port `9363` inside the Compose network. Do not publish
that port on the host; Prometheus can scrape `clickhouse:9363` directly.

Unlike a query-based exporter, the native ClickHouse Prometheus handler does
not execute SQL and does not authenticate as a ClickHouse database user. It
serializes internal counters directly. Creating a ClickHouse user and granting
access to `system.metrics`, `system.events`, or `system.asynchronous_metrics`
would therefore provide no protection because that user would never be used.

For this native endpoint, the least-privilege model is zero ClickHouse SQL
privileges combined with network isolation: port `9363` remains private to the
`dataload` network and only Prometheus connects to it. If a later deployment
requires per-client authentication and auditing, replace the native endpoint
with a query-based exporter running under a dedicated read-only ClickHouse user
instead of creating an unused account.

## 4. Add Kafka and PostgreSQL Exporters

Add the following services under `services` in `deploy/docker-compose.yml`:

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:v1.9.0
    container_name: kafka-exporter
    hostname: kafka-exporter
    command:
      - "--kafka.server=kafka-1:9094"
    networks:
      - dataload
    depends_on:
      - kafka-1
    restart: on-failure

  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:v0.17.1
    container_name: postgres-exporter
    hostname: postgres-exporter
    environment:
      DATA_SOURCE_NAME: "postgresql://${POSTGRES_EXPORTER_USER}:${POSTGRES_EXPORTER_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable"
    networks:
      - dataload
    depends_on:
      - postgres
    restart: on-failure
```

The Kafka exporter connects to the broker's internal listener. It provides
topic, partition, offset, and consumer-group metrics, including lag. It does
not expose Kafka JVM internals; JMX Exporter can be added in a later experiment
when broker heap, garbage collection, request handlers, or network processors
become relevant.

The PostgreSQL exporter now uses only the dedicated monitoring role. The
consumer continues to use the application credentials from its own DSN.

## 5. Configure Prometheus

Create the directory:

```bash
mkdir -p deploy/prometheus
```

Create `deploy/prometheus/prometheus.yml`:

```yaml
global:
  scrape_interval: 10s
  evaluation_interval: 10s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["prometheus:9090"]

  - job_name: producer
    static_configs:
      - targets: ["producer:9100"]

  - job_name: consumer
    static_configs:
      - targets: ["consumer:9100"]

  - job_name: kafka
    static_configs:
      - targets: ["kafka-exporter:9308"]

  - job_name: postgres
    static_configs:
      - targets: ["postgres-exporter:9187"]

  - job_name: clickhouse
    static_configs:
      - targets: ["clickhouse:9363"]
```

Ten seconds is intentionally frequent enough to make short experiments visible.
A longer interval reduces storage and scrape overhead when rapid feedback is no
longer necessary.

## 6. Provision the Grafana Data Source

Create the provisioning directory:

```bash
mkdir -p deploy/grafana/provisioning/datasources
```

Create `deploy/grafana/provisioning/datasources/prometheus.yml`:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

Provisioning the data source makes Grafana usable after every clean deployment
without repeating the connection setup in the UI.

## 7. Add Prometheus and Grafana to Compose

Add these services under `services` in `deploy/docker-compose.yml`:

```yaml
  prometheus:
    image: prom/prometheus:v3.5.0
    container_name: prometheus
    hostname: prometheus
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    networks:
      - dataload
    restart: on-failure

  grafana:
    image: grafana/grafana:12.1.0
    container_name: grafana
    hostname: grafana
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    ports:
      - "3000:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana-data:/var/lib/grafana
    networks:
      - dataload
    depends_on:
      - prometheus
    restart: on-failure
```

Declare the named volumes at the end of the Compose file, alongside the
existing top-level `networks` section:

```yaml
volumes:
  prometheus-data:
  grafana-data:
```

Named volumes avoid host permission differences for the Prometheus and Grafana
data directories. They also change reset behavior: `docker compose down` keeps
their data, while `docker compose down -v` deletes the monitoring history and
Grafana state. The Kafka, ClickHouse, and PostgreSQL bind-mounted data remains
under `deploy/data/` and is not removed by `-v`.

The `admin` password is acceptable only for this isolated practice stand.
Change it before exposing Grafana beyond a trusted development network.

## 8. Validate and Start the Complete Stand

Validate the effective configuration:

```bash
cd deploy
docker compose config --quiet
```

Start all services together:

```bash
docker compose up --build -d
```

Inspect their state:

```bash
docker compose ps
```

Follow startup logs when a service restarts or remains unavailable:

```bash
docker compose logs -f \
  producer consumer kafka-exporter postgres-exporter prometheus grafana
```

Compose startup order is not readiness. Exporters and Prometheus may report
temporary failures while Kafka, PostgreSQL, ClickHouse, and the applications
initialize. The targets should become healthy after the stand settles.

## 9. Verify Prometheus Collection through Grafana

Metric endpoints are intentionally private to the Compose network. Prometheus
uses service discovery through Docker DNS and scrapes each endpoint by service
name. Prometheus is also private to that network because Grafana connects to it
at `http://prometheus:9090`. Only the Grafana user interface is published on the
host.

Confirm that Prometheus is running and inspect its logs for configuration or
scrape errors:

```bash
docker compose ps prometheus
docker compose logs prometheus
```

Open Grafana at:

```text
http://localhost:3000
```

Log in with `admin` / `admin`, open **Explore**, select the provisioned
Prometheus data source, and run:

```promql
up
```

The query should return value `1` for the `prometheus`, `producer`, `consumer`,
`kafka`, `postgres`, and `clickhouse` jobs.

Check several additional queries in Explore:

```promql
rate(beaver_producer_events_produced_total[5m])
```

```promql
sum(kafka_consumergroup_lag{consumergroup="beaver-consumer"})
```

```promql
pg_up
```

```promql
ClickHouseMetrics_Query
```

Metric names can change between exporter or ClickHouse versions. When a query
returns no data, use the metric selector in Grafana Explore or query series for
the corresponding `job` label. Use the actual series name rather than assuming
that the example is universal.

## 10. Create the Initial Grafana Dashboard

Create a dashboard named `BEAVER Overview`. Start with the following panels:

| Panel | Suggested PromQL | Visualization |
|---|---|---|
| Targets up | `up` | State timeline or table |
| Producer input rate | `rate(beaver_producer_events_received_total[5m])` | Time series |
| Producer Kafka rate | `rate(beaver_producer_events_produced_total[5m])` | Time series |
| Producer errors | `rate(beaver_producer_produce_errors_total[5m])` | Time series |
| Consumer poll rate | `rate(beaver_consumer_records_polled_total[5m])` | Time series |
| Consumer writes | `sum by (sink) (rate(beaver_consumer_events_written_total[5m]))` | Time series |
| Consumer lag | `sum(kafka_consumergroup_lag{consumergroup="beaver-consumer"})` | Time series |
| Consumer write p95 | `histogram_quantile(0.95, sum by (le, sink) (rate(beaver_consumer_write_duration_seconds_bucket[5m])))` | Time series |
| PostgreSQL up | `pg_up` | Stat |
| PostgreSQL connections | `sum by (state) (pg_stat_activity_count{datname="beaver"})` | Time series |
| PostgreSQL commits | `rate(pg_stat_database_xact_commit{datname="beaver"}[5m])` | Time series |
| ClickHouse queries | `ClickHouseMetrics_Query` | Time series |
| ClickHouse inserted rows | `rate(ClickHouseProfileEvents_InsertedRows[5m])` | Time series |
| Application CPU | `rate(process_cpu_seconds_total{job=~"producer|consumer"}[5m])` | Time series |
| Application memory | `process_resident_memory_bytes{job=~"producer|consumer"}` | Time series |

This is a starting dashboard, not a fixed specification. Remove panels that do
not help answer an operational question and add new ones when an experiment
requires more context.

Set a refresh interval of 10 seconds and use a recent time range such as the
last 15 minutes while running short experiments.

## 11. Record a Normal-Operation Baseline

Let the stand run without deliberate failures. Observe and record:

- normal producer and consumer event rates;
- typical Kafka consumer lag and whether it returns to zero;
- normal batch and write latency;
- PostgreSQL connection states and commit rate;
- ClickHouse query and inserted-row activity;
- producer and consumer CPU and resident memory.

Exact values are less important than knowing their shape and relationship. A
future spike is meaningful only when it can be compared with normal behavior.

## 12. Run Small Failure Experiments

Stop the consumer for one minute:

```bash
docker compose stop consumer
```

Watch producer throughput continue while consumer lag grows. Start it again:

```bash
docker compose start consumer
```

Observe the consumer processing rate and lag recovery.

Stop PostgreSQL briefly:

```bash
docker compose stop postgres
```

Observe `pg_up` fall to zero while the exporter target itself remains reachable.
Also observe consumer retries or restart behavior, Kafka lag, and the difference
between ClickHouse and PostgreSQL write activity. Restore PostgreSQL:

```bash
docker compose start postgres
```

These experiments are intentionally simple. Their purpose is to confirm that
the dashboard explains behavior across multiple components instead of showing
isolated graphs with no operational meaning.

## Completion Check

STEP 01 is complete when:

- all six Prometheus scrape jobs are present and healthy after startup;
- Kafka consumer lag is available and changes when the consumer stops;
- PostgreSQL activity metrics are available;
- ClickHouse native metrics are available;
- producer and consumer application metrics are stored historically;
- Grafana uses the provisioned Prometheus data source;
- the `BEAVER Overview` dashboard answers the basic questions from the STEP
  README;
- Prometheus and Grafana data survives `docker compose down` followed by
  `docker compose up -d`;
- monitoring starts with the stand and remains available for the next STEP.
