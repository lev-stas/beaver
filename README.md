# BEAVER
### Bus/Event Aggregation, Validation, ETL & Routing

BEAVER is a hands-on practice stand for operating Kafka, ClickHouse and
PostgreSQL together — a real pipeline you can run, break, tune and observe,
built to develop day-to-day ops competency (HA, performance tuning,
observability) rather than to serve production traffic.

It ingests the [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)
`recentchange` feed (a live, public, never-ending stream of real Wikipedia
edits) as a realistic source of continuous load, so the Kafka/ClickHouse/
PostgreSQL exercises have actual data flowing through them instead of
synthetic fixtures.

## Architecture

```
Wikimedia recentchange (SSE)
        |
        v
   [ producer ]  --batches, compressed-->  [ Kafka ]
                                               |
                                               v
                                         [ consumer ]
                                          /         \
                                         v           v
                                  [ ClickHouse ]  [ PostgreSQL ]
                                   raw_events       page_state
                                  (event log)   (materialized state)
```

- **producer** subscribes to the Wikimedia SSE feed and batches records into
  Kafka (flush on batch size or time interval, whichever comes first;
  compression codec configurable). It creates its Kafka topic on startup if
  it doesn't already exist.
- **consumer** polls Kafka, parses and validates each event, and writes it to
  two different places for two different purposes:
  - **ClickHouse `raw_events`** — an append-only log of every event exactly
    as received, kept for analytics/audit.
  - **PostgreSQL `page_state`** — one row per wiki page, upserted on every
    new event, holding only the *current* known state (latest revision,
    author, comment, edit count). Not an event log — a materialized view.

  Offsets are committed manually, only after both writes succeed, so a
  crash or a downstream outage replays the batch instead of losing data.

## Repository layout

```
cmd/producer/     producer entrypoint, Dockerfile, config.example.yml
cmd/consumer/     consumer entrypoint, Dockerfile, config.example.yml
internal/config/  YAML config loading for both apps
internal/producer/  SSE -> Kafka orchestration (batching, topic creation)
internal/consumer/  Kafka -> ClickHouse/PostgreSQL orchestration, manual offsets
internal/event/     recentchange JSON parsing and validation
internal/clickhouse/ ClickHouse client
internal/postgres/   PostgreSQL client
deploy/            docker-compose.yml, stock Postgres/ClickHouse configs
```

## Prerequisites

- Docker and Docker Compose v2 (the `docker compose` subcommand).
- Outbound internet access (to reach the Wikimedia SSE feed and to pull
  images on first run).

## Deploying the stand

1. **Configure the apps.** Each app reads a `config.yml` that sits next to
   its binary and is not committed to git (it's environment-specific).
   Start from the provided examples:

   ```
   cp cmd/producer/config.example.yml cmd/producer/config.yml
   cp cmd/consumer/config.example.yml cmd/consumer/config.yml
   ```

   For a stock run of this compose stack, the only field you must change in
   both files is `kafka.brokers` — the example lists placeholder addresses
   for a future multi-broker cluster, but this compose file ships a single
   broker reachable at `kafka-1:9092`:

   ```yaml
   kafka:
     brokers:
       - "kafka-1:9092"
   ```

   Everything else in the examples (topic name, compression, batch size,
   ClickHouse/PostgreSQL addresses, table names, credentials) already
   matches this compose stack's service names and can be left as-is.

2. **Set environment values.**

   ```
   cp deploy/.env.example deploy/.env
   ```

   This file holds two things `docker-compose.yml` reads at startup:

   - Kafka's advertised listener address. The default
     (`KAFKA_LISTENER_IP=kafka-1`) works out of the box because
     producer/consumer run on the same Docker network as the broker. See the
     comments in `deploy/.env.example` if you need to reach Kafka from
     outside Docker instead.
   - `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`, applied only on
     first init of an empty `deploy/data/postgres/`. The defaults already
     match the DSN baked into `cmd/consumer/config.example.yml`; if you
     change them here, update `postgres.dsn` in `cmd/consumer/config.yml`
     to match.

3. **Build and start everything.**

   ```
   cd deploy
   docker compose up --build -d
   ```

   This builds the producer/consumer images and starts Kafka, PostgreSQL,
   ClickHouse, the producer and the consumer, all on one Docker network.

4. **Check it's alive.**

   ```
   docker compose logs -f producer consumer
   ```

   Within a few seconds you should see the producer logging Wikipedia
   revision IDs and the consumer processing batches. To look at the data
   directly:

   ```
   docker exec -it clickhouse clickhouse-client \
     --query "SELECT count() FROM beaver.raw_events"

   docker exec -it postgres psql -U beaver -d beaver \
     -c "SELECT wiki, title, edit_count, updated_at FROM page_state ORDER BY updated_at DESC LIMIT 5;"
   ```

5. **Stop the stand.**

   ```
   docker compose down
   ```

   Data lives in bind-mounted host directories under `deploy/data/`, not
   named Docker volumes, so it survives this and `-v` has no effect on it.
   To wipe Kafka/ClickHouse/PostgreSQL data and start clean, also run
   `rm -rf data/` after `down` (irreversible — only do this if you actually
   want to lose all stand data).

## Configuration reference

Both apps take a `--config` flag (default `./config.yml`).

**Producer** (`cmd/producer/config.yml`):

| Field | Meaning |
|---|---|
| `kafka.brokers` | Kafka bootstrap addresses |
| `kafka.topic` | topic to produce to (created on startup if missing) |
| `kafka.partitions` / `kafka.replication-factor` | used only when creating the topic |
| `kafka.compression` | `none`, `gzip`, `snappy`, `lz4`, or `zstd` |
| `producer.sse-url` | Wikimedia (or compatible) SSE stream URL |
| `producer.user-agent` | required by Wikimedia's SSE endpoint, or it returns 403 |
| `producer.batch-size` | max records per Kafka batch send |
| `producer.flush-interval` | max time to wait before flushing a partial batch |

**Consumer** (`cmd/consumer/config.yml`):

| Field | Meaning |
|---|---|
| `kafka.brokers` / `kafka.topic` | same as above |
| `consumer.consumer-group` | consumer group name |
| `clickhouse.address` / `.database` / `.table` | native protocol address, e.g. `clickhouse:9000` |
| `postgres.dsn` / `.table` | full `postgres://` connection string |

## What this stand is (and isn't) for

This is a practice environment, not a template for production deployment —
single-instance Kafka/ClickHouse/PostgreSQL, no TLS, no auth beyond a shared
password. It's meant to be broken on purpose: kill a broker, fill a disk,
starve a connection pool, replay a topic from the beginning, and see what
the system (and the code) actually does.

## Infrastructure STEPs

Versioned starting points and suggested experiments are documented in
[docs/README.md](docs/README.md). Each STEP describes its initial state and
includes one example solution that can be used as a reference.
