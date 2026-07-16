# STEP 00 Solution: Deploy the Stand

This document presents one possible way to deploy STEP 00. It is a reference,
not the only valid procedure. Adapt commands and configuration to your host
and verify the result independently.

## 1. Check the Prerequisites

The host needs:

- Git;
- Docker Engine or Docker Desktop;
- Docker Compose v2, available as the `docker compose` subcommand;
- outbound internet access for container images and the Wikimedia SSE stream.

Check the local tools:

```bash
git --version
docker --version
docker compose version
```

Make sure the Docker daemon is running:

```bash
docker info
```

## 2. Get the STEP 00 Source

Clone the repository if it is not already available:

```bash
git clone https://github.com/lev-stas/beaver.git
cd beaver
```

After the STEP 00 release has been published, check out its immutable state:

```bash
git fetch --tags
git switch --detach step-00
```

If you plan to modify the stand, create a branch from the tag:

```bash
git switch -c my-step-00-experiments step-00
```

## 3. Prepare Application Configuration

The real `config.yml` files are environment-specific and ignored by Git.
Create them from the version-controlled examples:

```bash
cp cmd/producer/config.example.yml cmd/producer/config.yml
cp cmd/consumer/config.example.yml cmd/consumer/config.yml
```

The example configuration is ready for the provided Compose network. Both
applications connect to Kafka at `kafka-1:9092`; the consumer connects to
ClickHouse at `clickhouse:9000` and PostgreSQL at `postgres:5432`.

Review the generated files before starting:

```bash
less cmd/producer/config.yml
less cmd/consumer/config.yml
```

The producer configuration also contains the Wikimedia stream URL, a required
User-Agent, batch size, flush interval, and Kafka topic creation settings.

## 4. Prepare Compose Environment Values

Create the local Compose environment file:

```bash
cp deploy/.env.example deploy/.env
```

The defaults configure Kafka to advertise `kafka-1:9092` inside the Compose
network and create the PostgreSQL database and user expected by the consumer.
If you change the PostgreSQL values, make the same change in
`cmd/consumer/config.yml` before the first deployment.

## 5. Validate the Compose Configuration

Render and validate the effective Compose model:

```bash
cd deploy
docker compose config --quiet
```

No output means that Compose accepted the configuration.

## 6. Build and Start the Stand

From the `deploy` directory, run:

```bash
docker compose up --build -d
```

This builds both Go applications and starts Kafka, PostgreSQL, ClickHouse, the
producer, and the consumer on the `dataload` network.

Check container state:

```bash
docker compose ps
```

The infrastructure containers may need time to initialize. Compose
`depends_on` controls startup order but does not guarantee that Kafka or the
databases are ready to accept connections. The producer and consumer use
`restart: on-failure`, so they may restart while the dependencies become
available.

## 7. Observe the Data Pipeline

Follow the application logs:

```bash
docker compose logs -f producer consumer
```

The producer should connect to Wikimedia, create or find the Kafka topic, and
print incoming event IDs. The consumer should begin polling Kafka and writing
batches to both databases. Press `Ctrl-C` to stop following logs; this does
not stop the containers.

If an application keeps restarting, inspect its recent output:

```bash
docker compose logs --tail=100 producer
docker compose logs --tail=100 consumer
```

## 8. Verify Kafka

Describe the topic created by the producer:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic bearer-events
```

The topic should have three partitions and a replication factor of one.

Inspect the consumer group and its lag:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --group beaver-consumer
```

The exact offsets and lag change continuously because the source stream does
not stop.

## 9. Verify ClickHouse

Count raw events:

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT count() FROM beaver.raw_events"
```

Inspect a few recent rows:

```bash
docker exec clickhouse clickhouse-client \
  --query "SELECT id, type, wiki, title, ts FROM beaver.raw_events ORDER BY ts DESC LIMIT 5"
```

Run the count again after a short wait. It should increase while the producer
and consumer are running.

## 10. Verify PostgreSQL

Count materialized page states:

```bash
docker exec postgres psql -U beaver -d beaver \
  -c "SELECT count(*) FROM page_state;"
```

Inspect recently updated pages:

```bash
docker exec postgres psql -U beaver -d beaver \
  -c "SELECT wiki, title, edit_count, updated_at FROM page_state ORDER BY updated_at DESC LIMIT 5;"
```

ClickHouse is the raw event log, while PostgreSQL holds one current state row
per `(wiki, namespace, title)` key. Their row counts are therefore not
expected to match.

## 11. Test Persistence

Stop and remove the containers without deleting their bind-mounted data:

```bash
docker compose down
```

Start the stand again:

```bash
docker compose up -d
```

Repeat the ClickHouse and PostgreSQL queries. Existing rows should still be
present because data is stored under `deploy/data/` on the host.

## 12. Stop or Reset the Stand

To stop the stand while retaining data:

```bash
docker compose down
```

To perform a complete reset, first stop all containers and then delete the
bind-mounted data directories:

```bash
docker compose down
rm -rf data/
```

The removal is irreversible. Run it only from the repository's `deploy`
directory and only when all Kafka, ClickHouse, and PostgreSQL data may be
discarded. The next `docker compose up --build -d` creates a clean stand.

## Completion Check

STEP 00 is complete when:

- all five containers remain running;
- the Kafka topic exists and the consumer group is active;
- the ClickHouse raw event count grows over time;
- PostgreSQL contains current page state rows;
- a normal `docker compose down` and restart preserves data;
- the difference between stopping the stand and resetting its data is clear.
