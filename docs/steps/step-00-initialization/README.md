# STEP 00: Project Initialization

STEP 00 is the base checkpoint for the BEAVER stand. The source code and
container definitions exist, but the stand has not yet been deployed. The
purpose of this STEP is to turn that source tree into a running data pipeline
and establish a known baseline for later infrastructure experiments.

## Initial State

The repository defines five single-instance components:

- a Go producer that reads the Wikimedia `recentchange` SSE stream;
- one Kafka node running as both broker and KRaft controller;
- a Go consumer in a single Kafka consumer group;
- one ClickHouse server for the append-oriented raw event log;
- one PostgreSQL server for the current state of each wiki page.

The intended data flow is:

```text
Wikimedia EventStreams
        |
        v
    producer
        |
        v
      Kafka
        |
        v
    consumer
      /   \
     v     v
ClickHouse PostgreSQL
```

All components run on one Docker host and share one Docker Compose network.
Kafka, ClickHouse, and PostgreSQL persist their data under `deploy/data/`.
There is no high availability, TLS, external secrets management, or production
hardening at this point.

## Problems to Address

A source tree alone does not prove that the system works. Before changing the
topology, we need to answer several basic operational questions:

- Are the required environment-specific configuration files present?
- Can all images be built and all containers be started together?
- Can the applications resolve and connect to infrastructure services?
- Does live data travel through the entire pipeline?
- Can we inspect the resulting records in both databases?
- What happens to persisted data when the stand is stopped and started again?
- How can the stand be reset to a completely clean state?

## Goals

In this STEP, try to achieve the following outcomes:

- prepare local application and Compose configuration;
- build and start the complete stand;
- observe component startup and application logs;
- verify that Kafka receives events from the producer;
- verify that the consumer writes raw events to ClickHouse;
- verify that the consumer updates page state in PostgreSQL;
- stop and restart the stand without losing persisted data;
- understand how to remove all data and repeat the deployment from scratch.

## Why This STEP Matters

Completing this STEP creates a reproducible baseline. Later experiments with
scaling, failures, replication, observability, and performance are only useful
when there is a known working state to compare them against.

It also introduces several practical skills:

- reading a Compose topology and following service dependencies;
- separating version-controlled examples from local configuration;
- observing container and application startup;
- checking an asynchronous pipeline end to end instead of testing only that
  containers are running;
- distinguishing container lifecycle from persistent data lifecycle;
- recovering the stand to a clean and repeatable state.

The expected result is not a production-ready platform. It is a small,
understandable system that continuously processes real events and can be
changed or deliberately broken in later STEPs.

One possible deployment procedure is provided in [SOLUTION.md](SOLUTION.md).
