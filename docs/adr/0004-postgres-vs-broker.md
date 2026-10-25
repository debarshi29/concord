# ADR-0004: Postgres is the write-ahead log, queue, ledger and state store for v1
Status: accepted
Date: 2026-10-19

## Context
Needed: durable write-ahead before ack, transactional "apply + record idempotency", delayed retries, DLQ with
context, ad-hoc operator queries. Load target 50 ev/s sustained, 500 ev/s burst.

## Decision
Use one Postgres database for all of it. Claiming uses `FOR UPDATE SKIP LOCKED`; retries are rows with
`next_attempt_at`; the DLQ is a table; the whole processing step is one transaction, so the state change, the
effect record and the idempotency record commit or roll back together. No broker in v1.

## Alternatives considered
* **Kafka/Redpanda + consumer state in Postgres.** Better at throughput and replay; rejected because
  "apply + commit offset" becomes a dual write, which reintroduces the very problem (duplicate effects) we are
  proving away, and operational weight dwarfs a 3-week warm-up.
* **SQS/RabbitMQ.** Cheap delayed retries and DLQ built in, but state change and ack are still two systems.
* **Redis for idempotency.** Fast, but separate from the state transaction → race windows.

## Consequences
Easy: exactly-once effects with plain transactions; SQL for every operator question; embedded-Postgres tests.
Hard: throughput ceiling in the low thousands/s on one primary; `events` table growth needs partitioning/retention
(retention shipped in 1.1.0 as `RETENTION_SECONDS`; time partitioning is still deferred); polling claim loop adds up to `poll_interval` latency. Reversibility: one-way-ish — moving the WAL to a
broker later changes ingestion semantics, but the pattern library's interfaces were written to make that swap local.

## Revisit criteria
Sustained > 1 000 events/s, claim-query p95 > 20 ms, or `events` > 500 GB.
