# Changelog
All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com),
versioning: [SemVer](https://semver.org).

## [Unreleased]

## [1.0.0] - 2026-10-20
First release: the warm-up project (P14) of the Business-First AI Engineering Portfolio.

### Added
- Write-ahead ingestion: HMAC-SHA256 verification, envelope validation, idempotency-key derivation, typed and
  recorded rejections, `202` only after commit.
- Single-transaction processing: idempotency record + per-aggregate version ordering + state change + effect log,
  buffer with bounded gap window (`park_and_alert` or `last_writer_wins`).
- Budgeted exponential backoff with full jitter, dead-letter queue with `list` / `redrive` / `discard`, threshold alerts.
- Reconciler that heals `missing` / `behind` / `divergent` drift and flags `local_ahead` / `orphan`.
- Reusable pattern library (`reconciler.patterns`) for P3, P7 and P8.
- Operator CLI (`migrate`, `serve`, `worker`, `sweep`, `reconcile`, `stats`, `alerts`, `dlq ...`) and OpenAPI contract
  guarded by a drift test.
- Chaos harness (Hypothesis-generated and seeded plans), naive baseline handler, harness-sensitivity tests.
- OpenTelemetry tracing with the ingest trace context stored on the event so retries and re-drives join one trace.
- Benchmarks and committed results; mutation testing; HLD + review memo, 6 ADRs, LLD, runbook, interview Q&A.

### Measured
- 1 000 / 1 000 hostile and 1 000 / 1 000 mild chaos permutations converge; 0 duplicate effects; naive handler wrong on
  1 000 / 1 000 and 874 / 1 000.
- HTTP ack p95 2.6 ms; 259 events/s on one worker, 579 events/s peak; 97 % coverage; 99.5 % mutation kill rate.
