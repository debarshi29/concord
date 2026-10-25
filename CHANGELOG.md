# Changelog
All notable changes to this project are documented here. Format: [Keep a Changelog](https://keepachangelog.com),
versioning: [SemVer](https://semver.org).

## [Unreleased]

## [1.1.0] - 2026-10-26
Fixes from a full review of `src/`, plus history retention. Three of the fixes concern valid events that could be lost
or mis-handled; none had been caught by the chaos harness because it never sent those inputs.

### Added
- History retention (`RETENTION_SECONDS`, default 30 days, `0` disables): the sweep deletes finished events, effect-log
  rows, corrections and resolved DLQ entries past the window in bounded batches. Events pinned by an idempotency
  record, an open DLQ entry or the buffer, and the latest reconcile run, are never deleted. Migration `0003` adds the
  supporting indexes.
- `OPS_TOKEN`: when set, `/stats` and `/alerts` require `Authorization: Bearer <token>`. Unset keeps them open (the API
  logs a warning at boot). The compose file passes it through.
- `MAX_BODY_BYTES` (1 MiB) and `REJECTED_RETENTION_SECONDS` (7 days) settings; `reconciler sweep` reports
  `rejected_purged` and `retention_purged`.

### Fixed
- A correctly signed event carrying an ISO-8601 `occurred_at` was rejected with `422` (the envelope was fully strict),
  so the sender stopped retrying and the event was lost.
- A non-ASCII `X-Signature` raised `TypeError` and returned `500` without recording the request.
- Signed bodies Postgres cannot store (NUL, lone surrogates, `1e999`) failed the insert with `500`, so the sender
  retried forever; they are now recorded as `invalid_envelope`. `NaN` / `Infinity` are rejected as malformed JSON.
- `migrate` released its advisory lock before the migration transaction committed, so concurrent migrators could both
  run `0001`; a failure in migration N also rolled back migrations 1..N-1.
- `reconcile` re-logged `local_ahead` and `orphan` flags on every run (so `corrections_last_24h` could never reach
  zero) and flagged aggregates created while the source listing was in flight as orphans.
- `reconcile` ran inside the worker loop, so a slow or hung source stopped claims, gap sweeps and alerts; it now has
  its own thread.
- The worker's idle sleep could not be interrupted by shutdown.

### Changed
- **Behaviour change:** history older than 30 days is now deleted by default. Set `RECONCILER_RETENTION_SECONDS=0` to
  keep it.
- Webhook bodies over `MAX_BODY_BYTES` get `413`, refused unread and unrecorded; unauthenticated rejections are stored
  truncated (2 KiB) and purged after `REJECTED_RETENTION_SECONDS`.
- Claim query is served by a partial index on `(seq) WHERE status = 'received'` (migration `0002`): with 200 000
  processed events ahead of the backlog a claim fell from 14.9 ms to 0.075 ms.
- Alert evaluation reads four indexed values instead of running the full stats report every sweep.
- Docker base image python 3.12.8 to 3.12.15; `certifi` 2026.10.23 (PYSEC-2026-4320).

### Measured
- 219 tests, 97 % coverage; mutation kill rate 182 / 183 (99.5 %), the one survivor being the documented equivalent
  mutant in `ordering.py`.
- Benchmarks re-run on the fixed code: 1 000 / 1 000 hostile permutations converge with 0 duplicate effects (naive handler
  wrong on 1 000 / 1 000); ack p50 / p95 / p99 1.6 / 2.4 / 3.0 ms; 314 / 547 / 552 / 512 events/s at 1 / 2 / 4 / 8 workers.
  Throughput differences from 1.0.0 are within run-to-run noise on this laptop.

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
