# Idempotent Webhook Reconciler

[![ci](https://github.com/debarshi29/concord/actions/workflows/ci.yml/badge.svg)](https://github.com/debarshi29/concord/actions/workflows/ci.yml)

A small service that receives webhooks over an unreliable network and keeps a local copy of the sender's data
correct: every change takes effect **exactly once**, in the right order, and any drift from the source is detected and
repaired.

Webhook delivery is at-least-once and unordered. Events arrive twice, out of order, late, malformed, or not at all. The
usual consequences are duplicate invoices, orders processed twice, and records that quietly diverge from the source
system. This service handles each of those cases explicitly and is tested against all of them together.

## Features

- **Durable ingestion.** Signatures are verified (HMAC-SHA256), the payload is validated, and the event is committed to
  Postgres *before* the `202` is returned. Rejected events are recorded with a typed reason and are never dropped.
- **Exactly-once effects.** The state change, an effect-log row and the idempotency record are written in one
  transaction, so a replay is a no-op.
- **Ordering.** Events carry a per-aggregate version. Out-of-order events are buffered for a bounded window and applied
  in order once the gap fills; if it never does, they are parked and an alert fires.
- **Retries and a dead-letter queue.** Transient failures are retried with exponential backoff, full jitter and a hard
  attempt budget. Terminal failures and exhausted budgets go to a DLQ you can list, re-drive, or discard.
- **Failure isolation.** One bad event stalls only its own aggregate; everything else keeps flowing.
- **Reconciliation.** A periodic job compares local state with the source's list endpoint, repairs drift, and records
  every correction.
- **Observability.** Structured JSON logs, OpenTelemetry traces that follow an event from receipt through retries and
  re-drives, and `stats` / `alerts` commands.
- **Reusable building blocks.** [`reconciler.patterns`](src/reconciler/patterns/) contains the ordering decision, retry
  policy, idempotency store and DLQ operations with no framework dependencies.

## How it works

```mermaid
flowchart TB
    WH["Webhook"] --> VER["Verify signature + schema"]
    VER -->|invalid| REJ["Typed rejection, recorded"]
    VER --> WAL[("events (write-ahead)")]
    WAL --> ACK["202 Ack"]
    WAL --> W["Worker (per aggregate)"]
    W --> TX{"Single transaction"}
    TX --> ID{"Idempotency key seen?"}
    ID -->|yes| DONE["Return prior outcome"]
    ID -->|no| ORD{"Version in order?"}
    ORD -->|gap| BUF[("buffer")]
    BUF -->|gap fills / window expires| ORD
    ORD -->|yes| APPLY["Apply state + effect + idempotency record"]
    APPLY -->|downstream failure| RETRY["Backoff + jitter, budgeted"]
    RETRY -->|exhausted| DLQ[("dlq")]
    DLQ --> RD["Re-drive CLI"]
    SRC["Source of truth"] --> RECON["Reconciler (periodic)"]
    RECON -->|heals drift, logs correction| APPLY
```

Postgres is the write-ahead log, the work queue, the ledger and the state store, which is what lets one transaction
cover claim, state change and idempotency record. Workers claim events with `FOR UPDATE SKIP LOCKED` and serialise per
aggregate with a row lock, so adding workers raises throughput without affecting correctness.

Full design: [`docs/hld.md`](docs/hld.md) (architecture and review memo), [`docs/lld.md`](docs/lld.md) (contracts,
schemas, state machine, sequences, invariants) and the decision records in [`docs/adr/`](docs/adr/).

## Quick start

Requirements: Python 3.12 and [`uv`](https://docs.astral.sh/uv/). Tests and the local dev server use an embedded
Postgres, so no database install or Docker is needed.

```bash
git clone https://github.com/debarshi29/concord && cd concord
make dev     # install locked dependencies and git hooks
make test    # run the full suite against a real Postgres
make run     # embedded Postgres + API on :8000 + worker (secret: dev-secret, docs at /docs)
```

Without `make`, use `uv sync --frozen`, `uv run pytest --cov` and `uv run python -m reconciler.devserver`.

Send a webhook to the running server:

```python
import hashlib, hmac, json, httpx

body = json.dumps(
    {
        "type": "order.updated",
        "aggregate_id": "o1",
        "version": 1,
        "data": {"status": "paid"},
    }
).encode()
sig = "sha256=" + hmac.new(b"dev-secret", body, hashlib.sha256).hexdigest()
r = httpx.post("http://127.0.0.1:8000/webhooks", content=body, headers={"X-Signature": sig})
print(r.status_code, r.json())  # 202 {...}; send it again and the effect still happens once
print(httpx.get("http://127.0.0.1:8000/stats").json()["events_by_status"])
```

### Running against your own Postgres

```bash
export RECONCILER_DATABASE_URL=postgresql://user:pass@host:5432/reconciler
export RECONCILER_WEBHOOK_SECRET=<shared secret>
reconciler migrate
reconciler serve --host 0.0.0.0 --port 8000     # ingestion API
reconciler worker                                # processing, sweeps, alerts, reconciliation
```

A `Dockerfile` and `docker-compose.yml` (Postgres, migrate, API, worker) are included: `docker compose up --build`.

## HTTP API

`POST /webhooks` with an `X-Signature: sha256=<HMAC-SHA256 of the raw body>` header and an optional `Idempotency-Key`
header. Body:

```json
{
  "event_id": "evt-123",
  "type": "order.updated",
  "aggregate_id": "order-42",
  "version": 3,
  "data": { "status": "shipped" }
}
```

| Status | Meaning |
|---|---|
| `202` | Stored durably; processing is asynchronous |
| `401` | Missing or invalid signature |
| `400` | Body is not valid JSON |
| `422` | Invalid envelope (including values Postgres cannot store), or unsupported event type |
| `413` | Body over `MAX_BODY_BYTES` (default 1 MiB); refused unread and not recorded |

Every error carries a typed `error.code` and the id of the recorded event. Also available: `GET /healthz`,
`GET /readyz`, `GET /stats`, `GET /alerts`. The machine-readable contract is [`docs/openapi.json`](docs/openapi.json),
and a test fails if it drifts from the running app.

## Command line

| Command | Purpose |
|---|---|
| `reconciler migrate` | Apply pending schema migrations |
| `reconciler serve` | Run the ingestion API |
| `reconciler worker [--until-idle]` | Process events; sweep, alert and reconcile on a schedule |
| `reconciler sweep` | Resolve expired gap windows and purge expired idempotency records |
| `reconciler reconcile` | Run one reconciliation pass now |
| `reconciler stats` | Events by status, DLQ depth, corrections, last reconcile |
| `reconciler alerts` | Print firing alerts; exits 1 if any fire |
| `reconciler dlq list` / `redrive` / `discard` | Operate the dead-letter queue |
| `reconciler openapi` | Print the OpenAPI document |

## Configuration

All settings are environment variables prefixed `RECONCILER_`, validated at startup. The most important:

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | local Postgres | Connection string |
| `WEBHOOK_SECRET` | required | HMAC secret (`WEBHOOK_SECRET_PREVIOUS` is also accepted during rotation) |
| `IDEMPOTENCY_TTL_SECONDS` | 604800 (7 days) | How long idempotency records are kept |
| `BUFFER_WINDOW_SECONDS` | 300 | How long a version gap is tolerated |
| `GAP_POLICY` | `park_and_alert` | Or `last_writer_wins` |
| `RETRY_BASE_SECONDS` / `RETRY_CAP_SECONDS` / `RETRY_MAX_ATTEMPTS` | 1 / 300 / 6 | Backoff and attempt budget |
| `RECONCILE_SOURCE_URL` / `RECONCILE_INTERVAL_SECONDS` | unset / 900 | Enables the reconciler against a list endpoint |
| `OPS_TOKEN` | unset | Bearer token required by `/stats` and `/alerts` when set |
| `MAX_BODY_BYTES` | 1048576 | Largest accepted webhook body |
| `REJECTED_RETENTION_SECONDS` | 604800 (7 days) | How long unauthenticated rejections are kept (stored truncated to 2 KiB) |
| `OTEL_EXPORTER` | `none` | `console` or `otlp` (install the `otlp` extra) |

The complete list is in [`docs/lld.md`](docs/lld.md#6-configuration-reconciler_-validated-at-boot-by-pydantic-settings).
The source's list endpoint must return `{"items": [{"aggregate_id", "version", "state"}], "next_cursor": null | "..."}`.

## Operations

`reconciler alerts` fires on DLQ depth, parked events, long-buffered events and a stale reconciler. The
[runbook](docs/runbook.md) gives symptom, scope, fix and verification for each, and states which procedures have been
exercised by automated drills:

| Situation | Exercised by an automated drill |
|---|---|
| Downstream outage, events in the DLQ | yes |
| Poison message | yes |
| Version gap that never fills | yes |
| Worker killed mid-transaction | yes |
| Database failover, backup restore | no |

## Correctness

The central test generates hostile delivery schedules (duplicates, reordering, delays, drops, corrupted and poison
payloads, transient downstream failures, optionally several concurrent workers) and asserts that, after time passes
and one reconciliation, the final state equals the state of a perfect in-order, exactly-once delivery with no duplicate
effects. Hypothesis generates and shrinks the schedules. Separate tests sabotage the code and check that the harness
notices.

| 1,000 seeded permutations | This service | Naive handler¹ |
|---|---:|---:|
| Hostile traffic: converged | **1,000** | 0 |
| Mild traffic: converged | **1,000** | 126 |
| Duplicate effects (hostile) | **0** | 3,124 |

¹ Verifies signatures, then applies each event in arrival order with no idempotency, versioning, retry or
reconciliation. Results are per permutation (a stream of about 15 events) and are in
[`bench/results/`](bench/results/); regenerate with `make chaos`.

Hostile traffic is 15% dropped, 30% duplicated and 15% corrupted events, heavy reordering and 10% sink failures. In the
hostile run an independent audit found 1,968 drifted aggregates before reconciliation and the reconciler corrected
all 1,968.

The suite has 178 tests at 97% coverage against a real Postgres. Mutation testing of the decision modules kills 181 of
182 mutants (99.5%); see [`docs/mutation.md`](docs/mutation.md).

## Performance

Measured on one machine (Windows 11, 16 logical CPUs, embedded Postgres 16.2, in-process worker threads); treat these
as indicative, not as capacity guarantees.

| Metric | Result |
|---|---|
| HTTP acknowledgement latency, p50 / p95 / p99 | 2.1 / 2.6 / 2.9 ms (sequential client) |
| Processing throughput, 1 / 2 / 4 / 8 workers | 259 / 512 / 579 / 532 events/s |
| Worker time spent in the Postgres driver / in this code | 93.4% / 5.2% |

Scripts: `make bench`, `make profile`. The flamegraph is
[`bench/results/flamegraph.svg`](bench/results/flamegraph.svg).

## Limitations

- **External side effects are at-least-once.** The downstream sink runs inside the transaction; making an external call
  there is not atomic with the commit. A transactional outbox would close this gap.
- **Convergence after a lost event is not immediate.** It takes up to the gap window plus the reconcile interval
  (5 plus 15 minutes by default).
- **A single hot aggregate is serialised** by its row lock, and ordering is per aggregate only.
- **`/stats` and `/alerts` are open unless `OPS_TOKEN` is set.** With it set they require `Authorization: Bearer <token>`; without it, expose them on an internal network only (the API logs a warning at boot).
- **No replay-attack window.** Signatures cover the body, not a timestamp.
- **`events` is never trimmed.** There is no partitioning or retention yet; at 50 events/s, roughly 10 GB per day if
  every duplicate is kept.
- **One Postgres primary** bounds throughput.

## Roadmap

Transactional outbox for outbound publication · multi-source reconciliation with conflict policies · versioned event
schemas with compatibility checks in CI · `events` partitioning and retention · authentication for the operational
endpoints and a signed-timestamp replay window · nightly mutation testing of the Postgres-backed modules.

## Development

```bash
make lint        # ruff check + format check
make typecheck   # mypy --strict
make test        # tests + coverage gate (90%)
make chaos       # 1,000 chaos permutations (writes bench/results/)
make bench       # throughput and latency
make mutate      # mutation tests (run in a clean git worktree; the tool edits files in place)
make openapi     # regenerate docs/openapi.json
```

```text
src/reconciler/            service: ingest, processor, reconcile, worker, api, cli
src/reconciler/patterns/   reusable ordering, retry, idempotency and DLQ building blocks
src/reconciler/chaos/      chaos harness and naive baseline
tests/{unit,integration,property,e2e}/
bench/                     benchmarks and committed results
docs/                      design, ADRs, runbook, OpenAPI, mutation report
```

CI (GitHub Actions) runs lint, strict type checking, a secrets scan, the test suite with a coverage gate against a
`postgres:16` service, a chaos smoke test and a dependency audit on every pull request; mutation testing runs on pushes
to `main`. Commits follow Conventional Commits and releases follow SemVer; see [`CHANGELOG.md`](CHANGELOG.md).

## Documentation

| Document | Contents |
|---|---|
| [`docs/hld.md`](docs/hld.md) | Architecture, component responsibilities, requirements, failure-mode table |
| [`docs/lld.md`](docs/lld.md) | HTTP contract, schemas, event state machine, sequences, invariants, locking |
| [`docs/adr/`](docs/adr/) | Decision records (idempotency window, ordering policy, partitioning, Postgres vs broker, language, test database) |
| [`docs/runbook.md`](docs/runbook.md) | Operating procedures |
| [`docs/mutation.md`](docs/mutation.md) | Mutation-testing method and results |
| [`docs/openapi.json`](docs/openapi.json) | OpenAPI contract |
