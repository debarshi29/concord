# LLD — contracts, schemas, sequences

Companion to [`hld.md`](hld.md). The HTTP contract is machine-readable in [`openapi.json`](openapi.json) and a
test (`tests/integration/test_openapi_contract.py`) fails if it drifts from the running app.

## 1. HTTP contract

`POST /webhooks` — headers: `X-Signature: sha256=<hex HMAC-SHA256 of the raw body>` (required),
`Idempotency-Key` (optional). Body: the envelope below.

| Status | `error.code` | Meaning | Recorded? | Sender should |
|---|---|---|---|---|
| 202 | – | durably stored; processing is asynchronous | `events.status='received'` | stop retrying |
| 401 | `missing_signature` | no `X-Signature` | yes, `rejected` | fix config |
| 401 | `invalid_signature` | HMAC mismatch (current or previous secret) | yes, `rejected` | fix secret / do not retry as-is |
| 400 | `malformed_json` | body is not JSON | yes, `rejected` | fix payload |
| 422 | `invalid_envelope` | envelope schema violated (`error.detail` lists fields) | yes, `rejected` | fix payload |
| 422 | `unsupported_event_type` | `type` is not `order.updated` | yes, `rejected` | stop sending |
| 413 | `payload_too_large` | body over `MAX_BODY_BYTES` | **no**: refused unread, so an anonymous flood cannot fill the table | send less |

Every error body is `{"error": {"code", "detail", "retryable": false, "event_id"}}`; `event_id` is the id of the
**recorded** event, so a rejected delivery can be found later. The ack (202) is sent only after the insert has
committed. Other endpoints: `GET /healthz` (liveness), `GET /readyz` (`SELECT 1`, 503 on failure),
`GET /stats`, `GET /alerts`.

### Envelope (boundary schema, `reconciler.models.Envelope`)

| Field | Type | Constraint |
|---|---|---|
| `event_id` | string, optional | 1–200 chars; the sender's id, used as idempotency key unless a header is given |
| `type` | string | must be in `SUPPORTED_EVENT_TYPES` (`order.updated`) |
| `aggregate_id` | string | 1–128 chars |
| `version` | integer (strict) | 1 … 2 000 000 000; dense and monotonic per aggregate |
| `data` | object | validated by the **domain handler**, not the boundary |
| `occurred_at` | datetime, optional | informational |

Boundary vs. domain validation is deliberate: a schema-valid event with business-invalid `data` (a *poison*
message) is accepted, stored and dead-lettered with full context rather than refused where nobody can inspect it.

### Domain patch (`reconciler.domain.OrderPatch`, `extra="forbid"`, strict)

`status ∈ {pending, paid, shipped, cancelled}`, `amount_cents ≥ 0`, `currency ~ ^[A-Z]{3}$`,
`customer_id ≤ 128 chars`. Only the keys **present** are applied (shallow merge). Violations raise `TerminalError`.

## 2. Data model

```mermaid
erDiagram
    EVENTS ||--o| IDEMPOTENCY : "applied event records its key"
    EVENTS }o--|| AGGREGATES : mutates
    EVENTS ||--o| BUFFER : "parked behind a gap"
    EVENTS ||--o| DLQ : "dead-lettered"
    EVENTS ||--o{ EFFECT_LOG : "one row per applied version"
    AGGREGATES ||--o{ CORRECTIONS : "healed by reconciler"
    EVENTS {
        uuid id PK
        bigint seq "arrival order"
        text idempotency_key
        text aggregate_id
        int version
        text raw_body "exactly what was received"
        jsonb payload "NULL if unparsable"
        text signature_status "valid|invalid|missing"
        text status "see state machine"
        int attempts
        timestamptz next_attempt_at
        text last_error
        text traceparent "W3C trace context of the ingest span"
    }
    IDEMPOTENCY { text key PK
        jsonb outcome
        timestamptz expires_at }
    AGGREGATES { text id PK
        jsonb state
        int version }
    BUFFER { uuid event_id PK
        text aggregate_id
        int awaiting_version
        timestamptz window_expires }
    DLQ { uuid event_id PK
        int attempts
        text last_error
        timestamptz failed_at
        timestamptz resolved_at
        text resolution "redriven|discarded" }
    EFFECT_LOG { bigint id PK
        text aggregate_id
        int version
        uuid event_id }
    CORRECTIONS { bigint id PK
        text aggregate_id
        text kind "missing|behind|divergent|local_ahead|orphan"
        jsonb before
        jsonb after }
```

Schema source of truth: `src/reconciler/migrations/0001_init.sql` (forward-only; see runbook §9).
`effect_log` has **no unique constraint** on `(aggregate_id, version)` on purpose: the duplicate-effect metric is
`count(*) − count(distinct (aggregate_id, version))`, which must be a measurement, not an exception.

### Event status state machine

```mermaid
stateDiagram-v2
    [*] --> rejected: bad signature / malformed / unsupported
    [*] --> received: valid, persisted before ack
    received --> processed: applied (state + effect + idempotency record, one tx)
    received --> duplicate: idempotency key already recorded
    received --> stale: version <= aggregate.version
    received --> buffered: version gap
    received --> received: retryable error, attempts+1, next_attempt_at = now + jittered backoff
    received --> dead: terminal error OR retry budget exhausted
    buffered --> parked: gap window expired (park_and_alert)
    buffered --> processed: gap filled / last_writer_wins at expiry
    parked --> processed: gap filled (reconciler heals aggregate, buffer drains)
    parked --> stale: reconciler healed past it
    buffered --> stale: reconciler healed past it
    dead --> received: dlq redrive (fresh budget)
    dead --> dead: dlq discard (resolution recorded, aggregate healed by reconcile)
```

## 3. Sequences

### 3.1 Ingestion (persist, then ack)

```mermaid
sequenceDiagram
    participant S as Sender
    participant A as API
    participant P as Postgres
    S->>A: POST /webhooks (body, X-Signature)
    A->>A: verify HMAC (constant time) -> parse -> envelope -> derive key
    alt any check fails
        A->>P: INSERT events (status='rejected', reject_reason)
        P-->>A: committed
        A-->>S: 401 / 400 / 422 {code, event_id}
    else valid
        A->>P: INSERT events (status='received', traceparent)
        P-->>A: committed
        A-->>S: 202 {event_id, idempotency_key}
    end
```

### 3.2 Processing transaction (the heart of the project)

```mermaid
sequenceDiagram
    participant W as Worker
    participant P as Postgres
    W->>P: BEGIN
    W->>P: SELECT ... FROM events WHERE status='received' AND next_attempt_at<=now ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED
    W->>P: INSERT aggregates ON CONFLICT DO NOTHING / SELECT ... FOR UPDATE (aggregate lock)
    W->>P: SELECT idempotency WHERE key AND expires_at > now
    alt key seen
        W->>P: events.status='duplicate'
    else version decision
        Note over W: ordering.decide(current, event.version)
        W->>P: SAVEPOINT
        alt APPLY
            W->>W: domain.apply_event (TerminalError on poison)
            W->>W: sink(conn, event) (RetryableError on outage)
            W->>P: UPDATE aggregates / INSERT effect_log / UPSERT idempotency / events.status='processed'
            W->>P: RELEASE SAVEPOINT, then drain buffered successors in version order
        else BUFFER
            W->>P: INSERT buffer (window_expires = now + window) / status='buffered'
        else STALE
            W->>P: status='stale'
        end
        opt exception inside the savepoint
            W->>P: ROLLBACK TO SAVEPOINT (failed attempt leaves no trace)
            W->>P: attempts+1 and next_attempt_at (retry) OR status='dead' + INSERT dlq
        end
    end
    W->>P: COMMIT
```

### 3.3 Reconciliation

```mermaid
sequenceDiagram
    participant R as Reconciler
    participant X as Source of truth (list endpoint)
    participant P as Postgres
    loop each page
        R->>X: GET ?cursor=...
        X-->>R: items[{aggregate_id, version, state}]
    end
    loop each aggregate (own transaction)
        R->>P: lock aggregate row
        alt local.version < source.version
            R->>P: INSERT corrections(kind=missing|behind) / overwrite state+version / drain buffer
        else same version, different state
            R->>P: INSERT corrections(kind=divergent) / overwrite / drain buffer
        else local.version > source.version
            R->>P: INSERT corrections(kind=local_ahead) / NEVER overwrite
        else in sync
            R->>P: drain buffer only
        end
    end
    R->>P: flag aggregates the source no longer lists (kind=orphan) / INSERT reconcile_runs
```

## 4. Invariants (each is enforced by a named test)

| # | Invariant | Where it is proven |
|---|---|---|
| I1 | An acked event is durable | `test_valid_event_is_persisted_before_ack` |
| I2 | A rejected event is recorded, typed, and never processable | `test_bad_signatures_are_recorded_and_never_processable`, `test_malformed_events_get_a_typed_rejection_and_are_recorded` |
| I3 | Replaying any delivered event any number of times changes neither state nor effects | `test_replaying_any_delivered_event_any_number_of_times_changes_nothing` (Hypothesis) |
| I4 | A replay after the idempotency TTL is a no-op (version backstop) | `test_replay_after_ttl_is_noop` |
| I5 | Nothing is applied on top of missing history under `park_and_alert` | `test_gap_window_expiry_parks_and_alerts_under_default_policy` |
| I6 | A failed attempt leaves no trace; retries are budgeted and jittered | `test_retryable_failure_rolls_back_and_schedules_backoff`, `test_backoff_ceiling_grows_and_is_budgeted` |
| I7 | A poison event blocks only its own aggregate | `test_poison_message_only_blocks_its_own_aggregate` |
| I8 | Exactly one effect per `(aggregate, version)` regardless of worker count | `test_shuffled_stream_with_duplicates_converges_under_concurrency` |
| I9 | After chaos + time + one reconcile pass, state equals perfect in-order exactly-once delivery | `test_chaos_converges_*`, `bench/chaos_run.py` (1000 permutations) |
| I10 | The chaos harness can fail | `tests/property/test_harness_sensitivity.py` |

## 5. Locking and isolation

Order of acquisition, in every code path: **event row** (`FOR UPDATE SKIP LOCKED`) → **aggregate row**
(`FOR UPDATE`) → **buffered event rows of that aggregate** (`FOR UPDATE OF e`). Workers only *claim* events with
status `received`; buffered/parked rows are touched only by whoever holds the aggregate lock, so there is no lock
cycle. Default isolation (read committed) is sufficient because every decision is made while holding the aggregate
row lock. The reconciler takes the same aggregate lock per aggregate, in its own short transaction.

## 6. Configuration (`RECONCILER_*`, validated at boot by `pydantic-settings`)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | local postgres | connection string |
| `WEBHOOK_SECRET` / `WEBHOOK_SECRET_PREVIOUS` | – (required to serve) | HMAC secrets; previous accepted during rotation |
| `IDEMPOTENCY_TTL_SECONDS` | 604800 (7 d) | ADR-0001 |
| `BUFFER_WINDOW_SECONDS` | 300 | ADR-0002 |
| `GAP_POLICY` | `park_and_alert` | or `last_writer_wins` |
| `RETRY_BASE_SECONDS` / `RETRY_CAP_SECONDS` / `RETRY_MAX_ATTEMPTS` | 1 / 300 / 6 | backoff `U(0, min(cap, base·2^(n−1)))` |
| `WORKER_POLL_SECONDS` / `SWEEP_INTERVAL_SECONDS` | 0.5 / 15 | worker loop cadence |
| `RECONCILE_SOURCE_URL` / `RECONCILE_INTERVAL_SECONDS` | – / 900 | enables the reconciler job |
| `OPS_TOKEN` | – | when set, `/stats` and `/alerts` require `Authorization: Bearer <token>` |
| `MAX_BODY_BYTES` | 1048576 | larger bodies get 413 without being read or stored |
| `REJECTED_RETENTION_SECONDS` | 604800 | sweep deletes unauthenticated `rejected` rows older than this (bodies stored truncated to 2 KiB) |
| `RETENTION_SECONDS` | 2592000 | sweep deletes finished events, effect-log rows, corrections and resolved DLQ rows older than this, in batches of 5000 per table; `0` disables. Never deletes events still referenced by an idempotency record, an open DLQ entry or the buffer, nor the latest reconcile run |
| `ALERT_DLQ_DEPTH` / `ALERT_PARKED_EVENTS` / `ALERT_OLDEST_BUFFERED_SECONDS` / `ALERT_RECONCILE_AGE_SECONDS` | 1 / 1 / 900 / 3600 | alert thresholds |
| `OTEL_EXPORTER` | `none` | `console` or `otlp` (install the `otlp` extra) |
| `FAULT_DOWNSTREAM_OUTAGE_FILE` | – | fault injection for drills: sink fails while the file exists |

## 7. Error taxonomy (`reconciler.errors`)

`RetryableError` (server-side, transient → backoff, budgeted) · `TerminalError` (poison → DLQ immediately) ·
`IngestRejected` (client fault, typed `RejectCode`, never retried by us). Unexpected exceptions inside processing are
wrapped as retryable so they consume the budget instead of hot-looping. Each failure is logged **once**, in
`processor._fail` (or the worker-loop boundary), with `event_id`, `aggregate_id`, `attempts`, `error`.

## 8. Observability

* **Logs**: JSON on stderr, context in `extra` fields.
* **Traces**: the ingest span's `traceparent` is stored on the event row; every later `process_event` attempt
  (including after a DLQ re-drive) is a child span of the *original* webhook trace, with `outcome` and `attempt`
  attributes. `reconcile` spans carry `checked`/`corrected`.
* **Alerts** (`reconciler alerts`, exit 1 when any fires): `dlq_depth`, `parked_events`, `buffer_age`,
  `reconcile_stale`.
* **Metrics-as-SQL** (`reconciler stats`): `events_by_status`, `duplicate_effects`, `corrections_last_24h`
  (should trend to zero), `dlq_mean_time_to_resolution_s`.
