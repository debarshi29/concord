# Runbook

Written to be executed at 3 a.m. by an engineer who has never seen this codebase. Every procedure uses only the
`reconciler` CLI (and, where stated, `psql`). Each entry says whether it has been **drilled**; a procedure marked
*not drilled* is a known gap, not a validated one.

| § | Situation | Drilled by |
|---|---|---|
| 3 | `dlq_depth` alert — downstream outage | `tests/e2e/test_incident_drill.py::test_drill_downstream_outage_to_dlq_to_redrive` |
| 4 | `dlq_depth` alert — poison message | `…::test_drill_poison_message_to_discard_and_reconcile` |
| 5 | `parked_events` alert — version gap never fills | `…::test_drill_parked_events_to_reconcile` |
| – | Worker killed mid-transaction | `…::test_drill_crash_mid_transaction_loses_nothing` |
| 6 | Reconciler stale / corrections trending up | partially (`test_reconcile_staleness_alert_only_when_a_source_is_configured`) |
| 7 | Database unavailable | partially (`test_readiness_fails_when_database_is_unreachable`) |
| 8 | Webhook secret rotation | `test_secret_rotation_accepts_previous_secret` |
| 9 | Migration / rollback | **not drilled** (needs managed backups) |

## 1. Orientation

```text
reconciler alerts            # prints firing alerts as JSON; exit code 1 if any fire (use in cron/monitoring)
reconciler stats             # events by status, dlq depth, buffer depth, duplicate effects, corrections, last reconcile
reconciler dlq list          # open dead-letter entries with attempts, last error, aggregate and version
reconciler dlq redrive --all | --id <uuid>    # give dead events a fresh retry budget
reconciler dlq discard --id <uuid>            # give up on one dead event (the reconciler heals the aggregate)
reconciler sweep             # expire gap windows per policy, purge expired idempotency rows, rejections and old history
reconciler reconcile         # run one reconciliation pass now (needs RECONCILER_RECONCILE_SOURCE_URL)
reconciler worker --until-idle   # drain everything due and exit
```

Environment: `RECONCILER_DATABASE_URL` is required for every command. Full list: `docs/lld.md` §6.
If `RECONCILER_OPS_TOKEN` is set, `/stats` and `/alerts` need `-H 'Authorization: Bearer <token>'`; `/healthz` and
`/readyz` never do. The CLI reads the database directly and is unaffected.

## 2. First five minutes (any page)

1. `reconciler alerts` — which alert fired? Jump to its section.
2. `reconciler stats` — read `events_by_status`. Healthy: almost everything `processed`, `dead = 0`, `parked = 0`,
   `duplicate_effects = 0`.
3. `curl -s localhost:8000/readyz` — if 503, go to §7 first; nothing else matters until the database is back.
4. **`duplicate_effects` is the one number that must never be non-zero.** If it is, stop and escalate: it means the
   exactly-once guarantee was violated. Preserve `effect_log` and `events` for the post-mortem.

## 3. `dlq_depth` — downstream outage

**Symptom**: `alerts` shows `dlq_depth`; `dlq list` shows many entries whose `last_error` is the same infrastructure
error (e.g. `RetryableError: downstream ...`) and `attempts` equal to the budget (default 6).

**Scope check** — other aggregates are unaffected by design (ADR-0003); `stats` shows them still `processed`. Events
behind a dead one are `buffered`, not lost.

**Fix**
1. Confirm the downstream is healthy again (the sink is the only thing that raised).
2. `reconciler dlq redrive --all` — returns every open dead event to the queue with a **fresh** attempt budget.
3. Wait for the worker (or `reconciler worker --until-idle`).

**Verify**: `reconciler alerts` exits 0; `stats` shows `dlq_depth: 0`, `buffer_depth: 0`, `duplicate_effects: 0`.
Effects were applied exactly once: a failed attempt rolls back with its transaction, so nothing was half-done.

*Drilled*: 5.7 s from first alert to verified resolution on a developer laptop.

## 4. `dlq_depth` — poison message

**Symptom**: `dlq list` shows **one** entry (or a few) with `last_error` starting `TerminalError: invalid order patch
(<fields>)` and `attempts: 1`. Terminal errors skip retries by design — retrying cannot make bad data good.

**Scope**: exactly one aggregate is stalled; its later versions sit in `buffered` (and become `parked` after the gap
window). Everything else flows.

**Fix**
1. Decide: is the event wrong (sender bug) or is our validation too strict?
   * Sender bug → `reconciler dlq discard --id <event_id>`, then heal the aggregate from the source of truth:
     `reconciler reconcile`. The reconciler overwrites the aggregate to the source's version and drains the buffered
     successors; the poison event itself is retained in `events` for the sender to investigate.
   * Our validation too strict → fix and deploy the handler, then `reconciler dlq redrive --id <event_id>`.
2. Do **not** hand-edit `aggregates`; the reconciler logs every correction in `corrections`.

**Verify**: `alerts` exits 0; the aggregate's version equals the source's; `stats.buffer_depth` is 0.

*Drilled*: 3.1 s.

## 5. `parked_events` — a version gap that never fills

**Symptom**: `alerts` shows `parked_events`. Events in `parked` status are waiting behind a missing version that did
not arrive within the window (default 300 s). Under the default policy nothing is applied on top of missing history
(ADR-0002), so the aggregate is **stale but never wrong**.

**Fix**
1. `reconciler reconcile` — heals the aggregate from the source and drains the parked events (they become `stale` if
   the heal covered them, `processed` if they are still ahead).
2. If `RECONCILER_RECONCILE_SOURCE_URL` is unset on the operator shell, set it and retry.
3. Recurrent parking means the sender is losing events or the window is too short: see ADR-0002's revisit criteria.

**Verify**: `alerts` exits 0.

*Drilled*: 2.5 s. (`reconciler sweep` is what turns an expired buffered event into `parked`; the worker loop does it
every `SWEEP_INTERVAL_SECONDS`.)

## 6. Reconciler stale / corrections trending up

* `reconcile_stale` alert: the reconciler has not completed a pass within `ALERT_RECONCILE_AGE_SECONDS`. Run
  `reconciler reconcile` by hand; if it errors, the source's list endpoint is unreachable or returning non-2xx — the
  command prints the HTTP error. Drift persists until it succeeds but nothing else is affected.
* `stats.corrections_last_24h` **should trend to zero**. A rising number means the main path is losing events:
  1. Count rejections by reason: `select reject_reason, count(*) from events where status='rejected' and received_at > now() - interval '1 day' group by 1;`
     A spike in `invalid_signature` points at secret rotation (§8); `invalid_envelope` at a sender change.
  2. `select kind, count(*) from corrections where corrected_at > now() - interval '1 day' group by 1;`
     mostly `missing` ⇒ events never delivered (sender/network); mostly `divergent` ⇒ a handler bug — escalate.
* `local_ahead` and `orphan` corrections are logged once per standing condition, not once per run, so a persistent
  flag does not inflate `corrections_last_24h`; it is logged again if the aggregate is healed and later drifts again.
* `local_ahead` corrections are flags, never overwrites: the replica has newer data than the source's list endpoint
  (usually a lagging source). `orphan` flags aggregates the source no longer lists.

## 7. Database unavailable

**Symptom**: `/readyz` returns 503; workers log `worker iteration failed`; ingestion returns 5xx so senders retry
(at-least-once is the contract).

1. Restore the database (managed failover / restart). No application-side action is needed: pools reconnect and
   workers resume; an interrupted transaction rolled back, so nothing is half-applied.
2. After recovery run `reconciler stats`: expect a backlog of `received` events draining, then
   `reconciler alerts`. Run `reconciler reconcile` once to close any drift caused by events senders gave up on.

*Partially drilled*: readiness failure is tested; a real failover is **not** drilled.

## 8. Webhook secret rotation

1. Set `RECONCILER_WEBHOOK_SECRET_PREVIOUS` to the **current** secret and `RECONCILER_WEBHOOK_SECRET` to the new one;
   restart the API. Both are accepted.
2. Give the sender the new secret. Watch `select reject_reason, count(*) from events where status='rejected' group by 1;`
   — `invalid_signature` should stay flat.
3. Once the sender uses only the new secret, unset `RECONCILER_WEBHOOK_SECRET_PREVIOUS` and restart.

An empty secret never validates (tested), and the API refuses to boot without one.

## 9. Migrations and rollback

* Migrations are **forward-only** and versioned (`src/reconciler/migrations/NNNN_*.sql`); `reconciler migrate` applies
  pending ones under an advisory lock that is held until the migration has committed, so concurrent deploys are safe.
  Each migration is its own transaction: if migration N fails, migrations before it stay applied, N leaves no trace,
  the lock is released and a rerun retries N. Deploy the schema **before** the code that uses
  it (expand → migrate → contract).
* **Rollback story**: restore from backup / point-in-time recovery to just before the migration, then redeploy the
  previous release. There is no down-migration. This procedure is **not drilled**: it depends on the managed
  database's backup tooling, which this repository does not own.

## 10. What has *not* been exercised (honest list)

Real database failover (§7), PITR restore (§9), a sink that performs a genuine external side effect (the sink runs
inside the transaction; a real call there is at-least-once — the outbox extension closes it), and behaviour above
~600 events/s on a single node (see `bench/results/throughput.csv`).

Also not load-tested: the retention sweep at production table sizes (it is batch-bounded, 5000 rows per table per
sweep, so a large first-time backlog drains over many sweeps), and the `OPS_TOKEN` path beyond the integration tests
and one manual run in the compose stack.

## 11. Retention and storage

* The sweep deletes finished history older than `RECONCILER_RETENTION_SECONDS` (default 30 days; `0` keeps everything):
  `events`, `effect_log`, `corrections`, resolved `dlq` rows and old `reconcile_runs`. Unauthenticated rejections go
  after `RECONCILER_REJECTED_RETENTION_SECONDS` (7 days). `reconciler sweep` prints how many rows each pass removed
  (`purged`, `rejected_purged`, `retention_purged`).
* Storage plateaus at roughly the retention window of traffic: about 10 GB per day at 50 events/s if every duplicate
  is kept, so ~300 GB at 30 days. Shorten the window or partition `events` if that is too much.
* Cost of the window: after it, a `duplicate_effects` audit can no longer see older effects, and a standing
  `local_ahead` / `orphan` flag is logged once more when its old correction row ages out.
* Nothing referenced or in flight is deleted (an event with an idempotency record, an open DLQ entry or a buffer row,
  the latest reconcile run), so shortening the window below the idempotency TTL is safe; those events simply wait.
* Backlog after enabling or shortening retention drains at 5000 rows per table per sweep (every 15 s by default).
  To watch it: `select count(*) from events where received_at < now() - interval '30 days';`.
* Oversized requests: bodies over `RECONCILER_MAX_BODY_BYTES` (1 MiB) get `413` unread and unrecorded, so a flood shows
  in the API logs (`webhook body over limit`), not in the `events` table.
