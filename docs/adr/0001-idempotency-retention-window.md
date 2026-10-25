# ADR-0001: Keep idempotency records 7 days; the per-aggregate version check is the backstop at the window's edge
Status: accepted
Date: 2026-10-19

## Context
Every applied event writes an `idempotency(key, outcome, expires_at)` row in the same transaction as the state
change. Unbounded retention grows without limit (~150 B/row; 30 M rows ≈ 4.5 GB at 50 ev/s × 7 d). Senders retry
for hours, occasionally days (a stuck sender queue draining after an outage). A replay arriving *after* its
idempotency record expired must not re-apply.

## Decision
* `idempotency_ttl = 7 days` (config `RECONCILER_IDEMPOTENCY_TTL_SECONDS`).
* Lookup ignores rows with `expires_at <= now`; a purge job (`worker sweep`) deletes expired rows in bounded batches.
* **At the edge of the window**, correctness is preserved by the version check, not the idempotency table: an
  event whose `version <= aggregate.version` is classified `stale` and has no effect. Idempotency records are
  therefore an optimisation plus an audit trail ("what outcome did we return?"), and the *safety* property rests
  on monotonically increasing per-aggregate versions.
* Key derivation: `Idempotency-Key` header, else envelope `event_id`, else SHA-256 of the canonical envelope.

## Alternatives considered
* **Keep forever.** Simplest; rejected for unbounded growth. Would have been better at forensic lookups.
* **24 hours.** Cheaper (0.6 GB), but shorter than observed sender back-off tails; relies on the version backstop
  more often. Rejected as needlessly aggressive.
* **Idempotency without version check** (key only). Fails at the window edge: a very late replay re-applies.
  Rejected — this is the exact bug the chaos harness is designed to catch (`test_replay_after_ttl_is_noop`).

## Consequences
Easy: bounded storage; safety independent of the TTL. Hard: for *unversioned* aggregates (not in v1) the TTL would
be a correctness boundary and would need to exceed the sender's maximum retry horizon. We are deliberately bad at
answering "was this key seen 8 days ago?" Reversibility: two-way door (config change; no schema change).

## Revisit criteria
Reopen if (a) a sender is onboarded whose events are not versioned per aggregate, or (b) idempotency storage
exceeds 10 % of total database size.

## Update (1.1.0)
Finished `events` rows are now trimmed too (`RECONCILER_RETENTION_SECONDS`, default 30 days). The two windows interact
safely: an event that an idempotency record still references is never deleted, and the sweep purges expired
idempotency records first, so retention shorter than this TTL simply leaves those events in place until the record
expires. The decision above is unchanged.
