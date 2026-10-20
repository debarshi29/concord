# Interview Q&A — Idempotent Webhook Reconciler (P14)

Part of the portfolio's Definition of Done: *every project ships with the interview questions it will provoke, and
answers an engineer can defend from the repo.* Every number below comes from this repository (`bench/results/`,
`docs/`, the test suite), so each answer can be backed up by opening a file.

How to use it: say the **short answer** first (≤ 30 s), then go one level deeper only if asked. The **probe** line is
what a good interviewer asks next.

---

## A. The pitch

**Q1. Describe the project in two minutes.**
Webhooks arrive twice, out of order, late, corrupted, or never. A naive handler duplicates invoices and lets state
drift from the sender's. I built a small service that makes the *effects* exactly-once on top of at-least-once
delivery and **proves it**: a chaos harness throws duplicated, reordered, delayed, dropped and corrupted deliveries at
it and asserts the final state equals a perfect in-order exactly-once delivery. Across 1,000 generated permutations it
converged 1,000 times with zero duplicate effects; a naive handler on the same plans was wrong 1,000 times
(3,124 duplicate effects). It is deliberately small — three weeks, one event type, one aggregate type — because its job
was to be the first shipped project and the pattern library the later ones import.
*Probe: "Why is it a good first project?" → small enough to finish, exercises every discipline (ADRs, property tests,
chaos, runbook, drills), and the template itself got debugged on it.*

**Q2. Who has this problem and what does it cost them?**
Anyone integrating with a payment, ERP or e-commerce system. The cost is duplicate invoices/orders and silent state
drift, plus the debugging time of "why does this customer's record differ from the source?". The metric I committed to
*before the first commit*: 100 % convergence over 1,000 permutations and zero duplicate effects under replay.

**Q3. What is "exactly-once effects over at-least-once delivery"? Isn't exactly-once impossible?**
Exactly-once *delivery* is impossible over an unreliable network. Exactly-once *effects* is achievable: deliver as
many times as you like, but make applying an event idempotent and atomic with its bookkeeping. Here the state change,
the effect row and the idempotency record commit in **one Postgres transaction**, so a replay sees the completed
record (or a version that is already covered) and does nothing. The honest limit: if the effect is an *external* call
inside that transaction it is only at-least-once; the outbox pattern closes that and is listed as an extension.

## B. Design decisions

**Q4. Walk me through what happens to one webhook.**
Verify HMAC over the raw body → validate the envelope → derive an idempotency key → **insert into `events` and
commit** → only then `202`. A worker claims a due event with `FOR UPDATE SKIP LOCKED`, locks the aggregate row, checks
the idempotency key, decides apply / buffer / stale from the per-aggregate version, then writes state + effect +
idempotency record atomically and drains any buffered successors. See `docs/lld.md` §3.

**Q5. Why acknowledge only after persisting?**
So a crash after the ack cannot lose an event (write-ahead). The cost is one durable write on the hot path: measured
HTTP ack latency is p50 2.1 ms / p95 2.6 ms / p99 2.9 ms against a 50 ms p95 target.

**Q6. How do you pick the idempotency key, and how long do you keep it?** (ADR-0001)
Precedence: `Idempotency-Key` header, then the sender's `event_id`, else a SHA-256 of the canonical content. Kept 7
days. The subtle part is the edge of the window: a replay after expiry is still harmless because the per-aggregate
**version check** classifies it `stale`. So idempotency records are an optimisation and audit trail; safety rests on
monotonic versions. I proved that with `test_replay_after_ttl_is_noop`.
*Probe: "What if the sender doesn't version events?" → then the TTL becomes a correctness boundary and must exceed the
sender's maximum retry horizon; that is ADR-0001's revisit trigger.*

**Q7. Events arrive out of order. What do you do?** (ADR-0002)
Per-aggregate versions are dense (1, 2, 3 …). A gap puts the event in a buffer for a bounded window (300 s). When the
missing version arrives the buffer drains in order inside the same transaction. If the window expires, the policy
decides: default **park-and-alert** (apply nothing on top of missing history; the reconciler heals), or opt-in
**last-writer-wins** (apply anyway, reconciler repairs divergence). It is a *business* decision — a wrong order state
costs more than a late one — so I made it explicit and configurable instead of accidental.

**Q8. Why not a global FIFO queue — it would be simpler.** (ADR-0003)
Because one poison message would block everything, which is exactly the failure the project exists to prevent.
Ordering only matters within an aggregate, so I serialise per aggregate with a row lock and let workers claim
*events*, not queue heads. Blast radius of a bad event = one aggregate; worker count is a pure throughput knob.
`test_poison_message_only_blocks_its_own_aggregate` shows the other aggregates flowing.

**Q9. Why Postgres as the queue instead of Kafka or SQS?** (ADR-0004)
Because "apply the change and commit the offset" would be a dual write — the very problem (duplicate effects) I'm
proving away. With Postgres, claim + state change + idempotency record is one transaction, retries are rows with
`next_attempt_at`, and the DLQ is a table I can query. Cost: a throughput ceiling in the low thousands/s and `events`
growth needing partitioning. Revisit at sustained >1,000 events/s.
*Probe: "At what point does that break?" → measured ~260 events/s per worker, plateau near 580/s at 4 workers on a
laptop with an embedded database; the ceiling is the single primary, and the next design is hash-partitioning
`events` or moving the write-ahead log to a broker.*

**Q10. Why Python and not Rust/Go?** (ADR-0005)
The hard part is transaction semantics, not CPU. I profiled with py-spy (including idle samples): **93.4 %** of a
worker's time is inside the psycopg driver (round-trips to Postgres) and **5.2 %** is my code, pydantic and JSON. My
own threshold for a rewrite was 30 %, so Python stays. Writing down the criterion *before* measuring is what makes
the decision defensible.

**Q11. How does retry work, and why jitter?**
Exponential backoff `U(0, min(cap, base·2^(n−1)))` — "full jitter" — with a **hard attempt budget** (default 6). Jitter
de-correlates clients so a recovering downstream isn't hit by a synchronized herd; the budget exists because unbudgeted
retries turn a partial failure into a self-inflicted outage. Exhausting the budget, or a terminal error, goes to the
DLQ with full context.

**Q12. What is the reconciler for if you already have idempotency?**
Idempotency handles *duplicates*; it cannot recover an event that was **never delivered**. The reconciler compares
`(version, state)` with the source's list endpoint, heals drift, and logs every correction. "We handle retries"
becomes "the systems agree". Corrections are also a health metric that should trend to zero.
*Probe: "Doesn't that mask bugs?" → yes, which is why corrections are counted and alerted on trend, and why chaos
measures drift with an independent audit before the reconciler runs (1,968 drifted aggregates found, 1,968 corrected).*

## C. Correctness and testing

**Q13. How do you know it works?**
A property-based chaos test that is the project's thesis: generate a plan (duplicate, reorder, delay, drop, corrupt,
transient sink failures), run it, assert final state equals the state of a perfect delivery, zero duplicate effects.
Hypothesis generates *and shrinks* the plan, so a failure becomes a minimal printable counterexample. I ran 1,000
seeded permutations under two traffic profiles: 1,000/1,000 each.

**Q14. How do you know the test can fail?**
`test_harness_sensitivity.py` sabotages the real system — removes the version check, removes the idempotency lookup,
loses the write-ahead — and asserts the harness notices each time. And the naive handler must fail the same plans:
1,000/1,000 on the hostile profile and 874/1,000 on a mild one. A test that cannot fail proves nothing.

**Q15. What is the oracle, and is it circular?**
The expected state is a plain dict fold of the ideal history, deliberately independent of the domain handler, so the
harness checks *delivery semantics*, not the domain logic against itself.

**Q16. Coverage is 97 %. Is that enough?**
Coverage only says what executed. I also ran mutation testing (cosmic-ray) over the pure decision modules: the first
run killed **81.5 %** of mutants, exposing real gaps — my property test almost never hit the `current + 1` boundary
because it drew `event` independently of `current`. After fixing the tests: **181/182 (99.5 %)**, 13 equivalent mutants
skipped with justified pragmas, and the one survivor argued equivalent in `docs/mutation.md`. I did not mutation-test
the Postgres-backed modules (≈ an hour per pass); those are pinned by the chaos and sensitivity tests.

**Q17. Why test against a real Postgres instead of mocks?** (ADR-0006)
`SKIP LOCKED`, row locks, savepoints and rollback of side effects are Postgres behaviours; a mock tests my assumptions
about them. Tests use an embedded Postgres locally (so `git clone && make dev && make test` needs no Docker) and a
`postgres:16` service container in CI. Time is injected, so backoff and window expiry are tested without sleeping.

**Q18. How do you test concurrency?**
Real threads against real Postgres: 2/4/8 workers over a shuffled stream with duplicates, and one hot aggregate hit by
8 workers; assert exactly one effect per `(aggregate, version)`. The chaos property also runs with 4 workers. Lock order
is always event row → aggregate row → that aggregate's buffered rows, so there is no cycle.

## D. Failure modes and operations

**Q19. A poison message arrives. What happens and what do you do?**
It passes the envelope (schema-valid) but fails business validation → `TerminalError` → straight to the DLQ with
`attempts: 1`. Only its aggregate stalls. The runbook: inspect, then either `dlq discard` + `reconcile` (sender bug) or
fix the handler + `dlq redrive` (our validation too strict). I drilled it end-to-end through the CLI: 3.1 s.

**Q20. The downstream goes down for an hour.**
Events retry with backoff, exhaust the budget, land in the DLQ; `alerts` fires on depth ≥ 1. Other aggregates are
unaffected; events behind dead ones are buffered, not lost. When the downstream is healthy: `dlq redrive --all`
(fresh budget). A failed attempt rolls back with its transaction, so nothing is half-applied. Drilled: 5.7 s.

**Q21. What was your 2 a.m. story and did you test it?**
Yes — it's in the review memo and in `tests/e2e/test_incident_drill.py`, which follows the runbook using only the
operator CLI. The standard I held myself to: a failure-mode table that was never exercised is fiction with nice
formatting. The runbook also lists what I did **not** drill (real DB failover, PITR restore).

**Q22. Worker crashes mid-transaction.**
The transaction rolls back, the event row lock is released, another worker claims it. A drill simulates a hard stop
inside the transaction and asserts nothing was half-applied and the event finishes later.

## E. Performance and scale

**Q23. What are the numbers?**
Single laptop, embedded Postgres: ack latency p95 2.6 ms; processing 259 / 512 / 579 / 532 events/s at 1 / 2 / 4 / 8
workers. These are evidence for the NFR table, not a capacity promise; I did not chase the plateau because the
revisit criteria (ADR-0004) are far away.

**Q24. How would it scale 10×?**
Add workers first (share-nothing, correctness independent of count). Then partition `events` by hash of
`aggregate_id` with one claim loop per partition, add retention/partitioning for `events` (≈ 10 GB/day at 50 ev/s if
every duplicate is retained), and only then consider moving the write-ahead log to a broker — accepting that the
atomic "apply + commit" would need an outbox/inbox design.

## F. Trade-offs, mistakes, and what I'd change

**Q25. What is it deliberately bad at?**
Low-latency convergence after a *lost* event (up to window + reconcile interval); a hot aggregate (row lock); cross-
aggregate invariants; unauthenticated ops endpoints; replay-attack protection (no signed timestamp window); and
exactly-once for *external* side effects. All listed in the README with the extension that fixes each.

**Q26. Tell me about a mistake you made and how you caught it.**
Three real ones. (1) An interrupted mutation run left a mutant (`attempt ** 1`) in `retry.py`; I caught it by diffing
against git before committing and now run mutation in an isolated worktree. (2) A `git add -A` leaked stale first-run
mutation results into `main`; the next PR replaced them and I now stage explicitly. (3) My own drill tests had races
(stopping at the first DLQ alert, wrong expectation about a version-0 aggregate row) — I fixed the tests, not the code,
because the system was right.

**Q27. If you had another week?**
(1) Outbox for outbound publication so external effects are exactly-once too; (2) nightly mutation testing of
`processor.py`; (3) `events` partitioning + retention; (4) signed-timestamp replay window and auth on the ops endpoints.
I'd do (1) first because it removes the one place the headline claim has an asterisk.

## G. Curveballs

**Q28. The sender reuses an idempotency key with a different body.** The first one wins: the key is recorded
with the first applied event and later ones classify as `duplicate`. Tested: `test_same_key_different_content_cannot_overwrite`.
Whether to alert on key reuse with different content is a reasonable extension.

**Q29. Two workers get the same event.** They can't: the claim is `FOR UPDATE SKIP LOCKED`, and even if one lost its
lock the aggregate-row lock and version check make the second apply a no-op (`stale`).

**Q30. What if the reconciler's source is itself wrong or stale?** It never overwrites a *newer* local aggregate
(`local_ahead` is flagged, not applied) and never deletes aggregates the source stops listing (`orphan` is flagged).
Healing only moves a replica forward to the source's version or fixes same-version divergence.
