# ADR-0003: Serialise per aggregate (row lock), not globally
Status: accepted
Date: 2026-10-19

## Context
A single poison event must not block healthy traffic. Ordering is only meaningful within one aggregate (order 7's
events relate to each other, not to order 8's).

## Decision
* Workers claim *events* (`FOR UPDATE SKIP LOCKED`, oldest `seq` first among due events) — there is no queue head to block on.
* Within the processing transaction the worker locks the aggregate row (`SELECT … FOR UPDATE`), creating it
  at version 0 on first sight. Two workers on the same aggregate serialise; different aggregates run in parallel.
* A failing event blocks exactly its aggregate: successors buffer (and later park) behind it.
* Worker count is therefore a pure throughput knob; correctness does not depend on it (verified by
  `tests/integration/test_concurrency.py` and the chaos harness with 1–4 worker threads).

## Alternatives considered
* **Global FIFO.** Trivially ordered, but one poison message blocks everything — the failure the project exists to
  prevent. Better at simplicity.
* **Static hash partitions with one consumer each** (Kafka-style). Strong per-key ordering and cheap, but couples
  throughput to partition count and makes rebalancing a project. Revisit with ADR-0004.
* **Advisory locks keyed by aggregate.** Equivalent, but a row lock also gives us the row we need to update.

## Consequences
Easy: blast radius of one bad event = one aggregate; scale-out by adding workers. Hard: aggregate-row lock
contention if one aggregate receives a very hot stream (not the case for orders); cross-aggregate invariants are
unsupported. Reversibility: two-way door until another component depends on cross-aggregate ordering.

## Revisit criteria
A single aggregate sustained above ~200 events/s, or any requirement spanning two aggregates.

## Update (1.1.0)
The claim query (`status = 'received' AND next_attempt_at <= now ORDER BY seq LIMIT 1`) is served by a partial index on
`(seq) WHERE status = 'received'` (migration `0002`). The original `(next_attempt_at, seq)` index could not return rows
in `seq` order, so the planner walked every processed row first: 14.9 ms per claim with 200 000 processed events ahead
of the backlog, 0.075 ms with the partial index. The decision above is unchanged.
