# ADR-0006: Test against a real Postgres (embedded locally, service container in CI), not mocks
Status: accepted
Date: 2026-10-19

## Context
The behaviours that matter — `SKIP LOCKED`, row locks, savepoints, transactional rollback of side effects — are
properties of Postgres, not of our Python. Mocking the database would test our assumptions about it.
`git clone && make dev && make test` must work on a machine with no Docker and no Postgres.

## Decision
`tests/conftest.py` resolves the database in this order: `RECONCILER_TEST_DATABASE_URL` (CI uses a `postgres:16`
service) → an embedded server from the `pgserver` dev dependency started in a temp directory. Tests reset tables
with `TRUNCATE` between cases. Time is injected (`FakeClock`) so backoff and window expiry are tested without sleeping.

## Alternatives considered
* **testcontainers.** Production-identical, but requires a running Docker daemon; breaks the clone test. Used by CI
  indirectly via service containers.
* **SQLite.** No `SKIP LOCKED`/`FOR UPDATE`; would hide the concurrency behaviour under test.
* **Mocks.** Fast, but cannot catch lock-ordering or transaction-boundary bugs.

## Consequences
Easy: zero-setup tests with real semantics; concurrency tests are real. Hard: embedded Postgres version may differ
from production (pinned in CI to 16; `pgserver` bundles 16.x); first run downloads wheels. Two-way door.

## Revisit criteria
Embedded and production major versions diverge, or the suite exceeds 5 minutes.
