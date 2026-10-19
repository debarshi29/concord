# ADR-0005: Python 3.12 with hand-written SQL (psycopg 3); Rust deliberately not used
Status: accepted
Date: 2026-10-19

## Context
The language strategy reserves Rust for projects whose hard part is *latency in the hot path*. Here the hard part
is transaction semantics and ordering logic; a processing step is dominated by ~5 database round-trips, not CPU.

## Decision
* Python 3.12, `mypy --strict`, `ruff`, FastAPI for ingestion, psycopg 3 + `psycopg_pool`.
* No ORM: each transaction is a short sequence of explicit SQL statements so locks, isolation and boundaries
  are visible in code review. SQL is built only from constant fragments (no interpolated user input).
* The pattern library is pure Python with no framework imports so P3/P7/P8 can import it.

## Alternatives considered
* **Rust (tokio + sqlx).** Better at p99 and memory; rejected — the per-event CPU budget is microseconds against a
  ~5 ms DB path, so a rewrite would not move any NFR, and it would slow the 3-week target.
* **SQLAlchemy.** Better at portability and migrations; rejected for hiding the transaction boundaries this project
  is about.

## Consequences
Easy: fast iteration, shared tooling with the rest of the slate. Hard: GIL limits one worker process to one core
(scale with processes; the design is share-nothing). Reversibility: two-way door; the DB schema is the contract.

## Revisit criteria
Profiling (py-spy) shows > 30 % of worker time in Python rather than waiting on Postgres at target load.
