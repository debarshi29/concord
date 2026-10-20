# Mutation testing

Coverage says what *executed*; the mutation score says what is actually *asserted*. A mutant is a one-token change
to the source (`==` → `<=`, `1` → `0`, `True` → `False`); a test suite that does not fail on it does not really
pin that behaviour.

**Tool**: `cosmic-ray` (mutmut has no native Windows support). **Run**: `make mutate`
(`bench/mutation_run.py`; CI runs it on every push to `main`). **Gate**: kill rate ≥ 90 %.
**Scope**: the pure, fast-to-test decision modules — ordering, retry policy, signing, domain handler, envelope.

## Result (recorded in `bench/results/mutation.csv`)

| Module | Mutants | Killed | Survived | Skipped (pragma) | Kill rate |
|---|---:|---:|---:|---:|---:|
| `patterns/ordering.py` | 47 | 46 | 1 | 0 | 97.9 % |
| `patterns/retry.py` | 84 | 84 | 0 | 0 | 100 % |
| `signing.py` | 4 | 4 | 0 | 11 | 100 % |
| `domain.py` | 29 | 29 | 0 | 2 | 100 % |
| `models.py` | 18 | 18 | 0 | 0 | 100 % |
| **Total** | **182** | **181** | **1** | **13** | **99.5 %** |

## What the first run found (81.5 % → 99.5 %)

The first run killed 159 of 195 mutants. Every survivor was either a real gap or an equivalent mutant:

* **Real test gaps, fixed by adding tests**
  * `ordering.decide` was only checked with *even* current versions, so `current + 1` → `current | 1` survived.
    The Hypothesis property drew `event` independently of `current`, so it almost never hit the `current + 1` boundary;
    it is now centred on the boundary (`delta ∈ [-4, 4]`), and the table has odd versions.
  * `RetryPolicy`: immutability (`frozen=True`), `cap == base` and `max_attempts == 1` being legal, the float-overflow
    guard on the exponent, `rng` excluded from equality/repr, and the jitter **lower bound** being 0 (not 1).
  * Envelope/domain length and range limits had no boundary tests (`max_length` 128 vs 129, `version` up to
    2 000 000 000, `amount_cents == 0`).
* **Equivalent mutants, skipped with a justified `# pragma: no mutate`** (13 in total)
  * `str | None` in a type annotation (unevaluated under `from __future__ import annotations`): 11 mutants.
  * `amount_cents` default value: unobservable because only fields *present* in the patch are applied
    (`exclude_unset`): 2 mutants.

## The one survivor

`ordering.py:23` `==` → `<=` in `if event_version == current_version + 1`. It is **equivalent**: the preceding
branch already returns `STALE` for `event_version <= current_version`, so for every input that reaches this line
`event_version <= current_version + 1` and `event_version == current_version + 1` are the same predicate. Left in the
report rather than hidden behind a pragma, so the number is honest.

## Not covered (and why)

`processor.py`, `reconcile.py` and `ingest.py` are Postgres-backed; one mutant costs a multi-second integration run, so
a full pass takes roughly an hour. Their behaviour is pinned by the property/chaos tests and the harness-sensitivity
tests (which are a hand-written mutation test of the three most dangerous mutations: no idempotency lookup, no version
check, lost write-ahead). A slow nightly mutation job over those modules is the next step.

> Operational note: cosmic-ray edits files in place. An interrupted run once left a mutant (`attempt ** 1`) in
> `retry.py`; it was caught by diffing against git before anything was committed. `make mutate` should be run in a
> clean worktree (`git worktree add ../x && cd ../x && make mutate`), which is how the recorded run was made.
