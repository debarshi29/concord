"""Run N seeded chaos permutations against the reconciler *and* the naive baseline.

    uv run python -m bench.chaos_run --runs 1000

Writes bench/results/chaos.csv (one row per permutation) and chaos_summary.json. Exit code 1 if any
permutation fails to converge or produces a duplicate effect. Fully reproducible from the seed range.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import time

from pydantic import SecretStr

from bench.common import RESULTS, database, environment
from reconciler.chaos.generator import ChaosRates, random_plan
from reconciler.chaos.naive import run_plan_naive
from reconciler.chaos.runner import reset_database, run_plan
from reconciler.clock import FakeClock
from reconciler.context import build_context
from reconciler.logs import configure_logging
from reconciler.patterns.retry import RetryPolicy
from reconciler.settings import GapPolicy, Settings

HOSTILE = ChaosRates()  # 15% drop, 30% duplicate, 15% corrupt, heavy reordering, 10% sink failures
MILD = ChaosRates(
    drop=0.02, duplicate=0.1, corrupt=0.02, reorder=0.1, tick=0.1, work=0.5, sink_failure=0.02
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1000)
    ap.add_argument("--seed0", type=int, default=0)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--profile", choices=["hostile", "mild"], default="hostile")
    ap.add_argument("--no-write", action="store_true", help="do not write result files (CI smoke)")
    args = ap.parse_args(argv)
    configure_logging(
        "CRITICAL"
    )  # the harness provokes warnings on purpose; keep the output readable
    rates = HOSTILE if args.profile == "hostile" else MILD
    suffix = "" if args.profile == "hostile" else "_mild"

    rows: list[dict[str, object]] = []
    with database() as (url, pool):
        env = environment(pool)
        settings = Settings(
            database_url=url,
            webhook_secret=SecretStr("test-secret"),
            buffer_window_seconds=60,
            retry_base_seconds=1,
            retry_cap_seconds=60,
            retry_max_attempts=4,
        )
        clock = FakeClock()
        ctx = build_context(
            pool, settings, clock=clock, retry=RetryPolicy(1, 60, 4, rng=random.Random(0))
        )
        started = time.monotonic()
        for i in range(args.runs):
            seed = args.seed0 + i
            plan = random_plan(random.Random(seed), rates)
            settings.gap_policy = (
                GapPolicy.PARK_AND_ALERT if seed % 2 == 0 else GapPolicy.LAST_WRITER_WINS
            )
            ctx.retry = RetryPolicy(1, 60, 4, rng=random.Random(seed))

            reset_database(ctx)
            t0 = time.monotonic()
            res = run_plan(ctx, plan, workers=args.workers)
            elapsed = time.monotonic() - t0
            reset_database(ctx)
            naive = run_plan_naive(ctx, plan)
            rows.append(
                {
                    "seed": seed,
                    "gap_policy": settings.gap_policy.value,
                    "events": len(plan.events),
                    "steps": len(plan.steps),
                    "converged": int(res.converged),
                    "duplicate_effects": res.duplicate_effects,
                    "unresolved_events": res.unresolved_events,
                    "drift_before_reconcile": res.drift_before_reconcile,
                    "corrections": res.corrections,
                    "dlq_depth": res.dlq_depth,
                    "rejected": res.rejected,
                    "naive_converged": int(naive.converged),
                    "naive_duplicate_effects": naive.duplicate_effects,
                    "seconds": round(elapsed, 3),
                }
            )
            if not res.converged:
                print(f"FAIL seed={seed}: {res.mismatches}")
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{args.runs} permutations", flush=True)
        total = time.monotonic() - started

    n = len(rows)
    passed = sum(int(r["converged"]) for r in rows)  # type: ignore[call-overload]
    naive_bad = sum(1 for r in rows if not r["naive_converged"] or r["naive_duplicate_effects"])
    summary = {
        "permutations": n,
        "converged": passed,
        "pass_rate_pct": round(100 * passed / n, 2) if n else 0,
        "duplicate_effects_total": sum(int(r["duplicate_effects"]) for r in rows),  # type: ignore[call-overload]
        "unresolved_events_total": sum(int(r["unresolved_events"]) for r in rows),  # type: ignore[call-overload]
        "aggregates_drifted_before_reconcile": sum(int(r["drift_before_reconcile"]) for r in rows),  # type: ignore[call-overload]
        "reconciler_corrections": sum(int(r["corrections"]) for r in rows),  # type: ignore[call-overload]
        "naive_failed_permutations": naive_bad,
        "naive_failure_rate_pct": round(100 * naive_bad / n, 2) if n else 0,
        "naive_duplicate_effects_total": sum(int(r["naive_duplicate_effects"]) for r in rows),  # type: ignore[call-overload]
        "profile": args.profile,
        "workers": args.workers,
        "wall_seconds": round(total, 1),
        "environment": env,
    }
    print(json.dumps(summary, indent=2))
    if not args.no_write:
        RESULTS.mkdir(exist_ok=True)
        with (RESULTS / f"chaos{suffix}.csv").open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)
        (RESULTS / f"chaos_summary{suffix}.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if passed == n and summary["duplicate_effects_total"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
