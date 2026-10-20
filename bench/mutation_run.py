"""Mutation-test the pure decision modules with cosmic-ray (mutmut has no native Windows support).

    uv run python -m bench.mutation_run

Coverage says what executed; the mutation score says what is actually asserted. Scope is the pure,
fast-to-test modules (ordering decision, retry policy, signing, domain handler, envelope). The
Postgres-backed `processor.py` is covered by property/chaos tests and is a candidate for a slow
nightly mutation job (docs/mutation.md). Writes bench/results/mutation.csv and mutation_survivors.txt;
exits 1 if the overall kill rate is below THRESHOLD.
"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "bench" / "results"
WORK = OUT / "mutation_work"
THRESHOLD = 0.90

TARGETS: dict[str, str] = {
    "src/reconciler/patterns/ordering.py": "tests/unit/test_ordering_and_retry.py",
    "src/reconciler/patterns/retry.py": "tests/unit/test_ordering_and_retry.py",
    "src/reconciler/signing.py": "tests/unit/test_signing_models_domain.py",
    "src/reconciler/domain.py": "tests/unit/test_signing_models_domain.py",
    "src/reconciler/models.py": "tests/unit/test_signing_models_domain.py",
}


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv
        [sys.executable, "-m", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )


def session_results(session: Path) -> list[tuple[dict[str, object], dict[str, object]]]:
    dump = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "cosmic_ray.cli", "dump", str(session)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [tuple(json.loads(line)) for line in dump.splitlines() if line.strip()]


def main() -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, object]] = []
    survivors: list[str] = []
    for module, test_file in TARGETS.items():
        name = Path(module).stem
        cfg, session = WORK / f"{name}.toml", WORK / f"{name}.sqlite"
        session.unlink(missing_ok=True)
        pytest_cmd = (
            f"{Path(sys.executable).as_posix()} -m pytest -x -q -p no:cacheprovider {test_file}"
        )
        cfg.write_text(
            "[cosmic-ray]\n"
            f"module-path = '{(ROOT / module).as_posix()}'\n"
            "timeout = 60.0\n"
            "excluded-modules = []\n"
            f"test-command = '{pytest_cmd}'\n"
            "[cosmic-ray.distributor]\nname = 'local'\n"
        )
        for step in (
            ("cosmic_ray.cli", "init", str(cfg), str(session)),
            # skip mutants on lines carrying `# pragma: no mutate` (each use is justified inline)
            ("cosmic_ray.tools.filters.pragma_no_mutate", str(session)),
            ("cosmic_ray.cli", "baseline", str(cfg)),
        ):
            proc = run(*step)
            if proc.returncode != 0:
                print(f"{name}: {step[0]} {step[1]} failed\n{proc.stdout}\n{proc.stderr}")
                return 2
        run("cosmic_ray.cli", "exec", str(cfg), str(session))
        outcomes: Counter[str] = Counter()
        skipped = 0
        for work_item, result in session_results(session):
            if result.get("worker_outcome") == "skipped":  # pragma-filtered equivalent mutant
                skipped += 1
                continue
            outcome = str(result.get("test_outcome"))
            outcomes[outcome] += 1
            if outcome == "survived":
                m = work_item["mutations"][0]  # type: ignore[index]
                survivors.append(
                    f"{module}:{m['start_pos'][0]} {m['operator_name']}#{m['occurrence']}"
                )
        total = sum(outcomes.values())
        killed = outcomes["killed"] + outcomes["incompetent"]
        rows.append(
            {
                "module": module,
                "mutants": total,
                "killed": outcomes["killed"],
                "incompetent": outcomes["incompetent"],
                "survived": outcomes["survived"],
                "skipped_by_pragma": skipped,
                "kill_rate_pct": round(100 * killed / total, 1) if total else 100.0,
            }
        )
        print(f"{module}: {killed}/{total} killed ({rows[-1]['kill_rate_pct']}%)", flush=True)

    total = sum(int(r["mutants"]) for r in rows)  # type: ignore[call-overload]
    killed = sum(int(r["killed"]) + int(r["incompetent"]) for r in rows)  # type: ignore[call-overload]
    rate = killed / total if total else 1.0
    rows.append(
        {
            "module": "TOTAL",
            "mutants": total,
            "killed": sum(int(r["killed"]) for r in rows),  # type: ignore[call-overload]
            "incompetent": sum(int(r["incompetent"]) for r in rows),  # type: ignore[call-overload]
            "survived": total - killed,
            "skipped_by_pragma": sum(int(r["skipped_by_pragma"]) for r in rows),  # type: ignore[call-overload]
            "kill_rate_pct": round(100 * rate, 1),
        }
    )
    with (OUT / "mutation.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    (OUT / "mutation_survivors.txt").write_text("\n".join(survivors) + ("\n" if survivors else ""))
    print(
        f"TOTAL: {killed}/{total} mutants killed ({100 * rate:.1f}%); survivors: {len(survivors)}"
    )
    return 0 if rate >= THRESHOLD else 1


if __name__ == "__main__":
    raise SystemExit(main())
