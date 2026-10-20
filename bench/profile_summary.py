"""Attribute py-spy samples (raw format, --idle) inside `process_next` to where the time goes.

    uv run py-spy record --idle --subprocesses --format raw -o bench/results/profile_raw.txt -- \
        python -m bench.profile_processing
    uv run python -m bench.profile_summary

Evidence for ADR-0005: if "reconciler logic + pydantic + json" were > 30 % the Rust criterion would trigger.
"""

from __future__ import annotations

import collections
import re
import sys
from pathlib import Path

RAW = Path(__file__).parent / "results" / "profile_raw.txt"
OUT = Path(__file__).parent / "results" / "profile_summary.txt"


def classify(leaf: str) -> str:
    if "psycopg" in leaf:
        return "psycopg driver (query send/recv/decode, incl. waiting on Postgres)"
    if "pydantic" in leaf:
        return "pydantic validation"
    if "reconciler" in leaf or "processor.py" in leaf:
        return "reconciler logic"
    if re.search(r"json|decoder|encoder", leaf):
        return "json"
    return "other"


def main() -> int:
    total = 0
    cats: collections.Counter[str] = collections.Counter()
    for line in RAW.read_text().splitlines():
        stack, _, n = line.rpartition(" ")
        if "process_next" not in stack:
            continue
        count = int(n)
        total += count
        cats[classify(stack.split(";")[-1])] += count
    lines = [f"samples inside process_next: {total} (py-spy 200 Hz, --idle)"]
    lines += [f"{100 * v / total:5.1f}%  {k}" for k, v in cats.most_common()]
    own = sum(
        v for k, v in cats.items() if k in {"reconciler logic", "pydantic validation", "json"}
    )
    lines.append(
        f"our Python (logic + pydantic + json): {100 * own / total:.1f}%  (ADR-0005 threshold: 30%)"
    )
    OUT.write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
