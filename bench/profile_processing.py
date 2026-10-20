"""Single-worker processing loop, for `py-spy record` (see `make profile`).

The flamegraph in bench/results/ shows where a worker's time goes: if it is mostly waiting on Postgres
(psycopg/socket frames) the Rust-rewrite criterion in ADR-0005 is not met.
"""

from __future__ import annotations

import json

from pydantic import SecretStr

from bench.common import database
from reconciler import signing
from reconciler.chaos.runner import reset_database
from reconciler.context import build_context
from reconciler.ingest import ingest
from reconciler.logs import configure_logging
from reconciler.processor import process_next
from reconciler.settings import Settings

SECRET = "bench-secret"  # noqa: S105


def main() -> None:
    configure_logging("CRITICAL")
    with database() as (url, pool):
        ctx = build_context(pool, Settings(database_url=url, webhook_secret=SecretStr(SECRET)))
        reset_database(ctx)
        for i in range(4000):
            body = json.dumps(
                {
                    "event_id": f"p-{i}",
                    "type": "order.updated",
                    "aggregate_id": f"order-{i % 100}",
                    "version": i // 100 + 1,
                    "data": {"status": "paid", "amount_cents": i},
                }
            ).encode()
            ingest(ctx, body, signing.sign(body, SECRET))
        while process_next(ctx) is not None:
            pass


if __name__ == "__main__":
    main()
