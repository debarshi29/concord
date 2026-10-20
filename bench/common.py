"""Shared benchmark plumbing: a database (env URL or embedded Postgres) and a migrated pool."""

from __future__ import annotations

import os
import platform
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from reconciler.db import Pool, make_pool, migrate

RESULTS = Path(__file__).parent / "results"


@contextmanager
def database(max_pool: int = 12) -> Iterator[tuple[str, Pool]]:
    url = os.environ.get("RECONCILER_TEST_DATABASE_URL")
    if url:
        pool = make_pool(url, 1, max_pool)
        migrate(pool)
        try:
            yield url, pool
        finally:
            pool.close()
        return
    import pgserver

    with tempfile.TemporaryDirectory(prefix="reconciler-bench-", ignore_cleanup_errors=True) as d:
        server = pgserver.get_server(d)  # type: ignore[attr-defined]
        pool = make_pool(str(server.get_uri()), 1, max_pool)
        migrate(pool)
        try:
            yield str(server.get_uri()), pool
        finally:
            pool.close()
            server.cleanup()


def environment(pool: Pool) -> dict[str, str | int]:
    with pool.connection() as conn:
        row = conn.execute("SHOW server_version").fetchone()
    assert row is not None
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": os.cpu_count() or 0,
        "postgres": str(row["server_version"]),
    }
