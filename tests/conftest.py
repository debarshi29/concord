"""Shared fixtures. Real Postgres, never mocks (ADR-0006): `RECONCILER_TEST_DATABASE_URL` if set
(CI service container), otherwise an embedded server started in a temp dir."""

from __future__ import annotations

import os
import random
import tempfile
from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from reconciler.clock import FakeClock
from reconciler.context import AppContext, build_context
from reconciler.db import Pool, make_pool, migrate, truncate_all
from reconciler.patterns.retry import RetryPolicy
from reconciler.settings import Settings

SECRET = "test-secret"


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    url = os.environ.get("RECONCILER_TEST_DATABASE_URL")
    if url:
        yield url
        return
    import pgserver

    with tempfile.TemporaryDirectory(prefix="reconciler-pg-", ignore_cleanup_errors=True) as d:
        server = pgserver.get_server(d)  # type: ignore[attr-defined]
        try:
            yield str(server.get_uri())
        finally:
            server.cleanup()


@pytest.fixture(scope="session")
def pool(database_url: str) -> Iterator[Pool]:
    p = make_pool(database_url, min_size=1, max_size=12)
    migrate(p)
    yield p
    p.close()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        database_url=database_url,
        webhook_secret=SecretStr(SECRET),
        buffer_window_seconds=60,
        retry_base_seconds=1,
        retry_cap_seconds=60,
        retry_max_attempts=4,
        idempotency_ttl_seconds=3600,
    )


@pytest.fixture
def ctx(pool: Pool, settings: Settings, clock: FakeClock) -> AppContext:
    context = build_context(
        pool,
        settings,
        clock=clock,
        retry=RetryPolicy(
            settings.retry_base_seconds,
            settings.retry_cap_seconds,
            settings.retry_max_attempts,
            rng=random.Random(1234),
        ),
    )
    truncate_all(pool)
    return context
