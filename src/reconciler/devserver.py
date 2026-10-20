"""`make run`: embedded Postgres + API + worker in one process, no Docker required.

For local exploration only; production uses the compose file / a managed Postgres.
"""

from __future__ import annotations

import threading
from pathlib import Path

import uvicorn
from pydantic import SecretStr

from reconciler.api import create_app
from reconciler.context import build_context
from reconciler.db import make_pool, migrate
from reconciler.logs import configure_logging
from reconciler.settings import Settings
from reconciler.worker import run_forever


def main() -> None:
    import pgserver  # dev dependency

    pgdata = Path(".pgdata")
    server = pgserver.get_server(pgdata, cleanup_mode=None)  # type: ignore[attr-defined]
    settings = Settings(database_url=server.get_uri(), webhook_secret=SecretStr("dev-secret"))
    configure_logging(settings.log_level)
    pool = make_pool(settings.database_url)
    migrate(pool)
    ctx = build_context(pool, settings)
    stop = threading.Event()
    threading.Thread(target=run_forever, args=(ctx, stop), daemon=True).start()
    print("API on http://127.0.0.1:8000  (secret: dev-secret; docs at /docs)")
    try:
        uvicorn.run(create_app(ctx), host="127.0.0.1", port=8000, log_config=None)
    finally:
        stop.set()


if __name__ == "__main__":
    main()
