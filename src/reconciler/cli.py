"""`reconciler` command line: serve, worker, migrate, reconcile, stats, alerts, dlq {list,redrive,discard}."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from collections.abc import Callable, Sequence
from typing import cast
from uuid import UUID

from pydantic import SecretStr

from reconciler.clock import SystemClock
from reconciler.context import AppContext, build_context
from reconciler.db import Pool, make_pool, migrate
from reconciler.logs import configure_logging
from reconciler.patterns import dlq
from reconciler.patterns.retry import RetryPolicy
from reconciler.processor import sweep
from reconciler.reconcile import HttpSource, reconcile
from reconciler.settings import Settings, get_settings
from reconciler.sinks import noop_sink
from reconciler.stats import alert_dicts, check_alerts, collect_stats
from reconciler.telemetry import setup_tracing
from reconciler.worker import run_forever, run_until_idle

_pools: list[Pool] = []


def _pool(settings: Settings, min_size: int, max_size: int) -> Pool:
    pool = make_pool(settings.database_url, min_size, max_size)
    _pools.append(pool)
    return pool


def _ctx(settings: Settings) -> AppContext:
    return build_context(_pool(settings, settings.pool_min_size, settings.pool_max_size), settings)


def _print(obj: object) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _cmd_migrate(args: argparse.Namespace, settings: Settings) -> int:
    _print({"applied": migrate(_pool(settings, 1, 2))})
    return 0


def _cmd_serve(args: argparse.Namespace, settings: Settings) -> int:
    import uvicorn

    from reconciler.api import create_app

    ctx = _ctx(settings)
    uvicorn.run(create_app(ctx), host=args.host, port=args.port, log_config=None)
    return 0


def _cmd_worker(args: argparse.Namespace, settings: Settings) -> int:
    ctx = _ctx(settings)
    if args.until_idle:
        outcomes = run_until_idle(ctx)
        _print(
            {
                "processed": len(outcomes),
                "by_kind": {
                    k: sum(o.kind == k for o in outcomes) for k in {o.kind for o in outcomes}
                },
            }
        )
        return 0
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    source = HttpSource(settings.reconcile_source_url) if settings.reconcile_source_url else None
    run_forever(ctx, stop, source)
    return 0


def _cmd_reconcile(args: argparse.Namespace, settings: Settings) -> int:
    if not settings.reconcile_source_url:
        print("RECONCILER_RECONCILE_SOURCE_URL is not set", file=sys.stderr)
        return 2
    report = reconcile(_ctx(settings), HttpSource(settings.reconcile_source_url))
    _print(report.__dict__)
    return 0


def _cmd_sweep(args: argparse.Namespace, settings: Settings) -> int:
    result = sweep(_ctx(settings))
    _print(
        {
            "parked": result.parked,
            "force_applied": result.force_applied,
            "purged": result.purged,
            "rejected_purged": result.rejected_purged,
            "retention_purged": result.retention_purged,
        }
    )
    return 0


def _cmd_stats(args: argparse.Namespace, settings: Settings) -> int:
    _print(collect_stats(_ctx(settings)))
    return 0


def _cmd_alerts(args: argparse.Namespace, settings: Settings) -> int:
    alerts = check_alerts(_ctx(settings))
    _print(alert_dicts(alerts))
    return 1 if alerts else 0  # non-zero exit lets cron/monitoring treat any alert as a failure


def _cmd_dlq(args: argparse.Namespace, settings: Settings) -> int:
    ctx = _ctx(settings)
    now = ctx.clock.now()
    with ctx.pool.connection() as conn, conn.transaction():
        if args.dlq_cmd == "list":
            _print(dlq.list_open(conn, args.limit))
        elif args.dlq_cmd == "redrive":
            if not args.all and not args.id:
                print("specify --id <uuid> or --all", file=sys.stderr)
                return 2
            _print({"redriven": dlq.redrive(conn, now, None if args.all else UUID(args.id))})
        elif args.dlq_cmd == "discard":
            _print({"discarded": dlq.discard(conn, now, UUID(args.id))})
    return 0


def _cmd_openapi(args: argparse.Namespace, settings: Settings) -> int:
    from reconciler.api import create_app

    ctx = AppContext(  # schema generation never touches the database
        pool=cast(Pool, None),
        settings=settings.model_copy(update={"webhook_secret": SecretStr("openapi")}),
        clock=SystemClock(),
        sink=noop_sink,
        retry=RetryPolicy(1.0, 1.0, 1),
    )
    print(json.dumps(create_app(ctx).openapi(), indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="reconciler", description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply pending schema migrations").set_defaults(fn=_cmd_migrate)
    s = sub.add_parser("serve", help="run the ingestion API")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8000)
    s.set_defaults(fn=_cmd_serve)
    w = sub.add_parser("worker", help="run the processing worker")
    w.add_argument("--until-idle", action="store_true", help="drain due events and exit")
    w.set_defaults(fn=_cmd_worker)
    sub.add_parser("reconcile", help="run one reconciliation pass now").set_defaults(
        fn=_cmd_reconcile
    )
    sub.add_parser(
        "sweep", help="resolve expired gap windows and purge idempotency rows"
    ).set_defaults(fn=_cmd_sweep)
    sub.add_parser("stats", help="print operational stats").set_defaults(fn=_cmd_stats)
    sub.add_parser("alerts", help="evaluate alerts; exit 1 if any firing").set_defaults(
        fn=_cmd_alerts
    )
    sub.add_parser("openapi", help="print the OpenAPI document").set_defaults(fn=_cmd_openapi)
    d = sub.add_parser("dlq", help="operate the dead-letter queue")
    dsub = d.add_subparsers(dest="dlq_cmd", required=True)
    dl = dsub.add_parser("list")
    dl.add_argument("--limit", type=int, default=100)
    dr = dsub.add_parser("redrive")
    dr.add_argument("--id")
    dr.add_argument("--all", action="store_true")
    dd = dsub.add_parser("discard")
    dd.add_argument("--id", required=True)
    d.set_defaults(fn=_cmd_dlq)
    return p


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level)
    setup_tracing(settings)
    fn: Callable[[argparse.Namespace, Settings], int] = args.fn
    try:
        return fn(args, settings)
    finally:
        for pool in _pools:
            pool.close()
        _pools.clear()


if __name__ == "__main__":
    raise SystemExit(main())
