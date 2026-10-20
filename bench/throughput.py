"""Measure ack latency (over real HTTP) and processing throughput vs worker count.

    uv run python -m bench.throughput [--events 3000] [--requests 1000]

Writes bench/results/throughput.csv and throughput_env.json. Numbers are for the machine in the JSON
file (embedded Postgres, single node); they are evidence for the NFR table in docs/hld.md, not a promise.
"""

from __future__ import annotations

import argparse
import csv
import json
import socket
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import uvicorn
from pydantic import SecretStr

from bench.common import RESULTS, database, environment
from reconciler import signing
from reconciler.api import create_app
from reconciler.chaos.runner import reset_database
from reconciler.context import AppContext, build_context
from reconciler.ingest import ingest
from reconciler.logs import configure_logging
from reconciler.processor import process_next
from reconciler.settings import Settings

SECRET = "bench-secret"  # noqa: S105


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _pct(values: list[float], p: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))]


def bench_http_ack(ctx: AppContext, requests: int) -> list[dict[str, object]]:
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(ctx), host="127.0.0.1", port=port, log_level="error", log_config=None
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    while not server.started:
        time.sleep(0.05)
    lat: list[float] = []
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}") as client:
            for i in range(requests + 50):  # first 50 are warm-up
                body = json.dumps(
                    {
                        "event_id": f"h-{i}",
                        "type": "order.updated",
                        "aggregate_id": f"order-{i % 200}",
                        "version": i // 200 + 1,
                        "data": {"status": "paid", "amount_cents": i},
                    }
                ).encode()
                t0 = time.perf_counter()
                r = client.post(
                    "/webhooks", content=body, headers={"X-Signature": signing.sign(body, SECRET)}
                )
                dt = (time.perf_counter() - t0) * 1000
                assert r.status_code == 202, r.text
                if i >= 50:
                    lat.append(dt)
    finally:
        server.should_exit = True
        thread.join(timeout=10)
    return [
        {
            "metric": "http_ack_latency_ms",
            "variant": f"p{p}",
            "n": requests,
            "value": round(_pct(lat, p), 2),
        }
        for p in (50, 95, 99)
    ] + [
        {
            "metric": "http_ack_latency_ms",
            "variant": "mean",
            "n": requests,
            "value": round(statistics.mean(lat), 2),
        }
    ]


def bench_processing(ctx: AppContext, events: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    aggregates = 100
    for workers in (1, 2, 4, 8):
        reset_database(ctx)
        for i in range(events):
            body = json.dumps(
                {
                    "event_id": f"p-{i}",
                    "type": "order.updated",
                    "aggregate_id": f"order-{i % aggregates}",
                    "version": i // aggregates + 1,
                    "data": {"status": "paid", "amount_cents": i},
                }
            ).encode()
            ingest(ctx, body, signing.sign(body, SECRET))

        def loop() -> int:
            n = 0
            while process_next(ctx) is not None:
                n += 1
            return n

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            done = sum(f.result() for f in [pool.submit(loop) for _ in range(workers)])
        dt = time.perf_counter() - t0
        assert done == events
        rows.append(
            {
                "metric": "processing_events_per_s",
                "variant": f"{workers}_workers",
                "n": events,
                "value": round(events / dt, 1),
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=3000)
    ap.add_argument("--requests", type=int, default=1000)
    args = ap.parse_args(argv)
    configure_logging("CRITICAL")
    with database() as (url, pool):
        settings = Settings(database_url=url, webhook_secret=SecretStr(SECRET))
        ctx = build_context(pool, settings)
        reset_database(ctx)
        rows = bench_http_ack(ctx, args.requests) + bench_processing(ctx, args.events)
        env = environment(pool)
    RESULTS.mkdir(exist_ok=True)
    with (RESULTS / "throughput.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["metric", "variant", "n", "value"])
        w.writeheader()
        w.writerows(rows)
    (RESULTS / "throughput_env.json").write_text(json.dumps(env, indent=2) + "\n")
    for r in rows:
        print(f"{r['metric']:<28}{r['variant']:<12}{r['value']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
