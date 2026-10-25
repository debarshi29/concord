from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from reconciler.clock import FakeClock
from reconciler.context import AppContext
from reconciler.reconcile import HttpSource, SourceRecord, reconcile
from reconciler.worker import run_until_idle
from tests.helpers import aggregate, scalar, send, statuses
from tests.integration.test_retry_dlq import ListSource


def _corrections(ctx: AppContext) -> list[str]:
    with ctx.pool.connection() as conn:
        return [r["kind"] for r in conn.execute("SELECT kind FROM corrections ORDER BY id")]


def test_missing_aggregate_is_created_from_source(ctx: AppContext) -> None:
    report = reconcile(ctx, ListSource(SourceRecord("order-9", 4, {"status": "paid"})))
    assert (report.checked, report.corrected) == (1, 1)
    assert aggregate(ctx, "order-9") == (4, {"status": "paid"})
    assert _corrections(ctx) == ["missing"]


def test_behind_aggregate_is_healed(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    run_until_idle(ctx)
    reconcile(ctx, ListSource(SourceRecord("order-1", 3, {"status": "shipped", "amount_cents": 5})))
    assert aggregate(ctx) == (3, {"status": "shipped", "amount_cents": 5})
    assert _corrections(ctx) == ["behind"]


def test_same_version_but_divergent_state_is_healed(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    run_until_idle(ctx)
    reconcile(ctx, ListSource(SourceRecord("order-1", 1, {"status": "cancelled"})))
    assert aggregate(ctx) == (1, {"status": "cancelled"})
    assert _corrections(ctx) == ["divergent"]


def test_in_sync_aggregates_produce_no_corrections(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    run_until_idle(ctx)
    report = reconcile(ctx, ListSource(SourceRecord("order-1", 1, {"status": "pending"})))
    assert (report.checked, report.corrected, report.local_ahead) == (1, 0, 0)
    assert _corrections(ctx) == []
    assert scalar(ctx, "SELECT corrected FROM reconcile_runs") == 0


def test_local_ahead_is_flagged_and_never_overwritten(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    send(ctx, version=2, data={"status": "paid"})
    run_until_idle(ctx)
    report = reconcile(ctx, ListSource(SourceRecord("order-1", 1, {"status": "pending"})))
    assert report.local_ahead == 1 and report.corrected == 0
    assert aggregate(ctx) == (2, {"status": "paid"})
    assert _corrections(ctx) == ["local_ahead"]


def test_orphans_are_flagged_not_deleted(ctx: AppContext) -> None:
    send(ctx, "order-gone", 1, {"status": "pending"})
    run_until_idle(ctx)
    report = reconcile(ctx, ListSource())
    assert report.orphans == 1
    assert aggregate(ctx, "order-gone") is not None
    assert _corrections(ctx) == ["orphan"]


def test_healing_drains_buffered_successors(ctx: AppContext, clock: FakeClock) -> None:
    send(ctx, version=5, data={"amount_cents": 50})  # v1..v4 never arrive
    run_until_idle(ctx)
    assert statuses(ctx)[("order-1", 5)] == ["buffered"]
    reconcile(ctx, ListSource(SourceRecord("order-1", 4, {"status": "paid"})))
    # healed to v4, so the buffered v5 became applicable and drained in the same pass
    assert aggregate(ctx) == (5, {"status": "paid", "amount_cents": 50})
    assert statuses(ctx)[("order-1", 5)] == ["processed"]


def test_reconcile_run_is_recorded(ctx: AppContext) -> None:
    reconcile(ctx, ListSource(SourceRecord("a", 1, {}), SourceRecord("b", 1, {})))
    assert scalar(ctx, "SELECT checked FROM reconcile_runs") == 2
    assert scalar(ctx, "SELECT corrected FROM reconcile_runs") == 2


def test_http_source_follows_pagination() -> None:
    pages = {
        None: {
            "items": [{"aggregate_id": "a", "version": 1, "state": {"x": 1}}],
            "next_cursor": "c2",
        },
        "c2": {"items": [{"aggregate_id": "b", "version": 2, "state": {}}], "next_cursor": None},
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=pages[request.url.params.get("cursor")])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    records = list(HttpSource("http://source.test/orders", client).list_aggregates())
    assert [(r.aggregate_id, r.version) for r in records] == [("a", 1), ("b", 2)]
    assert records[0].state == {"x": 1}


def test_http_source_surfaces_errors() -> None:
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    source = HttpSource("http://source.test/orders", client)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        list(source.list_aggregates())
    assert exc.value.response.status_code == 503


def test_an_unresolved_condition_is_logged_once_not_every_run(ctx: AppContext) -> None:
    """`corrections_last_24h` is a health metric that should trend to zero; a standing condition
    must not add a row per run."""
    send(ctx, "order-gone", 1, {"status": "pending"})
    send(ctx, "order-1", 1, {"status": "pending"})
    send(ctx, "order-1", 2, {"status": "paid"})
    run_until_idle(ctx)
    ahead = SourceRecord("order-1", 1, {"status": "pending"})
    for _ in range(3):
        report = reconcile(ctx, ListSource(ahead))
        assert (report.local_ahead, report.orphans) == (1, 1)  # still reported every run
    assert sorted(_corrections(ctx)) == ["local_ahead", "orphan"]


def test_a_condition_that_clears_and_returns_is_logged_again(ctx: AppContext) -> None:
    send(ctx, version=1, data={"status": "pending"})
    send(ctx, version=2, data={"status": "paid"})
    run_until_idle(ctx)
    behind = SourceRecord("order-1", 1, {"status": "pending"})
    reconcile(ctx, ListSource(behind))
    reconcile(
        ctx, ListSource(SourceRecord("order-1", 3, {"status": "shipped"}))
    )  # source overtakes
    reconcile(ctx, ListSource(SourceRecord("order-1", 2, {"status": "paid"})))  # ...then regresses
    assert _corrections(ctx) == ["local_ahead", "behind", "local_ahead"]


class _BusySource:
    """Lists nothing, but a new aggregate lands locally while the (slow) listing is in flight."""

    def __init__(self, ctx: AppContext, clock: FakeClock) -> None:
        self.ctx, self.clock = ctx, clock

    def list_aggregates(self) -> Iterator[SourceRecord]:
        self.clock.advance(5)
        send(self.ctx, "order-new", 1, {"status": "pending"})
        run_until_idle(self.ctx)
        yield from ()


def test_aggregate_created_during_the_run_is_not_an_orphan(
    ctx: AppContext, clock: FakeClock
) -> None:
    send(ctx, "order-gone", 1, {"status": "pending"})
    run_until_idle(ctx)
    clock.advance(1)
    report = reconcile(ctx, _BusySource(ctx, clock))
    assert aggregate(ctx, "order-new") is not None
    assert report.orphans == 1  # only order-gone, which predates the run
    with ctx.pool.connection() as conn:
        flagged = [r["aggregate_id"] for r in conn.execute("SELECT aggregate_id FROM corrections")]
    assert flagged == ["order-gone"]
