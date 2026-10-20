from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from reconciler.clock import FakeClock, SystemClock
from reconciler.db import Conn
from reconciler.errors import RetryableError
from reconciler.logs import JsonFormatter, configure_logging, get_logger
from reconciler.models import EventRow
from reconciler.settings import GapPolicy, Settings, get_settings
from reconciler.sinks import OutageFileSink, noop_sink


def test_defaults_are_sane() -> None:
    s = Settings()
    assert s.gap_policy is GapPolicy.PARK_AND_ALERT
    assert s.idempotency_ttl_seconds == 7 * 24 * 3600
    assert s.buffer_window_seconds == 300


def test_environment_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RECONCILER_GAP_POLICY", "last_writer_wins")
    monkeypatch.setenv("RECONCILER_RETRY_MAX_ATTEMPTS", "9")
    monkeypatch.setenv("RECONCILER_WEBHOOK_SECRET", "shh")
    s = Settings()
    assert s.gap_policy is GapPolicy.LAST_WRITER_WINS
    assert s.retry_max_attempts == 9
    assert s.webhook_secret.get_secret_value() == "shh"
    assert "shh" not in repr(s)  # secrets never leak into logs/reprs


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pool_min_size": 5, "pool_max_size": 2},
        {"retry_base_seconds": 10, "retry_cap_seconds": 1},
        {"retry_max_attempts": 0},
        {"idempotency_ttl_seconds": 0},
        {"gap_policy": "yolo"},
        {"otel_exporter": "carrier-pigeon"},
    ],
)
def test_misconfiguration_fails_loudly_at_boot(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Settings(**kwargs)  # type: ignore[arg-type]


def test_secret_is_required_for_signature_verifying_processes() -> None:
    with pytest.raises(ValueError, match="WEBHOOK_SECRET"):
        Settings(webhook_secret=SecretStr("")).require_webhook_secret()
    Settings(webhook_secret=SecretStr("x")).require_webhook_secret()


def test_get_settings_is_cached() -> None:
    get_settings.cache_clear()
    assert get_settings() is get_settings()


def test_fake_clock_advances_and_system_clock_is_aware() -> None:
    c = FakeClock()
    t0 = c.now()
    c.advance(90)
    assert c.now() - t0 == timedelta(seconds=90)
    assert SystemClock().now().tzinfo is not None


def test_json_logs_carry_extra_context() -> None:
    record = logging.LogRecord(
        "reconciler.x", logging.WARNING, __file__, 1, "hello %s", ("w",), None
    )
    record.event_id = "e-1"
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "hello w" and out["level"] == "WARNING" and out["event_id"] == "e-1"


def test_json_logs_include_exceptions() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record = logging.LogRecord("r", logging.ERROR, __file__, 1, "failed", (), sys.exc_info())
    assert "RuntimeError: boom" in json.loads(JsonFormatter().format(record))["exc"]


def test_configure_logging_installs_one_json_handler() -> None:
    configure_logging("debug")
    configure_logging("info")
    root = logging.getLogger("reconciler")
    assert len(root.handlers) == 1 and root.level == logging.INFO
    assert get_logger("x").name == "reconciler.x"


def test_outage_file_sink_fails_only_while_file_exists(tmp_path: Path) -> None:
    flag = tmp_path / "outage"
    sink = OutageFileSink(str(flag))
    conn: Conn = None  # type: ignore[assignment]
    event: EventRow = None  # type: ignore[assignment]
    sink(conn, event)  # no file: fine
    flag.write_text("x")
    with pytest.raises(RetryableError):
        sink(conn, event)
    flag.unlink()
    sink(conn, event)
    noop_sink(conn, event)
    OutageFileSink("")(conn, event)  # empty path disables the hook
