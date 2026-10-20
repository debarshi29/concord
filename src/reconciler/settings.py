"""Twelve-factor configuration: one typed object, validated at startup."""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GapPolicy(StrEnum):
    """What to do when a version gap does not fill within the buffer window (ADR-0002)."""

    PARK_AND_ALERT = "park_and_alert"
    LAST_WRITER_WINS = "last_writer_wins"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RECONCILER_", env_file=".env", extra="ignore")

    database_url: str = "postgresql://postgres:postgres@localhost:5432/reconciler"
    pool_min_size: int = Field(default=1, ge=1)
    pool_max_size: int = Field(default=10, ge=1)

    webhook_secret: SecretStr = SecretStr("")
    webhook_secret_previous: SecretStr = SecretStr("")

    idempotency_ttl_seconds: int = Field(default=7 * 24 * 3600, ge=1)
    buffer_window_seconds: int = Field(default=300, ge=0)
    gap_policy: GapPolicy = GapPolicy.PARK_AND_ALERT

    retry_base_seconds: float = Field(default=1.0, gt=0)
    retry_cap_seconds: float = Field(default=300.0, gt=0)
    retry_max_attempts: int = Field(default=6, ge=1)

    worker_poll_seconds: float = Field(default=0.5, gt=0)
    sweep_interval_seconds: float = Field(default=15.0, gt=0)

    reconcile_source_url: str = ""
    reconcile_interval_seconds: float = Field(default=900.0, gt=0)

    alert_dlq_depth: int = Field(default=1, ge=1)
    alert_parked_events: int = Field(default=1, ge=1)
    alert_oldest_buffered_seconds: float = Field(default=900.0, gt=0)
    alert_reconcile_age_seconds: float = Field(default=3600.0, gt=0)

    otel_exporter: Literal["none", "console", "otlp"] = "none"
    log_level: str = "INFO"

    # Fault injection used by the deliberate-incident drill: while this file exists the
    # downstream sink fails with a retryable error. Empty string disables the hook.
    fault_downstream_outage_file: str = ""

    @model_validator(mode="after")
    def _check(self) -> Settings:
        if self.pool_max_size < self.pool_min_size:
            raise ValueError("pool_max_size must be >= pool_min_size")
        if self.retry_cap_seconds < self.retry_base_seconds:
            raise ValueError("retry_cap_seconds must be >= retry_base_seconds")
        return self

    def require_webhook_secret(self) -> None:
        """Fail loudly at boot for processes that verify signatures."""
        if not self.webhook_secret.get_secret_value():
            raise ValueError("RECONCILER_WEBHOOK_SECRET must be set")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
