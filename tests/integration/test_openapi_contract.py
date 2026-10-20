"""docs/openapi.json is the published contract (LLD). It must be exactly what the code serves."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from reconciler.api import create_app
from reconciler.context import AppContext

SPEC = Path(__file__).resolve().parents[2] / "docs" / "openapi.json"


def test_committed_openapi_matches_the_running_app(ctx: AppContext) -> None:
    served = TestClient(create_app(ctx)).get("/openapi.json").json()
    assert json.loads(SPEC.read_text(encoding="utf-8")) == served, "run `make openapi` and commit"
