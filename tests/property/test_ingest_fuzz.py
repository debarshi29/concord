"""Whatever bytes arrive, ingest either accepts or records a typed rejection: never a raw exception.

The invariant behind "nothing is silently dropped": every call leaves exactly one `events` row.
"""

from __future__ import annotations

import contextlib
import json

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from reconciler import signing
from reconciler.chaos.runner import reset_database
from reconciler.context import AppContext
from reconciler.errors import IngestRejected
from reconciler.ingest import ingest
from tests.helpers import SECRET, scalar

_fuzz = settings(
    max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)

# Every code point, including NUL and lone surrogates: the characters real senders get wrong.
_text = st.text(alphabet=st.characters(codec=None), max_size=20)
_json = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats() | _text,
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(_text, inner, max_size=3),
    max_leaves=8,
)


def _outcome(ctx: AppContext, body: bytes, signature: str | None) -> None:
    before = scalar(ctx, "SELECT count(*) FROM events")
    with contextlib.suppress(IngestRejected):
        ingest(ctx, body, signature)
    assert scalar(ctx, "SELECT count(*) FROM events") == before + 1


@_fuzz
@given(body=st.binary(max_size=200), signature=st.none() | _text)
def test_arbitrary_bytes_and_headers_are_always_recorded(
    ctx: AppContext, body: bytes, signature: str | None
) -> None:
    reset_database(ctx)
    _outcome(ctx, body, signature)


@_fuzz
@given(
    aggregate_id=_text,
    event_id=st.none() | _text,
    data=st.dictionaries(_text, _json, max_size=4),
    extra=st.dictionaries(_text, _json, max_size=2),
)
def test_arbitrary_signed_envelopes_are_always_recorded(
    ctx: AppContext,
    aggregate_id: str,
    event_id: str | None,
    data: dict[str, object],
    extra: dict[str, object],
) -> None:
    reset_database(ctx)
    envelope = {
        **extra,
        "type": "order.updated",
        "aggregate_id": aggregate_id,
        "event_id": event_id,
        "version": 1,
        "data": data,
    }
    # allow_nan keeps NaN/Infinity in the wire bytes, as a hostile sender would.
    body = json.dumps(envelope).encode("utf-8", errors="surrogatepass")
    _outcome(ctx, body, signing.sign(body, SECRET))
