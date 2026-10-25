from __future__ import annotations

import json
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from reconciler import signing
from reconciler.domain import apply_event
from reconciler.errors import ReconcilerError, RetryableError, TerminalError
from reconciler.models import Envelope
from reconciler.patterns.idempotency import derive_key, key_fingerprint

# ----------------------------------------------------------------------------- signing


def test_signature_has_the_documented_format() -> None:
    sig = signing.sign(b"{}", "s")
    assert sig.startswith("sha256=") and len(sig) == len("sha256=") + 64


def test_verify_accepts_current_and_previous_secret_only() -> None:
    body = b'{"a":1}'
    assert signing.verify(body, signing.sign(body, "new"), ["new", "old"])
    assert signing.verify(body, signing.sign(body, "old"), ["new", "old"])
    assert not signing.verify(body, signing.sign(body, "other"), ["new", "old"])


def test_verify_rejects_tampering_missing_and_empty_secrets() -> None:
    body = b'{"a":1}'
    sig = signing.sign(body, "k")
    assert not signing.verify(body + b" ", sig, ["k"])
    assert not signing.verify(body, None, ["k"])
    assert not signing.verify(body, "", ["k"])
    assert not signing.verify(
        body, signing.sign(body, ""), [""]
    )  # an unset secret must never validate


@given(body=st.binary(max_size=200), secret=st.text(min_size=1, max_size=20))
def test_sign_verify_roundtrip(body: bytes, secret: str) -> None:
    assert signing.verify(body, signing.sign(body, secret), [secret])


# ----------------------------------------------------------------------------- envelope


def env(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "type": "order.updated",
        "aggregate_id": "o1",
        "version": 1,
        "data": {"a": 1},
    }
    return {**base, **over}


def test_envelope_accepts_minimal_valid_event() -> None:
    e = Envelope.model_validate(env())
    assert (e.aggregate_id, e.version, e.event_id) == ("o1", 1, None)


@pytest.mark.parametrize(
    "bad",
    [
        env(version=0),
        env(version="1"),
        env(version=True),
        env(version=2_000_000_001),
        env(aggregate_id=""),
        env(aggregate_id="x" * 129),
        env(data=[1]),
        env(data=None),
        env(type=""),
        env(event_id=""),
    ],
)
def test_envelope_rejects_invalid(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Envelope.model_validate(bad)


def test_content_hash_ignores_key_order_and_event_id_but_not_content() -> None:
    a = Envelope.model_validate(env(data={"x": 1, "y": 2}, event_id="e1"))
    b = Envelope.model_validate(env(data={"y": 2, "x": 1}, event_id="e2"))
    c = Envelope.model_validate(env(data={"x": 1, "y": 3}))
    d = Envelope.model_validate(env(data={"x": 1, "y": 2}, version=2))
    assert a.content_hash() == b.content_hash()
    assert a.content_hash() not in {c.content_hash(), d.content_hash()}


def test_key_derivation_precedence() -> None:
    assert derive_key("h", "e", "x") == "h"
    assert derive_key(None, "e", "x") == "e"
    assert derive_key("", "", "abc") == "sha256:abc"
    assert len(key_fingerprint("k")) == 12


# ----------------------------------------------------------------------------- domain


def test_apply_merges_only_present_fields() -> None:
    state = apply_event({"status": "pending", "amount_cents": 5}, {"status": "paid"})
    assert state == {"status": "paid", "amount_cents": 5}


def test_apply_does_not_mutate_input_state() -> None:
    before: dict[str, Any] = {"status": "pending"}
    apply_event(before, {"status": "paid"})
    assert before == {"status": "pending"}


@pytest.mark.parametrize(
    "data",
    [
        {"status": "banana"},
        {"status": None},
        {"amount_cents": -1},
        {"amount_cents": "12"},
        {"amount_cents": 1.5},
        {"currency": "usd"},
        {"currency": "USDX"},
        {"unknown_field": 1},
    ],
)
def test_invalid_patches_are_terminal(data: dict[str, Any]) -> None:
    with pytest.raises(TerminalError):
        apply_event({}, data)


@given(
    data=st.fixed_dictionaries(
        {},
        optional={
            "status": st.sampled_from(["paid", "shipped"]),
            "amount_cents": st.integers(0, 99),
        },
    )
)
def test_applying_a_patch_twice_equals_once(data: dict[str, Any]) -> None:
    once = apply_event({"customer_id": "c"}, data)
    assert apply_event(once, data) == once


# ----------------------------------------------------------------------------- errors


def test_error_taxonomy() -> None:
    assert RetryableError.retryable is True
    assert TerminalError.retryable is False
    assert issubclass(RetryableError, ReconcilerError)
    assert json.dumps({"retryable": RetryableError.retryable})


@pytest.mark.parametrize(
    ("field", "max_len"), [("event_id", 200), ("type", 100), ("aggregate_id", 128)]
)
def test_envelope_string_length_boundaries(field: str, max_len: int) -> None:
    Envelope.model_validate(env(**{field: "a"}))  # shortest legal
    Envelope.model_validate(env(**{field: "a" * max_len}))  # longest legal
    with pytest.raises(ValidationError):
        Envelope.model_validate(env(**{field: "a" * (max_len + 1)}))
    with pytest.raises(ValidationError):
        Envelope.model_validate(env(**{field: ""}))


def test_envelope_version_boundaries() -> None:
    assert Envelope.model_validate(env(version=1)).version == 1
    assert Envelope.model_validate(env(version=2_000_000_000)).version == 2_000_000_000


def test_domain_field_boundaries() -> None:
    assert apply_event({}, {"amount_cents": 0}) == {"amount_cents": 0}
    assert apply_event({}, {"customer_id": "c" * 128}) == {"customer_id": "c" * 128}
    with pytest.raises(TerminalError):
        apply_event({}, {"customer_id": "c" * 129})
    assert apply_event({}, {"currency": "EUR"}) == {"currency": "EUR"}


def test_non_ascii_signature_header_is_a_mismatch_not_an_error() -> None:
    assert signing.verify(b"{}", "sha256=é", ["s"]) is False
    assert signing.verify(b"{}", "sha256=\ud800", ["s"]) is False
