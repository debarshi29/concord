"""HMAC-SHA256 webhook signatures over the raw request body."""

from __future__ import annotations

import hashlib
import hmac

SIGNATURE_PREFIX = "sha256="


def sign(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return f"{SIGNATURE_PREFIX}{digest}"


def verify(
    body: bytes,
    header: str | None,  # pragma: no mutate - annotation only, unevaluated
    secrets: list[str],
) -> bool:
    """Constant-time check against the current secret and (during rotation) the previous one."""
    if not header:
        return False
    return any(secret and hmac.compare_digest(sign(body, secret), header) for secret in secrets)
