"""The one domain handler: apply an `order.updated` patch. Pure; no DB access."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from reconciler.errors import TerminalError


class OrderPatch(BaseModel):
    """Business validity of `data`. Every field optional; only the fields present are applied.
    A violation is a *terminal* error: retrying cannot make bad data good."""

    model_config = ConfigDict(extra="forbid", strict=True)

    status: Literal["pending", "paid", "shipped", "cancelled"] = "pending"
    amount_cents: int = Field(
        default=0,  # pragma: no mutate - default unobservable (exclude_unset)
        ge=0,
    )
    currency: str = Field(default="USD", pattern=r"^[A-Z]{3}$")
    customer_id: str = Field(default="", max_length=128)


def apply_event(state: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    """Return the new aggregate state: a shallow merge of the validated patch."""
    try:
        patch = OrderPatch.model_validate(data)
    except ValidationError as exc:
        fields = ",".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
        raise TerminalError(f"invalid order patch ({fields})") from exc
    return {**state, **patch.model_dump(exclude_unset=True)}
