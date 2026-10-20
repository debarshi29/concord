"""Budgeted exponential backoff with full jitter.

Unbudgeted retries turn a partial failure into a self-inflicted outage, so every policy has a
hard attempt budget; exhausting it is the signal to dead-letter.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True)
class RetryPolicy:
    base_seconds: float
    cap_seconds: float
    max_attempts: int
    rng: random.Random = field(default_factory=random.Random, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.base_seconds <= 0 or self.cap_seconds < self.base_seconds or self.max_attempts < 1:
            raise ValueError("invalid retry policy")

    def ceiling(self, attempt: int) -> float:
        """Upper bound of the backoff after the `attempt`-th failure (1-based): base * 2^(n-1), capped."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        exponent = min(attempt - 1, 62)  # avoid float overflow on absurd attempt counts
        return float(min(self.cap_seconds, self.base_seconds * (2**exponent)))

    def delay(self, attempt: int) -> timedelta:
        """Full jitter: uniform in [0, ceiling(attempt)] so synchronized clients de-correlate."""
        return timedelta(seconds=self.rng.uniform(0.0, self.ceiling(attempt)))

    def exhausted(self, attempts_made: int) -> bool:
        """True once `attempts_made` failures have used the whole budget."""
        return attempts_made >= self.max_attempts
