"""
TTL (time-to-live) support.

Design choice: TTLs are stored as absolute expiry timestamps (monotonic
time + ttl_seconds) alongside the value, not as a separate cleanup thread.
Expiry is checked lazily on read (and swept opportunistically on write) --
this avoids needing a background timer thread and keeps the store's
behavior deterministic and easy to test.
"""

import time
from dataclasses import dataclass
from typing import Any


@dataclass
class TTLEntry:
    value: Any
    expires_at: float | None  # monotonic timestamp; None = no expiry

    def is_expired(self, now: float | None = None) -> bool:
        if self.expires_at is None:
            return False
        now = time.monotonic() if now is None else now
        return now >= self.expires_at


def make_entry(value: Any, ttl_seconds: float | None) -> TTLEntry:
    if ttl_seconds is None:
        return TTLEntry(value=value, expires_at=None)
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    return TTLEntry(value=value, expires_at=time.monotonic() + ttl_seconds)
