"""
Core in-memory key-value store.

This module intentionally starts minimal: a plain dict-backed store with
SET / GET / DELETE. TTL, LRU eviction, and durability (WAL) are layered on
top in later modules rather than baked in here, so each concern can be
tested in isolation.
"""

from threading import RLock
from typing import Any, Optional

from kvstore.ttl import TTLEntry, make_entry


class KeyNotFoundError(KeyError):
    """Raised when GET/DELETE is called on a key that doesn't exist."""


class Store:
    """A thread-safe in-memory key-value store with optional per-key TTL.

    Not durable and has no eviction policy on its own -- see the WAL layer
    and the LRU-enabled subclass for those behaviors.
    """

    def __init__(self) -> None:
        self._data: dict[str, TTLEntry] = {}
        self._lock = RLock()

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        with self._lock:
            self._data[key] = make_entry(value, ttl_seconds)

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._data.get(key)
            if entry is None or self._expire_if_needed(key, entry):
                raise KeyNotFoundError(key)
            return entry.value

    def get_optional(self, key: str) -> Optional[Any]:
        """Non-raising variant of get(); returns None if missing or expired."""
        with self._lock:
            entry = self._data.get(key)
            if entry is None or self._expire_if_needed(key, entry):
                return None
            return entry.value

    def delete(self, key: str) -> None:
        with self._lock:
            if key not in self._data:
                raise KeyNotFoundError(key)
            del self._data[key]

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return False
            return not self._expire_if_needed(key, entry)

    def keys(self) -> list[str]:
        with self._lock:
            self._sweep_expired()
            return list(self._data.keys())

    def _expire_if_needed(self, key: str, entry: TTLEntry) -> bool:
        """Returns True and evicts the key if it has expired."""
        if entry.is_expired():
            del self._data[key]
            return True
        return False

    def _sweep_expired(self) -> None:
        expired = [k for k, e in self._data.items() if e.is_expired()]
        for k in expired:
            del self._data[k]

    def __len__(self) -> int:
        with self._lock:
            self._sweep_expired()
            return len(self._data)
