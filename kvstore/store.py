"""
Core in-memory key-value store.

This module intentionally starts minimal: a plain dict-backed store with
SET / GET / DELETE. TTL, LRU eviction, and durability (WAL) are layered on
top in later modules rather than baked in here, so each concern can be
tested in isolation.
"""

from threading import RLock
from typing import Any, Optional


class KeyNotFoundError(KeyError):
    """Raised when GET/DELETE is called on a key that doesn't exist."""


class Store:
    """A thread-safe in-memory key-value store.

    Not durable and has no eviction policy on its own -- see WALStore
    and the LRU-enabled subclass for those behaviors.
    """

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._lock = RLock()

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: str) -> Any:
        with self._lock:
            if key not in self._data:
                raise KeyNotFoundError(key)
            return self._data[key]

    def get_optional(self, key: str) -> Optional[Any]:
        """Non-raising variant of get(); returns None if missing."""
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            if key not in self._data:
                raise KeyNotFoundError(key)
            del self._data[key]

    def exists(self, key: str) -> bool:
        with self._lock:
            return key in self._data

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)
