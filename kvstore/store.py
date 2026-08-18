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
from kvstore.lru import LRUTracker


class KeyNotFoundError(KeyError):
    """Raised when GET/DELETE is called on a key that doesn't exist."""


class Store:
    """A thread-safe in-memory key-value store with optional per-key TTL
    and optional LRU eviction once a capacity is set.

    Not durable on its own -- see the WAL layer for that.
    """

    def __init__(self, max_size: Optional[int] = None) -> None:
        self._data: dict[str, TTLEntry] = {}
        self._lock = RLock()
        self._lru = LRUTracker(capacity=max_size) if max_size else None

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        with self._lock:
            self._data[key] = make_entry(value, ttl_seconds)
            if self._lru is not None:
                self._lru.touch(key)
                self._evict_if_needed()

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._data.get(key)
            if entry is None or self._expire_if_needed(key, entry):
                raise KeyNotFoundError(key)
            if self._lru is not None:
                self._lru.touch(key)
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
            if self._lru is not None:
                self._lru.remove(key)

    def _evict_if_needed(self) -> None:
        """Evicts LRU keys until back under capacity. Caller holds the lock."""
        while self._lru.is_over_capacity():
            victim = self._lru.evict_candidate()
            if victim is None:
                break
            self._data.pop(victim, None)
            self._lru.remove(victim)

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
            if self._lru is not None:
                self._lru.remove(key)
            return True
        return False

    def _sweep_expired(self) -> None:
        expired = [k for k, e in self._data.items() if e.is_expired()]
        for k in expired:
            del self._data[k]
            if self._lru is not None:
                self._lru.remove(k)

    def __len__(self) -> int:
        with self._lock:
            self._sweep_expired()
            return len(self._data)
