"""
Token bucket rate limiter, per client ID.

Each client gets a bucket with `capacity` tokens that refills at
`refill_rate` tokens/second, up to `capacity`. Every request consumes
one token; if the bucket is empty, the request is rejected.

Token bucket (vs. e.g. fixed window counters) was chosen because it
allows short bursts up to `capacity` while still enforcing a steady
average rate over time, and refilling continuously (rather than
resetting a counter at fixed intervals) avoids the classic fixed-window
problem of allowing 2x the intended rate right at a window boundary.

Thread safety: refill + consume for a given bucket happens under that
bucket's own lock, not one global lock, so clients don't serialize
against each other under concurrent load.
"""

import time
from dataclasses import dataclass, field
from threading import Lock
from typing import Dict


@dataclass
class _Bucket:
    tokens: float
    last_refill: float
    lock: Lock = field(default_factory=Lock)


class TokenBucketRateLimiter:
    def __init__(self, capacity: int, refill_rate: float):
        """
        capacity: max tokens (and max burst size) per client
        refill_rate: tokens added per second
        """
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be positive")
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._buckets: Dict[str, _Bucket] = {}
        self._buckets_lock = Lock()  # guards creation of new client buckets only

    def _get_bucket(self, client_id: str) -> _Bucket:
        bucket = self._buckets.get(client_id)
        if bucket is not None:
            return bucket
        with self._buckets_lock:
            # re-check under lock in case of a race to create it
            bucket = self._buckets.get(client_id)
            if bucket is None:
                bucket = _Bucket(tokens=float(self.capacity), last_refill=time.monotonic())
                self._buckets[client_id] = bucket
            return bucket

    def allow(self, client_id: str) -> bool:
        """Returns True and consumes a token if one is available."""
        bucket = self._get_bucket(client_id)
        with bucket.lock:
            now = time.monotonic()
            elapsed = now - bucket.last_refill
            bucket.tokens = min(self.capacity, bucket.tokens + elapsed * self.refill_rate)
            bucket.last_refill = now
            if bucket.tokens >= 1:
                bucket.tokens -= 1
                return True
            return False

    def tokens_remaining(self, client_id: str) -> float:
        """Read-only peek at current tokens (refills first)."""
        bucket = self._get_bucket(client_id)
        with bucket.lock:
            now = time.monotonic()
            elapsed = now - bucket.last_refill
            return min(self.capacity, bucket.tokens + elapsed * self.refill_rate)
