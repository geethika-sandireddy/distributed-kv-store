import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from kvstore.rate_limiter import TokenBucketRateLimiter


def test_allows_up_to_capacity():
    rl = TokenBucketRateLimiter(capacity=5, refill_rate=1)
    results = [rl.allow("client-a") for _ in range(5)]
    assert all(results)


def test_rejects_once_capacity_exhausted():
    rl = TokenBucketRateLimiter(capacity=3, refill_rate=1)
    for _ in range(3):
        assert rl.allow("client-a")
    assert not rl.allow("client-a")


def test_refills_over_time():
    rl = TokenBucketRateLimiter(capacity=2, refill_rate=20)  # fast refill for testing
    assert rl.allow("client-a")
    assert rl.allow("client-a")
    assert not rl.allow("client-a")
    time.sleep(0.1)  # ~2 tokens should refill at 20/s
    assert rl.allow("client-a")


def test_clients_are_independent():
    rl = TokenBucketRateLimiter(capacity=1, refill_rate=1)
    assert rl.allow("client-a")
    assert not rl.allow("client-a")
    assert rl.allow("client-b")  # separate bucket, unaffected by client-a


def test_refill_capped_at_capacity():
    rl = TokenBucketRateLimiter(capacity=2, refill_rate=1000)
    rl.allow("client-a")
    time.sleep(0.05)  # would refill way past capacity without the cap
    assert rl.tokens_remaining("client-a") <= 2


def test_invalid_capacity_rejected():
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(capacity=0, refill_rate=1)


def test_invalid_refill_rate_rejected():
    with pytest.raises(ValueError):
        TokenBucketRateLimiter(capacity=1, refill_rate=0)


def test_tokens_remaining_does_not_consume():
    rl = TokenBucketRateLimiter(capacity=3, refill_rate=1)
    before = rl.tokens_remaining("client-a")
    after = rl.tokens_remaining("client-a")
    assert before == after == 3


# --- Concurrency: many threads hammering the same client bucket ---

def test_concurrent_requests_never_exceed_capacity():
    """
    Fire far more concurrent requests than the bucket's capacity at a
    single client, with no time for refill in between, and check that
    the number of accepted requests is exactly capacity -- never more,
    proving the per-bucket lock actually serializes consume operations
    rather than allowing a race where two threads both read tokens=1
    and both decrement.
    """
    capacity = 10
    rl = TokenBucketRateLimiter(capacity=capacity, refill_rate=0.001)  # negligible refill

    def attempt(_):
        return rl.allow("shared-client")

    with ThreadPoolExecutor(max_workers=50) as pool:
        results = list(pool.map(attempt, range(200)))

    assert sum(results) == capacity


def test_concurrent_requests_across_many_clients():
    """Different clients under concurrent load stay independent."""
    rl = TokenBucketRateLimiter(capacity=5, refill_rate=0.001)
    client_ids = [f"client-{i}" for i in range(20)]

    def attempt(client_id):
        return sum(rl.allow(client_id) for _ in range(10))

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(attempt, client_ids))

    # each client independently gets exactly `capacity` allowed requests
    assert all(r == 5 for r in results)
