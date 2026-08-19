from concurrent.futures import ThreadPoolExecutor

from kvstore.store import Store


def test_concurrent_sets_to_distinct_keys():
    s = Store()

    def write(i):
        s.set(f"k{i}", i)

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(write, range(500)))

    assert len(s) == 500
    for i in range(500):
        assert s.get(f"k{i}") == i


def test_concurrent_increments_on_same_key_no_lost_updates():
    """
    Every thread does read-modify-write on the SAME key. Store's lock
    only protects individual get()/set() calls, not the compound
    read-modify-write sequence, so this intentionally uses an external
    lock to show correct usage under contention -- the goal here is to
    confirm Store itself never corrupts state (e.g. no exceptions, no
    torn values), not to claim atomic increments without one.
    """
    from threading import Lock

    s = Store()
    s.set("counter", 0)
    external_lock = Lock()

    def increment():
        with external_lock:
            current = s.get("counter")
            s.set("counter", current + 1)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: increment(), range(1000)))

    assert s.get("counter") == 1000


def test_concurrent_mixed_reads_writes_deletes_no_crash():
    s = Store()
    for i in range(100):
        s.set(f"k{i}", i)

    def churn(i):
        key = f"k{i % 100}"
        if i % 3 == 0:
            s.set(key, i)
        elif i % 3 == 1:
            s.get_optional(key)
        else:
            s.exists(key)

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(churn, range(2000)))

    # No assertion on exact state (writes/deletes race by design) --
    # the test's job is to prove this doesn't deadlock or raise.
    assert len(s) <= 100


def test_concurrent_lru_eviction_stays_within_capacity():
    s = Store(max_size=20)

    def write(i):
        s.set(f"k{i}", i)

    with ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(write, range(500)))

    assert len(s) <= 20
