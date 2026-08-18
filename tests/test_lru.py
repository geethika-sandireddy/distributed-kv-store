import pytest

from kvstore.lru import LRUTracker


def test_touch_new_key():
    t = LRUTracker(capacity=2)
    t.touch("a")
    assert len(t) == 1
    assert t.evict_candidate() == "a"


def test_least_recently_used_is_evict_candidate():
    t = LRUTracker(capacity=3)
    t.touch("a")
    t.touch("b")
    t.touch("c")
    assert t.evict_candidate() == "a"


def test_touch_moves_key_to_front():
    t = LRUTracker(capacity=3)
    t.touch("a")
    t.touch("b")
    t.touch("c")
    t.touch("a")  # re-touch a -> b becomes LRU
    assert t.evict_candidate() == "b"


def test_remove_key():
    t = LRUTracker(capacity=3)
    t.touch("a")
    t.touch("b")
    t.remove("a")
    assert len(t) == 1
    assert t.evict_candidate() == "b"


def test_remove_missing_key_is_noop():
    t = LRUTracker(capacity=3)
    t.remove("missing")  # should not raise
    assert len(t) == 0


def test_is_over_capacity():
    t = LRUTracker(capacity=2)
    t.touch("a")
    t.touch("b")
    assert not t.is_over_capacity()
    t.touch("c")
    assert t.is_over_capacity()


def test_evict_candidate_empty():
    t = LRUTracker(capacity=2)
    assert t.evict_candidate() is None


def test_zero_capacity_rejected():
    with pytest.raises(ValueError):
        LRUTracker(capacity=0)


def test_full_eviction_cycle():
    t = LRUTracker(capacity=2)
    t.touch("a")
    t.touch("b")
    t.touch("c")
    assert t.is_over_capacity()
    victim = t.evict_candidate()
    assert victim == "a"
    t.remove(victim)
    assert not t.is_over_capacity()
    assert t.evict_candidate() == "b"
