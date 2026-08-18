import pytest

from kvstore.store import Store, KeyNotFoundError


def test_no_eviction_without_max_size():
    s = Store()
    for i in range(100):
        s.set(f"k{i}", i)
    assert len(s) == 100


def test_eviction_at_capacity():
    s = Store(max_size=2)
    s.set("a", 1)
    s.set("b", 2)
    s.set("c", 3)  # should evict "a"
    assert len(s) == 2
    with pytest.raises(KeyNotFoundError):
        s.get("a")
    assert s.get("b") == 2
    assert s.get("c") == 3


def test_get_refreshes_lru_order():
    s = Store(max_size=2)
    s.set("a", 1)
    s.set("b", 2)
    s.get("a")  # a is now most-recently-used
    s.set("c", 3)  # should evict "b", not "a"
    assert s.exists("a")
    assert not s.exists("b")
    assert s.exists("c")


def test_explicit_delete_updates_lru():
    s = Store(max_size=2)
    s.set("a", 1)
    s.set("b", 2)
    s.delete("a")
    s.set("c", 3)  # no eviction needed, only b+c now
    assert s.exists("b")
    assert s.exists("c")


def test_overwrite_does_not_double_count():
    s = Store(max_size=2)
    s.set("a", 1)
    s.set("a", 2)  # overwrite, still 1 key
    s.set("b", 3)
    assert len(s) == 2
    assert s.get("a") == 2
