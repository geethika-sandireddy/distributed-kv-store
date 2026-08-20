import time

from kvstore.store import Store


def test_snapshot_entries_no_ttl():
    s = Store()
    s.set("a", 1)
    s.set("b", "hello")
    entries = s.snapshot_entries()
    assert entries["a"] == (1, None)
    assert entries["b"] == ("hello", None)


def test_snapshot_entries_with_ttl_returns_remaining_seconds():
    s = Store()
    s.set("a", 1, ttl_seconds=10.0)
    entries = s.snapshot_entries()
    remaining = entries["a"][1]
    assert remaining is not None
    assert 9.0 < remaining <= 10.0  # some time has elapsed since set()


def test_snapshot_entries_excludes_expired_keys():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    s.set("b", 2)
    time.sleep(0.08)
    entries = s.snapshot_entries()
    assert "a" not in entries
    assert "b" in entries


def test_snapshot_entries_empty_store():
    s = Store()
    assert s.snapshot_entries() == {}
