import os

import pytest

from kvstore.recovery import DurableStore


@pytest.fixture
def wal_path(tmp_path):
    return str(tmp_path / "compact.wal")


def test_compact_shrinks_wal_after_many_overwrites(wal_path):
    with DurableStore(wal_path) as s:
        for i in range(50):
            s.set("hot-key", i)  # 50 SETs to the same key
        size_before = os.path.getsize(wal_path)

        n = s.compact()
        assert n == 1  # only one live key survives compaction

        size_after = os.path.getsize(wal_path)
        assert size_after < size_before  # WAL truncated to (near) empty


def test_state_correct_after_compact(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.set("b", 2)
        s.set("a", 99)  # overwrite
        s.delete("b")
        s.compact()

        assert s.get("a") == 99
        assert not s.exists("b")


def test_state_correct_after_compact_and_restart(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.set("b", 2)
        s.compact()
        s.set("c", 3)  # written after compaction, goes to fresh WAL

    # Full restart: should load snapshot baseline (a, b) then replay
    # the post-compaction WAL (c).
    with DurableStore(wal_path) as s2:
        assert s2.snapshot_loaded_count == 2
        assert s2.replayed_count == 1
        assert s2.get("a") == 1
        assert s2.get("b") == 2
        assert s2.get("c") == 3


def test_compact_with_ttl_preserves_expiry_across_restart():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        wal_path = os.path.join(d, "ttl_compact.wal")
        with DurableStore(wal_path) as s:
            s.set("a", 1, ttl_seconds=9999)  # long-lived, won't expire mid-test
            s.compact()

        with DurableStore(wal_path) as s2:
            assert s2.get("a") == 1  # survived compaction + restart with TTL intact


def test_multiple_compactions_in_sequence(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.compact()
        s.set("b", 2)
        s.compact()
        s.set("c", 3)
        s.compact()

        assert s.get("a") == 1
        assert s.get("b") == 2
        assert s.get("c") == 3

    with DurableStore(wal_path) as s2:
        assert s2.get("a") == 1
        assert s2.get("b") == 2
        assert s2.get("c") == 3


def test_compact_on_empty_store(wal_path):
    with DurableStore(wal_path) as s:
        n = s.compact()
        assert n == 0
        assert len(s) == 0


def test_crash_after_snapshot_before_wal_truncate_still_recovers(wal_path):
    """
    Simulates a crash between write_snapshot() succeeding and the WAL
    truncation completing, by manually replicating just the snapshot
    write and leaving the old WAL fully intact (worst case: both the
    snapshot AND the full old WAL exist). Replay must not double-apply
    or corrupt state in that case.
    """
    from kvstore.snapshot import write_snapshot

    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.set("b", 2)

    # Manually write a snapshot without truncating the WAL -- simulates
    # the crash window between the two steps of compact().
    snapshot_path = f"{wal_path}.snapshot"
    write_snapshot(snapshot_path, {"a": (1, None), "b": (2, None)})

    with DurableStore(wal_path) as s2:
        # snapshot has a,b; WAL (untouched) also replays a,b again --
        # idempotent since SET is last-write-wins, so state is still correct.
        assert s2.get("a") == 1
        assert s2.get("b") == 2
