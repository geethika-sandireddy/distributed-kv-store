from concurrent.futures import ThreadPoolExecutor

import pytest

from kvstore.recovery import DurableStore


@pytest.fixture
def wal_path(tmp_path):
    return str(tmp_path / "concurrent.wal")


def test_concurrent_writes_all_land_in_wal(wal_path):
    """
    Many threads calling set() on the same DurableStore concurrently.
    WAL.append() holds an internal write lock around the write+fsync
    sequence, so concurrent appends never interleave bytes in the log
    file. This test checks the net effect end-to-end: every write
    shows up correctly after a fresh replay from disk, none dropped
    or corrupted.
    """
    with DurableStore(wal_path) as s:
        def write(i):
            s.set(f"k{i}", i)

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(write, range(300)))

        assert len(s) == 300

    # Reopen fresh and replay from disk -- confirms the WAL itself
    # (not just in-memory state) captured every concurrent write.
    with DurableStore(wal_path) as s2:
        assert s2.replayed_count == 300
        for i in range(300):
            assert s2.get(f"k{i}") == i


def test_concurrent_reads_during_writes_no_crash(wal_path):
    with DurableStore(wal_path) as s:
        for i in range(50):
            s.set(f"k{i}", i)

        def reader(i):
            s.get_optional(f"k{i % 50}")

        def writer(i):
            s.set(f"k{i % 50}", i)

        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = []
            for i in range(500):
                futures.append(pool.submit(reader if i % 2 == 0 else writer, i))
            for f in futures:
                f.result()

        assert len(s) == 50
