import os

import pytest

from kvstore.wal import WAL, WALRecord, OpType


@pytest.fixture
def wal_path(tmp_path):
    return str(tmp_path / "test.wal")


def test_append_and_read_single_record(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))

    records = list(WAL.read_all(wal_path))
    assert len(records) == 1
    assert records[0].op == OpType.SET
    assert records[0].key == "a"
    assert records[0].value == 1


def test_append_multiple_records_preserves_order(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))
        wal.append(WALRecord(op=OpType.SET, key="b", value=2))
        wal.append(WALRecord(op=OpType.DELETE, key="a"))

    records = list(WAL.read_all(wal_path))
    assert [r.key for r in records] == ["a", "b", "a"]
    assert [r.op for r in records] == [OpType.SET, OpType.SET, OpType.DELETE]


def test_read_all_on_missing_file_returns_empty(wal_path):
    assert list(WAL.read_all(wal_path)) == []


def test_ttl_roundtrips(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1, ttl_seconds=30.0))

    records = list(WAL.read_all(wal_path))
    assert records[0].ttl_seconds == 30.0


def test_reopen_appends_not_overwrites(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="b", value=2))

    records = list(WAL.read_all(wal_path))
    assert [r.key for r in records] == ["a", "b"]


def test_truncated_header_at_eof_is_ignored(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))
    # simulate crash mid-write: append 3 garbage bytes (less than an 8-byte header)
    with open(wal_path, "ab") as f:
        f.write(b"\x01\x02\x03")

    records = list(WAL.read_all(wal_path))
    assert len(records) == 1
    assert records[0].key == "a"


def test_truncated_payload_is_ignored(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))
        good_size = os.path.getsize(wal_path)
        wal.append(WALRecord(op=OpType.SET, key="b", value="x" * 100))

    # Truncate the file partway through the second record's payload,
    # simulating a crash after the header was written but before the
    # full payload hit disk.
    with open(wal_path, "r+b") as f:
        f.truncate(good_size + 8 + 10)

    records = list(WAL.read_all(wal_path))
    assert len(records) == 1
    assert records[0].key == "a"


def test_corrupt_checksum_is_ignored(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))

    # Flip a byte inside the payload so the CRC no longer matches.
    with open(wal_path, "r+b") as f:
        f.seek(8 + 2)  # into the JSON payload, past the 8-byte header
        f.write(b"\xff")

    records = list(WAL.read_all(wal_path))
    assert records == []


def test_crash_between_records_keeps_earlier_valid_ones(wal_path):
    with WAL(wal_path) as wal:
        wal.append(WALRecord(op=OpType.SET, key="a", value=1))
        wal.append(WALRecord(op=OpType.SET, key="b", value=2))
        size_after_two = os.path.getsize(wal_path)

    # Third record never made it to disk at all (process died before
    # the write() call). Nothing to truncate -- file just ends there.
    assert os.path.getsize(wal_path) == size_after_two

    records = list(WAL.read_all(wal_path))
    assert [r.key for r in records] == ["a", "b"]


def test_concurrent_appends_do_not_interleave_bytes(wal_path):
    """
    Fire many threads at append() simultaneously on the same WAL
    instance and confirm every record survives replay intact -- if
    the internal write lock weren't there, concurrent writes could
    interleave partial records and corrupt the log.
    """
    from concurrent.futures import ThreadPoolExecutor

    with WAL(wal_path) as wal:
        def do_append(i):
            wal.append(WALRecord(op=OpType.SET, key=f"k{i}", value="x" * 50))

        with ThreadPoolExecutor(max_workers=32) as pool:
            list(pool.map(do_append, range(300)))

    records = list(WAL.read_all(wal_path))
    assert len(records) == 300
    assert {r.key for r in records} == {f"k{i}" for i in range(300)}
