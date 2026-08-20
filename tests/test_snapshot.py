import os

import pytest

from kvstore.snapshot import write_snapshot, read_snapshot


@pytest.fixture
def snap_path(tmp_path):
    return str(tmp_path / "snap.bin")


def test_write_and_read_roundtrip(snap_path):
    write_snapshot(snap_path, {"a": (1, None), "b": ("hello", 30.0)})
    records = {r["key"]: r for r in read_snapshot(snap_path)}
    assert records["a"] == {"key": "a", "value": 1, "ttl_remaining_seconds": None}
    assert records["b"]["value"] == "hello"
    assert records["b"]["ttl_remaining_seconds"] == 30.0


def test_read_missing_file_returns_empty(snap_path):
    assert list(read_snapshot(snap_path)) == []


def test_write_is_atomic_no_temp_file_left_behind(snap_path):
    write_snapshot(snap_path, {"a": (1, None)})
    assert os.path.exists(snap_path)
    assert not os.path.exists(f"{snap_path}.tmp")


def test_overwriting_existing_snapshot_replaces_fully(snap_path):
    write_snapshot(snap_path, {"a": (1, None), "b": (2, None)})
    write_snapshot(snap_path, {"c": (3, None)})  # should NOT contain a/b anymore
    records = {r["key"] for r in read_snapshot(snap_path)}
    assert records == {"c"}


def test_empty_snapshot_roundtrips(snap_path):
    write_snapshot(snap_path, {})
    assert list(read_snapshot(snap_path)) == []


def test_torn_snapshot_write_stops_at_tear(snap_path):
    write_snapshot(snap_path, {"a": (1, None), "b": (2, None)})
    good_size = os.path.getsize(snap_path)
    with open(snap_path, "ab") as f:
        f.write(b"\x00\x00\x00\xff")  # bogus partial header

    records = list(read_snapshot(snap_path))
    assert len(records) == 2  # both original records intact, garbage ignored
