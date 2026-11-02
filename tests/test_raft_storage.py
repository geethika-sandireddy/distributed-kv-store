import os

from kvstore.raft.storage import FileStorage, MemoryStorage


def test_roundtrip_state_and_log(tmp_path):
    s = FileStorage(str(tmp_path))
    s.save_state(3, 2)
    s.append([(1, {"op": "SET", "key": "a"}), (3, None)])
    s.close()
    r = FileStorage(str(tmp_path))
    assert (r.term, r.voted_for) == (3, 2)
    assert r.entries == [(1, {"op": "SET", "key": "a"}), (3, None)]


def test_latest_state_record_wins(tmp_path):
    s = FileStorage(str(tmp_path))
    s.save_state(1, 1)
    s.save_state(2, None)
    s.close()
    r = FileStorage(str(tmp_path))
    assert (r.term, r.voted_for) == (2, None)


def test_truncate_removes_conflicting_tail(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append([(1, "a"), (1, "b"), (2, "c")])
    s.truncate_from(2)
    s.append([(3, "d")])
    s.close()
    assert FileStorage(str(tmp_path)).entries == [(1, "a"), (3, "d")]


def test_torn_last_record_is_ignored_and_file_is_repaired(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append([(1, "a")])
    s.append([(1, "b")])
    s.close()
    path = os.path.join(tmp_path, "raft.log")
    size = os.path.getsize(path)
    with open(path, "r+b") as f:
        f.truncate(size - 3)  # tear the last record
    r = FileStorage(str(tmp_path))
    assert r.entries == [(1, "a")]
    r.append([(2, "c")])  # new records must land after valid data, not after garbage
    r.close()
    assert FileStorage(str(tmp_path)).entries == [(1, "a"), (2, "c")]


def test_corrupt_crc_stops_replay(tmp_path):
    s = FileStorage(str(tmp_path))
    s.append([(1, "a")])
    s.append([(1, "b")])
    s.close()
    path = os.path.join(tmp_path, "raft.log")
    data = bytearray(open(path, "rb").read())
    data[-1] ^= 0xFF
    open(path, "wb").write(bytes(data))
    assert FileStorage(str(tmp_path)).entries == [(1, "a")]


def test_fsync_is_called_on_every_mutation(tmp_path, monkeypatch):
    calls = []
    real = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), real(fd))[1])
    s = FileStorage(str(tmp_path))
    n = len(calls)
    s.save_state(1, None)
    s.append([(1, "a")])
    s.truncate_from(1)
    assert len(calls) - n == 3
    s.close()


def test_memory_storage_has_same_semantics():
    m = MemoryStorage()
    m.append([(1, "a"), (1, "b")])
    m.truncate_from(2)
    m.save_state(4, 1)
    assert m.entries == [(1, "a")] and (m.term, m.voted_for) == (4, 1)
