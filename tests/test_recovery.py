import os
import struct

import pytest

from kvstore.recovery import DurableStore
from kvstore.store import KeyNotFoundError
from kvstore.wal import WAL, WALRecord, OpType


@pytest.fixture
def wal_path(tmp_path):
    return str(tmp_path / "test.wal")


def test_fresh_store_no_existing_wal(wal_path):
    with DurableStore(wal_path) as s:
        assert s.replayed_count == 0
        assert len(s) == 0


def test_basic_durability_across_restart(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.set("b", 2)
        s.delete("a")

    # Simulate a full process restart: new DurableStore, same wal_path
    with DurableStore(wal_path) as s2:
        assert s2.replayed_count == 3
        assert not s2.exists("a")
        assert s2.get("b") == 2


def test_multiple_restarts_are_idempotent(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
    with DurableStore(wal_path) as s:
        s.set("b", 2)
    with DurableStore(wal_path) as s:
        s.set("c", 3)
    with DurableStore(wal_path) as s:
        assert s.get("a") == 1
        assert s.get("b") == 2
        assert s.get("c") == 3
        assert len(s) == 3


def test_ttl_survives_replay(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1, ttl_seconds=9999)  # long TTL, won't expire mid-test

    with DurableStore(wal_path) as s2:
        assert s2.get("a") == 1


# --- Failure injection: crash BEFORE the WAL append lands on disk ---
# We simulate this by never calling append() at all for the "crashed"
# write -- i.e. it's as if the process died between constructing the
# record and the fsync completing.

def test_crash_before_wal_append_write_is_invisible(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        # Simulate: client called set("b", 2), but process died before
        # WAL.append() returned. We never issue the call at all.

    with DurableStore(wal_path) as s2:
        assert s2.exists("a")
        assert not s2.exists("b")  # correctly never happened


# --- Failure injection: crash AFTER WAL append, BEFORE memory applied ---
# This is the case DurableStore is specifically designed to survive:
# append the record directly to the WAL (bypassing the in-memory apply
# entirely) to simulate the process dying in between those two steps.

def test_crash_after_wal_append_before_memory_apply_recovers_on_restart(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)

    # Simulate crash-after-fsync-before-memory-write: append a raw WAL
    # record without ever touching the in-memory store, then "restart".
    with WAL(wal_path) as raw_wal:
        raw_wal.append(WALRecord(op=OpType.SET, key="b", value=2))
    # (at this point, if we were inspecting the crashed process's
    # memory, "b" would NOT be there -- but the WAL has it)

    with DurableStore(wal_path) as s2:
        assert s2.replayed_count == 2
        assert s2.get("a") == 1
        assert s2.get("b") == 2  # recovered purely from the WAL


# --- Failure injection: incomplete final WAL record (torn write) ---

def test_incomplete_final_record_is_dropped_rest_survives(wal_path):
    with DurableStore(wal_path) as s:
        s.set("a", 1)
        s.set("b", 2)

    # Append a well-formed header claiming a large payload, then only
    # write a few bytes of it -- simulates the process dying mid-write
    # on the *next* operation the client had issued.
    with open(wal_path, "ab") as f:
        header = struct.pack(">II", 500, 12345)  # length=500, bogus crc
        f.write(header)
        f.write(b"only a few bytes, not 500")

    with DurableStore(wal_path) as s2:
        assert s2.replayed_count == 2
        assert s2.get("a") == 1
        assert s2.get("b") == 2
        assert not s2.exists("c")


# --- Repeated restart cycles under ongoing writes ---

def test_repeated_restart_cycles_preserve_all_committed_writes(wal_path):
    expected = {}
    for cycle in range(10):
        with DurableStore(wal_path) as s:
            key = f"key{cycle}"
            s.set(key, cycle)
            expected[key] = cycle
            if cycle % 3 == 0 and cycle > 0:
                victim = f"key{cycle - 1}"
                s.delete(victim)
                expected.pop(victim, None)

    with DurableStore(wal_path) as final:
        assert final.replayed_count >= 10
        for key, value in expected.items():
            assert final.get(key) == value


def test_delete_of_expired_key_does_not_error_on_replay(wal_path):
    # A DELETE logged for a key that no longer exists in memory (e.g.
    # it expired) must not blow up replay.
    with WAL(wal_path) as raw_wal:
        raw_wal.append(WALRecord(op=OpType.DELETE, key="never-existed"))

    with DurableStore(wal_path) as s:
        assert s.replayed_count == 1
        assert not s.exists("never-existed")
