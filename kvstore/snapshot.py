"""
Snapshotting / WAL compaction.

Problem this solves: the WAL grows forever. Every SET and DELETE ever
issued stays in the log, even if a key was overwritten a thousand times
or deleted right after. Left unchecked, replay time on restart grows
without bound relative to the *current* size of the dataset, not the
history of operations ever issued against it.

Approach: write the current in-memory state out as a sequence of
length-prefixed, checksummed records (same framing as wal.py, so a
torn snapshot write is detectable the same way replay detects a torn
WAL write), then atomically replace the WAL with a fresh, empty one.
Replay on startup becomes: load the snapshot (if any), then replay
whatever WAL records came after it.

Atomicity of the swap: the snapshot is written to a temp file first,
fsynced, then os.replace()'d into place -- POSIX rename is atomic, so
a crash mid-compaction leaves either the old snapshot+full WAL (fine,
just not yet compacted) or the new snapshot+empty WAL (fine, already
compacted), never a half-written snapshot masquerading as valid.

TTL note: entries carry ttl_remaining_seconds (computed at snapshot
time relative to now), not an absolute expiry timestamp -- the WAL/
Store use time.monotonic() internally, which resets across process
restarts, so only the *remaining* duration is meaningful to persist.
"""

import json
import os
import struct
import zlib
from collections.abc import Iterator
from typing import Any

_HEADER = struct.Struct(">II")  # length, crc32 -- same framing as wal.py


def fsync_directory(dirpath: str) -> None:
    """Make a rename durable by fsyncing the containing directory (POSIX only; best effort)."""
    try:
        fd = os.open(dirpath, os.O_RDONLY)
    except OSError:
        return  # e.g. Windows: directories cannot be opened this way
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_snapshot(path: str, entries: dict[str, tuple[Any, float | None]], fsync: bool = True) -> None:
    """entries: {key: (value, ttl_remaining_seconds_or_None)}"""
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "wb") as f:
        for key, (value, ttl_remaining) in entries.items():
            payload = json.dumps(
                {"key": key, "value": value, "ttl_remaining_seconds": ttl_remaining}
            ).encode("utf-8")
            crc = zlib.crc32(payload)
            f.write(_HEADER.pack(len(payload), crc))
            f.write(payload)
        f.flush()
        if fsync:
            os.fsync(f.fileno())
    os.replace(tmp_path, path)  # atomic on POSIX -- old file, if any, is fully replaced
    if fsync:
        fsync_directory(os.path.dirname(os.path.abspath(path)))


def read_snapshot(path: str) -> Iterator[dict]:
    """Yields {key, value, ttl_remaining_seconds} dicts.

    Same torn-write tolerance as WAL.read_all: stops silently at the
    first truncated header, truncated payload, or checksum mismatch,
    treating that as the (expected, harmless) tail of a crash mid-write
    rather than an error.
    """
    if not os.path.exists(path):
        return
    with open(path, "rb") as f:
        while True:
            header = f.read(_HEADER.size)
            if len(header) < _HEADER.size:
                break
            length, expected_crc = _HEADER.unpack(header)
            payload = f.read(length)
            if len(payload) < length:
                break
            if zlib.crc32(payload) != expected_crc:
                break
            try:
                yield json.loads(payload)
            except json.JSONDecodeError:
                break
