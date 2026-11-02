"""Durable Raft state: current term, vote, and the log.

Same framing as the WAL (length, CRC32, JSON payload), as an append-only file of records:

    {"t": "state", "term": T, "vote": V}        latest one wins
    {"t": "append", "entries": [[term, cmd]]}   entries appended at the end of the log
    {"t": "trunc", "from": I}                   drop log entries with index >= I (1-based)

Every mutating call fsyncs before returning, and the Raft node only builds a reply
after calling storage, so "persist before replying to any RPC" holds by construction.
On open, a torn or corrupt tail record is detected by length/CRC and the file is
truncated back to the last good record.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from typing import Any

from kvstore.snapshot import fsync_directory

_HEADER = struct.Struct(">II")  # payload length, crc32
Entry = tuple[int, Any]  # (term, command)


def _frame(obj: dict) -> bytes:
    payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    return _HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def _read_records(path: str) -> tuple[list[dict], int]:
    """Return (valid records, byte offset just after the last valid record)."""
    records: list[dict] = []
    good = 0
    if not os.path.exists(path):
        return records, good
    with open(path, "rb") as f:
        while True:
            header = f.read(_HEADER.size)
            if len(header) < _HEADER.size:
                break
            length, crc = _HEADER.unpack(header)
            payload = f.read(length)
            if len(payload) < length or zlib.crc32(payload) != crc:
                break
            try:
                records.append(json.loads(payload))
            except json.JSONDecodeError:
                break
            good = f.tell()
    return records, good


def _replay(records: list[dict]) -> tuple[int, int | None, list[Entry]]:
    term, vote, entries = 0, None, []
    for r in records:
        if r["t"] == "state":
            term, vote = r["term"], r["vote"]
        elif r["t"] == "append":
            entries.extend((e[0], e[1]) for e in r["entries"])
        elif r["t"] == "trunc":
            del entries[r["from"] - 1 :]
    return term, vote, entries


class MemoryStorage:
    """Same interface, no disk. Survives a simulated node crash because the cluster
    simulator keeps the object and hands it to the restarted node."""

    def __init__(self) -> None:
        self.term = 0
        self.voted_for: int | None = None
        self.entries: list[Entry] = []

    def save_state(self, term: int, voted_for: int | None) -> None:
        self.term, self.voted_for = term, voted_for

    def append(self, entries: list[Entry]) -> None:
        self.entries.extend((t, c) for t, c in entries)

    def truncate_from(self, index: int) -> None:
        del self.entries[index - 1 :]

    def close(self) -> None:
        pass


class FileStorage:
    def __init__(self, dirpath: str, fsync: bool = True) -> None:
        os.makedirs(dirpath, exist_ok=True)
        self.path = os.path.join(dirpath, "raft.log")
        self._fsync = fsync
        records, good = _read_records(self.path)
        if os.path.exists(self.path) and os.path.getsize(self.path) > good:
            os.truncate(self.path, good)  # drop the torn tail so new records follow valid data
        self.term, self.voted_for, self.entries = _replay(records)
        new_file = not os.path.exists(self.path)
        self._fh = open(self.path, "ab")
        if new_file and fsync:
            fsync_directory(os.path.abspath(dirpath))

    def _write(self, obj: dict) -> None:
        self._fh.write(_frame(obj))
        self._fh.flush()
        if self._fsync:
            os.fsync(self._fh.fileno())

    def save_state(self, term: int, voted_for: int | None) -> None:
        self._write({"t": "state", "term": term, "vote": voted_for})
        self.term, self.voted_for = term, voted_for

    def append(self, entries: list[Entry]) -> None:
        if entries:
            self._write({"t": "append", "entries": [[t, c] for t, c in entries]})
            self.entries.extend((t, c) for t, c in entries)

    def truncate_from(self, index: int) -> None:
        self._write({"t": "trunc", "from": index})
        del self.entries[index - 1 :]

    def close(self) -> None:
        self._fh.close()
