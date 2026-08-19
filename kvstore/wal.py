"""
Write-Ahead Log.

Record format (binary, one after another in the log file):

    [4 bytes: record length N, big-endian uint32]
    [4 bytes: CRC32 checksum of the JSON payload]
    [N bytes: JSON payload]

Why length-prefixed + checksummed instead of newline-delimited JSON:
a crash mid-write can leave a partial record on disk (e.g. the process
dies after writing 40 of 80 bytes). With newline-delimited JSON, a
truncated line might still happen to parse as valid-but-wrong JSON in
rare cases. With a length prefix, we know exactly how many bytes to
expect; if the file doesn't have that many bytes left, or the checksum
doesn't match what's there, we know it's a torn write and stop
replaying right there. That's what makes crash recovery safe.

Thread safety: append() holds an internal write lock for the
write+flush+fsync sequence, so concurrent callers never interleave
partial records into the file -- each append() call is atomic with
respect to other append() calls on the same WAL instance.

Durability contract: append() calls flush() + os.fsync() by default,
meaning the record is durable on disk before append() returns. This is
also the answer to "when can I tell a client their write succeeded?"
-- see recovery.py and the README for the full writeup.
"""

import json
import os
import struct
import zlib
from dataclasses import dataclass
from enum import Enum
from threading import Lock
from typing import Any, BinaryIO, Iterator, Optional

_HEADER = struct.Struct(">II")  # length, crc32


class OpType(str, Enum):
    SET = "SET"
    DELETE = "DELETE"


@dataclass
class WALRecord:
    op: OpType
    key: str
    value: Any = None
    ttl_seconds: Optional[float] = None

    def to_bytes(self) -> bytes:
        payload = json.dumps(
            {
                "op": self.op.value,
                "key": self.key,
                "value": self.value,
                "ttl_seconds": self.ttl_seconds,
            }
        ).encode("utf-8")
        crc = zlib.crc32(payload)
        return _HEADER.pack(len(payload), crc) + payload

    @classmethod
    def from_payload(cls, payload: dict) -> "WALRecord":
        return cls(
            op=OpType(payload["op"]),
            key=payload["key"],
            value=payload.get("value"),
            ttl_seconds=payload.get("ttl_seconds"),
        )


class CorruptRecordError(Exception):
    """Raised internally when a record's checksum doesn't match."""


class WAL:
    """Append-only write-ahead log backed by a single file.

    Every write to the store must go through WAL.append() and return
    (i.e. be fsynced) *before* the in-memory store is mutated. That
    ordering is what makes replay-on-restart correct.
    """

    def __init__(self, path: str, fsync: bool = True):
        self.path = path
        self._fsync = fsync
        # 'ab' so we always append, never clobber an existing log
        self._fh: BinaryIO = open(self.path, "ab")
        self._write_lock = Lock()

    def append(self, record: WALRecord) -> None:
        data = record.to_bytes()
        with self._write_lock:
            self._fh.write(data)
            self._fh.flush()
            if self._fsync:
                os.fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "WAL":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @staticmethod
    def read_all(path: str) -> Iterator[WALRecord]:
        """Reads and yields every *valid, complete* record in the log.

        Stops silently (does not raise) at the first sign of a torn
        write -- a truncated header, a truncated payload, or a payload
        whose checksum doesn't match. That's the expected shape of the
        tail of a log after a crash mid-append, and recovery should
        treat "everything before the tear" as the durable, correct
        state rather than erroring out.
        """
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            while True:
                header = f.read(_HEADER.size)
                if len(header) < _HEADER.size:
                    break  # torn or clean EOF on header
                length, expected_crc = _HEADER.unpack(header)
                payload = f.read(length)
                if len(payload) < length:
                    break  # torn write: promised N bytes, file has fewer
                actual_crc = zlib.crc32(payload)
                if actual_crc != expected_crc:
                    break  # corrupt/torn payload
                try:
                    parsed = json.loads(payload)
                except json.JSONDecodeError:
                    break
                yield WALRecord.from_payload(parsed)
