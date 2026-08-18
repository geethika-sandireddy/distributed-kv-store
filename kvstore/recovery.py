"""
Durable store: wires the WAL and the in-memory Store together, and
handles replay on startup.

Write ordering (this is the answer to "when can you tell a client
their write succeeded?"):

    1. Serialize the operation as a WALRecord.
    2. WAL.append() it -- this blocks until fsync() returns, i.e. the
       record is durable on disk.
    3. Only *after* append() returns do we apply the mutation to the
       in-memory Store.
    4. Only *after* the in-memory apply do we return success to the
       caller.

If the process crashes between steps 1 and 2, the record never hit
disk -- as if the write never happened. That's fine: the client never
got a success response for it either, so nothing was promised.

If the process crashes between steps 2 and 3 (WAL fsynced, memory not
yet updated) -- also fine: on restart, replay() reads the WAL from
scratch and reapplies every record including this one, so the memory
state converges to what the log says regardless of exactly when the
crash happened relative to the in-memory write. The WAL is the source
of truth; memory is a cache rebuilt from it.

So: a write is "successful" the instant WAL.append() returns, not
before. Anything the caller does with that guarantee downstream
(applying to memory, responding to the client) is safe to do
afterwards because replay will always reconstruct it correctly even if
the process dies one line of code later.
"""

from typing import Any, Optional

from kvstore.store import Store, KeyNotFoundError
from kvstore.wal import WAL, WALRecord, OpType


class DurableStore:
    """A Store fronted by a WAL for durability and crash recovery.

    On construction, replays any existing WAL at `wal_path` to rebuild
    state before accepting new writes.
    """

    def __init__(self, wal_path: str, max_size: Optional[int] = None, fsync: bool = True):
        self._store = Store(max_size=max_size)
        self.replayed_count = self._replay(wal_path)
        self._wal = WAL(wal_path, fsync=fsync)

    def _replay(self, wal_path: str) -> int:
        count = 0
        for record in WAL.read_all(wal_path):
            if record.op == OpType.SET:
                self._store.set(record.key, record.value, ttl_seconds=record.ttl_seconds)
            elif record.op == OpType.DELETE:
                # A DELETE for a key that isn't present (e.g. it expired
                # via TTL before the delete was replayed) is not an error
                # during replay -- the end state is the same either way.
                try:
                    self._store.delete(record.key)
                except KeyNotFoundError:
                    pass
            count += 1
        return count

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        self._wal.append(WALRecord(op=OpType.SET, key=key, value=value, ttl_seconds=ttl_seconds))
        self._store.set(key, value, ttl_seconds=ttl_seconds)

    def delete(self, key: str) -> None:
        # Log the delete even if the key is already gone from memory
        # (e.g. expired) -- that keeps the WAL an accurate record of
        # client-issued operations, which matters if you ever want to
        # audit or replicate off of it.
        self._wal.append(WALRecord(op=OpType.DELETE, key=key))
        try:
            self._store.delete(key)
        except KeyNotFoundError:
            pass

    def get(self, key: str) -> Any:
        return self._store.get(key)

    def get_optional(self, key: str) -> Optional[Any]:
        return self._store.get_optional(key)

    def exists(self, key: str) -> bool:
        return self._store.exists(key)

    def keys(self) -> list[str]:
        return self._store.keys()

    def close(self) -> None:
        self._wal.close()

    def __enter__(self) -> "DurableStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __len__(self) -> int:
        return len(self._store)
