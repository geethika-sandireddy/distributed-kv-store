"""Replicated state machine: applies committed Raft commands to the existing Store.

Determinism rules (replicas must never diverge):
  * Nothing here reads a clock. The leader stamps every command with `ts` (wall time) when
    it accepts it; expiry is `ts + ttl`, an absolute value that is identical on every replica.
    GET is also a log command, so it is evaluated at the leader-supplied timestamp too.
  * Every command carries (client_id, seq). A command with a seq already applied for that
    client is not applied again; the cached result is returned (the session table).
The wrapped Store is used for values and LRU eviction only; its own monotonic TTL is unused.
"""

from __future__ import annotations

from collections import OrderedDict

from kvstore.store import KeyNotFoundError, Store

RESULT_CACHE = 1024


class KVStateMachine:
    def __init__(self, max_size: int | None = None) -> None:
        self.store = Store(max_size=max_size)
        self.expiry: dict[str, float] = {}
        self.sessions: dict[str, tuple[int, dict]] = {}
        self.results: OrderedDict[int, dict] = OrderedDict()  # log index -> result, for waiting clients

    def _purge_if_expired(self, key: str, ts: float) -> None:
        exp = self.expiry.get(key)
        if exp is not None and ts >= exp:
            self.expiry.pop(key, None)
            try:
                self.store.delete(key)
            except KeyNotFoundError:
                pass
        elif exp is not None and not self.store.exists(key):
            self.expiry.pop(key, None)  # evicted by LRU; forget its expiry

    def apply(self, index: int, term: int, cmd: dict) -> dict:
        cid, seq = cmd.get("client_id"), cmd.get("seq")
        if cid is not None and cid in self.sessions:
            last_seq, last_result = self.sessions[cid]
            if seq is not None and seq <= last_seq:
                result = last_result if seq == last_seq else {"ok": False, "error": "stale_seq"}
                self._remember(index, result)
                return result
        result = self._execute(cmd)
        if cid is not None and seq is not None:
            self.sessions[cid] = (seq, result)
        self._remember(index, result)
        return result

    def _remember(self, index: int, result: dict) -> None:
        self.results[index] = result
        while len(self.results) > RESULT_CACHE:
            self.results.popitem(last=False)

    def _execute(self, cmd: dict) -> dict:
        op, key, ts = cmd["op"], cmd["key"], cmd["ts"]
        self._purge_if_expired(key, ts)
        if op == "SET":
            self.store.set(key, cmd["value"])
            ttl = cmd.get("ttl")
            if ttl is not None:
                self.expiry[key] = ts + ttl
            else:
                self.expiry.pop(key, None)
            return {"ok": True}
        if op == "GET":
            try:
                return {"ok": True, "value": self.store.get(key)}
            except KeyNotFoundError:
                return {"ok": False, "error": "not_found"}
        if op == "DELETE":
            try:
                self.store.delete(key)
            except KeyNotFoundError:
                return {"ok": False, "error": "not_found"}
            self.expiry.pop(key, None)
            return {"ok": True}
        return {"ok": False, "error": f"unknown_op:{op}"}

    def digest(self) -> tuple:
        """Canonical view of the replicated state, for comparing replicas in tests."""
        items = sorted((k, repr(v), self.expiry.get(k)) for k, (v, _e) in self.store.snapshot_entries().items())
        return tuple(items), tuple(sorted((c, s) for c, (s, _r) in self.sessions.items()))
