"""Raft consensus core as a pure, deterministic state machine.

Nothing in here reads a clock, opens a socket or starts a thread. The driver (the cluster
simulator in tests, the HTTP layer in production) passes in the current time, feeds messages
to `handle()`, calls `tick()` regularly and sends whatever those calls return. Randomness
comes from an injected `random.Random`. Messages are plain JSON-able dicts.

Log indexes are 1-based. A command of `None` is a no-op entry (appended by every new leader).
Rule references (Figure 2 of the Raft paper) are in the docstrings; see README for the table.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from kvstore.raft.storage import Entry

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"
Outgoing = list[tuple[int, dict]]


def quorum(cluster_size: int) -> int:
    """Smallest number of nodes that is a majority."""
    return cluster_size // 2 + 1


@dataclass
class Config:
    election_timeout: tuple[float, float] = (0.15, 0.30)  # seconds, randomized per election
    heartbeat_interval: float = 0.05  # much shorter than the election timeout
    max_batch: int = 64


class RaftNode:
    def __init__(
        self,
        node_id: int,
        peers: list[int],
        storage: Any,
        rng: random.Random,
        config: Config | None = None,
        apply_fn: Callable[[int, int, Any], Any] | None = None,
        now: float = 0.0,
    ) -> None:
        self.id = node_id
        self.peers = [p for p in peers if p != node_id]
        self.cluster_size = len(self.peers) + 1
        self.storage = storage
        self.rng = rng
        self.cfg = config or Config()
        self.apply_fn = apply_fn
        # Persistent state (reloaded from storage after a crash).
        self.term: int = storage.term
        self.voted_for: int | None = storage.voted_for
        self.log: list[Entry] = list(storage.entries)
        # Volatile state.
        self.commit_index = 0
        self.last_applied = 0
        self.role = FOLLOWER
        self.leader_id: int | None = None
        self.votes: set[int] = set()
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self._outbox: Outgoing = []
        self._election_deadline = 0.0
        self._next_heartbeat = 0.0
        self._reset_election_timer(now)

    # ---------- small helpers ----------
    @property
    def last_log_index(self) -> int:
        return len(self.log)

    @property
    def last_log_term(self) -> int:
        return self.log[-1][0] if self.log else 0

    def term_at(self, index: int) -> int:
        return 0 if index == 0 else self.log[index - 1][0]

    def _reset_election_timer(self, now: float) -> None:
        lo, hi = self.cfg.election_timeout
        self._election_deadline = now + self.rng.uniform(lo, hi)

    def _send(self, dest: int, msg: dict) -> None:
        msg["from"] = self.id
        self._outbox.append((dest, msg))

    def _drain(self) -> Outgoing:
        out, self._outbox = self._outbox, []
        return out

    def status(self) -> dict:
        return {"id": self.id, "role": self.role, "term": self.term, "leader_id": self.leader_id,
                "commit_index": self.commit_index, "last_applied": self.last_applied,
                "log_length": len(self.log)}

    # ---------- time ----------
    def tick(self, now: float) -> Outgoing:
        if self.role == LEADER:
            if now >= self._next_heartbeat:
                self._broadcast_append(now)
        elif now >= self._election_deadline:
            self._start_election(now)
        return self._drain()

    # ---------- elections (Figure 2: Rules for Servers; RequestVote RPC) ----------
    def _start_election(self, now: float) -> None:
        """Candidate: increment term, vote for self, reset timer, request votes."""
        self.term += 1
        self.role = CANDIDATE
        self.voted_for = self.id
        self.leader_id = None
        self.votes = {self.id}
        self.storage.save_state(self.term, self.voted_for)
        self._reset_election_timer(now)
        if len(self.votes) >= quorum(self.cluster_size):
            self._become_leader(now)
            return
        for p in self.peers:
            self._send(p, {"type": "request_vote", "term": self.term,
                           "last_log_index": self.last_log_index, "last_log_term": self.last_log_term})

    def _become_leader(self, now: float) -> None:
        self.role = LEADER
        self.leader_id = self.id
        self.next_index = {p: self.last_log_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # No-op in the new term: lets the leader commit entries from earlier terms (Section 8).
        entry = (self.term, None)
        self.storage.append([entry])
        self.log.append(entry)
        self._advance_commit()
        self._broadcast_append(now)

    def _step_down(self, term: int) -> None:
        """Any RPC or reply with a higher term makes us a follower of that term."""
        if term > self.term:
            self.term = term
            self.voted_for = None
            self.storage.save_state(self.term, self.voted_for)
            self.role = FOLLOWER
            self.leader_id = None
            self.votes = set()

    def _log_up_to_date(self, last_term: int, last_index: int) -> bool:
        """Candidate's log is at least as up to date as ours: last term first, then length."""
        if last_term != self.last_log_term:
            return last_term > self.last_log_term
        return last_index >= self.last_log_index

    def _on_request_vote(self, msg: dict, now: float) -> None:
        self._step_down(msg["term"])
        granted = False
        if (msg["term"] == self.term  # an older term is always rejected
                and self.voted_for in (None, msg["from"])
                and self._log_up_to_date(msg["last_log_term"], msg["last_log_index"])):
            granted = True
            self.voted_for = msg["from"]
            self.storage.save_state(self.term, self.voted_for)  # persisted before the reply is sent
            self._reset_election_timer(now)
        self._send(msg["from"], {"type": "vote_reply", "term": self.term, "granted": granted})

    def _on_vote_reply(self, msg: dict, now: float) -> None:
        self._step_down(msg["term"])
        if self.role != CANDIDATE or msg["term"] != self.term or not msg["granted"]:
            return
        self.votes.add(msg["from"])
        if len(self.votes) >= quorum(self.cluster_size):
            self._become_leader(now)

    # ---------- log replication (AppendEntries RPC) ----------
    def _send_append(self, peer: int) -> None:
        nxt = self.next_index[peer]
        prev = nxt - 1
        entries = self.log[nxt - 1 : nxt - 1 + self.cfg.max_batch]
        self._send(peer, {"type": "append", "term": self.term, "prev_index": prev,
                          "prev_term": self.term_at(prev), "entries": [[t, c] for t, c in entries],
                          "leader_commit": self.commit_index})

    def _broadcast_append(self, now: float) -> None:
        self._next_heartbeat = now + self.cfg.heartbeat_interval
        for p in self.peers:
            self._send_append(p)

    def _on_append(self, msg: dict, now: float) -> None:
        self._step_down(msg["term"])
        if msg["term"] < self.term:  # stale leader
            self._send(msg["from"], {"type": "append_reply", "term": self.term, "success": False,
                                     "match_index": 0, "conflict_index": 1})
            return
        # Same term: this is the legitimate leader. A candidate of this term gives up.
        self.role = FOLLOWER
        self.leader_id = msg["from"]
        self._reset_election_timer(now)
        prev, prev_term = msg["prev_index"], msg["prev_term"]
        if prev > self.last_log_index:  # missing entries
            self._reply_append(msg, False, 0, self.last_log_index + 1)
            return
        if prev > 0 and self.term_at(prev) != prev_term:  # entry before the new ones doesn't match
            bad = self.term_at(prev)
            first = prev
            while first > 1 and self.term_at(first - 1) == bad:
                first -= 1
            self._reply_append(msg, False, 0, first)
            return
        entries = [(e[0], e[1]) for e in msg["entries"]]
        # Skip entries we already have; delete a conflicting entry and everything after it.
        k = 0
        while k < len(entries):
            idx = prev + 1 + k
            if idx > self.last_log_index:
                break
            if self.term_at(idx) != entries[k][0]:
                self.storage.truncate_from(idx)
                del self.log[idx - 1 :]
                break
            k += 1
        new = entries[k:]
        if new:
            self.storage.append(new)  # fsynced before we reply
            self.log.extend(new)
        last_new = prev + len(entries)
        if msg["leader_commit"] > self.commit_index:
            self.commit_index = min(msg["leader_commit"], last_new)
            self._apply_committed()
        self._reply_append(msg, True, last_new, 0)

    def _reply_append(self, msg: dict, ok: bool, match: int, conflict: int) -> None:
        self._send(msg["from"], {"type": "append_reply", "term": self.term, "success": ok,
                                 "match_index": match, "conflict_index": conflict})

    def _on_append_reply(self, msg: dict, now: float) -> None:
        self._step_down(msg["term"])
        if self.role != LEADER or msg["term"] != self.term:
            return
        f = msg["from"]
        if msg["success"]:
            self.match_index[f] = max(self.match_index[f], msg["match_index"])  # replies may be reordered
            self.next_index[f] = self.match_index[f] + 1
            self._advance_commit()
            if self.next_index[f] <= self.last_log_index:
                self._send_append(f)
        else:
            lowered = min(self.next_index[f] - 1, msg["conflict_index"])
            self.next_index[f] = max(self.match_index[f] + 1, lowered, 1)
            self._send_append(f)

    def _advance_commit(self) -> None:
        """Leader: commit N when a majority has it AND log[N] is from the current term (Figure 8)."""
        for n in range(self.last_log_index, self.commit_index, -1):
            if self.term_at(n) != self.term:
                break
            replicas = 1 + sum(1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicas >= quorum(self.cluster_size):
                self.commit_index = n
                self._apply_committed()
                break

    def _apply_committed(self) -> None:
        """Apply committed entries to the state machine strictly in order."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            term, cmd = self.log[self.last_applied - 1]
            if cmd is not None and self.apply_fn is not None:
                self.apply_fn(self.last_applied, term, cmd)

    # ---------- client entry point ----------
    def propose(self, command: Any, now: float) -> tuple[int | None, Outgoing]:
        """Leader appends a command and starts replicating it. Returns (index, messages),
        or (None, []) if this node is not the leader."""
        if self.role != LEADER:
            return None, []
        entry = (self.term, command)
        self.storage.append([entry])
        self.log.append(entry)
        index = self.last_log_index
        self._advance_commit()  # a single-node cluster commits immediately
        self._broadcast_append(now)
        return index, self._drain()

    # ---------- message dispatch ----------
    def handle(self, msg: dict, now: float) -> Outgoing:
        kind = msg["type"]
        if kind == "request_vote":
            self._on_request_vote(msg, now)
        elif kind == "vote_reply":
            self._on_vote_reply(msg, now)
        elif kind == "append":
            self._on_append(msg, now)
        elif kind == "append_reply":
            self._on_append_reply(msg, now)
        return self._drain()
