"""Deterministic cluster simulator for testing the Raft node.

A fake clock, a lossy network (drop, delay, duplicate, reorder, partition) and node
crash/restart (state reloaded from storage). Everything random comes from one seed, so a
failing seed replays exactly. Invariants are checked after every step.
"""

from __future__ import annotations

import copy
import heapq
import random
from collections.abc import Callable
from typing import Any

from kvstore.raft.node import LEADER, Config, RaftNode
from kvstore.raft.storage import MemoryStorage


class InvariantViolation(AssertionError):
    pass


class SimCluster:
    def __init__(self, n: int = 3, seed: int = 0, drop: float = 0.0, dup: float = 0.0,
                 min_delay: float = 0.001, max_delay: float = 0.03, config: Config | None = None,
                 storage_factory: Callable[[int], Any] | None = None, check: bool = True) -> None:
        self.seed = seed
        self.ids = list(range(1, n + 1))
        self.cfg = config or Config()
        self.net = random.Random(seed * 7919 + 1)
        self.drop, self.dup, self.min_delay, self.max_delay = drop, dup, min_delay, max_delay
        self.time, self.dt = 0.0, 0.01
        self.queue: list[tuple[float, int, int, int, dict]] = []
        self._seq = 0
        self.group: dict[int, int] | None = None  # partition: node -> group id
        self.storages = {i: (storage_factory or (lambda _i: MemoryStorage()))(i) for i in self.ids}
        self.nodes: dict[int, RaftNode | None] = {}
        self.applied: dict[int, list[tuple[int, Any]]] = {i: [] for i in self.ids}
        self.restarts = {i: 0 for i in self.ids}
        self.leaders_by_term: dict[int, set[int]] = {}
        self.committed: dict[int, tuple[int, Any]] = {}
        self.acked: list[tuple[int, int, Any]] = []  # (index, term, command) seen committed
        self._pending: list[tuple[int, int, Any]] = []
        self.check = check
        for i in self.ids:
            self._start(i)

    # ---------- node lifecycle ----------
    def _start(self, i: int) -> None:
        self.applied[i] = []  # the state machine is rebuilt by re-applying committed entries
        rng = random.Random(self.seed * 100003 + i * 131 + self.restarts[i])
        self.nodes[i] = RaftNode(i, self.ids, self.storages[i], rng, self.cfg,
                                 apply_fn=lambda idx, _t, cmd, i=i: self.applied[i].append((idx, cmd)),
                                 now=self.time)

    def crash(self, i: int) -> None:
        if self.nodes.get(i) is not None:
            self.nodes[i] = None

    def restart(self, i: int) -> None:
        if self.nodes.get(i) is None:
            self.restarts[i] += 1
            self._start(i)

    def up(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n is not None]

    # ---------- network ----------
    def partition(self, groups: list[set[int]]) -> None:
        self.group = {i: g for g, members in enumerate(groups) for i in members}

    def heal(self) -> None:
        self.group = None

    def _blocked(self, a: int, b: int) -> bool:
        return self.group is not None and self.group.get(a) != self.group.get(b)

    def _send(self, src: int, outs: list[tuple[int, dict]]) -> None:
        for dest, msg in outs:
            if self._blocked(src, dest) or self.net.random() < self.drop:
                continue
            copies = 2 if self.net.random() < self.dup else 1
            for _ in range(copies):
                delay = self.net.uniform(self.min_delay, self.max_delay)  # random delay => reordering
                self._seq += 1
                heapq.heappush(self.queue, (self.time + delay, self._seq, src, dest, copy.deepcopy(msg)))

    # ---------- driving ----------
    def step(self) -> None:
        self.time += self.dt
        while self.queue and self.queue[0][0] <= self.time:
            _, _, src, dest, msg = heapq.heappop(self.queue)
            node = self.nodes.get(dest)
            if node is None or self._blocked(src, dest):
                continue
            self._send(dest, node.handle(msg, self.time))
        for i in self.ids:
            node = self.nodes[i]
            if node is not None:
                self._send(i, node.tick(self.time))
        self._track_acks()
        if self.check:
            self.check_invariants()

    def run(self, seconds: float) -> None:
        for _ in range(int(round(seconds / self.dt))):
            self.step()

    def run_until(self, pred: Callable[[], bool], timeout: float = 10.0) -> bool:
        for _ in range(int(timeout / self.dt)):
            if pred():
                return True
            self.step()
        return pred()

    # ---------- clients ----------
    def leader(self) -> RaftNode | None:
        leaders = [n for n in self.up() if n.role == LEADER]
        return max(leaders, key=lambda n: n.term) if leaders else None

    def propose(self, command: Any, node_id: int | None = None) -> tuple[int, int] | None:
        node = self.nodes[node_id] if node_id is not None else self.leader()
        if node is None or node.role != LEADER:
            return None
        index, outs = node.propose(command, self.time)
        self._send(node.id, outs)
        self._pending.append((index, node.term, command))
        return index, node.term

    def _track_acks(self) -> None:
        still = []
        for index, term, cmd in self._pending:
            if self.committed.get(index) == (term, cmd) or any(
                    n.commit_index >= index and n.log[index - 1] == (term, cmd) for n in self.up()):
                self.acked.append((index, term, cmd))
            elif any(n.term > term + 3 for n in self.up()):
                continue  # superseded long ago; it may legitimately never commit
            else:
                still.append((index, term, cmd))
        self._pending = still

    # ---------- invariants ----------
    def _fail(self, msg: str) -> None:
        raise InvariantViolation(f"seed={self.seed} t={self.time:.2f}: {msg}")

    def check_invariants(self) -> None:
        ups = self.up()
        for n in ups:  # 1. at most one leader per term
            if n.role == LEADER:
                s = self.leaders_by_term.setdefault(n.term, set())
                s.add(n.id)
                if len(s) > 1:
                    self._fail(f"two leaders in term {n.term}: {sorted(s)}")
        for n in ups:  # 2. committed entries never change
            for idx in range(1, n.commit_index + 1):
                entry = n.log[idx - 1]
                if self.committed.setdefault(idx, entry) != entry:
                    self._fail(f"committed entry {idx} changed on node {n.id}: {self.committed[idx]} vs {entry}")
        histories = sorted((self.applied[n.id] for n in ups), key=len)  # 3. applied histories are prefixes
        for h in histories:
            if histories[-1][: len(h)] != h:
                self._fail("applied histories diverge")
        for a in ups:  # 4. log matching
            for b in ups:
                if a.id < b.id:
                    for idx in range(min(len(a.log), len(b.log)), 0, -1):
                        if a.log[idx - 1][0] == b.log[idx - 1][0]:
                            if a.log[:idx] != b.log[:idx]:
                                self._fail(f"log matching violated between {a.id} and {b.id} at {idx}")
                            break

    def converged(self) -> bool:
        ups = self.up()
        if not ups or len({(n.commit_index, repr(n.log)) for n in ups}) != 1:
            return False
        return ups[0].commit_index == len(ups[0].log)

    def check_acked_present(self) -> None:
        """Every write that was ever seen committed is in every live node's log."""
        for index, term, cmd in self.acked:
            for n in self.up():
                if len(n.log) < index or n.log[index - 1] != (term, cmd):
                    self._fail(f"acknowledged write {index}:{cmd} missing on node {n.id}")
