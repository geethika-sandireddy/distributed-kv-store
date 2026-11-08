"""Linearizability checker for a single read/write register (Wing & Gong style search).

Each key of the store is an independent register, so histories are checked per key. An
operation has a real-time interval [start, end]; it must take effect at one instant inside it.
A write whose outcome is unknown (client gave up) has end=inf and may be skipped entirely.
"""

from __future__ import annotations

import math
import sys
from dataclasses import dataclass
from functools import cache
from typing import Any


@dataclass(frozen=True)
class Op:
    kind: str  # "w" or "r"
    value: Any  # value written, or value observed by the read (None = key absent)
    start: float
    end: float
    maybe: bool = False  # unknown-outcome write: may or may not have taken effect


def is_linearizable(ops: list[Op], initial: Any = None) -> bool:
    sys.setrecursionlimit(max(10_000, 50 * len(ops)))

    @cache
    def solve(remaining: frozenset[int], state: Any) -> bool:
        if all(ops[i].maybe for i in remaining):
            return True
        horizon = min(ops[i].end for i in remaining)
        for i in remaining:
            op = ops[i]
            if op.start > horizon:
                continue  # some other op finished before this one even started: it must go first
            rest = remaining - {i}
            if op.kind == "w":
                if solve(rest, op.value):
                    return True
                if op.maybe and solve(rest, state):  # the unknown write never happened
                    return True
            elif op.value == state and solve(rest, state):
                return True
        return False

    return solve(frozenset(range(len(ops))), initial)


def check_history(history: dict[str, list[Op]]) -> tuple[bool, str | None]:
    """Check every key; returns (ok, name of the first key that is not linearizable)."""
    for key, ops in history.items():
        if not is_linearizable(sorted(ops, key=lambda o: o.start)):
            return False, key
    return True, None


INF = math.inf
