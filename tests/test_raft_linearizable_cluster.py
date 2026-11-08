"""Concurrent clients against a real 3-process cluster while the leader is SIGKILLed.
The recorded history must be linearizable (checked per key)."""

import random
import threading
import time

from kvstore.raft.linearizability import INF, Op, check_history
from kvstore.raft.localcluster import LocalCluster


def test_history_is_linearizable_through_leader_kill(tmp_path):
    c = LocalCluster(3, str(tmp_path), election=(0.3, 0.6))
    history: dict[str, list[Op]] = {"a": [], "b": []}
    lock = threading.Lock()
    counter = iter(range(1, 10_000))

    def worker(wid: int) -> None:
        rng = random.Random(wid)
        for n in range(14):
            key = rng.choice(["a", "b"])
            t0 = time.time()
            if rng.random() < 0.5:
                with lock:
                    value = next(counter)
                try:
                    c.put(key, value, client_id=f"w{wid}", seq=n + 1)
                    op = Op("w", value, t0, time.time())
                except TimeoutError:
                    op = Op("w", value, t0, INF, maybe=True)
            else:
                try:
                    op = Op("r", c.read(key), t0, time.time())
                except TimeoutError:
                    continue  # a read that never returned has no effect
            with lock:
                history[key].append(op)
            time.sleep(rng.uniform(0.0, 0.03))

    try:
        c.start_all()
        leader = c.wait_leader()
        threads = [threading.Thread(target=worker, args=(w,)) for w in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.5)
        c.kill(leader)  # SIGKILL mid-workload
        time.sleep(2.5)
        c.start(leader)
        for t in threads:
            t.join(timeout=90)
        total = sum(len(v) for v in history.values())
        assert total >= 40, f"workload produced too few operations ({total})"
        ok, bad_key = check_history(history)
        assert ok, f"history for key {bad_key!r} is NOT linearizable: {history[bad_key]}"
    finally:
        c.stop()
