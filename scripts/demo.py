"""Failover demo: start 3 real processes, write, SIGKILL the leader, show nothing acknowledged is lost.

    python scripts/demo.py
"""

import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kvstore.raft.localcluster import LocalCluster  # noqa: E402


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        c = LocalCluster(3, tmp)
        try:
            c.start_all()
            leader = c.wait_leader()
            print(f"cluster up, leader is node {leader}")
            for n in range(10):
                c.put(f"user:{n}", {"name": f"user{n}"})
            print("10 writes acknowledged (each one committed by a majority)")
            c.kill(leader)
            print(f"SIGKILLed leader node {leader}")
            t0 = time.time()
            new = c.wait_leader(exclude=leader)
            print(f"node {new} elected leader after {time.time() - t0:.2f}s")
            ok = all(c.get(new, f"user:{n}").json()["value"] == {"name": f"user{n}"} for n in range(10))
            print("all 10 acknowledged writes still readable:", ok)
            c.put("user:after", {"name": "after-failover"})
            c.start(leader)
            time.sleep(2)
            print(f"old leader {leader} restarted and rejoined as:", c.status(leader)["role"])
        finally:
            c.stop()


if __name__ == "__main__":
    main()
