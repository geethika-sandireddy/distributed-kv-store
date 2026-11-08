"""Start and control a real multi-process Raft cluster on localhost (demo, benchmark, tests)."""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import sys
import time

import httpx


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class LocalCluster:
    def __init__(self, n: int, base_dir: str, fsync: bool = True, election=(0.3, 0.6), heartbeat=0.05):
        self.ids = list(range(1, n + 1))
        self.ports = {i: free_port() for i in self.ids}
        self.peers = ",".join(f"{i}=http://127.0.0.1:{p}" for i, p in self.ports.items())
        self.base_dir, self.fsync, self.election, self.heartbeat = base_dir, fsync, election, heartbeat
        self.procs: dict[int, subprocess.Popen] = {}
        self.http = httpx.Client(timeout=3.0)
        self.seq = 0

    def url(self, i: int) -> str:
        return f"http://127.0.0.1:{self.ports[i]}"

    def start(self, i: int) -> None:
        env = {**os.environ, "NODE_ID": str(i), "PEERS": self.peers, "DATA_DIR": os.path.join(self.base_dir, f"n{i}"),
               "KVSTORE_FSYNC": "1" if self.fsync else "0", "RAFT_ELECTION_MIN": str(self.election[0]),
               "RAFT_ELECTION_MAX": str(self.election[1]), "RAFT_HEARTBEAT": str(self.heartbeat)}
        self.procs[i] = subprocess.Popen([sys.executable, "-m", "uvicorn", "kvstore.raft.server:create_app", "--factory",
                                          "--port", str(self.ports[i]), "--log-level", "warning"], env=env)

    def start_all(self) -> None:
        for i in self.ids:
            self.start(i)

    def alive(self) -> list[int]:
        return [i for i, p in self.procs.items() if p.poll() is None]

    def kill(self, i: int) -> None:
        os.kill(self.procs[i].pid, signal.SIGKILL)  # no graceful shutdown
        self.procs[i].wait()

    def status(self, i: int) -> dict | None:
        try:
            return self.http.get(self.url(i) + "/status").json()
        except (httpx.HTTPError, ValueError):
            return None

    def wait_leader(self, timeout: float = 20.0, exclude: int | None = None) -> int:
        end = time.time() + timeout
        while time.time() < end:
            for i in self.alive():
                s = self.status(i)
                if s and s["role"] == "leader" and i != exclude:
                    return i
            time.sleep(0.1)
        raise TimeoutError("no leader elected in time")

    def put(self, key: str, value, client_id: str = "client", seq: int | None = None, start: int | None = None) -> int:
        """Write via the leader (following 503 hints), retrying with the same (client, seq).
        Returns the id of the node that acknowledged it."""
        if seq is None:
            self.seq += 1
            seq = self.seq
        target = start or self.alive()[0]
        end = time.time() + 20
        while time.time() < end:
            try:
                r = self.http.put(self.url(target) + f"/kv/{key}", json={"value": value},
                                  headers={"X-Client-ID": client_id, "X-Seq": str(seq)})
                if r.status_code == 200:
                    return target
                hint = r.json().get("leader_id")
                if hint in self.alive():
                    target = hint
            except (httpx.HTTPError, ValueError):
                pass
            if target not in self.alive():
                target = self.alive()[0]
            time.sleep(0.05)
        raise TimeoutError(f"write {key} was never acknowledged")

    def get(self, i: int, key: str) -> httpx.Response:
        return self.http.get(self.url(i) + f"/kv/{key}")

    def stop(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
            p.wait()
        self.http.close()
