"""Real 3-process cluster: SIGKILL the leader, check failover and that acknowledged writes survive."""

import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import pytest


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Cluster:
    def __init__(self, tmp_path):
        self.ports = {i: free_port() for i in (1, 2, 3)}
        self.peers = ",".join(f"{i}=http://127.0.0.1:{p}" for i, p in self.ports.items())
        self.tmp, self.procs = tmp_path, {}
        self.http = httpx.Client(timeout=3.0)

    def url(self, i):
        return f"http://127.0.0.1:{self.ports[i]}"

    def start(self, i):
        env = {**os.environ, "NODE_ID": str(i), "PEERS": self.peers, "DATA_DIR": str(self.tmp / f"n{i}"),
               "RAFT_ELECTION_MIN": "0.3", "RAFT_ELECTION_MAX": "0.6", "RAFT_HEARTBEAT": "0.05"}
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "kvstore.raft.server:create_app", "--factory",
             "--port", str(self.ports[i]), "--log-level", "warning"], env=env)

    def status(self, i):
        try:
            return self.http.get(self.url(i) + "/status").json()
        except httpx.HTTPError:
            return None

    def wait_leader(self, among, timeout=20.0, exclude=None):
        end = time.time() + timeout
        while time.time() < end:
            for i in among:
                s = self.status(i)
                if s and s["role"] == "leader" and i != exclude:
                    return i
            time.sleep(0.1)
        raise AssertionError("no leader elected in time")

    def put(self, key, value, seq, cid="tester"):
        """Write via whichever node is leader; follow the leader hint; retry with the same (client, seq)."""
        end = time.time() + 20
        target = next(iter(self.procs))
        while time.time() < end:
            try:
                r = self.http.put(self.url(target) + f"/kv/{key}", json={"value": value},
                                  headers={"X-Client-ID": cid, "X-Seq": str(seq)})
                if r.status_code == 200:
                    return
                hint = r.json().get("leader_id")
                target = hint if hint in self.procs and self.procs[hint].poll() is None else target
            except httpx.HTTPError:
                pass
            alive = [i for i, p in self.procs.items() if p.poll() is None]
            target = target if target in alive else alive[0]
            time.sleep(0.1)
        raise AssertionError(f"write {key} never acknowledged")

    def get(self, i, key):
        return self.http.get(self.url(i) + f"/kv/{key}")

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
            p.wait()
        self.http.close()


@pytest.fixture
def cluster(tmp_path):
    c = Cluster(tmp_path)
    for i in (1, 2, 3):
        c.start(i)
    yield c
    c.stop()


def test_kill_leader_failover_and_acknowledged_writes_survive(cluster):
    leader = cluster.wait_leader([1, 2, 3])
    for n in range(20):
        cluster.put(f"k{n}", f"v{n}", seq=n + 1)  # each returns only after a majority committed it
    follower = next(i for i in (1, 2, 3) if i != leader)
    r = cluster.get(follower, "k0")
    assert r.status_code == 503 and r.json()["leader_id"] == leader  # followers point at the leader

    os.kill(cluster.procs[leader].pid, signal.SIGKILL)  # no graceful shutdown
    cluster.procs[leader].wait()
    survivors = [i for i in (1, 2, 3) if i != leader]
    new_leader = cluster.wait_leader(survivors, exclude=leader)
    assert new_leader != leader

    for n in range(20):  # every acknowledged write survived the failover
        r = cluster.get(new_leader, f"k{n}")
        assert r.status_code == 200 and r.json()["value"] == f"v{n}", f"lost k{n}"
    cluster.put("after", "failover", seq=100)

    cluster.start(leader)  # old leader rejoins from its on-disk log and catches up
    end = time.time() + 20
    while time.time() < end:
        a, b = cluster.status(leader), cluster.status(new_leader)
        if a and b and a["log_length"] == b["log_length"] and a["commit_index"] == b["commit_index"]:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("restarted node did not catch up")
    assert cluster.status(leader)["role"] == "follower"
