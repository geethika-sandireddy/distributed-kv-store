import time

from fastapi.testclient import TestClient

from kvstore.raft.server import create_app


def wait_leader(client, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if client.get("/status").json()["role"] == "leader":
            return
        time.sleep(0.02)
    raise AssertionError("single node never became leader")


def make(tmp_path, monkeypatch):
    monkeypatch.setenv("RAFT_ELECTION_MIN", "0.05")
    monkeypatch.setenv("RAFT_ELECTION_MAX", "0.1")
    monkeypatch.setenv("RAFT_HEARTBEAT", "0.02")
    return create_app(node_id=1, peers={1: "http://127.0.0.1:1"}, data_dir=str(tmp_path), fsync=False)


def test_put_get_delete_through_the_log(tmp_path, monkeypatch):
    with TestClient(make(tmp_path, monkeypatch)) as c:
        wait_leader(c)
        assert c.put("/kv/a", json={"value": {"n": 1}}).json() == {"ok": True}
        assert c.get("/kv/a").json() == {"key": "a", "value": {"n": 1}}
        assert c.delete("/kv/a").status_code == 200
        assert c.get("/kv/a").status_code == 404
        assert c.delete("/kv/a").status_code == 404


def test_retry_with_same_client_and_seq_applies_once(tmp_path, monkeypatch):
    with TestClient(make(tmp_path, monkeypatch)) as c:
        wait_leader(c)
        h = {"X-Client-ID": "cli", "X-Seq": "1"}
        assert c.put("/kv/k", json={"value": "first"}, headers=h).status_code == 200
        c.put("/kv/k", json={"value": "second"}, headers={"X-Client-ID": "cli", "X-Seq": "2"})
        # the client times out and retries seq 2 with a different value: must be ignored
        assert c.put("/kv/k", json={"value": "IGNORED"}, headers={"X-Client-ID": "cli", "X-Seq": "2"}).status_code == 200
        assert c.get("/kv/k", headers={"X-Client-ID": "other", "X-Seq": "1"}).json()["value"] == "second"


def test_ttl_expires_by_leader_timestamp(tmp_path, monkeypatch):
    with TestClient(make(tmp_path, monkeypatch)) as c:
        wait_leader(c)
        c.put("/kv/t", json={"value": 1, "ttl_seconds": 0.2})
        assert c.get("/kv/t").status_code == 200
        time.sleep(0.3)
        assert c.get("/kv/t").status_code == 404


def test_state_survives_restart_by_replaying_the_log(tmp_path, monkeypatch):
    with TestClient(make(tmp_path, monkeypatch)) as c:
        wait_leader(c)
        c.put("/kv/persist", json={"value": "yes"})
    with TestClient(make(tmp_path, monkeypatch)) as c:
        wait_leader(c)
        assert c.get("/kv/persist").json()["value"] == "yes"
