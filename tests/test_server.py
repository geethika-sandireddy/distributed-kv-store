import importlib
import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    # Each test gets its own WAL file and a fresh import of the server
    # module, since it builds a module-level DurableStore at import time.
    monkeypatch.setenv("KVSTORE_WAL_PATH", str(tmp_path / "test.wal"))
    monkeypatch.delenv("KVSTORE_MAX_SIZE", raising=False)
    import kvstore.server as server_module
    importlib.reload(server_module)
    return TestClient(server_module.app)


def test_set_and_get(client):
    r = client.put("/keys/a", json={"value": 1})
    assert r.status_code == 200
    r = client.get("/keys/a")
    assert r.status_code == 200
    assert r.json() == {"key": "a", "value": 1}


def test_get_missing_key_404(client):
    r = client.get("/keys/missing")
    assert r.status_code == 404


def test_delete_key(client):
    client.put("/keys/a", json={"value": 1})
    r = client.delete("/keys/a")
    assert r.status_code == 200
    r = client.get("/keys/a")
    assert r.status_code == 404


def test_delete_missing_key_404(client):
    r = client.delete("/keys/missing")
    assert r.status_code == 404


def test_list_keys(client):
    client.put("/keys/a", json={"value": 1})
    client.put("/keys/b", json={"value": 2})
    r = client.get("/keys")
    assert set(r.json()["keys"]) == {"a", "b"}


def test_status_endpoint(client):
    client.put("/keys/a", json={"value": 1})
    r = client.get("/status")
    body = r.json()
    assert body["size"] == 1
    assert body["replayed_on_startup"] == 0


def test_set_with_ttl(client):
    r = client.put("/keys/a", json={"value": 1, "ttl_seconds": 30})
    assert r.status_code == 200
    r = client.get("/keys/a")
    assert r.json()["value"] == 1


def test_rate_limit_returns_429_once_exceeded(monkeypatch, tmp_path):
    # Small capacity + effectively no refill so this test isn't timing-sensitive.
    monkeypatch.setenv("KVSTORE_WAL_PATH", str(tmp_path / "rl.wal"))
    monkeypatch.setenv("KVSTORE_RATE_LIMIT_CAPACITY", "3")
    monkeypatch.setenv("KVSTORE_RATE_LIMIT_REFILL", "0.001")
    import kvstore.server as server_module
    importlib.reload(server_module)
    rl_client = TestClient(server_module.app)

    for _ in range(3):
        r = rl_client.get("/keys/missing", headers={"X-Client-ID": "test-client"})
        assert r.status_code == 404  # allowed through, just no such key

    r = rl_client.get("/keys/missing", headers={"X-Client-ID": "test-client"})
    assert r.status_code == 429


def test_status_endpoint_exempt_from_rate_limit(monkeypatch, tmp_path):
    monkeypatch.setenv("KVSTORE_WAL_PATH", str(tmp_path / "rl2.wal"))
    monkeypatch.setenv("KVSTORE_RATE_LIMIT_CAPACITY", "1")
    monkeypatch.setenv("KVSTORE_RATE_LIMIT_REFILL", "0.001")
    import kvstore.server as server_module
    importlib.reload(server_module)
    rl_client = TestClient(server_module.app)

    for _ in range(5):
        r = rl_client.get("/status")
        assert r.status_code == 200
