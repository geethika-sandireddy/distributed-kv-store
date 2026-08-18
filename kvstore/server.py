"""
HTTP API layer over DurableStore.

Kept intentionally thin: this module's only job is request/response
translation. All the actual logic (durability, TTL, eviction) lives in
recovery.py/store.py/wal.py where it's unit-tested without needing an
HTTP client.
"""

import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from kvstore.recovery import DurableStore
from kvstore.store import KeyNotFoundError

WAL_PATH = os.environ.get("KVSTORE_WAL_PATH", "data/kvstore.wal")
MAX_SIZE = int(os.environ["KVSTORE_MAX_SIZE"]) if os.environ.get("KVSTORE_MAX_SIZE") else None

os.makedirs(os.path.dirname(WAL_PATH) or ".", exist_ok=True)

app = FastAPI(title="Persistent KV Store")
store = DurableStore(WAL_PATH, max_size=MAX_SIZE)


class SetRequest(BaseModel):
    value: Any
    ttl_seconds: Optional[float] = None


class GetResponse(BaseModel):
    key: str
    value: Any


class StatusResponse(BaseModel):
    size: int
    replayed_on_startup: int


@app.get("/status", response_model=StatusResponse)
def status():
    return StatusResponse(size=len(store), replayed_on_startup=store.replayed_count)


@app.put("/keys/{key}")
def set_key(key: str, body: SetRequest):
    store.set(key, body.value, ttl_seconds=body.ttl_seconds)
    return {"key": key, "status": "ok"}


@app.get("/keys/{key}", response_model=GetResponse)
def get_key(key: str):
    try:
        value = store.get(key)
    except KeyNotFoundError:
        raise HTTPException(status_code=404, detail=f"key '{key}' not found")
    return GetResponse(key=key, value=value)


@app.delete("/keys/{key}")
def delete_key(key: str):
    if not store.exists(key):
        raise HTTPException(status_code=404, detail=f"key '{key}' not found")
    store.delete(key)
    return {"key": key, "status": "deleted"}


@app.get("/keys")
def list_keys():
    return {"keys": store.keys()}
