"""
HTTP API layer over DurableStore.

Kept intentionally thin: this module's only job is request/response
translation. All the actual logic (durability, TTL, eviction) lives in
recovery.py/store.py/wal.py where it's unit-tested without needing an
HTTP client.
"""

import os
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvstore.recovery import DurableStore
from kvstore.store import KeyNotFoundError
from kvstore.rate_limiter import TokenBucketRateLimiter

WAL_PATH = os.environ.get("KVSTORE_WAL_PATH", "data/kvstore.wal")
MAX_SIZE = int(os.environ["KVSTORE_MAX_SIZE"]) if os.environ.get("KVSTORE_MAX_SIZE") else None
RATE_LIMIT_CAPACITY = int(os.environ.get("KVSTORE_RATE_LIMIT_CAPACITY", "100"))
RATE_LIMIT_REFILL = float(os.environ.get("KVSTORE_RATE_LIMIT_REFILL", "50"))

os.makedirs(os.path.dirname(WAL_PATH) or ".", exist_ok=True)

app = FastAPI(title="Persistent KV Store")
store = DurableStore(WAL_PATH, max_size=MAX_SIZE)
rate_limiter = TokenBucketRateLimiter(capacity=RATE_LIMIT_CAPACITY, refill_rate=RATE_LIMIT_REFILL)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    # /status and /health are exempt so monitoring/health checks never get throttled.
    if request.url.path in ("/status", "/health"):
        return await call_next(request)
    client_id = request.headers.get("X-Client-ID", request.client.host if request.client else "unknown")
    if not rate_limiter.allow(client_id):
        return JSONResponse(status_code=429, content={"detail": "rate limit exceeded"})
    return await call_next(request)


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


@app.get("/health")
def health():
    """Liveness check, distinct from /status: this only confirms the
    process is up and able to respond, not the store's contents -- the
    kind of thing a load balancer or orchestrator polls frequently and
    shouldn't need to reason about dataset size for."""
    return {"status": "ok"}


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


class BatchSetRequest(BaseModel):
    items: dict[str, Any]
    ttl_seconds: Optional[float] = None


@app.post("/keys:batch")
def batch_set(body: BatchSetRequest):
    """Sets multiple keys in one request. Each key still gets its own
    WAL record/fsync internally (no batched-fsync fast path yet -- see
    README for that as a known limitation) -- this endpoint saves
    round-trips, not fsyncs."""
    for key, value in body.items.items():
        store.set(key, value, ttl_seconds=body.ttl_seconds)
    return {"count": len(body.items), "status": "ok"}


@app.post("/admin/compact")
def compact():
    n = store.compact()
    return {"snapshot_entries": n, "status": "ok"}
