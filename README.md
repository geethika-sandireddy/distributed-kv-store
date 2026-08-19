# Persistent Key-Value Store

A from-scratch persistent key-value store with crash recovery and rate
limiting. No Redis, no external database libraries — the storage engine,
durability layer, and eviction policy are all hand-built.

## Why

Most CRUD projects call a database and stop thinking. This one *is* the
database: an in-memory hash map, a write-ahead log for durability, replay-based
crash recovery, LRU eviction under a size cap, and a token-bucket rate
limiter in front of it — the pieces you'd otherwise get for free from
Postgres or Redis, built and tested individually.

## Architecture

```
kvstore/
  store.py        # thread-safe hashmap + TTL + LRU eviction
  ttl.py           # lazy expiry (checked on read, not a background thread)
  lru.py           # doubly linked list LRU tracker, O(1) touch/evict
  wal.py           # write-ahead log: length-prefixed, checksummed records
  recovery.py      # DurableStore: WAL + Store, replay-based crash recovery
  server.py        # FastAPI HTTP layer (thin — logic lives above)
  rate_limiter.py  # token bucket, per-client
tests/
  test_store.py / test_ttl.py / test_lru.py / test_store_lru.py
  test_wal.py           # includes torn-write / corruption cases
  test_recovery.py      # failure-injection: crash at each stage of a write
  test_server.py
  test_rate_limiter.py
benchmarks/
  benchmark.py     # throughput + p50/p95/p99 latency at varying concurrency
```

## Durability model

Every write goes: **WAL.append() (fsync-blocking) → apply to memory → return
to caller.** A write is durable the instant `append()` returns — before the
in-memory store is touched. On restart, `DurableStore` replays the WAL from
scratch, so memory always converges to what the log says, regardless of
exactly when a previous process crashed relative to its in-memory apply.

The WAL record format is `[4-byte length][4-byte CRC32][JSON payload]`
rather than newline-delimited JSON, specifically so replay can detect a
torn write (the process died mid-`write()`) deterministically: if the file
doesn't have the promised number of bytes, or the checksum doesn't match,
that's the tail of a crash, and replay stops there rather than guessing.
Everything before the tear is kept.

`tests/test_recovery.py` simulates this directly: crash before the WAL
record lands, crash after it's fsynced but before the in-memory apply, and
a truncated final record — each verified to leave exactly the state the
durability contract promises.

## Running it

```bash
pip install -r requirements.txt
uvicorn kvstore.server:app --reload
```

Env vars: `KVSTORE_WAL_PATH` (default `data/kvstore.wal`), `KVSTORE_MAX_SIZE`
(unset = no eviction cap).

```bash
curl -X PUT localhost:8000/keys/foo -d '{"value": "bar", "ttl_seconds": 60}'
curl localhost:8000/keys/foo
curl -X DELETE localhost:8000/keys/foo
curl localhost:8000/status
```

## Tests

```bash
pytest tests/ -v
```

## Benchmarks

```bash
python benchmarks/benchmark.py
```
