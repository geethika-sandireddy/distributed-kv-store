# Contributing

This started as a learning project (from-scratch KV store internals: WAL,
crash recovery, LRU, rate limiting), but PRs and issues are welcome.

## Setup

```bash
git clone https://github.com/geethika-sandireddy/distributed-kv-store.git
cd distributed-kv-store
pip install -r requirements.txt
pip install ruff mypy  # for lint/type-checking, matches CI
```

## Before opening a PR

```bash
pytest tests/ -v
ruff check .
mypy kvstore/
```

All three run in CI on every push/PR — a PR that fails any of them won't
merge cleanly.

## Code style

- Type hints on public functions/methods.
- Docstrings that explain *why* a design choice was made, not just what the
  code does — see `kvstore/wal.py` or `kvstore/recovery.py` for the tone
  this project aims for. A comment justifying a length-prefixed binary
  format over newline-delimited JSON is more useful than one restating
  "this writes a record."
- New behavior gets a test in the same commit (or the very next one) that
  adds it, not batched in later. Check `tests/` for the file that matches
  the module you're touching — `test_wal.py` for `wal.py`, etc.

## Reporting bugs

Open an issue with: what you ran, what you expected, what happened instead.
If it's a durability/crash-recovery bug specifically, include the sequence
of operations that led to it if you can reproduce it — those are the bugs
this project cares most about getting right.

## Scope

Single-node only, on purpose (see README's "Why" and the note on skipping
distributed mode). PRs adding replication/clustering are probably out of
scope for this repo, though I'm happy to be argued out of that.
