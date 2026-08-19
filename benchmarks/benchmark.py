"""
Throughput + latency benchmark for DurableStore, bypassing the HTTP
layer (measuring the store directly, not network/ASGI overhead --
that's a separate, honest number from an HTTP-level load test).

Measures, at each concurrency level: total ops/sec, and p50/p95/p99
latency per operation in milliseconds. Uses a thread pool since the
store's operations are I/O-bound (fsync) rather than CPU-bound, so
threads (not processes) are the right tool here and avoid the
complexity of sharing a WAL file handle across processes.

Run:
    python benchmarks/benchmark.py
"""

import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kvstore.recovery import DurableStore

OPS_PER_CLIENT = 200
CONCURRENCY_LEVELS = [1, 10, 50, 100]


def percentile(data: list[float], p: float) -> float:
    data = sorted(data)
    k = (len(data) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(data) - 1)
    if f == c:
        return data[f]
    return data[f] + (k - f) * (data[c] - data[f])


def worker(store: DurableStore, client_idx: int, n_ops: int) -> list[float]:
    latencies = []
    for i in range(n_ops):
        key = f"bench-client{client_idx}-key{i}"
        start = time.perf_counter()
        store.set(key, {"payload": "x" * 64})
        store.get(key)
        latencies.append((time.perf_counter() - start) * 1000)  # ms
    return latencies


def run_at_concurrency(wal_path: str, concurrency: int) -> None:
    store = DurableStore(wal_path, fsync=True)
    all_latencies: list[float] = []

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, store, i, OPS_PER_CLIENT) for i in range(concurrency)]
        for f in futures:
            all_latencies.extend(f.result())
    elapsed = time.perf_counter() - start
    store.close()

    total_ops = concurrency * OPS_PER_CLIENT * 2  # set + get each counted
    throughput = total_ops / elapsed

    print(f"\nConcurrency: {concurrency}")
    print(f"  Total ops:     {total_ops}")
    print(f"  Elapsed:       {elapsed:.3f}s")
    print(f"  Throughput:    {throughput:.1f} ops/sec")
    print(f"  Latency p50:   {percentile(all_latencies, 50):.3f} ms")
    print(f"  Latency p95:   {percentile(all_latencies, 95):.3f} ms")
    print(f"  Latency p99:   {percentile(all_latencies, 99):.3f} ms")


def main():
    import tempfile
    import os

    print("Persistent KV Store -- benchmark")
    print(f"({OPS_PER_CLIENT} set+get pairs per client, fsync=True on every write)")

    for concurrency in CONCURRENCY_LEVELS:
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "bench.wal")
            run_at_concurrency(wal_path, concurrency)


if __name__ == "__main__":
    main()
