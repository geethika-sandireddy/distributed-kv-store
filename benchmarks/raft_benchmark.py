"""Commit latency and throughput: 1-node vs 3-node Raft cluster, real processes over HTTP, fsync on.

    python benchmarks/raft_benchmark.py [--ops 300] [--concurrency 8]

Writes results/raft_bench.json (numbers + machine info) and prints a markdown table.
Latency = one sequential client doing PUTs; throughput = N concurrent clients.
Every write is acknowledged only after a majority has fsynced it.
"""

import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from kvstore.raft.localcluster import LocalCluster  # noqa: E402


def machine_info() -> dict:
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")), "unknown")
    return {"cpu": cpu, "logical_cpus": __import__("os").cpu_count(), "os": platform.platform(),
            "python": platform.python_version(), "fsync": True,
            "note": "sandbox/dev machine; rerun on your own hardware before quoting"}


def pct(data: list[float], p: float) -> float:
    s = sorted(data)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def run(n_nodes: int, ops: int, conc: int) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        c = LocalCluster(n_nodes, tmp, election=(0.3, 0.6) if n_nodes > 1 else (0.1, 0.2))
        try:
            c.start_all()
            leader = c.wait_leader()
            base = c.url(leader)
            for i in range(20):  # warm-up
                c.put(f"w{i}", i, client_id="warm", seq=i + 1, start=leader)
            lat = []
            with httpx.Client(timeout=5.0) as http:
                for i in range(ops):
                    t0 = time.perf_counter()
                    r = http.put(f"{base}/kv/lat{i}", json={"value": i}, headers={"X-Client-ID": "lat", "X-Seq": str(i + 1)})
                    assert r.status_code == 200, r.text
                    lat.append((time.perf_counter() - t0) * 1000)

            def worker(w: int) -> int:
                done = 0
                with httpx.Client(timeout=10.0) as http:
                    for i in range(ops // conc):
                        r = http.put(f"{base}/kv/t{w}_{i}", json={"value": i},
                                     headers={"X-Client-ID": f"w{w}", "X-Seq": str(i + 1)})
                        done += r.status_code == 200
                return done

            t0 = time.perf_counter()
            with ThreadPoolExecutor(conc) as ex:
                done = sum(ex.map(worker, range(conc)))
            elapsed = time.perf_counter() - t0
            return {"nodes": n_nodes, "sequential_ops": ops, "p50_ms": round(pct(lat, 50), 2),
                    "p95_ms": round(pct(lat, 95), 2), "p99_ms": round(pct(lat, 99), 2),
                    "mean_ms": round(statistics.mean(lat), 2), "concurrency": conc,
                    "throughput_ops_s": round(done / elapsed, 1), "concurrent_ops_ok": done}
        finally:
            c.stop()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=300)
    ap.add_argument("--concurrency", type=int, default=8)
    a = ap.parse_args()
    results = [run(1, a.ops, a.concurrency), run(3, a.ops, a.concurrency)]
    out = {"machine": machine_info(), "results": results, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())}
    Path("results").mkdir(exist_ok=True)
    Path("results/raft_bench.json").write_text(json.dumps(out, indent=2))
    print("| nodes | p50 (ms) | p95 (ms) | p99 (ms) | throughput (ops/s, %d clients) |\n|---|---|---|---|---|" % a.concurrency)
    for r in results:
        print(f"| {r['nodes']} | {r['p50_ms']} | {r['p95_ms']} | {r['p99_ms']} | {r['throughput_ops_s']} |")


if __name__ == "__main__":
    main()
