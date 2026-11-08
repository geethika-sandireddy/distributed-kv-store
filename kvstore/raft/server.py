"""HTTP layer for one node of a Raft cluster.

Run one process per node:

    NODE_ID=1 PEERS="1=http://127.0.0.1:8101,2=http://127.0.0.1:8102,3=http://127.0.0.1:8103" \
    DATA_DIR=data/n1 uvicorn kvstore.raft.server:create_app --factory --port 8101

Client API: PUT/GET/DELETE /kv/{key}. Writes AND reads go through the Raft log (simple and
linearizable), so a response means a majority committed the command. A follower answers 503
with the leader's address. Send X-Client-ID and X-Seq to make retries idempotent.
Peer API: POST /raft/request_vote, POST /raft/append_entries. Monitoring: GET /status.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from kvstore.raft.node import LEADER, Config, RaftNode
from kvstore.raft.statemachine import KVStateMachine
from kvstore.raft.storage import FileStorage

TICK_SECONDS = 0.01
CLIENT_TIMEOUT = 3.0
RPC_TIMEOUT = 0.3
MAX_INFLIGHT_PER_PEER = 2


def parse_peers(spec: str) -> dict[int, str]:
    peers = {}
    for part in spec.split(","):
        if part.strip():
            node_id, url = part.split("=", 1)
            peers[int(node_id)] = url.strip().rstrip("/")
    return peers


class NotLeader(Exception):
    pass


class RaftServer:
    def __init__(self, node_id: int, peers: dict[int, str], data_dir: str, config: Config | None = None,
                 fsync: bool = True, max_size: int | None = None) -> None:
        self.node_id, self.peers = node_id, peers
        self.sm = KVStateMachine(max_size=max_size)
        self.storage = FileStorage(data_dir, fsync=fsync)
        self.node = RaftNode(node_id, sorted(peers), self.storage, random.Random(),
                             config, apply_fn=self.sm.apply, now=time.monotonic())
        self.lock = asyncio.Lock()
        self.http = httpx.AsyncClient(timeout=RPC_TIMEOUT)
        self._task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()
        self._inflight = dict.fromkeys(peers, 0)

    # ---------- lifecycle ----------
    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        for t in list(self._tasks):
            t.cancel()
        await self.http.aclose()
        self.storage.close()

    async def _run(self) -> None:
        while True:
            async with self.lock:
                outs = self.node.tick(time.monotonic())
            self._dispatch(outs)
            await asyncio.sleep(TICK_SECONDS)

    # ---------- peer RPC ----------
    def _dispatch(self, outs: list[tuple[int, dict]]) -> None:
        for dest, msg in outs:
            if self._inflight.get(dest, 0) >= MAX_INFLIGHT_PER_PEER:
                continue  # a slow or dead peer must not pile up requests; Raft retries on the next heartbeat
            task = asyncio.create_task(self._send_one(dest, msg))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _send_one(self, dest: int, msg: dict) -> None:
        path = "/raft/request_vote" if msg["type"] == "request_vote" else "/raft/append_entries"
        self._inflight[dest] += 1
        try:
            resp = await self.http.post(self.peers[dest] + path, json=msg)
            if resp.status_code != 200:
                return
            reply = resp.json()
        except (httpx.HTTPError, ValueError):
            return
        finally:
            self._inflight[dest] -= 1
        async with self.lock:
            outs = self.node.handle(reply, time.monotonic())
        self._dispatch(outs)

    async def on_peer_message(self, msg: dict) -> dict | None:
        async with self.lock:
            outs = self.node.handle(msg, time.monotonic())  # persisted before the reply is returned
        reply = next((m for d, m in outs if d == msg["from"]), None)
        self._dispatch([(d, m) for d, m in outs if d != msg["from"]])
        return reply

    # ---------- client path ----------
    def leader_addr(self) -> str | None:
        lid = self.node.leader_id
        return self.peers.get(lid) if lid is not None else None

    async def execute(self, op: str, key: str, value: Any = None, ttl: float | None = None,
                      client_id: str | None = None, seq: int | None = None) -> dict:
        cmd = {"op": op, "key": key, "value": value, "ttl": ttl,
               "client_id": client_id or uuid.uuid4().hex, "seq": seq if seq is not None else 1}
        async with self.lock:
            if self.node.role != LEADER:
                raise NotLeader
            cmd["ts"] = time.time()  # the leader's clock stamps the command; replicas never read their own
            index, outs = self.node.propose(cmd, time.monotonic())
            term = self.node.term
        self._dispatch(outs)
        deadline = time.monotonic() + CLIENT_TIMEOUT
        while time.monotonic() < deadline:
            async with self.lock:
                if self.node.last_applied >= index:
                    if self.node.log[index - 1][0] != term:
                        raise NotLeader  # our entry was replaced by a newer leader's log
                    result = self.sm.results.get(index)
                    if result is not None:
                        return result
                    break
            await asyncio.sleep(0.003)
        raise TimeoutError


class PutBody(BaseModel):
    value: Any
    ttl_seconds: float | None = None


def create_app(node_id: int | None = None, peers: dict[int, str] | None = None,
               data_dir: str | None = None, fsync: bool | None = None) -> FastAPI:
    node_id = node_id if node_id is not None else int(os.environ["NODE_ID"])
    peers = peers or parse_peers(os.environ["PEERS"])
    data_dir = data_dir or os.environ.get("DATA_DIR", f"data/node{node_id}")
    if fsync is None:
        fsync = os.environ.get("KVSTORE_FSYNC", "1") != "0"
    cfg = Config(election_timeout=(float(os.environ.get("RAFT_ELECTION_MIN", "0.5")),
                                   float(os.environ.get("RAFT_ELECTION_MAX", "1.0"))),
                 heartbeat_interval=float(os.environ.get("RAFT_HEARTBEAT", "0.1")))
    server = RaftServer(node_id, peers, data_dir, cfg, fsync)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        server.start()
        yield
        await server.stop()

    app = FastAPI(title=f"Raft KV node {node_id}", lifespan=lifespan)
    app.state.server = server

    def not_leader() -> JSONResponse:
        addr = server.leader_addr()
        headers = {"X-Leader": addr} if addr else {}
        return JSONResponse(status_code=503, headers=headers, content={
            "detail": "not leader", "leader_id": server.node.leader_id, "leader_addr": addr})

    async def run(request: Request, op: str, key: str, **kw: Any) -> JSONResponse:
        seq = request.headers.get("X-Seq")
        try:
            result = await server.execute(op, key, client_id=request.headers.get("X-Client-ID"),
                                          seq=int(seq) if seq else None, **kw)
        except NotLeader:
            return not_leader()
        except TimeoutError:
            return JSONResponse(status_code=503, content={"detail": "commit timed out; outcome unknown, retry with the same X-Client-ID/X-Seq"})
        if not result["ok"]:
            code = 404 if result.get("error") == "not_found" else 409
            return JSONResponse(status_code=code, content={"detail": result.get("error")})
        body = {"key": key, "value": result["value"]} if op == "GET" else {"ok": True}
        return JSONResponse(content=body)

    @app.put("/kv/{key}")
    async def put(key: str, body: PutBody, request: Request):
        return await run(request, "SET", key, value=body.value, ttl=body.ttl_seconds)

    @app.get("/kv/{key}")
    async def get(key: str, request: Request):
        return await run(request, "GET", key)

    @app.delete("/kv/{key}")
    async def delete(key: str, request: Request):
        return await run(request, "DELETE", key)

    @app.post("/raft/request_vote")
    async def request_vote(msg: dict):
        return JSONResponse(content=await server.on_peer_message(msg))

    @app.post("/raft/append_entries")
    async def append_entries(msg: dict):
        return JSONResponse(content=await server.on_peer_message(msg))

    @app.get("/status")
    async def status():
        return {**server.node.status(), "leader_addr": server.leader_addr(), "keys": len(server.sm.store)}

    return app
