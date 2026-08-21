"""
Minimal CLI client for the running KV store server.

Usage:
    python -m kvstore.cli set foo bar
    python -m kvstore.cli set foo bar --ttl 60
    python -m kvstore.cli get foo
    python -m kvstore.cli delete foo
    python -m kvstore.cli keys
    python -m kvstore.cli status
    python -m kvstore.cli health
    python -m kvstore.cli compact

Talks to KVSTORE_URL (default http://localhost:8000) over HTTP. Kept
deliberately separate from server.py so the server has zero dependency
on how it's driven -- HTTP is the only real interface, this is just a
convenience wrapper around it.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = os.environ.get("KVSTORE_URL", "http://localhost:8000")


def _request(method: str, path: str, body: dict | None = None) -> dict:
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        detail = json.loads(e.read()).get("detail", str(e))
        print(f"Error ({e.code}): {detail}", file=sys.stderr)
        sys.exit(1)


def cmd_set(args):
    body = {"value": args.value}
    if args.ttl is not None:
        body["ttl_seconds"] = args.ttl
    result = _request("PUT", f"/keys/{args.key}", body)
    print(result)


def cmd_get(args):
    result = _request("GET", f"/keys/{args.key}")
    print(result["value"])


def cmd_delete(args):
    result = _request("DELETE", f"/keys/{args.key}")
    print(result)


def cmd_keys(args):
    result = _request("GET", "/keys")
    for k in result["keys"]:
        print(k)


def cmd_status(args):
    result = _request("GET", "/status")
    print(result)


def cmd_health(args):
    result = _request("GET", "/health")
    print(result)


def cmd_compact(args):
    result = _request("POST", "/admin/compact")
    print(result)


def main():
    parser = argparse.ArgumentParser(description="KV store CLI client")
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.add_argument("--ttl", type=float, default=None)
    p_set.set_defaults(func=cmd_set)

    p_get = sub.add_parser("get")
    p_get.add_argument("key")
    p_get.set_defaults(func=cmd_get)

    p_del = sub.add_parser("delete")
    p_del.add_argument("key")
    p_del.set_defaults(func=cmd_delete)

    p_keys = sub.add_parser("keys")
    p_keys.set_defaults(func=cmd_keys)

    p_status = sub.add_parser("status")
    p_status.set_defaults(func=cmd_status)

    p_health = sub.add_parser("health")
    p_health.set_defaults(func=cmd_health)

    p_compact = sub.add_parser("compact")
    p_compact.set_defaults(func=cmd_compact)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
