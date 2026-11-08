#!/usr/bin/env bash
# Kill the leader container of the docker-compose cluster and show the cluster recovers.
# Requires: docker compose up --build -d   (not tested in CI)
set -euo pipefail
for p in 8001 8002 8003; do
  role=$(curl -s localhost:$p/status | python3 -c "import sys,json;print(json.load(sys.stdin)['role'])" || true)
  [ "$role" = "leader" ] && LEADER_PORT=$p && LEADER=node$((LEADER_PORT - 8000))
done
echo "leader is $LEADER"
curl -s -X PUT localhost:$LEADER_PORT/kv/demo -H 'content-type: application/json' -d '{"value":"survives"}'; echo
docker compose kill "$LEADER"
sleep 3
for p in 8001 8002 8003; do
  [ "$p" = "$LEADER_PORT" ] && continue
  curl -s localhost:$p/status; echo
done
echo "read from the new leader (try each live port; followers answer 503 with the leader address):"
for p in 8001 8002 8003; do [ "$p" = "$LEADER_PORT" ] || curl -s localhost:$p/kv/demo; echo; done
