#!/usr/bin/env bash
# Usage: run-smoke.sh <appdir> <port>   (fresh data dir each run; prints smoke output)
set -u; cd "$1"; P=$2; rm -rf data; mkdir -p data
PORT=$P setsid npx tsx server.ts > server.log 2>&1 < /dev/null & 
for i in $(seq 1 60); do grep -q "up $P" server.log && break; grep -q "Error" server.log && break; sleep 1; done
if ! grep -q "up $P" server.log; then echo "SERVER FAILED"; grep -v "^\s*at " server.log | head -8; fi
PORT=$P timeout 120 npx tsx smoke.ts; rc=$?
ss -ltnp 2>/dev/null | grep ":$P " | grep -o "pid=[0-9]*" | cut -d= -f2 | sort -u | while read p; do kill $p; done
exit $rc
