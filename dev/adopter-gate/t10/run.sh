#!/usr/bin/env bash
# Usage: run.sh <jazz-tools server binary>   (deps of this dir decide the createJazz version)
cd "$(dirname "$0")"; BIN=$1; export APP_ID=$(node -p 'require("crypto").randomUUID()') CORE_SECRET=t10-admin-$RANDOM SIGNED_OUT=/tmp/t10-signedout; : > $SIGNED_OUT
kill_port() { for p in $(ss -ltnpH "sport = :$1" | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill $p; done; }
for p in 8630 8631 8632; do kill_port $p; done; sleep 1
setsid $BIN server $APP_ID --port 8630 --in-memory --admin-secret $CORE_SECRET > core.log 2>&1 < /dev/null &
for i in $(seq 30); do curl -sf localhost:8630/health >/dev/null && break; sleep 1; done
export CORE_URL=http://127.0.0.1:8630
PORT=8631 setsid npx tsx server.ts > a.log 2>&1 < /dev/null &
PORT=8632 setsid npx tsx server.ts > b.log 2>&1 < /dev/null &
for i in $(seq 60); do grep -q "up 8631" a.log && grep -q "up 8632" b.log && break; sleep 1; done
grep -q "up 8631" a.log && grep -q "up 8632" b.log || { echo "INSTANCES FAILED"; tail -5 a.log b.log core.log; }
A_URL=http://127.0.0.1:8631 B_URL=http://127.0.0.1:8632 timeout 90 npx tsx check.ts; rc=$?
echo "== Db lifecycle (jazz#5592)"
export KILL_A="for p in \$(ss -ltnpH 'sport = :8631' | grep -o 'pid=[0-9]*' | cut -d= -f2); do kill \$p; done"
export RESTART_A="cd $PWD && (PORT=8631 setsid npx tsx server.ts > a2.log 2>&1 < /dev/null &) && for i in \$(seq 60); do grep -q 'up 8631' a2.log && break; sleep 1; done"
A_URL=http://127.0.0.1:8631 timeout 90 npx tsx lifecycle.ts || rc=1
for p in 8630 8631 8632; do kill_port $p; done; exit $rc
