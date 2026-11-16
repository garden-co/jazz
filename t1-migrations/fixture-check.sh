#!/usr/bin/env bash
# Copy fixture-data (written on .65 (fixture-data-64: the older .64 store)) to a scratch dir, start the server on it, read existing rows.
cd "$(dirname "$0")"; rm -rf /tmp/t1fx && cp -r fixture-data /tmp/t1fx
PORT=8611 DATA=/tmp/t1fx/jazz setsid npx tsx server.ts > fx.log 2>&1 < /dev/null &
for i in $(seq 1 60); do grep -q "up 8611" fx.log && break; grep -q Error fx.log && break; sleep 1; done
grep -q "up 8611" fx.log || { echo "SERVER FAILED"; grep -v "^\s*at " fx.log | head -6; }
PORT=8611 timeout 60 npx tsx fixture-check.ts; rc=$?
ss -ltnp 2>/dev/null | grep ":8611 " | grep -o "pid=[0-9]*" | cut -d= -f2 | sort -u | while read p; do kill $p; done
exit $rc
