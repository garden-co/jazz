#!/bin/bash
# restart.sh <variant>: stop the served app (and any build), rebuild and serve <variant> (no reseed).
# Matches processes by their executable path, so it never matches the calling shell.
for p in $(ps -eo pid,args | awk '$2 ~ /node$/ && ($3 ~ /serve\.mjs$/ || $3 ~ /next/) {print $1}'); do kill $p 2>/dev/null; done
for p in $(ps -eo pid,args | awk '$2 ~ /^next-server/ {print $1}'); do kill $p 2>/dev/null; done
sleep 2
rm -f "$(dirname "$0")/../../../../examples/big-label/.next/lock"
cd "$(dirname "$0")" && BL_ROUTE_TIMING=${BL_ROUTE_TIMING:-} BL_AUTH_COOKIE_CACHE=${BL_AUTH_COOKIE_CACHE:-} nohup node serve.mjs --variant "$1" --no-seed > /tmp/serve.log 2>&1 &
