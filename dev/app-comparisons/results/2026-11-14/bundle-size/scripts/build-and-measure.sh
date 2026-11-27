#!/usr/bin/env bash
# Usage: build-and-measure.sh <app dir> <label>
# Builds the app thin and local-first (sourcemaps + visualizer), serves each with
# `vite preview` (Jazz backend in memory), loads it in headless Chromium and
# prints the gzip JS the page loaded plus the SharedWorker and WASM.
set -euo pipefail
APP=$1; LABEL=$2; HERE=$(cd "$(dirname "$0")" && pwd); OUT=${OUT:-/tmp/bundle-size/$LABEL}
mkdir -p "$OUT"; cp "$HERE/viz.config.ts" "$APP/viz.config.ts"
cd "$APP"
for v in thin lf; do
  if [ $v = thin ]; then export VITE_JAZZ_THIN=1; else unset VITE_JAZZ_THIN; fi
  VIZ_OUT="$OUT/$v" ./node_modules/.bin/vite build -c viz.config.ts --sourcemap --outDir "$OUT/dist-$v" > "$OUT/build-$v.log" 2>&1
  PORT=$((4600 + RANDOM % 300)); PPORT=$((PORT + 1000))
  JAZZ_IN_MEMORY=1 ./node_modules/.bin/vite preview -c viz.config.ts --outDir "$OUT/dist-$v" --port $PORT --strictPort > "$OUT/preview-$v.log" 2>&1 & SP=$!
  node "$HERE/logproxy.mjs" $PPORT $PORT > "$OUT/requests-$v.log" & PP=$!
  for i in $(seq 40); do curl -sf localhost:$PORT >/dev/null && break; sleep 0.25; done
  # Fail loudly if the server answering is not this build.
  curl -s localhost:$PORT | grep -q "$(basename "$(ls "$OUT/dist-$v"/assets/index-*.js | grep -v shim | head -1)")" || { echo "preview on $PORT is not serving $v"; exit 1; }
  node "$HERE/measure-load.mjs" "http://localhost:$PPORT/" 6000 > "$OUT/load-$v.json"
  kill $SP $PP 2>/dev/null || true; wait $SP $PP 2>/dev/null || true
  node -e '
    const [f, req, dist, v] = process.argv.slice(1); const fs = require("fs"), z = require("zlib");
    const { rows, errors, text } = JSON.parse(fs.readFileSync(f)); const kb = (x) => (x / 1024).toFixed(1);
    const js = rows.filter((r) => !r.file.endsWith(".wasm"));
    const init = js.filter((r) => r.phase === "initial");
    // SharedWorker scripts are not page responses: take them from the proxy log.
    const pageFiles = new Set(rows.map((r) => r.file));
    const workers = [...new Set(fs.readFileSync(req, "utf8").match(/\/assets\/[^ \n]*\.js/g) ?? [])].filter((w) => !pageFiles.has(w));
    const missing = [...new Set(fs.readFileSync(req, "utf8").match(/\/assets\/[^ \n]+/g) ?? [])].filter((a) => !fs.existsSync(dist + a));
    if (missing.length) console.log(`  MISSING assets requested: ${missing.join(", ")}`);
    const wgz = workers.reduce((a, w) => a + z.gzipSync(fs.readFileSync(dist + w), { level: 9 }).length, 0);
    const wasm = rows.filter((r) => r.file.endsWith(".wasm"));
    console.log(`  errors: ${errors.length} ${errors.slice(0,2).join(" | ").slice(0,200)}; page: "${text.slice(0,80)}"`); console.log(`${v}: page JS ${kb(js.reduce((a, r) => a + r.gzip, 0))} KB gzip (initial ${kb(init.reduce((a, r) => a + r.gzip, 0))}), worker ${kb(wgz)} KB, wasm ${kb(wasm.reduce((a, r) => a + r.gzip, 0))} KB gzip / ${kb(wasm.reduce((a, r) => a + r.br, 0))} KB br`);
  ' "$OUT/load-$v.json" "$OUT/requests-$v.log" "$OUT/dist-$v" $v
done
