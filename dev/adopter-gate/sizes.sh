#!/usr/bin/env bash
# Package sizes vs the previous release (default: npm's `latest`). Growth over the threshold is REVIEW, not FAIL.
# Usage: sizes.sh [previous version]   env: AG_SIZE_REVIEW_PCT (default 3)
. "$(dirname "$0")/lib.sh"
body() {
  local W=$AG/.work/sizes PREV=${1:-$(npm view jazz-tools dist-tags.latest)} V; V=$(ag_version); rm -rf "$W"; mkdir -p "$W/prev" "$W/rc"
  ( cd "$W/prev" && npm pack "jazz-wasm@$PREV" "jazz-tools@$PREV" >/dev/null 2>&1 ) || { echo "FAIL sizes: npm pack $PREV"; return; }
  for n in jazz-wasm jazz-tools; do tar xzf "$W/prev/$n-$PREV.tgz" -C "$W/prev" && mv "$W/prev/package" "$W/prev/$n"; tar xzf "$AG_TGZ/$n-$V.tgz" -C "$W/rc" && mv "$W/rc/package" "$W/rc/$n"; done
  node - "$W" "$PREV" "$V" "${AG_SIZE_REVIEW_PCT:-3}" <<'JS'
const fs = require("fs"), path = require("path"), zlib = require("zlib"), [W, prev, v, pct] = process.argv.slice(2);
const files = (d) => fs.readdirSync(d, { recursive: true }).map((f) => path.join(d, f)).filter((f) => fs.statSync(f).isFile());
const measure = (dir, pick) => files(dir).filter(pick).reduce((a, f) => { const b = fs.readFileSync(f); return { raw: a.raw + b.length, gz: a.gz + zlib.gzipSync(b, { level: 9 }).length }; }, { raw: 0, gz: 0 });
const rows = [["jazz-wasm .wasm", "jazz-wasm", (f) => f.endsWith(".wasm")], ["jazz-tools dist JS", "jazz-tools", (f) => /\/dist\/.*\.js$/.test(f)]];
const out = [];
for (const [label, pkg, pick] of rows) {
  const a = measure(path.join(W, "prev", pkg), pick), b = measure(path.join(W, "rc", pkg), pick);
  const d = a.gz ? ((b.gz - a.gz) / a.gz) * 100 : 0, verdict = d > Number(pct) ? "REVIEW" : "PASS";
  console.log(`${verdict} size ${label}: ${prev} → ${v}: raw ${a.raw} → ${b.raw} B, gzip ${a.gz} → ${b.gz} B (${d >= 0 ? "+" : ""}${d.toFixed(1)}%, review above +${pct}%)`);
  out.push({ label, prev, v, raw: [a.raw, b.raw], gzip: [a.gz, b.gz], pct: Number(d.toFixed(2)) });
}
fs.writeFileSync(path.join(W, "sizes.json"), JSON.stringify(out, null, 2));
JS
  cp "$W/sizes.json" "$AG_RESULTS/sizes.json"
}
ag_job sizes body "$@"
