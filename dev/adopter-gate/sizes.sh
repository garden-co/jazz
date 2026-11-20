#!/usr/bin/env bash
# Package sizes vs the previous release (default: npm's `latest`). Growth over the threshold is REVIEW, not FAIL.
# Usage: sizes.sh [previous version]   env: AG_SIZE_REVIEW_PCT (default 3)
. "$(dirname "$0")/lib.sh"
body() {
  local W=$AG/.work/sizes PREV=${1:-$(npm view jazz-tools dist-tags.latest)} V; V=$(ag_version); rm -rf "$W"; mkdir -p "$W/prev" "$W/rc"
  ( cd "$W/prev" && ag_retry 3 npm pack "jazz-wasm@$PREV" "jazz-tools@$PREV" >/dev/null 2>&1 ) || { echo "FAIL sizes: npm pack $PREV"; return; }
  for n in jazz-wasm jazz-tools; do tar xzf "$W/prev/$n-$PREV.tgz" -C "$W/prev" && mv "$W/prev/package" "$W/prev/$n"; tar xzf "$AG_TGZ/$n-$V.tgz" -C "$W/rc" && mv "$W/rc/package" "$W/rc/$n"; done
  node - "$W" "$PREV" "$V" "${AG_SIZE_REVIEW_PCT:-3}" <<'JS'
const fs = require("fs"), path = require("path"), zlib = require("zlib"), [W, prev, v, pct] = process.argv.slice(2);
const files = (d) => fs.readdirSync(d, { recursive: true }).map((f) => path.join(d, f)).filter((f) => fs.statSync(f).isFile());
const measure = (dir, pick) => files(dir).filter(pick).reduce((a, f) => { const b = fs.readFileSync(f); return { n: a.n + 1, raw: a.raw + b.length, gz: a.gz + zlib.gzipSync(b, { level: 9 }).length }; }, { n: 0, raw: 0, gz: 0 });
// What a browser downloads is pkg/jazz_wasm_bg.wasm; the package also carries server-only wasm (e.g. the Durable
// Object build), reported with no threshold so it can't masquerade as browser growth.
const rows = [
  ["jazz-wasm browser wasm (pkg/jazz_wasm_bg.wasm)", "jazz-wasm", (f) => /(^|\/)pkg\/jazz_wasm_bg\.wasm$/.test(f) && !f.includes("durable-object"), true],
  ["jazz-tools dist JS", "jazz-tools", (f) => /\/dist\/.*\.js$/.test(f), true],
];
const out = [];
for (const [label, pkg, pick, gated] of rows) {
  const a = measure(path.join(W, "prev", pkg), pick), b = measure(path.join(W, "rc", pkg), pick);
  const d = a.gz ? ((b.gz - a.gz) / a.gz) * 100 : 0;
  // A gated row that matches no file, or measures 0 bytes, on either side (a renamed or missing browser wasm) is FAIL:
  // measuring nothing must never read as "no growth".
  const empty = gated && (a.n === 0 || b.n === 0 || a.raw === 0 || b.raw === 0);
  const verdict = empty ? "FAIL" : gated && d > Number(pct) ? "REVIEW" : "PASS";
  if (empty) console.log(`FAIL size ${label}: no file matched or 0 bytes (${prev}: ${a.n} files, ${a.raw} B; ${v}: ${b.n} files, ${b.raw} B)`);
  if (!empty) console.log(`${verdict} size ${label}: ${prev} → ${v}: raw ${a.raw} → ${b.raw} B, gzip ${a.gz} → ${b.gz} B (${a.raw === 0 ? "new" : `${d >= 0 ? "+" : ""}${d.toFixed(1)}%`}${gated ? `, review above +${pct}%` : ", not gated"})`);
  out.push({ label, prev, v, raw: [a.raw, b.raw], gzip: [a.gz, b.gz], pct: Number(d.toFixed(2)) });
}
// Cloudflare Worker bundles jazz-wasm ships (the Durable Object host, and since jazz#6153 the workerd backend). The
// limit, per https://developers.cloudflare.com/workers/platform/limits/#worker-size (checked 2026-11-19): "Worker size
// (uncompressed) | 64 MiB | 64 MiB" (Free | Paid), "There is no compressed size limit. Only the uncompressed bundle
// size counts." So a bundle FAILs above 64 MiB uncompressed (Anselm, 2026-11-19: gate only that real limit; gzip is
// reported, not gated).
const CF_LIMIT = 64 * 1024 * 1024;
const listed = (pkgDir) => { try { return JSON.parse(fs.readFileSync(path.join(pkgDir, "package.json"), "utf8")).files ?? []; } catch { return []; } };
for (const [label, dir] of [["Cloudflare Worker: Durable Object host (durable-object/)", "durable-object"], ["Cloudflare Worker: workerd backend (backend/)", "backend"]]) {
  const pkgDir = path.join(W, "rc", "jazz-wasm"), sub = path.join(pkgDir, dir);
  const required = listed(pkgDir).some((f) => f.startsWith(dir + "/"));
  if (!fs.existsSync(sub)) {
    console.log(required ? `FAIL size ${label}: package.json lists ${dir}/ but the package has none` : `PASS size ${label}: not in ${v}'s package`);
    continue;
  }
  const m = measure(sub, () => true);
  const over = m.raw > CF_LIMIT;
  const verdict = m.n === 0 || m.raw === 0 ? "FAIL" : over ? "FAIL" : "PASS";
  console.log(`${verdict} size ${label}: ${m.n} files, ${m.raw} B uncompressed (Cloudflare limit ${CF_LIMIT} B, 64 MiB), gzip ${m.gz} B (not gated)${m.n === 0 ? ": empty" : ""}`);
  out.push({ label, v, raw: [null, m.raw], gzip: [null, m.gz], limit: CF_LIMIT });
}
fs.writeFileSync(path.join(W, "sizes.json"), JSON.stringify(out, null, 2));
JS
  cp "$W/sizes.json" "$AG_RESULTS/sizes.json"
}
ag_job sizes body "$@"
