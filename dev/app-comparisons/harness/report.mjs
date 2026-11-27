#!/usr/bin/env node
// Usage: node harness/report.mjs [results/<date>]  -> markdown tables on stdout
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { resolveTarget, resultsDir, STACKS } from "./apps.mjs";
import { locFor } from "./loc.mjs";

const dir = process.argv[2] ? resolve(process.argv[2]) : join(resultsDir, readdirSync(resultsDir).sort().at(-1));
const records = readdirSync(dir).filter((f) => f.endsWith(".json")).map((f) => JSON.parse(readFileSync(join(dir, f), "utf8")));
const ORDER = ["jazz:default", "jazz:now", "jazz:thin", "zero:default", "convex:default", "convex:optimistic", "instantdb:default", "postgres:default", "neon:default"];
const label = (r) => {
  const name = { jazz: "Jazz", zero: "Zero", convex: "Convex", instantdb: "InstantDB", postgres: "Postgres + custom sync", neon: "Neon Realtime" }[r.stack] ?? r.stack;
  // BigLabel ships thin upstream: its "default" is the thin start + local-first
  // upgrade of the other heroes, and its "thin" is upstream as shipped.
  if (r.stack === "jazz" && r.hero === "big-label")
    return `${name} (${{ default: "thin + upgrade", now: "now", thin: "thin, as shipped" }[r.variant] ?? r.variant})`;
  if (r.stack === "jazz") return `${name} (${r.variant === "default" ? "as shipped" : r.variant})`;
  return r.variant === "default" ? name : `${name} (${r.variant})`;
};
const rank = (r) => { const i = ORDER.indexOf(`${r.stack}:${r.variant}`); return i < 0 ? 99 : i; };

for (const hero of ["stage-plan", "band-chat", "wequencer", "big-label"]) {
  const rs = records.filter((r) => r.hero === hero).sort((a, b) => rank(a) - rank(b));
  if (!rs.length) continue;
  const metrics = Object.keys(rs[0]).filter((k) => rs[0][k]?.median !== undefined);
  console.log(`\n### ${hero}\n`);
  console.log(`| metric (median, ms) | ${rs.map(label).join(" | ")} |`);
  console.log(`|---|${rs.map(() => "---").join("|")}|`);
  for (const m of metrics) console.log(`| \`${m}\` | ${rs.map((r) => (r[m] ? `${Math.round(r[m].median)}` + (r[m].p95 > 2 * r[m].median ? ` (p95 ${Math.round(r[m].p95)})` : "") : "–")).join(" | ")} |`);
  console.log(`| JS loaded by the page (gzip KB) | ${rs.map((r) => (r.loaded_js_gzip_kb ? r.loaded_js_gzip_kb : "n/a")).join(" | ")} |`);
  console.log(`| WASM in build (gzip KB) | ${rs.map((r) => r.bundle?.wasm_gzip_kb ?? "–").join(" | ")} |`);
  console.log(`| worker JS in build (gzip KB) | ${rs.map((r) => r.bundle?.worker_js_gzip_kb ?? "–").join(" | ")} |`);
  console.log(`| rtt to backend (ms) | ${rs.map((r) => r.rtt_ms).join(" | ")} |`);
  console.log(`| backend | ${rs.map((r) => r.where).join(" | ")} |`);
  const locs = STACKS.filter((s) => existsSync(join(resolveTarget(`${hero}/${s}`).hooksDir, "loc.json"))).map((s) => [s, locFor(`${hero}/${s}`).loc]);
  console.log(`\n| LOC | ${locs.map(([s]) => s).join(" | ")} |\n|---|${locs.map(() => "---").join("|")}|`);
  for (const k of ["schema", "permissions", "server", "data", "data_layer", "ui", "total"]) console.log(`| ${k} | ${locs.map(([, l]) => l[k] ?? 0).join(" | ")} |`);
}
