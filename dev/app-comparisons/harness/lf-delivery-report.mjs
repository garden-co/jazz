#!/usr/bin/env node
// Tables for results/<date>/lf-delivery/README.md from the per-sample files of
// lf-delivery.mjs (untraced samples only) and the traces (lf-delivery-analyze.mjs).
//   node lf-delivery-report.mjs [2026-11-14]   (writes report.json, prints the Markdown tables)
import { existsSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { median } from "./lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const date = process.argv[2] ?? "2026-11-14";
const base = join(resolve(here, ".."), "results", date, "lf-delivery");
const cases = readdirSync(base).filter((d) => existsSync(join(base, d, "summary.json")));

const rowOf = ({ text, sent, a, b }) => {
  const mark = (s, name) => s.marks.find((m) => m.name === name && (m.detail?.text ?? m.detail?.key) === text);
  const at = (s, m) => (m ? s.timeOrigin + m.t - sent : undefined);
  return {
    committed: at(a, mark(a, "send:committed")),
    "render starts": at(a, mark(a, "data:newest")),
    "frame leaves a": a.ports.find((p) => p.needle && p.dir === "out")?.t - sent || a.ws.find((p) => p.needle && p.dir === "out")?.t - sent,
    "a dom": at(a, mark(a, "dom:needle")),
    "b dom": at(b, mark(b, "dom:needle")),
  };
};
const out = {};
for (const c of cases) {
  const files = readdirSync(join(base, c)).filter((f) => /^sample-\d+\.json$/.test(f));
  const rows = files.map((f) => JSON.parse(readFileSync(join(base, c, f), "utf8"))).filter((s) => !s.traced).map(rowOf);
  const med = {};
  for (const k of Object.keys(rows[0] ?? {})) {
    const xs = rows.map((r) => r[k]).filter((v) => typeof v === "number" && !Number.isNaN(v));
    med[k] = xs.length ? Math.round(median(xs)) : undefined;
  }
  const summary = JSON.parse(readFileSync(join(base, c, "summary.json"), "utf8"));
  // CPU split of the traced sample (written by lf-delivery.mjs right after the run, with that build's source maps):
  // click → the frame leaves a, or → a's bubble in the DOM when the frame left first.
  let split;
  const leftFirst = med["frame leaves a"] !== undefined && med["frame leaves a"] < (med["a dom"] ?? 0) - 20;
  const splitFile = readdirSync(join(base, c)).find((f) => f.endsWith(leftFirst || c.startsWith("thin") ? ".to-dom.json" : ".to-port.json"));
  if (splitFile) {
    const a = JSON.parse(readFileSync(join(base, c, splitFile), "utf8"));
    split = { window: `${a.window.to === "dom:needle" ? "→ a dom" : "→ frame"} ${Math.round(a.window.ms)}`, ...Object.fromEntries(a.selfByBucket.map((r) => [r.k, Math.round(r.ms)])) };
  }
  out[c] = { flags: summary.flags, n: rows.length, ...med, split };
}
const cols = ["committed", "render starts", "frame leaves a", "a dom", "b dom"];
const lines = [`| case | flags | n | ${cols.join(" | ")} |`, `|---|---|---:|${cols.map(() => "---:").join("|")}|`];
for (const [c, r] of Object.entries(out)) lines.push(`| ${c} | ${Object.entries(r.flags ?? {}).map(([k, v]) => `${k.replace(/^VITE_BENCH_|^BENCH_/, "")}=${v}`).join(" ") || "-"} | ${r.n} | ${cols.map((k) => r[k] ?? "").join(" | ")} |`);
const splitCols = ["react", "ui", "app", "jazz-js", "wasm", "native", "other-js", "idle"];
const lines2 = [`| case (traced sample) | window | ${splitCols.join(" | ")} |`, `|---|---|${splitCols.map(() => "---:").join("|")}|`];
for (const [c, r] of Object.entries(out)) if (r.split) lines2.push(`| ${c} | ${r.split.window} | ${splitCols.map((k) => r.split[k] ?? "").join(" | ")} |`);
writeFileSync(join(base, "report.json"), JSON.stringify(out, null, 1));
console.log(lines.join("\n") + "\n\n" + lines2.join("\n"));
