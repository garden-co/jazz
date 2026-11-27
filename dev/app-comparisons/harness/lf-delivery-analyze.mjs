#!/usr/bin/env node
// Splits a sender trace from lf-delivery.mjs between two user-timing marks
// (default: bench:click → probe:port-out-needle) by what was on the CPU:
//   node lf-delivery-analyze.mjs results/.../trace-03.json.gz [--dist band-chat/jazz/dist] [--to mark] [--json out.json]
// V8 samples are attributed through the build's source maps to:
//   wasm, jazz:<file> (jazz-tools), react (react-dom/scheduler), ui (@astryxdesign, @garden-co/design),
//   app (src/), other JS, and V8/Blink natives ((program), (garbage collector)).
// Also: inclusive time of named functions, a coarse sequence of phases, the
// stack that posted the frame, and the timeline's layout/style/GC events.
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { gunzipSync } from "node:zlib";
import { createRequire } from "node:module";

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(join(here, "..", "band-chat", "jazz", "package.json"));
const { SourceMapConsumer } = require(require.resolve("source-map-js", { paths: [join(here, "..", "band-chat", "jazz", "node_modules", ".pnpm", "node_modules")] }));

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    dist: { type: "string", default: join(here, "..", "band-chat", "jazz", "dist") },
    from: { type: "string", default: "bench:click" },
    to: { type: "string", default: "probe:port-out-needle" },
    json: { type: "string", default: "" },
    quiet: { type: "boolean", default: false },
  },
});
const file = positionals[0];
const raw = readFileSync(file);
const parsed = JSON.parse((file.endsWith(".gz") ? gunzipSync(raw) : raw).toString("utf8"));
const events = Array.isArray(parsed) ? parsed : parsed.traceEvents;

// The sender's renderer main thread: where the click mark is.
const click = events.find((e) => e.name === values.from && e.cat?.includes("user_timing"));
if (!click) throw new Error(`no ${values.from} mark in trace`);
const pid = click.pid;
const tid = click.tid;
const marks = events.filter((e) => e.pid === pid && e.cat?.includes("user_timing") && (e.ph === "R" || e.ph === "I" || e.ph === "n" || e.ph === "b"));
const markTs = (name) => marks.filter((m) => m.name === name).map((m) => m.ts).sort((a, b) => a - b);
const t0 = click.ts;
const tEnd = markTs(values.to).find((t) => t >= t0);
if (tEnd === undefined) throw new Error(`no ${values.to} mark after the click`);
const rel = (ts) => Math.round(((ts - t0) / 1000) * 10) / 10;

// ---- CPU samples (Profile + ProfileChunk events of this thread)
const nodes = new Map();
const samples = []; // { ts, node }
const chunks = events.filter((e) => e.pid === pid && (e.name === "Profile" || e.name === "ProfileChunk"));
const profileIds = new Set(chunks.filter((e) => e.name === "Profile" && e.tid === tid).map((e) => e.id));
let ts = null;
for (const e of chunks) {
  if (!profileIds.has(e.id)) continue;
  if (e.name === "Profile") {
    ts = e.args.data.startTime;
    continue;
  }
  const d = e.args.data;
  for (const n of d.cpuProfile?.nodes ?? []) nodes.set(n.id, n);
  const ss = d.cpuProfile?.samples ?? [];
  const dt = d.timeDeltas ?? [];
  for (let k = 0; k < ss.length; k++) {
    ts += dt[k] ?? 0;
    samples.push({ ts, node: ss[k] });
  }
}
samples.sort((a, b) => a.ts - b.ts);

// ---- source maps
const consumers = new Map();
function consumerFor(url) {
  if (consumers.has(url)) return consumers.get(url);
  let c = null;
  const m = url.match(/\/(assets\/[^?#]+\.js)/);
  if (m) {
    const p = join(values.dist, m[1] + ".map");
    if (existsSync(p)) c = new SourceMapConsumer(JSON.parse(readFileSync(p, "utf8")));
  }
  consumers.set(url, c);
  return c;
}
const frameInfo = new Map();
function info(nodeId) {
  if (frameInfo.has(nodeId)) return frameInfo.get(nodeId);
  const n = nodes.get(nodeId);
  const cf = n.callFrame;
  let source = cf.url || "";
  let name = cf.functionName || "(anonymous)";
  if (cf.url && consumerFor(cf.url)) {
    const pos = consumerFor(cf.url).originalPositionFor({ line: cf.lineNumber + 1, column: Math.max(0, cf.columnNumber) });
    if (pos.source) source = pos.source;
    if (pos.name && name.length <= 2) name = `${pos.name}~`;
  }
  const cat = category(cf, source, name);
  const short = shortSource(source);
  const out = { name, source: short, cat, key: `${name} @ ${short}${cf.url && !consumerFor(cf.url) ? "" : ""}`, parent: n.parent };
  frameInfo.set(nodeId, out);
  return out;
}
function category(cf, fullSource, name) {
  // Classify by the package path after the last node_modules/: pnpm directory names carry peer suffixes like _react-dom@19.
  const source = fullSource.includes("node_modules/") ? fullSource.slice(fullSource.lastIndexOf("node_modules/")) : fullSource;
  if (cf.codeType === "wasm" || /\.wasm|wasm:\/\//.test(cf.url) || /^\$?wasm-function|^\$/.test(name) && !cf.url) return "wasm";
  if (!cf.url) {
    if (name === "(garbage collector)") return "native:gc";
    if (name === "(program)") return "native:program";
    if (name === "(idle)") return "idle";
    if (name === "(root)") return "native:root";
    return "native:builtin";
  }
  if (/jazz-tools\//.test(source)) return `jazz:${source.split("jazz-tools/")[1].replace(/^dist\//, "")}`;
  if (/jazz-wasm\//.test(source)) return "jazz:wasm-bindgen glue";
  if (/^node_modules\/(react-dom|scheduler|react)\//.test(source)) return "react";
  if (/@astryxdesign|@garden-co\/design|@stylexjs/.test(source)) return "ui";
  if (!/node_modules/.test(source) && /(^|\/)(src|components|lib)\/|schema\.ts/.test(source)) return "app";
  return "other-js";
}
function shortSource(s) {
  return s.replace(/^.*node_modules\/(\.pnpm\/[^/]+\/node_modules\/)?/, "").replace(/^\.\.\/+/, "");
}
const bucket = (cat) => (cat.startsWith("jazz:") ? "jazz-js" : cat.startsWith("native:") ? "native" : cat);
function stackOf(nodeId) {
  const out = [];
  for (let id = nodeId; id; id = nodes.get(id)?.parent) out.push(info(id));
  return out; // leaf first
}
// parents: build from children lists
for (const n of nodes.values()) for (const c of n.children ?? []) if (nodes.has(c)) nodes.get(c).parent = n.id;

// ---- window [t0, tEnd]
const win = samples.filter((s) => s.ts >= t0 && s.ts <= tEnd);
const dur = (k) => ((win[k + 1]?.ts ?? Math.min(tEnd, win[k].ts + 200)) - win[k].ts) / 1000;
const self = new Map();
const selfCat = new Map();
const selfBucket = new Map();
const incl = new Map();
const add = (m, k, v) => m.set(k, (m.get(k) ?? 0) + v);
const seq = [];
for (let k = 0; k < win.length; k++) {
  const ms = dur(k);
  const st = stackOf(win[k].node);
  const leaf = st[0];
  add(self, leaf.key, ms);
  add(selfCat, leaf.cat, ms);
  add(selfBucket, bucket(leaf.cat), ms);
  for (const key of new Set(st.map((f) => f.key))) add(incl, key, ms);
  seq.push({ t: (win[k].ts - t0) / 1000, ms, phase: phaseOf(st) });
}
/** The outermost recognisable activity on the stack. */
function phaseOf(st) {
  const names = st.map((f) => f.name.replace(/~$/, ""));
  const has = (re) => names.some((n) => re.test(n));
  if (st[0].cat === "idle") return "idle";
  if (st.length <= 2 && st[0].cat.startsWith("native")) return st[0].name; // style/layout/paint/GC outside JS
  if (has(/^commitRoot|^commitMutationEffects|^commitLayoutEffects|^flushPassiveEffects|^commitPassiveMountEffects/)) return "react commit/effects";
  if (has(/^performWorkOnRoot|^renderRootSync|^workLoopSync|^performUnitOfWork|^beginWork/)) return "react render";
  if (has(/^drainSubscriptionPool$|^pumpSubscriptions$|^applyPooledBatch$/)) return "jazz subscriptions (pool drain + delivery)";
  if (has(/^driveCoreTicks$|^runCoreTick$|^progressPeerTransport$/)) return "jazz core tick";
  if (has(/^commitTransaction$|^transaction$|^runInTransaction$/)) return "jazz transaction";
  if (has(/^pump$|^drainOutboundFrames$/)) return "jazz worker pump";
  return `other (${bucket(st[0].cat)})`;
}
// merge the sequence into runs (ignoring blips < 2 ms)
const runs = [];
for (const s of seq) {
  const last = runs.at(-1);
  if (last && last.phase === s.phase) last.ms += s.ms;
  else runs.push({ from: Math.round(s.t * 10) / 10, phase: s.phase, ms: s.ms });
}
const merged = [];
for (const r of runs) {
  const last = merged.at(-1);
  if (last && (r.ms < 2 || last.phase === r.phase)) last.ms += r.ms;
  else merged.push({ ...r });
}

// the stack that posted the frame: the last sample before the mark
const poster = [...win].reverse().find((s) => stackOf(s.node).some((f) => /postMessage|sendFrames|drainOutboundFrames|^pump$/.test(f.name)));
const lastSample = win.at(-1);

// ---- timeline events of this thread in the window
const tl = new Map();
for (const e of events) {
  if (e.pid !== pid || e.tid !== tid || e.ph !== "X" || e.ts < t0 || e.ts > tEnd) continue;
  if (/^(Layout|UpdateLayoutTree|Paint|PrePaint|Layerize|MinorGC|MajorGC|V8.GC_SCAVENGER|ParseHTML|RunMicrotasks|FunctionCall|EventDispatch|RunTask|v8.callFunction|TimerFire|FireAnimationFrame|UpdateLayer|HitTest|ScheduleStyleRecalculation)$/.test(e.name)) {
    const v = tl.get(e.name) ?? { n: 0, ms: 0, first: rel(e.ts) };
    v.n++;
    v.ms += (e.dur ?? 0) / 1000;
    tl.set(e.name, v);
  }
}
const tasks = events
  .filter((e) => e.pid === pid && e.tid === tid && e.ph === "X" && e.name === "RunTask" && e.ts + (e.dur ?? 0) >= t0 && e.ts <= tEnd)
  .map((e) => ({ from: rel(e.ts), ms: Math.round((e.dur ?? 0) / 100) / 10 }))
  .filter((t) => t.ms >= 1);

const r1 = (v) => Math.round(v * 10) / 10;
const top = (m, n) => [...m].sort((a, b) => b[1] - a[1]).slice(0, n).map(([k, ms]) => ({ ms: r1(ms), k }));
const out = {
  trace: file,
  window: { from: values.from, to: values.to, ms: r1((tEnd - t0) / 1000), samples: win.length },
  marks: Object.fromEntries([...new Set(marks.map((m) => m.name))].map((n) => [n, markTs(n).filter((t) => t >= t0 - 1000 && t <= tEnd + 2e6).map(rel)])),
  selfByBucket: top(selfBucket, 20),
  selfByCategory: top(selfCat, 30),
  selfByFunction: top(self, 40),
  inclusive: top(incl, 60),
  phases: merged.map((r) => ({ from: r.from, ms: r1(r.ms), phase: r.phase })),
  tasks,
  timelineEvents: Object.fromEntries([...tl].map(([k, v]) => [k, { n: v.n, ms: r1(v.ms), first: v.first }])),
  posterStack: poster ? stackOf(poster.node).map((f) => f.key) : null,
  lastSampleStack: lastSample ? stackOf(lastSample.node).map((f) => f.key) : null,
};
if (values.json) writeFileSync(values.json, JSON.stringify(out, null, 1));
if (!values.quiet) {
  console.log(`window ${out.window.from} → ${out.window.to}: ${out.window.ms} ms, ${out.window.samples} samples`);
  console.log("marks:", JSON.stringify(out.marks));
  console.log("\nself by bucket:", out.selfByBucket.map((r) => `${r.k} ${r.ms}`).join(", "));
  console.log("\nself by category:"), out.selfByCategory.forEach((r) => console.log(`  ${r.ms}\t${r.k}`));
  console.log("\nphases:"), out.phases.forEach((r) => console.log(`  +${r.from}\t${r.ms}\t${r.phase}`));
  console.log("\ntasks ≥1ms:", JSON.stringify(out.tasks));
  console.log("\ntimeline:", JSON.stringify(out.timelineEvents));
  console.log("\ntop self:"), out.selfByFunction.slice(0, 25).forEach((r) => console.log(`  ${r.ms}\t${r.k}`));
  console.log("\ntop inclusive:"), out.inclusive.slice(0, 45).forEach((r) => console.log(`  ${r.ms}\t${r.k}`));
  console.log("\nposter stack:\n  " + (out.posterStack ?? []).join("\n  "));
}
