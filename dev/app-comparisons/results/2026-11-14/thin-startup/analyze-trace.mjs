#!/usr/bin/env node
// Reads a trace.json from thin-reload.mjs --trace and prints, for the reload:
//  - the main thread's top-level tasks (> 2 ms) with what they are,
//  - user timing marks,
//  - network requests and WebSocket events,
//  - CPU self time per phase (between marks), by function and by source file.
//
//   node analyze-trace.mjs runs/<label>/trace.json [--top 15]
import { readFileSync } from "node:fs";
import { parseArgs } from "node:util";

const { values, positionals } = parseArgs({ allowPositionals: true, options: { top: { type: "string", default: "15" } } });
const raw = JSON.parse(readFileSync(positionals[0], "utf8"));
const events = raw.traceEvents ?? raw;
const TOP = Number(values.top);

// The reload's navigation: the last navigationStart of the main frame.
const navs = events.filter((e) => e.name === "navigationStart" && e.args?.data?.isLoadingMainFrame);
const nav = navs.at(-1);
const T0 = nav.ts;
const pid = nav.pid;
const ms = (ts) => (ts - T0) / 1000;
const mainTid = events.find((e) => e.pid === pid && e.name === "thread_name" && e.args?.name === "CrRendererMain")?.tid;

// Marks (user timing): instant events in blink.user_timing.
console.log("## marks (ms from navigationStart)");
const marks = new Map();
for (const e of events) {
  if (e.pid !== pid || !e.cat?.includes("blink.user_timing") || e.ts < T0) continue;
  if (e.ph === "R" || e.ph === "I" || e.ph === "n" || e.ph === "b") {
    if (!marks.has(e.name)) marks.set(e.name, ms(e.ts));
  }
}
for (const [k, v] of [...marks].sort((a, b) => a[1] - b[1])) console.log(`${v.toFixed(1).padStart(8)}  ${k}`);

// Network.
console.log("\n## network");
const reqs = new Map();
for (const e of events) {
  if (e.ts < T0) continue;
  const d = e.args?.data;
  if (e.name === "ResourceSendRequest") reqs.set(d.requestId, { url: d.url, start: ms(e.ts), prio: d.priority });
  if (e.name === "ResourceReceiveResponse" && reqs.has(d.requestId)) reqs.get(d.requestId).resp = ms(e.ts);
  if (e.name === "ResourceFinish" && reqs.has(d.requestId)) Object.assign(reqs.get(d.requestId), { end: ms(e.ts), bytes: d.encodedDataLength });
  if (/^WebSocket/.test(e.name)) console.log(`${ms(e.ts).toFixed(1).padStart(8)}  ${e.name} ${d?.url ?? ""}`);
}
for (const r of [...reqs.values()].sort((a, b) => a.start - b.start)) {
  if (/\.woff2$/.test(r.url)) continue;
  console.log(`${r.start.toFixed(1).padStart(8)} → ${(r.end ?? NaN).toFixed(1).padStart(7)}  ${String(r.bytes ?? "").padStart(8)} B  ${r.url.replace(/^https?:\/\/[^/]+/, "")}`);
}

// Main-thread top-level tasks.
console.log("\n## main-thread tasks > 2 ms (start, dur, inner events)");
const main = events.filter((e) => e.pid === pid && e.tid === mainTid && e.ph === "X" && e.ts >= T0).sort((a, b) => a.ts - b.ts);
const tasks = main.filter((e) => e.name === "RunTask" || e.name === "ThreadControllerImpl::RunTask");
for (const t of tasks) {
  if (t.dur < 2000) continue;
  const inner = main.filter((e) => e.ts >= t.ts && e.ts + e.dur <= t.ts + t.dur && e !== t && e.dur > 1000 && !/RunTask/.test(e.name));
  const desc = inner.slice(0, 6).map((e) => `${e.name}${e.args?.data?.url ? `(${e.args.data.url.split("/").pop()})` : e.args?.data?.functionName ? `(${e.args.data.functionName})` : ""} ${(e.dur / 1000).toFixed(1)}`);
  console.log(`${ms(t.ts).toFixed(1).padStart(8)} ${(t.dur / 1000).toFixed(1).padStart(6)}  ${desc.join(" | ")}`);
}

// CPU profile (ProfileChunk samples) on the main thread.
const nodes = new Map();
const samples = [];
let lastTs = null;
for (const e of events) {
  if (e.pid !== pid || (e.name !== "Profile" && e.name !== "ProfileChunk")) continue;
  if (e.name === "Profile") {
    lastTs = e.args.data.startTime;
    continue;
  }
  const c = e.args.data.cpuProfile ?? {};
  for (const n of c.nodes ?? []) nodes.set(n.id, n);
  const deltas = e.args.data.timeDeltas ?? [];
  (c.samples ?? []).forEach((s, k) => {
    lastTs += deltas[k];
    samples.push({ id: s, ts: lastTs });
  });
}
if (!samples.length) {
  console.log("\n(no CPU samples)");
  process.exit(0);
}
// Parent links for stacks.
const parent = new Map();
for (const n of nodes.values()) if (n.parent) parent.set(n.id, n.parent);
for (const n of nodes.values()) for (const c of n.children ?? []) parent.set(c, n.id);
const fileOf = (n) => {
  const u = n.callFrame.url || "";
  if (!u) return n.callFrame.functionName || "(native)";
  return u.replace(/^https?:\/\/[^/]+/, "");
};
const label = (n) => `${n.callFrame.functionName || "(anon)"} ${fileOf(n).split("/").pop()}:${n.callFrame.lineNumber + 1}`;

const phases = [["nav", 0]];
for (const [k, v] of [...marks].sort((a, b) => a[1] - b[1])) if (/^(app:main|jz:|jazz:client-ready|ws:create|ws:open|data:show|data:tasks-100|render:board-100|dom:cards-100)/.test(k)) phases.push([k, v]);
console.log("\n## CPU self time per phase (main thread, sampled)");
for (let p = 0; p < phases.length; p++) {
  const [name, from] = phases[p];
  const to = phases[p + 1]?.[1] ?? Infinity;
  const inPhase = [];
  for (let k = 0; k < samples.length - 1; k++) {
    const t = ms(samples[k].ts);
    if (t >= from && t < to) inPhase.push({ id: samples[k].id, dt: (samples[k + 1].ts - samples[k].ts) / 1000 });
  }
  const total = inPhase.reduce((a, s) => a + s.dt, 0);
  const byFn = new Map();
  const byTotal = new Map();
  for (const s of inPhase) {
    const n = nodes.get(s.id);
    if (!n) continue;
    byFn.set(label(n), (byFn.get(label(n)) ?? 0) + s.dt);
    // inclusive: every distinct frame on the stack
    const seen = new Set();
    for (let id = s.id; id; id = parent.get(id)) {
      const l = label(nodes.get(id));
      if (seen.has(l)) continue;
      seen.add(l);
      byTotal.set(l, (byTotal.get(l) ?? 0) + s.dt);
    }
  }
  const idle = byFn.get("(idle) (idle):0") ?? 0;
  console.log(`\n### ${name} → ${phases[p + 1]?.[0] ?? "end"}  (${from.toFixed(1)}–${Number.isFinite(to) ? to.toFixed(1) : "…"} ms; sampled ${total.toFixed(1)} ms, idle ${idle.toFixed(1)})`);
  console.log("  self:");
  for (const [k, v] of [...byFn].sort((a, b) => b[1] - a[1]).slice(0, TOP)) console.log(`    ${v.toFixed(1).padStart(6)}  ${k}`);
  console.log("  inclusive:");
  for (const [k, v] of [...byTotal].sort((a, b) => b[1] - a[1]).slice(0, TOP)) console.log(`    ${v.toFixed(1).padStart(6)}  ${k}`);
}
