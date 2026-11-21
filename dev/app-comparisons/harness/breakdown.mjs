#!/usr/bin/env node
// Startup / cross-client breakdown for the Jazz apps (measurement only).
//
//   node breakdown.mjs stage-plan --variant default|now|thin [--log debug] [--wait0] [--runs 5]
//   node breakdown.mjs band-chat  --variant default|thin     [--log debug] [--samples 10]
//
// Builds the app with VITE_BENCH_MARKS=1 (+ VITE_JAZZ_LOG, VITE_FIRST_LOAD_WAIT_MS=0),
// then drives it like run.mjs while a raw CDP connection auto-attaches to every
// SharedWorker before it runs its first line (Target.setAutoAttach,
// waitForDebuggerOnStart) to inject breakdown-probe.js and stream its console.
// Pages get the same probe through addInitScript. Writes raw JSON per run to
// results/<date>/breakdown/<case>/ and prints the per-mark medians.
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { chromium } from "playwright";
import { resolveTarget, resultsDir } from "./apps.mjs";
import { median } from "./lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    variant: { type: "string", default: "default" },
    log: { type: "string", default: "" },
    wait0: { type: "boolean", default: false },
    runs: { type: "string", default: "5" },
    samples: { type: "string", default: "10" },
    settle: { type: "string", default: "1500" },
    profile: { type: "boolean", default: false },
    "no-seed": { type: "boolean", default: false },
    date: { type: "string", default: new Date().toISOString().slice(0, 10) },
    out: { type: "string", default: "" },
  },
});
const hero = positionals[0];
if (!["stage-plan", "band-chat"].includes(hero)) throw new Error("usage: breakdown.mjs stage-plan|band-chat --variant ...");
const settleTag = hero === "band-chat" && values.settle !== "1500" ? `settle${Math.round(Number(values.settle) / 1000)}s` : "";
const caseName = [hero, values.variant, values.wait0 ? "wait0" : "", settleTag, values.profile ? "profile" : "", values.log ? `log-${values.log}` : "marks"].filter(Boolean).join(".");
const outDir = values.out ? join(resolve(values.out), caseName) : join(resultsDir, values.date, "breakdown", caseName);
mkdirSync(outDir, { recursive: true });
const log = (...a) => console.error(...a);
const probeSource = readFileSync(join(here, "breakdown-probe.js"), "utf8");
const probeCall = (opts) => `(() => {\n${probeSource}\nbenchProbe(${JSON.stringify(opts)});\n})()`;

// Build flags (the app harness passes process.env through to `vite build`).
process.env.VITE_BENCH_MARKS = "1";
process.env.VITE_JAZZ_LOG = values.log;
process.env.VITE_FIRST_LOAD_WAIT_MS = values.wait0 ? "0" : "";

const app = await import(pathToFileURL(join(resolveTarget(`${hero}/jazz`).hooksDir, "harness.mjs")).href);
log(`# ${caseName}`);
const ctx = values["no-seed"] ? app.lastSeed() : await app.seed({ variant: values.variant });
const server = await app.start({ variant: values.variant });
log("serving", server.url);

const CDP_PORT = 9300 + Math.floor(Math.random() * 600);
const browser = await chromium.launch({ args: [`--remote-debugging-port=${CDP_PORT}`] });
const cdp = await connectCdp(CDP_PORT);
const workers = new Map(); // sessionId -> { targetId, url, browserContextId, attachedAt, console: [] }
await cdp.attachWorkers();

try {
  if (hero === "stage-plan") await stagePlan();
  else await bandChat();
} finally {
  cdp.close();
  await browser.close();
  await server.stop();
}

// ---------------------------------------------------------------- StagePlan

async function stagePlan() {
  const runs = Number(values.runs);
  const CARDS = `return document.querySelectorAll('[data-task-id]').length >= 100 && "100"`;
  const joinUrl = `${server.url}/#/join/${ctx.showId}/${ctx.code}`;
  const rows = { join: [], reload: [] };
  for (let i = 0; i <= runs; i++) {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.addInitScript(probeCall({ realm: "page", checks: [{ name: "dom:cards-100", cond: CARDS }] }));
    const contextId = await browserContextIdOf(page);
    const loads = countLoads(page);

    await page.goto(joinUrl);
    await waitForMark(page, "dom:cards-100");
    await page.waitForTimeout(1500); // as run.mjs: let the background upgrade settle
    const joined = await snapshot(page, contextId, loads);
    loads.reset();

    const markBefore = Date.now();
    await page.reload();
    await waitForMark(page, "dom:cards-100");
    await page.waitForTimeout(1500);
    const reload = await snapshot(page, contextId, loads, markBefore);
    await context.close();

    const tag = i === 0 ? "warmup" : `run-${i}`;
    writeFileSync(join(outDir, `${tag}.json`), JSON.stringify({ case: caseName, ctx, join: joined, reload }, null, 1));
    const j = timeline(joined);
    const r = timeline(reload);
    log(`  ${tag}: join cards ${fmt(j["page dom:cards-100"])} ms, reload cards ${fmt(r["page dom:cards-100"])} ms, self-loads ${joined.selfLoads}/${reload.selfLoads}`);
    if (i > 0) {
      rows.join.push(j);
      rows.reload.push(r);
    }
  }
  const summary = { case: caseName, variant: values.variant, log: values.log || "default", wait0: values.wait0, runs, join: medians(rows.join), reload: medians(rows.reload) };
  writeFileSync(join(outDir, "summary.json"), JSON.stringify(summary, null, 2) + "\n");
  console.log(JSON.stringify(summary, null, 2));
}

// ---------------------------------------------------------------- BandChat

async function bandChat() {
  const samples = Number(values.samples);
  const room = `${server.url}/?room=${ctx.busyRoomId}`;
  const BUBBLES = `return document.querySelectorAll('[data-message-id]').length >= 200 && "200"`;
  const NEEDLE = `const n = window.__bench?.needle; return n && [...document.querySelectorAll('[data-message-id]')].some((b) => b.textContent.includes(n)) && n`;
  const SEND = `[...document.querySelectorAll('button')].find((b) => (b.getAttribute('aria-label') ?? b.textContent.trim()) === 'Send').click();`;
  const open = async (user) => {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.addInitScript(probeCall({ realm: "page", checks: [{ name: "dom:bubbles-200", cond: BUBBLES }, { name: "dom:needle", cond: NEEDLE }] }));
    await app.signIn(page, user, ctx);
    const contextId = await browserContextIdOf(page);
    await page.goto(room);
    await waitForMark(page, "dom:bubbles-200");
    return { context, page, contextId };
  };
  const a = await open("a");
  const b = await open("b");
  const opened = Date.now();
  // As run.mjs waits before bc_cross_client (1.5 s); --settle 30000 waits out the background upgrade.
  await b.page.waitForTimeout(Number(values.settle));
  const rows = [];
  for (let i = 0; i <= samples; i++) {
    const text = `bench cross ${Date.now()}-${i}`;
    await setNeedle([a, b], text);
    await a.page.getByRole("textbox", { name: "Message input" }).fill(text);
    const profiling = values.profile && i === Math.min(5, samples) ? await startProfiles([a, b]) : null;
    const sent = await a.page.evaluate((act) => {
      const t = performance.timeOrigin + performance.now();
      new Function(act)();
      return t;
    }, SEND);
    await b.page.waitForFunction((n) => performance.getEntriesByName("dom:needle").some((m) => m.detail?.key === n), text, { timeout: 30_000 });
    await a.page.waitForTimeout(400); // trailing frames and acks
    if (profiling) await stopProfiles(profiling, i);
    const sender = await snapshot(a.page, a.contextId, null, sent - 50);
    const receiver = await snapshot(b.page, b.contextId, null, sent - 50);
    const sample = { text, sent, sinceOpenMs: Math.round(sent - opened), sender: windowed(sender, sent - 50), receiver: windowed(receiver, sent - 50) };
    const tag = i === 0 ? "warmup" : `sample-${String(i).padStart(2, "0")}`;
    writeFileSync(join(outDir, `${tag}.json`), JSON.stringify({ case: caseName, ...sample }, null, 1));
    const row = crossTimeline(sample);
    log(`  ${tag} (+${sample.sinceOpenMs} ms): ${Object.entries(row).map(([k, v]) => `${k} ${fmt(v)}`).join(", ")}`);
    if (i > 0) rows.push(row);
  }
  await a.context.close();
  await b.context.close();
  const summary = { case: caseName, variant: values.variant, log: values.log || "default", samples, settleMs: Number(values.settle), cross: medians(rows) };
  writeFileSync(join(outDir, "summary.json"), JSON.stringify(summary, null, 2) + "\n");
  console.log(JSON.stringify(summary, null, 2));
}

/** --profile: a CPU profile of both pages around one sample (CDP Profiler). */
async function startProfiles(clients) {
  const sessions = [];
  for (const [who, c] of [["a", clients[0]], ["b", clients[1]]]) {
    const s = await c.page.context().newCDPSession(c.page);
    await s.send("Profiler.enable");
    await s.send("Profiler.setSamplingInterval", { interval: 200 });
    await s.send("Profiler.start");
    sessions.push([who, s]);
  }
  return sessions;
}

async function stopProfiles(sessions, i) {
  const summary = {};
  for (const [who, s] of sessions) {
    const { profile } = await s.send("Profiler.stop");
    writeFileSync(join(outDir, `profile-${who}-sample-${i}.cpuprofile`), JSON.stringify(profile));
    summary[who] = topSelfTime(profile);
    await s.detach();
  }
  writeFileSync(join(outDir, `profile-sample-${i}.top.json`), JSON.stringify(summary, null, 1));
  for (const [who, rows] of Object.entries(summary)) {
    log(`  profile ${who}: ${rows.byFile.slice(0, 6).map((r) => `${r.file} ${r.ms}`).join(", ")}`);
  }
}

/** Self time by function and by script file, from a CDP CPU profile. */
function topSelfTime(profile) {
  const byId = new Map(profile.nodes.map((n) => [n.id, n]));
  const self = new Map();
  const dt = profile.timeDeltas;
  for (let k = 0; k < profile.samples.length; k++) self.set(profile.samples[k], (self.get(profile.samples[k]) ?? 0) + (dt[k] ?? 0) / 1000);
  const fn = new Map();
  const file = new Map();
  for (const [id, ms] of self) {
    const cf = byId.get(id).callFrame;
    const f = (cf.url || "(native)").split("/").pop() || "(native)";
    const key = `${cf.functionName || "(anonymous)"} @ ${f}`;
    fn.set(key, (fn.get(key) ?? 0) + ms);
    file.set(f, (file.get(f) ?? 0) + ms);
  }
  const top = (m, n) => [...m].sort((x, y) => y[1] - x[1]).slice(0, n).map(([k, ms]) => ({ ms: Math.round(ms), k }));
  return { byFile: top(file, 15).map(({ ms, k }) => ({ file: k, ms })), byFunction: top(fn, 30) };
}

async function setNeedle(clients, text) {
  for (const c of clients) await c.page.evaluate((n) => (window.__bench.needle = n), text);
  for (const [sessionId, w] of workers) {
    if (w.detached) continue;
    await cdp.send("Runtime.evaluate", { expression: `globalThis.__bench && (globalThis.__bench.needle = ${JSON.stringify(text)})` }, sessionId).catch(() => {});
  }
}

/** Milestones of one message, ms after the click on Send. */
function crossTimeline({ text, sent, sender, receiver }) {
  const firstMark = (snap, name, text) => snap.page.marks.find((m) => m.name === name && (m.detail?.text ?? m.detail?.key) === text);
  const t = (epoch) => (epoch === undefined ? undefined : epoch - sent);
  const pageT = (snap, m) => (m ? snap.page.timeOrigin + m.t : undefined);
  const firstNeedle = (list, dir) => list.find((e) => e.needle && e.dir === dir)?.t;
  const out = {};
  out["a page: send:start"] = t(pageT(sender, firstMark(sender, "send:start", text)));
  out["a page: send:committed"] = t(pageT(sender, firstMark(sender, "send:committed", text)));
  out["a page: port out"] = t(firstNeedle(sender.page.ports, "out"));
  out["a page: ws out"] = t(firstNeedle(sender.page.ws, "out"));
  for (const w of sender.workers) {
    out["a worker: port in"] ??= t(firstNeedle(w.ports, "in"));
    out["a worker: ws out"] ??= t(firstNeedle(w.ws, "out"));
  }
  for (const w of receiver.workers) {
    out["b worker: ws in"] ??= t(firstNeedle(w.ws, "in"));
    out["b worker: port out"] ??= t(firstNeedle(w.ports, "out"));
  }
  out["b page: ws in"] = t(firstNeedle(receiver.page.ws, "in"));
  out["b page: port in"] = t(firstNeedle(receiver.page.ports, "in"));
  out["b page: data:newest"] = t(pageT(receiver, firstMark(receiver, "data:newest", text)));
  out["b page: dom"] = t(pageT(receiver, firstMark(receiver, "dom:needle", text)));
  for (const k of Object.keys(out)) if (out[k] === undefined) delete out[k];
  return out;
}

/** Keep only events since `from` (epoch ms), so per-sample files stay small. */
function windowed(snap, from) {
  const keep = (list) => list.filter((e) => e.t >= from);
  const page = { ...snap.page, longtasks: keep(snap.page.longtasks ?? []), ws: keep(snap.page.ws), ports: keep(snap.page.ports), console: keep(snap.page.console), marks: snap.page.marks.filter((m) => snap.page.timeOrigin + m.t >= from) };
  const ws = snap.workers.map((w) => ({ ...w, ws: keep(w.ws), ports: keep(w.ports), console: keep(w.console), marks: w.marks.filter((m) => w.timeOrigin + m.t >= from) }));
  return { page, workers: ws };
}

// ---------------------------------------------------------------- collection

async function waitForMark(page, name) {
  for (let attempt = 0; ; attempt++) {
    try {
      await page.waitForFunction((n) => performance.getEntriesByName(n).length > 0, name, { timeout: 60_000, polling: 50 });
      return;
    } catch (error) {
      if (attempt > 2 || !String(error).includes("Execution context was destroyed")) {
        const dump = await page.evaluate(() => ({ body: document.body?.innerText.slice(0, 500), console: window.__bench?.console.slice(-30), marks: performance.getEntriesByType("mark").map((m) => m.name) })).catch((e) => String(e));
        log("  timed out waiting for", name, JSON.stringify(dump, null, 1));
        throw error;
      }
      log("  (page navigated while waiting; waiting in the new page)");
    }
  }
}

function countLoads(page) {
  const c = { n: 0, at: [] };
  page.on("load", () => {
    c.n++;
    c.at.push(Date.now());
  });
  c.reset = () => {
    c.n = 0;
    c.at = [];
  };
  return c;
}

async function browserContextIdOf(page) {
  const s = await page.context().newCDPSession(page);
  const { targetInfo } = await s.send("Target.getTargetInfo");
  await s.detach();
  return targetInfo.browserContextId;
}

/** Page probe data + the workers of this browser context (marks via Runtime.evaluate, console via CDP). */
async function snapshot(page, contextId, loads, since = 0) {
  const pageData = await page.evaluate(() => window.__bench.collect());
  const ws = [];
  for (const [sessionId, w] of workers) {
    if (w.browserContextId !== contextId) continue;
    let data = null;
    if (!w.detached) {
      try {
        const r = await cdp.send("Runtime.evaluate", { expression: "JSON.stringify(globalThis.__bench?.collect())", returnByValue: true }, sessionId);
        data = r.result.value ? JSON.parse(r.result.value) : null;
      } catch {}
    }
    data ??= w.lastData;
    if (data) w.lastData = data;
    ws.push({ sessionId, url: w.url, attachedAt: w.attachedAt, detached: !!w.detached, ...(data ?? { marks: [], ws: [], ports: [], timeOrigin: w.attachedAt }), console: w.console.filter((c) => c.t >= since) });
  }
  return { selfLoads: loads ? Math.max(0, loads.n - 1) : undefined, page: pageData, workers: ws };
}

/**
 * One navigation's milestones, ms from its navigation start: page marks,
 * worker marks (shifted by the worker's timeOrigin), first WebSocket frames,
 * and resource fetches (WASM binary, worker script, local-first chunk).
 */
function timeline(snap) {
  const t0 = snap.page.timeOrigin;
  const out = {};
  const put = (k, v) => {
    if (v !== undefined && out[k] === undefined) out[k] = Math.round(v * 10) / 10;
  };
  const nav = snap.page.navigation[0];
  if (nav) {
    put("nav:responseEnd", nav.responseEnd);
    put("nav:domContentLoaded", nav.domContentLoadedEventEnd);
    put("nav:load", nav.loadEventEnd);
  }
  for (const m of snap.page.marks) put(`page ${m.name}`, m.t);
  for (const r of snap.page.resources) {
    if (/\.wasm/.test(r.name)) put("page res:wasm", r.responseEnd);
    if (/jazz-broker-worker/.test(r.name)) put("page res:worker-js", r.responseEnd);
    if (/local-first-/.test(r.name)) put("page res:local-first-js", r.responseEnd);
  }
  put("page ws:first-out", snap.page.ws.find((e) => e.dir === "out")?.t - t0);
  put("page ws:first-in", snap.page.ws.find((e) => e.dir === "in")?.t - t0);
  portMilestones("page", snap.page.ports, t0, put);
  logMilestones("page", snap.page.console, t0, put);
  // Workers: only what happened during this navigation (a worker that outlived
  // the previous page shows up as "worker reused").
  for (const w of snap.workers) {
    const shift = w.timeOrigin - t0;
    if (shift < 0 && !w.detached) put("worker reused (timeOrigin)", shift);
    if (shift >= 0) put("worker timeOrigin", shift);
    for (const m of w.marks) if (m.t + shift >= 0) put(`worker ${m.name}`, m.t + shift);
    for (const r of w.resources ?? []) if (/\.wasm/.test(r.name) && r.responseEnd + shift >= 0) put("worker res:wasm", r.responseEnd + shift);
    put("worker ws:first-out", w.ws.find((e) => e.dir === "out" && e.t >= t0)?.t - t0);
    put("worker ws:first-in", w.ws.find((e) => e.dir === "in" && e.t >= t0)?.t - t0);
    portMilestones("worker", w.ports, t0, put);
    logMilestones("worker", w.console, t0, put);
  }
  return out;
}

/** First port message of each type and direction (the worker protocol's own names). */
function portMilestones(realm, ports, t0, put) {
  for (const e of ports) if (e.t >= t0 && e.type) put(`${realm} port:${e.dir}:${e.type}`, e.t - t0);
}

/**
 * Milestones from Jazz's debug log lines: the first message of each kind on
 * each hop (e.g. client→upstream Join, upstream→client Rows), the first Rows
 * that carry tasks, and when the tasks subscription was delivered and settled.
 */
function logMilestones(realm, lines, t0, put) {
  for (const c of lines) {
    if (c.t < t0) continue;
    const text = c.text.replace(/%c/g, "");
    const hop = text.match(/hop = "([^"]+)"/)?.[1];
    const kind = text.match(/kind = "([^"]+)"/)?.[1];
    if (hop && kind) {
      put(`${realm} log:${hop}:${kind}`, c.t - t0);
      if (kind === "Rows" && /"tasks"/.test(text)) put(`${realm} log:${hop}:Rows(tasks)`, c.t - t0);
      if (kind === "Settled" && /global=true/.test(text)) put(`${realm} log:${hop}:Settled(global)`, c.t - t0);
    }
    const delivered = text.match(/subscription delivered table = (\w+)/)?.[1];
    if (delivered) put(`${realm} log:delivered:${delivered}`, c.t - t0);
    const settled = text.match(/subscription settled table = (\w+);\s*tier = (\w+)/);
    if (settled) put(`${realm} log:settled:${settled[1]}:${settled[2]}`, c.t - t0);
  }
}

function medians(rows) {
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  const out = {};
  for (const k of keys) {
    const xs = rows.map((r) => r[k]).filter((v) => typeof v === "number" && !Number.isNaN(v));
    if (xs.length) out[k] = { median: Math.round(median(xs) * 10) / 10, n: xs.length };
  }
  return Object.fromEntries(Object.entries(out).sort((x, y) => x[1].median - y[1].median));
}

function fmt(v) {
  return v === undefined ? "-" : Math.round(v);
}

// ---------------------------------------------------------------- raw CDP

async function connectCdp(port) {
  let version;
  for (let i = 0; i < 50 && !version; i++) {
    try {
      version = await (await fetch(`http://127.0.0.1:${port}/json/version`)).json();
    } catch {
      await new Promise((r) => setTimeout(r, 100));
    }
  }
  const socket = new WebSocket(version.webSocketDebuggerUrl);
  await new Promise((r, j) => {
    socket.onopen = r;
    socket.onerror = j;
  });
  let nextId = 1;
  const pending = new Map();
  socket.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.id) {
      const p = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) p?.reject(new Error(msg.error.message));
      else p?.resolve(msg.result);
      return;
    }
    onEvent(msg);
  };
  const send = (method, params = {}, sessionId) =>
    new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    });
  const probe = probeCall({ realm: "worker" });
  async function onEvent({ method, params, sessionId }) {
    if (method === "Target.attachedToTarget") {
      const { sessionId: sid, targetInfo, waitingForDebugger } = params;
      workers.set(sid, { targetId: targetInfo.targetId, url: targetInfo.url, browserContextId: targetInfo.browserContextId, attachedAt: Date.now(), console: [] });
      try {
        await send("Runtime.enable", {}, sid);
        await send("Runtime.evaluate", { expression: probe }, sid);
      } finally {
        if (waitingForDebugger) await send("Runtime.runIfWaitingForDebugger", {}, sid).catch(() => {});
      }
    } else if (method === "Target.detachedFromTarget") {
      const w = workers.get(params.sessionId);
      if (w) w.detached = Date.now();
    } else if (method === "Runtime.consoleAPICalled" && workers.has(sessionId)) {
      const text = params.args.map((a) => a.value ?? a.description ?? a.unserializableValue ?? "").join(" ");
      workers.get(sessionId).console.push({ t: params.timestamp, level: params.type, text: String(text).slice(0, 4000) });
    } else if (method === "Runtime.exceptionThrown" && workers.has(sessionId)) {
      const d = params.exceptionDetails;
      workers.get(sessionId).console.push({ t: params.timestamp, level: "exception", text: d.exception?.description ?? d.text });
    }
  }
  return {
    send,
    close: () => socket.close(),
    attachWorkers: () =>
      send("Target.setAutoAttach", {
        autoAttach: true,
        waitForDebuggerOnStart: true,
        flatten: true,
        filter: [{ type: "shared_worker", exclude: false }, { exclude: true }],
      }),
  };
}
