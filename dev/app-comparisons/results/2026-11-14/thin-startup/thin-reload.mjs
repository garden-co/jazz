#!/usr/bin/env node
// Thin-client reload timeline for StagePlan/Jazz (measurement only).
//
//   node results/2026-11-14/thin-startup/thin-reload.mjs [--runs 9] [--label base]
//        [--jazz-tools <name>]  build against a patched copy of jazz-tools: a sibling dir
//                               <name> of the installed jazz-tools in the pnpm store (see
//                               patch/apply.sh); the app's node_modules/jazz-tools symlink is
//                               pointed at it for the build and restored after
//        [--trace]              also record a Chrome trace (with CPU profile) of the last reload
//        [--no-minify]          unminified build (readable names in the profile; not for timings)
//        [--rtt-ms 40]          add round-trip latency to all traffic (latency-proxy.mjs)
//        [--no-seed]
//
// Each run: fresh browser context, join the show, wait for 100 cards + 1.5 s,
// then reload and wait for 100 cards again (as harness/breakdown.mjs). Timings
// come from the reload: performance marks (app + jazz-tools patch marks
// "jz:*"), resource timing, WebSocket frames (harness/breakdown-probe.js).
// Writes <label>/run-N.json, <label>/summary.json, and <label>/trace.json.
import { createRequire } from "node:module";
import { mkdirSync, readFileSync, readlinkSync, symlinkSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../..");
const require = createRequire(join(root, "harness", "package.json"));
const { chromium } = require("playwright");
const { serve, sh } = await import(pathToFileURL(join(root, "harness/adapters/vite-app.mjs")).href);
const { values } = parseArgs({
  options: {
    runs: { type: "string", default: "9" },
    label: { type: "string", default: "base" },
    "jazz-tools": { type: "string", default: "" },
    trace: { type: "boolean", default: false },
    "no-minify": { type: "boolean", default: false },
    "no-seed": { type: "boolean", default: false },
    "rtt-ms": { type: "string", default: "0" },
  },
});
const appDir = join(root, "stage-plan/jazz");
const outDir = join(here, "runs", values.label);
mkdirSync(outDir, { recursive: true });
const log = (...a) => console.error(...a);
const probeSource = readFileSync(join(root, "harness/breakdown-probe.js"), "utf8");
const CARDS = `return document.querySelectorAll('[data-task-id]').length >= 100 && "100"`;
const probe = `(() => {\n${probeSource}\nbenchProbe(${JSON.stringify({ realm: "page", checks: [{ name: "dom:cards-100", cond: CARDS }] })});\n})()`;

const app = await import(pathToFileURL(join(appDir, "harness.mjs")).href);
const ctx = values["no-seed"] ? app.lastSeed() : await app.seed({ variant: "thin" });

// Build (thin, marks on), optionally against a patched jazz-tools, then serve.
const buildDir = `dist-${values.label}`;
const env = {
  VITE_BENCH_MARKS: "1",
  VITE_JAZZ_THIN: "1",
  VITE_JAZZ_LOCAL_FIRST: "",
  THIN_PROFILE_MINIFY: values["no-minify"] ? "0" : "1",
  THIN_PROFILE_OUT: buildDir,
};
const config = join(here, "vite.thin-profile.config.ts");
const link = join(appDir, "node_modules/jazz-tools");
const original = readlinkSync(link);
if (values["jazz-tools"]) {
  unlinkSync(link);
  symlinkSync(join(dirname(original), values["jazz-tools"]), link);
}
try {
  sh(appDir, `node_modules/.bin/vite build --logLevel error --config ${config}`, env);
} finally {
  unlinkSync(link);
  symlinkSync(original, link);
}
const PORT = 4402;
const rttMs = Number(values["rtt-ms"]);
const stopServer = await serve(appDir, `node_modules/.bin/vite preview --config ${config} --host 127.0.0.1 --port ${PORT} --strictPort`, `http://127.0.0.1:${PORT}`, { ...env, JAZZ_DATA_DIR: ".bench-data" });
// --rtt-ms: the browser talks to the backend through latency-proxy.mjs.
const { latencyProxy } = await import(pathToFileURL(join(here, "latency-proxy.mjs")).href);
const stopProxy = rttMs > 0 ? await latencyProxy({ listenPort: PORT + 1, targetPort: PORT, rttMs }) : () => {};
const url = `http://127.0.0.1:${rttMs > 0 ? PORT + 1 : PORT}`;
const stop = async () => {
  stopProxy();
  await stopServer();
};
const browser = await chromium.launch();
const joinUrl = `${url}/#/join/${ctx.showId}/${ctx.code}`;
const rows = [];
try {
  const runs = Number(values.runs);
  for (let i = 0; i <= runs; i++) {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.addInitScript(probe);
    await page.goto(joinUrl);
    await waitFor(page, "dom:cards-100");
    await page.waitForTimeout(1500);
    const tracing = values.trace && i === runs;
    if (tracing) {
      await browser.startTracing(page, {
        categories: [
          "devtools.timeline", "disabled-by-default-devtools.timeline", "v8.execute", "v8",
          "disabled-by-default-v8.cpu_profiler", "blink.user_timing", "loading", "netlog", "disabled-by-default-network",
        ],
      });
    }
    await page.reload();
    await waitFor(page, "dom:cards-100");
    await page.waitForTimeout(500);
    if (tracing) writeFileSync(join(outDir, "trace.json"), await browser.stopTracing());
    const data = await page.evaluate(() => window.__bench.collect());
    await context.close();
    const row = timeline(data);
    if (i > 0) {
      rows.push(row);
      writeFileSync(join(outDir, `run-${i}.json`), JSON.stringify(data, null, 1));
    }
    log(`  ${i ? `run-${i}` : "warmup"}: cards ${row["dom:cards-100"]?.toFixed(0)} ms, client-ready ${row["jazz:client-ready"]?.toFixed(0)}, ws:create ${row["ws:create"]?.toFixed(0)}`);
  }
} finally {
  await browser.close();
  await stop();
}
const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))];
const summary = Object.fromEntries(
  keys
    .map((k) => [k, median(rows.map((r) => r[k]).filter((v) => v !== undefined))])
    .sort((a, b) => a[1] - b[1]),
);
writeFileSync(join(outDir, "summary.json"), JSON.stringify({ label: values.label, runs: rows.length, jazzTools: values["jazz-tools"] || "published", rttMs: Number(values["rtt-ms"]), medianMsFromNavStart: summary }, null, 2) + "\n");
for (const [k, v] of Object.entries(summary)) console.log(`${k.padEnd(44)} ${v.toFixed(1).padStart(7)}`);

function timeline(d) {
  const out = {};
  const put = (k, v) => {
    if (v !== undefined && out[k] === undefined) out[k] = v;
  };
  const nav = d.navigation[0];
  if (nav) {
    put("nav:responseEnd", nav.responseEnd);
    put("nav:domInteractive", nav.domInteractive);
    put("nav:domContentLoaded", nav.domContentLoadedEventEnd);
  }
  for (const m of d.marks) put(m.name, m.t);
  for (const r of d.resources) {
    const name = r.name.replace(url, "").replace(/-[A-Za-z0-9_-]{8}\.(js|css)$/, ".$1");
    if (/\/assets\/index\.js$/.test(name)) put("res:main-js-end", r.responseEnd);
    if (/\/assets\/index\.css$/.test(name)) put("res:main-css-end", r.responseEnd);
    if (/\/jazz\//.test(name)) {
      put(`fetch:${name.split("?")[0]}:start`, r.start);
      put(`fetch:${name.split("?")[0]}:end`, r.responseEnd);
    }
  }
  const t0 = d.timeOrigin;
  const ins = d.ws.filter((f) => f.dir === "in");
  const outs = d.ws.filter((f) => f.dir === "out");
  outs.slice(0, 4).forEach((f, n) => put(`ws:out#${n + 1}`, f.t - t0));
  ins.slice(0, 6).forEach((f, n) => put(`ws:in#${n + 1}`, f.t - t0));
  return out;
}

async function waitFor(page, name) {
  for (let attempt = 0; ; attempt++) {
    try {
      await page.waitForFunction((n) => performance.getEntriesByName(n).length > 0, name, { timeout: 30_000, polling: 50 });
      return;
    } catch (e) {
      if (attempt > 2 || !String(e).includes("Execution context was destroyed")) throw e;
    }
  }
}

function median(xs) {
  const s = [...xs].sort((a, b) => a - b);
  return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2;
}
