#!/usr/bin/env node
// BandChat local-first sender-side delivery: what the sending tab does between
// the click on Send and posting the message's frame to its SharedWorker.
//
//   node lf-delivery.mjs --tag baseline [--variant default|thin] [--samples 10]
//        [--trace 3,6] [--settle 30000] [--env VITE_BENCH_HISTORY=50,...] [--no-seed]
//
// Builds BandChat (Jazz) with VITE_BENCH_MARKS=1, BENCH_SOURCEMAP=1 and the
// given --env flags, opens users a and b in the busy room, waits --settle ms
// for the background local-first upgrade, then sends --samples messages from a.
// Per sample, from the click (page marks and the probe's port/WebSocket log):
//   committed  a: db.transaction resolved (send:committed)
//   render     a: first React render with the message (data:newest)
//   port out   a: the frame carrying the message posted to a's worker
//   ws out     a: the frame carrying the message sent on a's own WebSocket (thin)
//   a dom      a: the bubble inserted (MutationObserver)
//   b dom      b: the bubble inserted on the receiver
// On the samples listed in --trace, a Chrome trace of a's renderer (timeline +
// V8 CPU samples, ~0.1 ms) is written as a raw .json.gz, and lf-delivery-analyze.mjs
// splits it into trace-NN.to-port.json and .to-dom.json. Traced samples are excluded from the timing medians.
// Output: results/<date>/lf-delivery/<tag>/{sample-NN.json, trace-NN.json.gz, summary.json}.
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { parseArgs } from "node:util";
import { gzipSync } from "node:zlib";
import { chromium } from "playwright";
import { resolveTarget, resultsDir } from "./apps.mjs";
import { median } from "./lib.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const { values } = parseArgs({
  options: {
    tag: { type: "string", default: "baseline" },
    variant: { type: "string", default: "default" },
    samples: { type: "string", default: "10" },
    trace: { type: "string", default: "3" },
    settle: { type: "string", default: "30000" },
    env: { type: "string", default: "" },
    bubbles: { type: "string", default: "" },
    "no-seed": { type: "boolean", default: false },
    date: { type: "string", default: "2026-11-14" },
  },
});
const outDir = join(resultsDir, values.date, "lf-delivery", values.tag);
mkdirSync(outDir, { recursive: true });
const log = (...a) => console.error(...a);
const probeSource = readFileSync(join(here, "breakdown-probe.js"), "utf8");
const probeCall = (opts) => `(() => {\n${probeSource}\nbenchProbe(${JSON.stringify(opts)});\n})()`;

process.env.VITE_BENCH_MARKS = "1";
process.env.BENCH_SOURCEMAP = "1";
const flags = Object.fromEntries(values.env.split(",").filter(Boolean).map((kv) => kv.split("=")));
Object.assign(process.env, flags);
const traced = new Set(values.trace.split(",").filter(Boolean).map(Number));

const app = await import(pathToFileURL(join(resolveTarget("band-chat/jazz").hooksDir, "harness.mjs")).href);
log(`# lf-delivery ${values.tag} (${values.variant}) ${JSON.stringify(flags)}`);
const ctx = values["no-seed"] ? app.lastSeed() : await app.seed({ variant: values.variant });
const server = await app.start({ variant: values.variant });
const browser = await chromium.launch();
const bubbles = Number(values.bubbles || Math.min(200, Number(flags.VITE_BENCH_RENDER_LIMIT ?? 200), Number(flags.VITE_BENCH_HISTORY ?? 200)));

try {
  await run();
} finally {
  await browser.close();
  await server.stop();
}
// Split each trace now, while dist/ still holds this build's source maps:
// click → frame posted to the worker (or sent on the WebSocket), and click → bubble in a's DOM.
for (const f of readdirSync(outDir).filter((f) => /^trace-\d+\.json\.gz$/.test(f))) {
  for (const [to, suffix] of [["probe:port-out-needle", "to-port"], ["dom:needle", "to-dom"]]) {
    try {
      execFileSync("node", [join(here, "lf-delivery-analyze.mjs"), join(outDir, f), "--to", to, "--quiet", "--json", join(outDir, f.replace(".json.gz", `.${suffix}.json`))]);
    } catch (e) {
      log(`  (no ${suffix} split for ${f}: ${String(e.stderr ?? e).split("\n").find((l) => l.includes("Error")) ?? ""})`);
    }
  }
}

async function run() {
  const samples = Number(values.samples);
  const room = `${server.url}/?room=${ctx.busyRoomId}`;
  const BUBBLES = `return document.querySelectorAll('[data-message-id]').length >= ${bubbles} && "ok"`;
  const NEEDLE = `const n = window.__bench?.needle; return n && [...document.querySelectorAll('[data-message-id]')].some((b) => b.textContent.includes(n)) && n`;
  const SEND = `[...document.querySelectorAll('button')].find((b) => (b.getAttribute('aria-label') ?? b.textContent.trim()) === 'Send').click();`;
  const open = async (user) => {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.addInitScript(probeCall({ realm: "page", checks: [{ name: "dom:bubbles", cond: BUBBLES }, { name: "dom:needle", cond: NEEDLE }] }));
    await app.signIn(page, user, ctx);
    await page.goto(room);
    await page.waitForFunction(() => performance.getEntriesByName("dom:bubbles").length > 0, null, { timeout: 60_000, polling: 100 });
    return { context, page };
  };
  const a = await open("a");
  const b = await open("b");
  const subs = await a.page.evaluate(() => performance.getEntriesByType("mark").length);
  log(`opened (a marks ${subs}); settling ${values.settle} ms`);
  await b.page.waitForTimeout(Number(values.settle));
  const rows = [];
  for (let i = 0; i <= samples; i++) {
    const text = `bench lf ${Date.now()}-${i}`;
    for (const c of [a, b]) await c.page.evaluate((n) => (window.__bench.needle = n), text);
    await a.page.getByRole("textbox", { name: "Message input" }).fill(text);
    await a.page.waitForTimeout(300);
    const tracing = traced.has(i) ? await startTrace(a.page) : null;
    const sent = await a.page.evaluate((act) => {
      performance.mark("bench:click");
      const t = performance.timeOrigin + performance.now();
      new Function(act)();
      return t;
    }, SEND);
    await b.page.waitForFunction((n) => performance.getEntriesByName("dom:needle").some((m) => m.detail?.key === n), text, { timeout: 30_000 });
    await a.page.waitForFunction((n) => performance.getEntriesByName("dom:needle").some((m) => m.detail?.key === n), text, { timeout: 30_000 });
    await a.page.waitForTimeout(500);
    if (tracing) writeFileSync(join(outDir, `trace-${String(i).padStart(2, "0")}.json.gz`), gzipSync(await tracing.stop()));
    const sa = await a.page.evaluate(() => window.__bench.collect());
    const sb = await b.page.evaluate(() => window.__bench.collect());
    const row = timeline({ text, sent, a: sa, b: sb });
    const tag = i === 0 ? "warmup" : `sample-${String(i).padStart(2, "0")}`;
    writeFileSync(join(outDir, `${tag}.json`), JSON.stringify({ tag: values.tag, flags, text, sent, traced: !!tracing, row, a: windowed(sa, sent - 50), b: windowed(sb, sent - 50) }, null, 1));
    log(`  ${tag}${tracing ? " (traced)" : ""}: ${Object.entries(row).map(([k, v]) => `${k} ${Math.round(v)}`).join(", ")}`);
    if (i > 0 && !tracing) rows.push(row);
  }
  const patchStats = await a.page.evaluate(() => ({ syncOutbound: globalThis.__jazzSyncOutbound, syncOutboundErrors: globalThis.__jazzSyncOutboundErrors, errors: window.__bench.console.filter((c) => c.level === "error").slice(-5).map((c) => c.text.slice(0, 300)) }));
  const summary = { tag: values.tag, variant: values.variant, flags, samples: rows.length, medians: medians(rows), patchStats };
  writeFileSync(join(outDir, "summary.json"), JSON.stringify(summary, null, 2) + "\n");
  console.log(JSON.stringify(summary));
}

function timeline({ text, sent, a, b }) {
  const mark = (s, name) => s.marks.find((m) => m.name === name && (m.detail?.text ?? m.detail?.key) === text);
  const at = (s, m) => (m ? s.timeOrigin + m.t - sent : undefined);
  const out = {
    committed: at(a, mark(a, "send:committed")),
    render: at(a, mark(a, "data:newest")),
    "port out": a.ports.find((p) => p.needle && p.dir === "out" && p.t >= sent)?.t - sent,
    "ws out": a.ws.find((p) => p.needle && p.dir === "out" && p.t >= sent)?.t - sent,
    "a dom": at(a, mark(a, "dom:needle")),
    "b dom": at(b, mark(b, "dom:needle")),
  };
  // Main-thread long tasks on a in the 2 s after the click.
  out["a longtask ms"] = (a.longtasks ?? []).filter((l) => l.t >= sent - 5 && l.t < sent + 2000).reduce((s, l) => s + l.ms, 0);
  for (const k of Object.keys(out)) if (out[k] === undefined || Number.isNaN(out[k])) delete out[k];
  return out;
}

function windowed(s, from) {
  const keep = (list) => (list ?? []).filter((e) => e.t >= from);
  return { timeOrigin: s.timeOrigin, marks: s.marks.filter((m) => s.timeOrigin + m.t >= from), ports: keep(s.ports), ws: keep(s.ws), longtasks: keep(s.longtasks) };
}

function medians(rows) {
  const keys = [...new Set(rows.flatMap((r) => Object.keys(r)))];
  return Object.fromEntries(keys.map((k) => [k, Math.round(median(rows.map((r) => r[k]).filter((v) => typeof v === "number")) * 10) / 10]));
}

/** A Chrome trace of one page: timeline events plus high-resolution V8 CPU samples. */
async function startTrace(page) {
  const s = await page.context().newCDPSession(page);
  const done = new Promise((r) => s.once("Tracing.tracingComplete", r));
  await s.send("Tracing.start", {
    transferMode: "ReturnAsStream",
    traceConfig: {
      includedCategories: [
        "devtools.timeline",
        "disabled-by-default-devtools.timeline",
        "disabled-by-default-devtools.timeline.frame",
        "blink.user_timing",
        "toplevel",
        "v8.execute",
        "disabled-by-default-v8.cpu_profiler",
        "disabled-by-default-v8.cpu_profiler.hires",
        "v8",
        "blink",
      ],
    },
  });
  return {
    async stop() {
      await s.send("Tracing.end");
      const { stream } = await done;
      const parts = [];
      for (;;) {
        const r = await s.send("IO.read", { handle: stream, size: 1 << 20 });
        parts.push(r.base64Encoded ? Buffer.from(r.data, "base64") : Buffer.from(r.data));
        if (r.eof) break;
      }
      await s.send("IO.close", { handle: stream });
      await s.detach();
      return Buffer.concat(parts);
    },
  };
}
