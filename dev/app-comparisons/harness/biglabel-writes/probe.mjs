#!/usr/bin/env node
// BigLabel write/paging breakdown for big-label/jazz (measurement only).
//
//   node probe.mjs --url http://127.0.0.1:4404 --variant thin|default|now [--samples 10] [--profile] [--only update,next,cross]
//
// Expects serve.mjs running (it seeded and serves the variant). Drives the same
// UI paths as harness/scenarios/big-label.mjs, and records per sample, in epoch ms:
// - page and SharedWorker network (CDP Network on both: request sent, response
//   headers, each data chunk of a streaming response), WebSocket frames;
// - the done condition's time (rAF poll + MutationObserver), and
//   every DOM mutation of the watched element;
// - optional CDP CPU profile of page and worker over the samples (--profile).
// Raw JSON per scenario goes to --out (default results/<date>/biglabel-writes/raw/<variant>/).
import { mkdirSync, writeFileSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { chromium } from "playwright";
import { resolveTarget, resultsDir } from "../apps.mjs";
import { median } from "../lib.mjs";
import { bigLabelFixture, releasesNewestFirst, BENCH_LABEL, PAGE_SIZE, releaseStatuses } from "../../fixtures/big-label.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const { values } = parseArgs({
  options: {
    url: { type: "string", default: "http://127.0.0.1:4404" },
    variant: { type: "string", default: "default" },
    samples: { type: "string", default: "10" },
    settle: { type: "string", default: "5000" },
    profile: { type: "boolean", default: false },
    bodies: { type: "boolean", default: false },
    fix: { type: "string", default: "" },
    gap: { type: "string", default: "0" }, // ms to wait after bl_next_page (31000 outlives the 30 s subscription cache)
    only: { type: "string", default: "update,next,cross" },
    tag: { type: "string", default: "" },
    out: { type: "string", default: "" },
  },
});
const url = values.url;
const N = Number(values.samples);
const only = new Set(values.only.split(","));
const caseName = [values.variant, values.fix && `fix-${values.fix.replaceAll(",", "+")}`, values.tag, values.profile ? "profile" : "marks"].filter(Boolean).join(".");
const outDir = values.out ? resolve(values.out) : join(resultsDir, "2026-11-14", "biglabel-writes", "raw", caseName);
mkdirSync(outDir, { recursive: true });
const log = (...a) => console.error(...a);
const ctx = JSON.parse(readFileSync(join(resolveTarget("big-label/jazz").hooksDir, ".bench-ctx.json"), "utf8"));

const fixture = bigLabelFixture();
const all = releasesNewestFirst(fixture, BENCH_LABEL);
const pageOf = (p) => all.slice((p - 1) * PAGE_SIZE, p * PAGE_SIZE).map((r) => r.catalogNumber);
const sentence = (s) => s[0].toUpperCase() + s.slice(1);

// ----------------------------------------------------------------- CDP
const CDP_PORT = 9300 + Math.floor(Math.random() * 600);
const browser = await chromium.launch({ args: [`--remote-debugging-port=${CDP_PORT}`] });
const cdp = await connectCdp(CDP_PORT);
const targets = new Map(); // sessionId -> { kind, url, net: [] , wallOffset }
await cdp.send("Target.setDiscoverTargets", { discover: true });
await cdp.send("Target.setAutoAttach", {
  autoAttach: true,
  waitForDebuggerOnStart: true,
  flatten: true,
  filter: [{ type: "shared_worker", exclude: false }, { type: "page", exclude: false }, { exclude: true }],
});

// Network events -> epoch ms. CDP Network timestamps are monotonic seconds;
// requestWillBeSent carries wallTime too, which gives the offset.
function onNet(t, method, p) {
  if (method === "Network.requestWillBeSent") {
    if (p.wallTime) t.offset = p.wallTime * 1000 - p.timestamp * 1000;
    t.req.set(p.requestId, p.request.url);
    t.net.push({ ev: "send", id: p.requestId, t: p.timestamp, method: p.request.method, url: p.request.url, bytes: p.request.postData?.length, body: values.bodies ? p.request.postData?.slice(0, 600) : undefined });
  } else if (method === "Network.responseReceived") {
    t.net.push({ ev: "resp", id: p.requestId, t: p.timestamp, status: p.response.status, url: p.response.url });
    if (values.bodies && /\/stream$/.test(p.response.url)) t.streamIds.add(p.requestId);
  } else if (method === "Network.dataReceived") {
    t.net.push({ ev: "data", id: p.requestId, t: p.timestamp, bytes: p.dataLength, url: t.req.get(p.requestId), body: p.data ? Buffer.from(p.data, "base64").toString("latin1").replace(/[^\x20-\x7e]/g, ".").slice(0, 600) : undefined });
  } else if (method === "Network.loadingFinished") {
    t.net.push({ ev: "done", id: p.requestId, t: p.timestamp, url: t.req.get(p.requestId) });
  } else if (method === "Network.webSocketFrameSent" || method === "Network.webSocketFrameReceived") {
    t.net.push({ ev: method.endsWith("Sent") ? "ws-out" : "ws-in", id: p.requestId, t: p.timestamp, bytes: p.response.payloadData.length });
  }
}
cdp.onEvent = async ({ method, params, sessionId }) => {
  if (method === "Target.attachedToTarget") {
    const { sessionId: sid, targetInfo, waitingForDebugger } = params;
    const t = { kind: targetInfo.type, url: targetInfo.url, targetId: targetInfo.targetId, browserContextId: targetInfo.browserContextId, net: [], req: new Map(), streamIds: new Set(), offset: undefined };
    targets.set(sid, t);
    try {
      await cdp.send("Network.enable", {}, sid);
    } catch {}
    if (waitingForDebugger) await cdp.send("Runtime.runIfWaitingForDebugger", {}, sid).catch(() => {});
  } else if (method?.startsWith("Network.") && targets.has(sessionId)) {
    onNet(targets.get(sessionId), method, params);
    if (values.bodies && method === "Network.responseReceived" && /\/stream$/.test(params.response.url))
      cdp.send("Network.streamResourceContent", { requestId: params.requestId }, sessionId).catch((e) => log("stream content", e.message));
  }
};
/** Network events of every target since `since` (epoch ms), with epoch times. */
function netSince(since, contextId) {
  const out = [];
  for (const [, t] of targets) {
    if (contextId && t.browserContextId !== contextId) continue;
    if (t.offset === undefined) continue;
    for (const e of t.net) {
      const ts = e.t * 1000 + t.offset;
      if (ts >= since - 2) out.push({ ...e, t: Math.round((ts - since) * 10) / 10, target: t.kind, path: shortUrl(e.url) });
    }
  }
  return out.sort((a, b) => a.t - b.t);
}
const shortUrl = (u) => (u ? u.replace(url, "").slice(0, 90) : undefined);

// ----------------------------------------------------------------- pages
async function signedInPage(user) {
  const context = await browser.newContext();
  const page = await context.newPage();
  const u = ctx.users[user];
  const res = await context.request.post(`${url}/api/auth/sign-in/email`, { data: { email: u.email, password: u.password }, headers: { origin: url } });
  if (!res.ok()) throw new Error(`sign-in ${res.status()}`);
  await page.addInitScript(({ id, fix }) => {
    localStorage.setItem("big-label-organization", id);
    if (fix) localStorage.setItem("bl-fix", fix); else localStorage.removeItem("bl-fix");
  }, { id: ctx.labelId, fix: values.fix });
  // DOM-side probe: time of the first rAF-polled or mutation-observed match.
  await page.addInitScript(() => {
    globalThis.__probe = {
      now: () => performance.timeOrigin + performance.now(),
      watch(fnSrc, arg) {
        const fn = new Function("arg", fnSrc);
        return new Promise((resolve) => {
          const muts = [];
          let done = false;
          const check = (via) => {
            if (done) return;
            let ok = false;
            try { ok = fn(arg); } catch {}
            if (ok) {
              done = true;
              obs.disconnect();
              resolve({ t: performance.timeOrigin + performance.now(), via, muts });
            }
          };
          const obs = new MutationObserver((list) => {
            muts.push({ t: performance.timeOrigin + performance.now(), n: list.length });
            check("mutation");
          });
          obs.observe(document.body, { subtree: true, childList: true, characterData: true, attributes: true });
          const raf = () => { check("raf"); if (!done) requestAnimationFrame(raf); };
          requestAnimationFrame(raf);
          check("sync");
        });
      },
    };
    // Long tasks on the page.
    globalThis.__longtasks = [];
    try {
      new PerformanceObserver((l) => { for (const e of l.getEntries()) __longtasks.push({ t: performance.timeOrigin + e.startTime, d: e.duration }); }).observe({ type: "longtask", buffered: true });
    } catch {}
  });
  return { context, page, contextId: undefined };
}
const ctxId = async (page) => {
  const s = await page.context().newCDPSession(page);
  const { targetInfo } = await s.send("Target.getTargetInfo");
  await s.detach();
  return targetInfo.browserContextId;
};

const ROWS_ARE = `
  const rows = [...document.querySelectorAll('tr')].filter((tr) => tr.querySelector('td a[href^="#/releases/"]'));
  const nums = rows.map((tr) => tr.querySelectorAll('td')[2]?.textContent.trim());
  return nums.length === arg.length && nums.every((n, i) => n === arg[i]);`;
const STATUS_IS = `return [...document.querySelectorAll('dt')].find((dt) => dt.textContent === 'Status')?.nextElementSibling?.textContent.trim() === arg`;
const SAVE = `[...document.querySelectorAll('dialog[open] button')].find((b) => b.textContent.trim() === 'Save').click();`;
const waitFor = (page, src, arg) => page.waitForFunction(({ src, arg }) => new Function("arg", src)(arg), { src, arg }, { polling: "raf", timeout: 30000 });

/** Arms the watcher, runs `act` in the page, returns { t0, done, muts } in epoch ms. */
async function timed(page, act, src, arg, other) {
  await page.evaluate(({ src, arg }) => { globalThis.__w = __probe.watch(src, arg); }, { src, arg });
  let otherP;
  if (other) await other.page.evaluate(({ src, arg }) => { globalThis.__w = __probe.watch(src, arg); }, { src: other.src, arg: other.arg });
  const t0 = await page.evaluate((act) => { const t = performance.timeOrigin + performance.now(); new Function(act)(); return t; }, act);
  const target = other ? other.page : page;
  const r = await target.evaluate(() => globalThis.__w);
  return { t0, done: r.t, ms: Math.round((r.t - t0) * 10) / 10, via: r.via, muts: r.muts.map((m) => Math.round((m.t - t0) * 10) / 10) };
}

async function editRelease(page, { title, status }) {
  await page.getByRole("button", { name: "Edit", exact: true }).click();
  const dialog = page.locator("dialog[open]");
  await dialog.getByRole("heading", { name: "Edit release" }).waitFor();
  const field = async (name) => {
    const id = await dialog.evaluate((d, name) => [...d.querySelectorAll("label")].find((l) => l.textContent.startsWith(name)).htmlFor, name);
    return page.locator(`[id="${id}"]`);
  };
  if (title !== undefined) await (await field("Title")).fill(title);
  if (status !== undefined) {
    await (await field("Status")).click();
    await page.getByRole("option", { name: sentence(status), exact: true }).click();
  }
}

async function profileStart() {
  if (!values.profile) return;
  for (const [sid, t] of targets) {
    if (t.kind !== "page" && t.kind !== "shared_worker") continue;
    try {
      await cdp.send("Profiler.enable", {}, sid);
      await cdp.send("Profiler.setSamplingInterval", { interval: 200 }, sid);
      await cdp.send("Profiler.start", {}, sid);
      t.profiling = true;
    } catch {}
  }
}
async function profileStop(name) {
  if (!values.profile) return;
  for (const [sid, t] of targets) {
    if (!t.profiling) continue;
    t.profiling = false;
    try {
      const { profile } = await cdp.send("Profiler.stop", {}, sid);
      const file = join(outDir, `${name}.${t.kind}.${t.targetId.slice(0, 6)}.cpuprofile`);
      writeFileSync(file, JSON.stringify(profile));
      log("  profile", file);
    } catch (e) { log("  profile failed", e.message); }
  }
}

// ----------------------------------------------------------------- run
const a = await signedInPage("a");
const b = await signedInPage("b");
for (const s of [a, b]) {
  await s.page.goto(`${url}/#/releases`);
  await waitFor(s.page, ROWS_ARE, pageOf(1));
  s.contextId = await ctxId(s.page);
}
await a.page.waitForTimeout(Number(values.settle));
const lf = await a.page.evaluate(() => [...document.querySelectorAll("script")].length);
const results = {};

if (only.has("next")) {
  log("# bl_next_page");
  const rows = [];
  await profileStart();
  let current = 1;
  for (let i = 0; i <= N; i++) {
    if (current + 1 > Math.ceil(all.length / PAGE_SIZE)) break;
    const since = Date.now();
    const r = await timed(a.page, `document.querySelector('button[aria-label="Go to next page"]').click()`, ROWS_ARE, pageOf(current + 1));
    current++;
    await a.page.waitForTimeout(300);
    r.net = netSince(r.t0, a.contextId);
    r.longtasks = (await a.page.evaluate(() => __longtasks.splice(0))).filter((x) => x.t >= r.t0 - 1).map((x) => ({ t: Math.round(x.t - r.t0), d: Math.round(x.d) }));
    log(`  page ${current}: ${r.ms} ms (${r.via}), net ${r.net.length}, longtasks ${JSON.stringify(r.longtasks)}`);
    if (i > 0) rows.push(r);
  }
  await profileStop("next");
  results.next = { median: median(rows.map((r) => r.ms)), samples: rows };
  writeFileSync(join(outDir, "next.json"), JSON.stringify(results.next, null, 1));
  if (Number(values.gap)) {
    // Leave the Releases list first, so the paged queries have no listener.
    await a.page.evaluate(() => (window.location.hash = "#/"));
    await a.page.waitForTimeout(Number(values.gap));
  }
}

if (only.has("update")) {
  log("# bl_update_status");
  await a.page.evaluate((id) => (window.location.hash = `#/releases/${id}`), ctx.statusReleaseId);
  // Earlier probe runs leave the status anywhere in the cycle: read it.
  await a.page.waitForFunction(() => [...document.querySelectorAll('dt')].some((dt) => dt.textContent === 'Status'));
  const shown = await a.page.evaluate(() => [...document.querySelectorAll('dt')].find((dt) => dt.textContent === 'Status').nextElementSibling.textContent.trim());
  let status = releaseStatuses.find((s) => sentence(s) === shown);
  await a.page.waitForTimeout(Number(values.settle));
  const rows = [];
  await profileStart();
  for (let i = 0; i <= N; i++) {
    status = releaseStatuses[(releaseStatuses.indexOf(status) + 1) % releaseStatuses.length];
    await editRelease(a.page, { status });
    const r = await timed(a.page, SAVE, STATUS_IS, sentence(status));
    await a.page.waitForSelector("dialog[open]", { state: "detached" }).catch(() => {});
    await a.page.waitForTimeout(400);
    r.net = netSince(r.t0, a.contextId);
    r.longtasks = (await a.page.evaluate(() => __longtasks.splice(0))).filter((x) => x.t >= r.t0 - 1).map((x) => ({ t: Math.round(x.t - r.t0), d: Math.round(x.d) }));
    log(`  ${status}: ${r.ms} ms (${r.via}) muts@${r.muts.slice(0, 6).join(",")}`);
    for (const e of r.net.filter((e) => e.t < r.ms + 50)) log(`     ${e.t}\t${e.target}\t${e.ev}\t${e.method ?? ""}\t${e.status ?? ""}\t${e.bytes ?? ""}\t${e.path.slice(-12)}\t${e.body ?? ""}`);
    if (i > 0) rows.push(r);
  }
  await profileStop("update");
  results.update = { median: median(rows.map((r) => r.ms)), samples: rows };
  writeFileSync(join(outDir, "update.json"), JSON.stringify(results.update, null, 1));
}

if (only.has("cross")) {
  log("# bl_cross_client");
  await a.page.evaluate((id) => (window.location.hash = `#/releases/${id}`), ctx.renameReleaseId);
  await a.page.getByRole("button", { name: "Edit", exact: true }).waitFor();
  await b.page.evaluate(() => (window.location.hash = "#/releases"));
  await b.page.waitForTimeout(Number(values.settle));
  const titleIs = `return document.querySelector('td a[href="#/releases/${ctx.renameReleaseId}"]')?.textContent === arg`;
  const aTitle = `return document.querySelector('h1')?.textContent?.includes(arg)`;
  const rows = [];
  await profileStart();
  for (let i = 0; i <= N; i++) {
    const title = `Renamed release ${values.variant} ${Date.now() % 100000} ${i}`;
    await editRelease(a.page, { title });
    await a.page.evaluate(({ src, arg }) => { globalThis.__wa = __probe.watch(src, arg); }, { src: aTitle, arg: title });
    const r = await timed(a.page, SAVE, titleIs, title, { page: b.page, src: titleIs, arg: title });
    const aSeen = await a.page.evaluate(() => globalThis.__wa);
    r.a_ms = Math.round((aSeen.t - r.t0) * 10) / 10;
    await a.page.waitForSelector("dialog[open]", { state: "detached" }).catch(() => {});
    await a.page.waitForTimeout(400);
    r.net_a = netSince(r.t0, a.contextId);
    r.net_b = netSince(r.t0, b.contextId);
    log(`  ${r.ms} ms on b (a's own page: ${r.a_ms} ms)`);
    for (const e of [...r.net_a.map((e) => ({ ...e, who: "a" })), ...r.net_b.map((e) => ({ ...e, who: "b" }))].sort((x, y) => x.t - y.t).filter((e) => e.t < r.ms + 30))
      log(`     ${e.t}\t${e.who}\t${e.target}\t${e.ev}\t${e.method ?? ""}\t${e.status ?? ""}\t${e.bytes ?? ""}\t${e.path.slice(-12)}\t${e.body ?? ""}`);
    if (i > 0) rows.push(r);
  }
  await profileStop("cross");
  results.cross = { median: median(rows.map((r) => r.ms)), a_median: median(rows.map((r) => r.a_ms)), samples: rows };
  writeFileSync(join(outDir, "cross.json"), JSON.stringify(results.cross, null, 1));
}

log(JSON.stringify(Object.fromEntries(Object.entries(results).map(([k, v]) => [k, { median: v.median, a_median: v.a_median }]))));
writeFileSync(join(outDir, "summary.json"), JSON.stringify(Object.fromEntries(Object.entries(results).map(([k, v]) => [k, { median: v.median, a_median: v.a_median, n: v.samples.length }])), null, 1));
cdp.close();
await browser.close();

// ----------------------------------------------------------------- raw CDP
async function connectCdp(port) {
  let version;
  for (let i = 0; i < 50 && !version; i++) {
    try { version = await (await fetch(`http://127.0.0.1:${port}/json/version`)).json(); } catch { await new Promise((r) => setTimeout(r, 100)); }
  }
  const socket = new WebSocket(version.webSocketDebuggerUrl);
  await new Promise((r, j) => { socket.onopen = r; socket.onerror = j; });
  let nextId = 1;
  const pending = new Map();
  const api = {
    onEvent: () => {},
    send: (method, params = {}, sessionId) => new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      socket.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }));
    }),
    close: () => socket.close(),
  };
  socket.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.id) {
      const p = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) p?.reject(new Error(msg.error.message)); else p?.resolve(msg.result);
      return;
    }
    api.onEvent(msg);
  };
  return api;
}
