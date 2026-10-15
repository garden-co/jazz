// Servers, browser and small Playwright helpers shared by the scenarios.
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { stripVTControlCharacters } from "node:util";
import { chromium } from "@playwright/test";
import { appConfig, generated, here } from "./apps.mjs";

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const now = () => performance.now();
export const random = () => Math.random().toString(36).slice(2, 8);

// ---------------------------------------------------------------------------
// Opt-in tracing (BENCH_TRACE=1): per-iteration phase times, CPU time per
// process group, and what the pages logged and sent while a case ran.

export const tracing = !!process.env.BENCH_TRACE;
const traceEvents = [];

/** Records a labelled point in time (only while tracing). */
export function mark(label) {
  if (tracing) traceEvents.push({ t: now(), what: label });
}

/** The trace events recorded since `from` (a `now()` value). */
export function eventsSince(from) {
  return traceEvents.filter((e) => e.t >= from);
}

const ticksPerSecond = 100;

/** CPU seconds used so far by each process group under this process. */
export function cpuByGroup() {
  const names = new Map();
  try {
    for (const f of readdirSync(pidDir)) {
      names.set(Number(readFileSync(join(pidDir, f), "utf8")), f.replace(/\.pgid$/, ""));
    }
  } catch {}
  const procs = new Map();
  for (const d of readdirSync("/proc")) {
    if (!/^\d+$/.test(d)) continue;
    try {
      const stat = readFileSync(`/proc/${d}/stat`, "utf8");
      const close = stat.lastIndexOf(")");
      const comm = stat.slice(stat.indexOf("(") + 1, close);
      const f = stat.slice(close + 2).split(" ");
      // Fields after the name: state ppid pgrp ... utime(12) stime(13).
      procs.set(Number(d), {
        comm,
        ppid: Number(f[1]),
        pgrp: Number(f[2]),
        cpu: (Number(f[11]) + Number(f[12])) / ticksPerSecond,
      });
    } catch {}
  }
  const mine = (pid) => {
    for (let p = pid, n = 0; p > 1 && n < 64; p = procs.get(p)?.ppid ?? 0, n++) {
      if (p === process.pid) return true;
    }
    return false;
  };
  const out = {};
  for (const [pid, p] of procs) {
    if (pid !== process.pid && !mine(pid)) continue;
    const name =
      pid === process.pid
        ? "runner"
        : (names.get(p.pgrp) ?? (/chrom|headless/i.test(p.comm) ? "browser" : p.comm));
    out[name] = (out[name] ?? 0) + p.cpu;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Servers: each runs in its own process group with a pidfile, so a crashed
// run's leftovers can be killed by group on the next start.

const pidDir = join(generated, "pids");

export function killStale() {
  mkdirSync(pidDir, { recursive: true });
  for (const f of readdirSync(pidDir)) {
    const pgid = Number(readFileSync(join(pidDir, f), "utf8"));
    try {
      process.kill(-pgid, "SIGKILL");
      console.log(`killed stale process group ${pgid} (${f})`);
    } catch {}
    rmSync(join(pidDir, f), { force: true });
  }
}

/** Starts a detached process group; resolves once `ready` matches its output. */
async function startProcess(name, { command, args, cwd, env = {}, ready, timeout = 180_000 }) {
  mkdirSync(pidDir, { recursive: true });
  const child = spawn(command, args, {
    cwd,
    env: { ...process.env, ...env },
    stdio: ["ignore", "pipe", "pipe"],
    detached: true,
  });
  const pidfile = join(pidDir, `${name}.pgid`);
  writeFileSync(pidfile, String(child.pid));
  let output = "";
  child.stdout.on("data", (c) => (output += c));
  child.stderr.on("data", (c) => (output += c));
  const exited = new Promise((r) => child.on("exit", r));
  const kill = (signal) => {
    try {
      process.kill(-child.pid, signal);
    } catch {}
  };
  const stop = async () => {
    kill("SIGTERM");
    const timer = setTimeout(() => kill("SIGKILL"), 10_000);
    await exited;
    clearTimeout(timer);
    kill("SIGKILL");
    rmSync(pidfile, { force: true });
  };
  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`${name} did not start in ${timeout} ms:\n${output.slice(-3000)}`)),
        timeout,
      );
      const poll = setInterval(() => {
        // CI forces colour, which splits e.g. Vite's "Local:" with escape codes.
        if (ready.test(stripVTControlCharacters(output))) {
          clearInterval(poll);
          clearTimeout(timer);
          resolve();
        } else if (child.exitCode !== null) {
          clearInterval(poll);
          clearTimeout(timer);
          reject(new Error(`${name} exited with ${child.exitCode}:\n${output.slice(-3000)}`));
        }
      }, 100);
    });
  } catch (e) {
    await stop();
    throw e;
  }
  return { name, stop, log: () => output };
}

/** The local Jazz sync server: a fresh, empty data dir each start. */
export function startSync(a) {
  return startProcess(`${a.id}.sync`, {
    command: process.execPath,
    args: [join(here, "sync.mjs"), a.dir, String(a.syncPort), JSON.stringify(a.sync)],
    cwd: a.dir,
    ready: /SYNC READY/,
    timeout: 60_000,
  });
}

/** `next start` or `vite preview` on the backend port, behind the CDN front. */
function startFramework(a) {
  const args =
    a.kind === "next"
      ? ["exec", "next", "start", "-H", "127.0.0.1", "-p", String(a.backendPort)]
      : [
          "exec",
          "vite",
          "preview",
          "--host",
          "127.0.0.1",
          "--port",
          String(a.backendPort),
          "--strictPort",
        ];
  return startProcess(`${a.id}.app`, {
    command: "pnpm",
    args,
    cwd: a.dir,
    env: a.env,
    ready: a.kind === "next" ? /Ready in/ : /Local:\s+http/,
  });
}

function startCdn(a) {
  return startProcess(`${a.id}.cdn`, {
    command: process.execPath,
    args: [
      join(here, "cdn.mjs"),
      "serve",
      String(a.port),
      String(a.backendPort),
      a.assetPrefix,
      a.staticDir,
    ],
    cwd: here,
    ready: /CDN READY/,
  });
}

/**
 * Starts one app's sync server, production server and CDN front, and loads
 * its start page once so the server has run every route module it needs
 * before anything is timed.
 */
export async function startApp(id) {
  const a = appConfig(id);
  const servers = { sync: null, app: null, cdn: null };
  const stop = async () => {
    for (const key of ["cdn", "app", "sync"]) {
      await servers[key]?.stop();
      servers[key] = null;
    }
  };
  try {
    servers.sync = await startSync(a);
    servers.app = await startFramework(a);
    servers.cdn = await startCdn(a);
    const r = await fetch(a.origin, { signal: AbortSignal.timeout(60_000) });
    if (r.status >= 500) throw new Error(`${a.origin} answered ${r.status}`);
  } catch (e) {
    await stop();
    throw e;
  }
  return {
    ...a,
    /** Replaces the sync server with a fresh, empty one (same app ID and URL). */
    async restartSync() {
      await servers.sync?.stop();
      servers.sync = await startSync(a);
    },
    log: () =>
      Object.entries(servers)
        .map(([key, s]) => `--- ${key}\n${s?.log().slice(-4000) ?? "(stopped)"}`)
        .join("\n"),
    stop,
  };
}

// ---------------------------------------------------------------------------
// Browser and devices.

export function launchBrowser() {
  return chromium.launch({
    // Local runs can point at an already-installed Chromium.
    executablePath: process.env.CHROMIUM_PATH || undefined,
    args: [
      "--disable-background-networking",
      "--disable-component-update",
      "--disable-features=AutofillServerCommunication,OptimizationHints,MediaRouter",
      // Everything the apps need is served from loopback. Any other host (a
      // font, analytics, a CDN script added later) would put the internet's
      // latency into the numbers and read as an app regression, so it fails
      // to resolve. Not page.route(): Playwright's routing turns off the HTTP
      // cache, which the reload and return-visit cases measure.
      "--host-resolver-rules=MAP * ~NOTFOUND, EXCLUDE localhost, EXCLUDE 127.0.0.1",
      // No SwiftShader GL flags: none of the apps uses WebGL (World Tour's
      // globe is canvas 2D), and with them Chromium rasterises through a
      // software GPU. World Tour redraws its globe every frame, so its open
      // pages then kept about 14 of the macro runner's 16 cores busy, and
      // a Join click took 17 s instead of 0.5 s.
    ],
  });
}

/**
 * A new device: a fresh browser context, so empty storage and an empty HTTP
 * cache (a cold profile). Page errors are collected for the caller to check.
 */
export async function device(browser, { viewport = { width: 1280, height: 800 } } = {}) {
  const context = await browser.newContext({ viewport, colorScheme: "dark" });
  const errors = [];
  const page = await context.newPage();
  page.setDefaultTimeout(60_000);
  page.on("pageerror", (e) => errors.push(e.stack || String(e)));
  if (tracing) {
    const id = random().slice(0, 4);
    const push = (what) => traceEvents.push({ t: now(), what: `[${id}] ${what}` });
    page.on("pageerror", (e) => push(`pageerror ${String(e).slice(0, 300)}`));
    page.on("console", (m) => push(`console.${m.type()} ${m.text().slice(0, 300)}`));
    page.on("requestfailed", (r) => push(`requestfailed ${r.url()} ${r.failure()?.errorText}`));
    page.on("framenavigated", (f) => f === page.mainFrame() && push(`navigated ${f.url()}`));
    page.on("load", () => push("load"));
    page.on("websocket", (ws) => {
      let sent = 0;
      let received = 0;
      push(`ws open ${ws.url()}`);
      ws.on("framesent", () => sent++ === 0 && push("ws first frame sent"));
      ws.on("framereceived", () => received++ === 0 && push("ws first frame received"));
      ws.on("close", () => push(`ws close (${sent} sent, ${received} received)`));
      ws.on("socketerror", (e) => push(`ws error ${e}`));
    });
  }
  return {
    page,
    context,
    errors,
    async close() {
      await context.close().catch(() => {});
    },
  };
}

/** Waits for the first match of `locator` to be visible (or `state`). */
export function visible(locator, { timeout = 60_000, state = "visible" } = {}) {
  return locator.first().waitFor({ state, timeout });
}

/** Polls `fn` until it returns truthy. */
export async function poll(fn, { timeout = 60_000, every = 25, what = "condition" } = {}) {
  const end = now() + timeout;
  for (;;) {
    let ok = false;
    try {
      ok = await fn();
    } catch {}
    if (ok) return;
    if (now() > end) throw new Error(`timed out after ${timeout} ms waiting for ${what}`);
    await sleep(every);
  }
}

/**
 * Clicks `button` until `target` shows up: a server-rendered button can be
 * visible before React hydrates it, and a click then does nothing.
 */
export async function clickUntil(button, target, { tries = 20, every = 1000 } = {}) {
  for (let i = 0; i < tries; i++) {
    await button
      .first()
      .click()
      .catch(() => {});
    try {
      await target.first().waitFor({ timeout: every });
      return;
    } catch {}
  }
  throw new Error("clickUntil: target never appeared");
}

/**
 * Better Auth rate-limits sign-up and sign-in in production builds: per IP
 * and path, at most 3 requests that each follow the previous one within 10 s.
 * Its counter only resets after a 10 s gap. Fixture and timed requests share
 * one loopback IP, so each app sends them through a gate like this one,
 * called untimed just before the request; it waits out the gap when needed.
 */
export function rateLimitGate({ max = 3, windowMs = 10_000, margin = 2_000 } = {}) {
  let count = 0;
  let last = -Infinity;
  return async () => {
    if (now() - last >= windowMs + margin) count = 0;
    if (count >= max) {
      await sleep(last + windowMs + margin - now());
      count = 0;
    }
    count++;
    last = now();
  };
}
