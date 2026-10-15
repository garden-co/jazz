// Servers, browser and small Playwright helpers shared by the scenarios.
import { spawn } from "node:child_process";
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { chromium } from "@playwright/test";
import { appConfig, generated, here } from "./apps.mjs";

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const now = () => performance.now();
export const random = () => Math.random().toString(36).slice(2, 8);

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
        if (ready.test(output.replace(/\x1b\[[0-9;]*m/g, ""))) {
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
      // World Tour draws its globe with WebGL; headless Chromium needs a
      // software GL for it.
      "--use-gl=angle",
      "--use-angle=swiftshader",
      "--enable-unsafe-swiftshader",
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
