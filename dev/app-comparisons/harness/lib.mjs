import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";

/** Samples retried because the page navigated mid-sample (reported in results). */
export const retries = { count: 0 };


/**
 * Settle time before reloads and before the echo and cross-client phases: long
 * enough for background work (e.g. Jazz's local-first upgrade) to finish, so
 * those phases measure the steady state. The same for every stack.
 */
export const SETTLE_MS = 5_000;

export const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
export const p95 = (xs) => [...xs].sort((a, b) => a - b)[Math.min(xs.length - 1, Math.floor(xs.length * 0.95))];
export const summary = (samples) => ({
  median: round(median(samples)),
  p95: round(p95(samples)),
  min: round(Math.min(...samples)),
  n: samples.length,
});
const round = (x) => Math.round(x * 10) / 10;

/**
 * Gzipped size of what a built app can ship, from its build output: every
 * .js file, and separately every .wasm file and every worker script (names
 * containing "worker"). The page-level "JS loaded" metric doesn't see what a
 * SharedWorker downloads itself (its script and its WASM), so these are
 * reported next to it.
 */
export function bundleGzip(dir) {
  let raw = 0;
  let gz = 0;
  let wasm = 0;
  let worker = 0;
  const walk = (d) => {
    for (const name of readdirSync(d)) {
      const p = join(d, name);
      if (statSync(p).isDirectory()) walk(p);
      else if (/\.(m?js)$/.test(name)) {
        const size = gzipSync(readFileSync(p)).length;
        raw += statSync(p).size;
        gz += size;
        if (/worker/i.test(name)) worker += size;
      } else if (/\.wasm$/.test(name)) wasm += gzipSync(readFileSync(p)).length;
    }
  };
  walk(dir);
  const kb = (n) => Math.round(n / 1024);
  return { raw_kb: kb(raw), gzip_kb: kb(gz), wasm_gzip_kb: kb(wasm), worker_js_gzip_kb: kb(worker) };
}

/** Gzipped JS the page actually loaded (scripts and workers), from a Playwright page. */
export function trackLoadedJs(page) {
  const seen = new Map();
  page.on("response", async (response) => {
    const type = response.request().resourceType();
    const url = response.url();
    if (!(type === "script" || /\.m?js(\?|$)/.test(url))) return;
    try {
      const body = await response.body();
      seen.set(url, gzipSync(body).length);
    } catch {}
  });
  page.context().on("serviceworker", () => {});
  return () => Math.round([...seen.values()].reduce((a, b) => a + b, 0) / 1024);
}

/** Median HTTP round trip to `url`, over `n` requests. */
export async function rtt(url, n = 15) {
  const samples = [];
  for (let i = 0; i < n; i++) {
    const t = performance.now();
    try {
      await fetch(url, { cache: "no-store" }).then((r) => r.arrayBuffer());
    } catch {}
    samples.push(performance.now() - t);
  }
  return round(median(samples.slice(2)));
}

/**
 * Waits are set up in the page by a short evaluate, which starts a
 * MutationObserver (plus an animation-frame check) and returns at once; the
 * observer writes its timestamp into `window.__bench[id]`, and Node polls for
 * it with short evaluates. No evaluate stays open while waiting, so nothing
 * depends on CDP keeping an awaited promise alive (it can report "Promise was
 * collected", which Playwright rewrites as "Execution context was destroyed").
 * Timestamps are taken in the page, so the polling interval doesn't affect them.
 */
const ARM = ({ id, done, act, arg, timeoutMs }) => {
  const results = (window.__bench ??= {});
  const isDone = new Function("arg", done);
  const t0 = performance.now();
  let finished = false;
  const settle = (value) => {
    if (finished) return;
    finished = true;
    observer.disconnect();
    results[id] = value;
  };
  const check = () => {
    if (!finished && isDone(arg)) settle({ at: performance.now(), t0 });
  };
  const observer = new MutationObserver(check);
  observer.observe(document.body, { subtree: true, childList: true, attributes: true, characterData: true });
  (window.__benchObservers ??= new Set()).add(observer);
  const tick = () => {
    if (finished) return;
    check();
    requestAnimationFrame(tick);
  };
  setTimeout(() => settle({ error: `timed out: ${done}` }), timeoutMs);
  if (act) new Function("arg", act)(arg);
  tick();
  return t0;
};

let nextId = 0;
async function settled(page, id, timeoutMs) {
  const deadline = Date.now() + timeoutMs + 5_000;
  for (;;) {
    const value = await page.evaluate((id) => {
      const v = window.__bench?.[id];
      if (v) delete window.__bench[id];
      return v ?? null;
    }, id);
    if (value) {
      if (value.error) throw new Error(value.error);
      return value;
    }
    if (Date.now() > deadline) throw new Error(`no result for wait ${id}`);
    await new Promise((r) => setTimeout(r, 15));
  }
}

/** In the page: run `act`, then the ms until `done` (a predicate body) holds. */
export async function timeInPage(page, { act, done, arg, timeoutMs = 30_000 }) {
  const id = `w${nextId++}`;
  await page.evaluate(ARM, { id, done, act, arg, timeoutMs });
  const { at, t0 } = await settled(page, id, timeoutMs);
  return at - t0;
}

/**
 * Arms a wait in the page and resolves once it is armed, with a promise of the
 * wall-clock time (epoch ms) at which `done` became true. Await the arming
 * before acting on another page.
 */
export async function armWhenInPage(page, { done, arg, timeoutMs = 30_000 }) {
  const id = `w${nextId++}`;
  await page.evaluate(ARM, { id, done, act: null, arg, timeoutMs });
  const origin = await page.evaluate(() => performance.timeOrigin);
  const result = settled(page, id, timeoutMs).then(({ at }) => origin + at);
  result.catch(() => {}); // handled by whoever awaits it
  return { result };
}

export const nowInPage = (page) => page.evaluate(() => performance.timeOrigin + performance.now());

/** Wall-clock ms until `predicate` holds in the page (polls on animation frames). */
export async function waitInPage(page, done, arg, timeoutMs = 60_000) {
  await page.waitForFunction(new Function("arg", done), arg, { timeout: timeoutMs, polling: "raf" });
}
