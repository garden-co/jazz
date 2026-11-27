// Usage: node measure-load.mjs <url> [settleMs]
// Records every JS / WASM response a fresh page loads (page + workers), with
// gzip and brotli sizes, at "load" and after the background settle.
import { chromium } from "../../../../harness/node_modules/playwright/index.mjs";
import { gzipSync, brotliCompressSync, constants } from "node:zlib";
const [url, settle = "6000"] = process.argv.slice(2);
const browser = await chromium.launch();
const context = await browser.newContext();
const page = await context.newPage();
const seen = new Map();
let phase = "initial";
const onResp = async (r) => {
  const u = r.url();
  if (!/\.(m?js|wasm)(\?|$)/.test(u) && r.request().resourceType() !== "script") return;
  try {
    const b = await r.body();
    if (!seen.has(u)) seen.set(u, { phase, raw: b.length, gzip: gzipSync(b, { level: 9 }).length,
      br: brotliCompressSync(b, { params: { [constants.BROTLI_PARAM_QUALITY]: 11 } }).length });
  } catch {}
};
context.on("response", onResp);
const errors = [];
page.on("pageerror", (e) => errors.push(String(e)));
page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
await page.goto(url, { waitUntil: "load" });
await page.waitForTimeout(1500);
phase = "deferred";
await page.waitForTimeout(+settle);
// Shared workers' own fetches are not page responses; list them via the browser's resource timing too.
const workerish = await page.evaluate(() => performance.getEntriesByType("resource").map((e) => e.name).filter((n) => /\.(js|wasm)/.test(n)));
for (const n of workerish) if (!seen.has(n)) {
  const res = await fetch(n); const b = Buffer.from(await res.arrayBuffer());
  seen.set(n, { phase: "deferred(timing)", raw: b.length, gzip: gzipSync(b, { level: 9 }).length, br: brotliCompressSync(b).length });
}
const rows = [...seen].map(([u, v]) => ({ file: u.replace(/^https?:\/\/[^/]+/, ""), ...v }));
// Smoke check: the app rendered and Jazz answered (the show list or its empty state).
const idb = await page.evaluate(async () => (await indexedDB.databases()).map((d) => d.name).join(","));
const text = `[idb: ${idb || "none"}] ` + (await page.locator("body").innerText()).slice(0, 200).replace(/\s+/g, " ");
if (process.env.SCREENSHOT) await page.screenshot({ path: process.env.SCREENSHOT });
console.log(JSON.stringify({ rows, errors, text }, null, 1));
await browser.close();
