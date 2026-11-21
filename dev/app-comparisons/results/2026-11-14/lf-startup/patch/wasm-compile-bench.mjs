// Chrome compile time for WASM binaries (from bytes, no code cache), alone and
// two at once (a page + a dedicated worker, like Jazz's page + SharedWorker).
import { chromium } from "../../../../harness/node_modules/playwright/index.mjs";
import { readFileSync } from "node:fs";
const files = process.argv.slice(2);
const browser = await chromium.launch();
for (const f of files) {
  const b64 = readFileSync(f).toString("base64");
  const alone = [], pair = [];
  for (let i = 0; i < 5; i++) {
    const page = await browser.newPage();
    await page.goto("about:blank");
    await page.evaluate((b) => { window.bytes = Uint8Array.from(atob(b), (c) => c.charCodeAt(0)); }, b64);
    alone.push(await page.evaluate(async () => { const t = performance.now(); await WebAssembly.compile(window.bytes); return performance.now() - t; }));
    pair.push(await page.evaluate(async () => {
      const src = `onmessage = async (e) => { const t = performance.now(); await WebAssembly.compile(e.data); postMessage(performance.now() - t); }`;
      const w = new Worker(URL.createObjectURL(new Blob([src])));
      const copy = window.bytes.slice();
      const t = performance.now();
      const inWorker = new Promise((r) => (w.onmessage = (e) => r(e.data)));
      w.postMessage(copy, [copy.buffer]);
      await WebAssembly.compile(window.bytes);
      await inWorker;
      return performance.now() - t;
    }));
    await page.close();
  }
  const med = (a) => a.sort((x, y) => x - y)[2].toFixed(0);
  console.log(`${f}: alone ${med(alone)} ms, page+worker together ${med(pair)} ms`);
}
await browser.close();
