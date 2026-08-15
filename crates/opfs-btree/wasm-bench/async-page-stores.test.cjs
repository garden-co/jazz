#!/usr/bin/env node
const fs = require("fs"), http = require("http"), path = require("path");
function playwright() { try { return require("playwright"); } catch { return require(path.join(__dirname, "..", "node_modules", "playwright")); } }

async function main() {
  const server = http.createServer((req, res) => {
    if (req.url === "/async-page-stores.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); fs.createReadStream(path.join(__dirname, "async-page-stores.js")).pipe(res); }
    else if (req.url === "/store-worker.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); fs.createReadStream(path.join(__dirname, "async-page-stores-worker.js")).pipe(res); }
    else if (req.url.startsWith("/pkg/")) { const file = path.join(__dirname, "pkg", req.url.slice(5)); res.writeHead(200, { "Content-Type": file.endsWith(".wasm") ? "application/wasm" : "text/javascript" }); fs.createReadStream(file).pipe(res); }
    else { res.writeHead(200, { "Content-Type": "text/html" }); res.end("<!doctype html>"); }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { chromium } = playwright(), browser = await chromium.launch({ headless: true }), context = await browser.newContext();
  try {
    const page = await context.newPage(); await page.goto(`http://127.0.0.1:${server.address().port}`);
    const result = await page.evaluate(() => new Promise((resolve, reject) => { const worker = new Worker("/store-worker.js", { type: "module" }); worker.onmessage = (e) => e.data.error ? reject(new Error(e.data.error)) : resolve(e.data.out); worker.onerror = () => reject(new Error("store worker failed")); worker.postMessage({}); }));
    const expected = { idb: [{ pageSize: 4096, logicalLen: 12288 }, 2, 2], opfs: [{ pageSize: 4096, logicalLen: 12288 }, 2, 2] };
    if (JSON.stringify(result.parity) !== JSON.stringify(expected)) throw new Error(JSON.stringify(result));
    console.log(JSON.stringify(result));
    console.log("async page stores: page commit, read-after-commit, clean reopen passed");
  } finally { await context.close(); await browser.close(); await new Promise((resolve) => server.close(resolve)); }
}
main().catch((e) => { console.error(e.stack || e); process.exit(1); });
