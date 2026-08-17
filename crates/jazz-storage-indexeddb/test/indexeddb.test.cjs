#!/usr/bin/env node
const fs = require("fs");
const http = require("http");
const path = require("path");

function playwright() {
  try {
    return require("playwright");
  } catch {
    return require(path.join(__dirname, "..", "node_modules", "playwright"));
  }
}

async function main() {
  const pageStore = path.join(
    __dirname,
    "..",
    "..",
    "opfs-btree",
    "wasm-bench",
    "async-page-stores.js",
  );
  const worker = `
    import init, { verify_indexeddb_groove_visibility, verify_indexeddb_ordered_storage } from "/pkg/jazz_storage_indexeddb.js";
    import { IndexedDbPageStore } from "/async-page-stores.js";
    self.onmessage = async () => {
      const name = "jazz-ordered-idb-" + Date.now() + "-" + Math.random();
      let store;
      try {
        await init();
        store = await IndexedDbPageStore.open(name);
        const receipt = await verify_indexeddb_ordered_storage(store);
        store.close();
        await IndexedDbPageStore.destroy(name);
        const grooveName = name + "-groove";
        store = await IndexedDbPageStore.open(grooveName);
        const grooveReceipt = await verify_indexeddb_groove_visibility(store);
        store.close();
        await IndexedDbPageStore.destroy(grooveName);
        self.postMessage({ receipt, grooveReceipt });
      } catch (error) {
        if (store) store.close();
        self.postMessage({ error: error.stack || error.message || String(error) });
      }
    };
  `;
  const server = http.createServer((request, response) => {
    if (request.url === "/async-page-stores.js") {
      response.writeHead(200, { "Content-Type": "text/javascript" });
      fs.createReadStream(pageStore).pipe(response);
    } else if (request.url === "/worker.js") {
      response.writeHead(200, { "Content-Type": "text/javascript" });
      response.end(worker);
    } else if (request.url.startsWith("/pkg/")) {
      const file = path.join(__dirname, "pkg", request.url.slice(5));
      response.writeHead(200, {
        "Content-Type": file.endsWith(".wasm") ? "application/wasm" : "text/javascript",
      });
      fs.createReadStream(file).pipe(response);
    } else {
      response.writeHead(200, { "Content-Type": "text/html" });
      response.end("<!doctype html>");
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const browser = await playwright().chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const result = await page.evaluate(
      () =>
        new Promise((resolve, reject) => {
          const worker = new Worker("/worker.js", { type: "module" });
          worker.onmessage = ({ data }) =>
            data.error ? reject(new Error(data.error)) : resolve(data);
          worker.onerror = (error) => reject(new Error(error.message || "worker failed"));
          worker.postMessage({});
        }),
    );
    if (result.receipt !== "ordered IndexedDB commit/scan/delete/reopen passed") {
      throw new Error(JSON.stringify(result));
    }
    if (result.grooveReceipt !== "Groove local visibility preceded IndexedDB durability") {
      throw new Error(JSON.stringify(result));
    }
    console.log(result.receipt);
    console.log(result.grooveReceipt);
  } finally {
    await browser.close();
    await new Promise((resolve) => server.close(resolve));
  }
}

main().catch((error) => {
  console.error(error.stack || error);
  process.exit(1);
});
