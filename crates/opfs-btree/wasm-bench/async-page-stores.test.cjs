#!/usr/bin/env node
const fs = require("fs"), http = require("http"), path = require("path");
function playwright() { try { return require("playwright"); } catch { return require(path.join(__dirname, "..", "node_modules", "playwright")); } }

async function main() {
  const source = path.join(__dirname, "async-page-stores.js");
  const worker = `import init, { WasmAsyncBTree } from "/pkg/opfs_btree.js"; import { IndexedDbPageStore, OpfsPageStore } from "/async-page-stores.js";
self.onmessage = async () => { try {
  await init();
  const pageSize = 4096, meta = { pageSize, logicalLen: pageSize * 3 }, pages = [{ pageId: 0, bytes: new Uint8Array(pageSize).fill(1) }, { pageId: 2, bytes: new Uint8Array(pageSize).fill(2) }];
  const idbName = "page-store-idb-" + Date.now() + "-" + Math.random(), opfsName = "page-store-opfs-" + Date.now() + "-" + Math.random();
  const idb = await IndexedDbPageStore.open(idbName), opfs = await OpfsPageStore.open(opfsName, pageSize);
  await Promise.all([idb.commit({ metadata: meta, writes: pages, deletedPageIds: [] }), opfs.commit({ metadata: meta, writes: pages, deletedPageIds: [] })]);
  const [idbRead, opfsRead] = await Promise.all([idb.readPages([0, 2]), opfs.readPages([0, 2])]); idb.close(); opfs.close();
  const idbReopen = await IndexedDbPageStore.open(idbName), opfsReopen = await OpfsPageStore.open(opfsName, pageSize);
  const out = { idb: [await idbReopen.metadata(), (await idbReopen.readPages([2]))[0].bytes[0]], opfs: [await opfsReopen.metadata(), (await opfsReopen.readPages([2]))[0].bytes[0]], immediate: [idbRead[1].bytes[0], opfsRead[1].bytes[0]] };
  idbReopen.close(); opfsReopen.close();
  const parityName = "page-tree-" + Date.now() + "-" + Math.random(); const parityStore = await IndexedDbPageStore.open(parityName); const tree = await WasmAsyncBTree.open(parityStore, pageSize, 3);
  for (let i=0;i<300;i++) await tree.put(new TextEncoder().encode("k"+String(i).padStart(4,"0")),new Uint8Array([i&255]));
  const value = await tree.get(new TextEncoder().encode("k0123")); await tree.checkpoint(); tree.free();
  const reopened = await WasmAsyncBTree.open(parityStore,pageSize,3); const reopenedValue = await reopened.get(new TextEncoder().encode("k0123")); reopened.free(); parityStore.close(); await IndexedDbPageStore.destroy(parityName);
  out.parity=[value[0],reopenedValue[0]];
  async function bench(Store, prefix, size) { const store=await Store.open(prefix+Date.now()+Math.random(), pageSize); const tree=await WasmAsyncBTree.open(store,pageSize,3); const enc=new TextEncoder(), val=(i)=>new Uint8Array(size).fill(i&255), key=(i)=>enc.encode("k"+String(i).padStart(4,"0")); const n=200, rows={}; let t=performance.now(); for(let i=0;i<n;i++)await tree.put(key(i),val(i)); rows.seq_put=n/(performance.now()-t)*1000; t=performance.now(); for(let i=n-1;i>=0;i--)await tree.put(key(i),val(i)); rows.rand_put=n/(performance.now()-t)*1000; t=performance.now(); for(let i=0;i<n;i++)await tree.get(key(i)); rows.seq_get=n/(performance.now()-t)*1000; t=performance.now(); for(let i=n-1;i>=0;i--)await tree.get(key(i)); rows.rand_get=n/(performance.now()-t)*1000; t=performance.now(); for(let i=0;i<n;i++){if(i%10)await tree.get(key(i));else await tree.put(key(i),val(i+1));} rows.mixed=n/(performance.now()-t)*1000; t=performance.now(); for(let i=0;i<n;i++)await tree.range(key(i),key(Math.min(n,i+32)),32); rows.range=n/(performance.now()-t)*1000; t=performance.now(); await tree.checkpoint(); tree.free(); const reopened=await WasmAsyncBTree.open(store,pageSize,3); await reopened.get(key(42)); rows.checkpoint_reopen=1/(performance.now()-t)*1000; reopened.free(); store.close(); await Store.destroy(prefix+Date.now()+Math.random()).catch(()=>{}); return rows; }
  out.bench={}; for(const size of [32,256]) { out.bench[size]={idb:[],opfs:[]}; for(let r=0;r<3;r++){out.bench[size].idb.push(await bench(IndexedDbPageStore,"b-idb-",size));out.bench[size].opfs.push(await bench(OpfsPageStore,"b-opfs-",size));} } await IndexedDbPageStore.destroy(idbName); await OpfsPageStore.destroy(opfsName); self.postMessage({out});
} catch (error) { self.postMessage({error: (error && (error.stack || error.message)) || String(error)}); } };`;
  const server = http.createServer((req, res) => {
    if (req.url === "/async-page-stores.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); fs.createReadStream(source).pipe(res); }
    else if (req.url.startsWith("/pkg/")) { const file=path.join(__dirname,"pkg",req.url.slice(5)); res.writeHead(200,{"Content-Type":file.endsWith(".wasm")?"application/wasm":"text/javascript"}); fs.createReadStream(file).pipe(res); }
    else if (req.url === "/store-worker.js") { res.writeHead(200, { "Content-Type": "text/javascript" }); res.end(worker); }
    else { res.writeHead(200, { "Content-Type": "text/html" }); res.end("<!doctype html>"); }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { chromium } = playwright(); const browser = await chromium.launch({ headless: true }); const context = await browser.newContext();
  try {
    const page = await context.newPage(); await page.goto(`http://127.0.0.1:${server.address().port}`);
    const result = await page.evaluate(() => new Promise((resolve, reject) => { const worker = new Worker("/store-worker.js", {type:"module"}); worker.onmessage = (e) => e.data.error ? reject(new Error(e.data.error)) : resolve(e.data.out); worker.onerror = () => reject(new Error("store worker failed")); worker.postMessage({}); }));
    if (JSON.stringify({idb:result.idb,opfs:result.opfs,immediate:result.immediate,parity:result.parity}) !== JSON.stringify({ idb: [{ pageSize: 4096, logicalLen: 12288 }, 2], opfs: [{ pageSize: 4096, logicalLen: 12288 }, 2], immediate: [2, 2], parity: [123,123] })) throw new Error(JSON.stringify(result)); console.log(JSON.stringify(result.bench));
    console.log("async page stores: atomic page commit, read-after-commit, reopen passed");
  } finally { await context.close(); await browser.close(); await new Promise((resolve) => server.close(resolve)); }
}
main().catch((e) => { console.error(e.stack || e); process.exit(1); });
