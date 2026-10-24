import init, * as wasm from "./pkg/jazz_wasm.js";
const t0 = performance.now();
let initMs = 0;
const ready = init().then((e) => {
  initMs = performance.now() - t0;
  return e;
});
function fileBytes(path) {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open("jazz-idb");
    req.onerror = () => reject(req.error);
    req.onsuccess = () => {
      const db = req.result;
      const tx = db.transaction("blocks", "readonly");
      const c = tx.objectStore("blocks").count(IDBKeyRange.bound([path, 0], [path, Infinity]));
      c.onsuccess = () => {
        db.close();
        resolve(c.result * 65536);
      };
      c.onerror = () => reject(c.error);
    };
  });
}
async function run({ fn, args }) {
  const exports = await ready;
  let out = {};
  if (fn === "seed") {
    const fixture = new Uint8Array(await (await fetch("./fixture.yjs")).arrayBuffer());
    out = await wasm.pagedBenchSeed(args[0], args[1], args[2], args[3], fixture);
    out.fileBytes = await fileBytes("/" + args[0]);
  } else if (fn === "clearHint" || fn === "corruptHint") {
    // The stored hint: removed, or replaced by one that parses as nothing.
    const key = ["/" + args[0], -1];
    const bad = { path: key[0], offset: -1, pages: 1e12, epochs: "x", fifo: "not an array", visited: [1, 2], hand: -5, counters: 7, mru: new Float64Array([NaN, -1, 1e300]) };
    await new Promise((resolve, reject) => {
      const req = indexedDB.open("jazz-idb");
      req.onerror = () => reject(req.error);
      req.onsuccess = () => {
        const db = req.result;
        const tx = db.transaction("blocks", "readwrite");
        if (fn === "clearHint") tx.objectStore("blocks").delete(key);
        else tx.objectStore("blocks").put(bad);
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
    });
  } else if (fn === "fileBytes") out.fileBytes = await fileBytes("/" + args[0]);
  else out = (await wasm[fn](...args)) || {};
  out.initMs = initMs;
  out.wasmMemoryBytes = exports.memory.buffer.byteLength;
  return out;
}
self.onmessage = async (m) => {
  try {
    self.postMessage({ ok: true, out: await run(m.data) });
  } catch (e) {
    self.postMessage({ ok: false, error: String((e && e.message) || e) });
  }
};
