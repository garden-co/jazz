import type { StorageAPI } from "cojson";
import { BfTreeStorageProxy } from "./proxy.js";

/**
 * Create a `StorageAPI` backed by a bf-tree B+ tree running in a
 * dedicated Web Worker with OPFS persistence.
 *
 * Usage:
 * ```ts
 * import { getBfTreeStorage } from "cojson-storage-bftree";
 *
 * const node = new LocalNode(/* … *​/);
 * node.setStorage(await getBfTreeStorage());
 * ```
 *
 * @param dbName     OPFS filename (default `"jazz-bftree.db"`)
 * @param cacheSizeBytes  In-memory cache size, must be power of 2 (default 32 MB)
 */
export async function getBfTreeStorage(
  dbName = "jazz-bftree.db",
  cacheSizeBytes = 32 * 1024 * 1024,
): Promise<StorageAPI> {
  const worker = new Worker(new URL("./worker.js", import.meta.url), {
    type: "module",
  });

  // Wait for the worker to initialise cojson-core-wasm + open bf-tree on OPFS
  await new Promise<void>((resolve, reject) => {
    const onMessage = (event: MessageEvent) => {
      worker.removeEventListener("message", onMessage);
      if (event.data.type === "ready") {
        resolve();
      } else if (event.data.type === "error") {
        reject(new Error(event.data.message));
      } else {
        reject(new Error("Unexpected worker response"));
      }
    };
    worker.addEventListener("message", onMessage);
    worker.postMessage({ type: "init", dbName, cacheSizeBytes });
  });

  // Returns StorageAPI directly — no StorageApiAsync wrapper needed!
  return new BfTreeStorageProxy(worker);
}

export { BfTreeStorageProxy } from "./proxy.js";
export type {
  WorkerRequest,
  WorkerResponse,
  WorkerInitRequest,
  WorkerInitResponse,
  WorkerFireAndForget,
} from "./protocol.js";
