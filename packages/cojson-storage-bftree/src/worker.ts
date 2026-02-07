import { initialize, open_bftree_opfs } from "cojson-core-wasm";
import { BfTreeWorkerBackend } from "./workerBackend.js";
import type { WorkerIncoming, WorkerRequest } from "./protocol.js";

let backend: BfTreeWorkerBackend | undefined;

const ctx = self as unknown as DedicatedWorkerGlobalScope;

ctx.onmessage = async (event: MessageEvent<WorkerIncoming>) => {
  const msg = event.data;

  // Handle initialization
  if ("type" in msg && msg.type === "init") {
    try {
      // Load the single cojson-core WASM binary (crypto + storage in one module)
      await initialize();
      // Open bf-tree backed by OPFS (requires Web Worker context)
      const tree = await open_bftree_opfs(msg.dbName, msg.cacheSizeBytes);
      backend = new BfTreeWorkerBackend(tree);
      ctx.postMessage({ type: "ready" });
    } catch (e) {
      ctx.postMessage({ type: "error", message: String(e) });
    }
    return;
  }

  // Handle RPC calls
  const req = msg as WorkerRequest;
  if (!backend) {
    ctx.postMessage({ reqId: req.reqId, error: "Worker not initialized" });
    return;
  }

  try {
    const result = backend.dispatch(req.method, req.args);
    ctx.postMessage({ reqId: req.reqId, result });
  } catch (e) {
    ctx.postMessage({
      reqId: req.reqId,
      error: e instanceof Error ? e.message : String(e),
    });
  }
};
