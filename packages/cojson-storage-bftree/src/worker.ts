import { initialize, open_bftree_opfs } from "cojson-core-wasm";
import { BfTreeWorkerBackend } from "./workerBackend.js";
import { DeletedCoValuesEraserScheduler } from "cojson";
import type {
  WorkerIncoming,
  WorkerRequest,
  WorkerFireAndForget,
} from "./protocol.js";

let backend: BfTreeWorkerBackend | undefined;
let eraserScheduler: DeletedCoValuesEraserScheduler | undefined;
let eraserController: AbortController | undefined;

const ctx = self as unknown as DedicatedWorkerGlobalScope;

function interruptEraser() {
  if (eraserController) {
    eraserController.abort("interrupted");
    eraserController = undefined;
  }
}

ctx.onmessage = async (event: MessageEvent<WorkerIncoming>) => {
  const msg = event.data;

  // Handle initialization
  if ("type" in msg && msg.type === "init") {
    try {
      await initialize();
      const tree = await open_bftree_opfs(msg.dbName, msg.cacheSizeBytes);
      backend = new BfTreeWorkerBackend(tree);
      ctx.postMessage({ type: "ready" });
    } catch (e) {
      ctx.postMessage({ type: "error", message: String(e) });
    }
    return;
  }

  if (!backend) {
    if ("reqId" in msg) {
      ctx.postMessage({
        reqId: msg.reqId,
        type: "error",
        message: "Worker not initialized",
      });
    }
    return;
  }

  // Fire-and-forget messages (no reqId)
  if (!("reqId" in msg)) {
    handleFireAndForget(backend, msg as WorkerFireAndForget);
    return;
  }

  // Request/response messages
  const req = msg as WorkerRequest;

  try {
    switch (req.method) {
      case "load": {
        interruptEraser();
        const { messages, found } = backend.loadContent(req.id);
        for (const data of messages) {
          ctx.postMessage({ reqId: req.reqId, type: "load:data", data });
        }
        ctx.postMessage({ reqId: req.reqId, type: "load:done", found });
        break;
      }

      case "store": {
        interruptEraser();
        const result = backend.storeContent(
          req.data,
          new Set(req.deletedCoValues),
        );
        ctx.postMessage({
          reqId: req.reqId,
          type: "store:result",
          knownState: result.knownState,
          storedCoValueRowID: result.storedCoValueRowID,
        });
        break;
      }

      case "loadKnownState": {
        const knownState = backend.getCoValueKnownState(req.id);
        ctx.postMessage({
          reqId: req.reqId,
          type: "result",
          value: knownState,
        });
        break;
      }

      case "eraseAllDeletedCoValues": {
        eraserController = new AbortController();
        backend.eraseAllDeletedCoValues(eraserController.signal);
        eraserController = undefined;
        ctx.postMessage({
          reqId: req.reqId,
          type: "result",
          value: undefined,
        });
        break;
      }

      case "getUnsyncedCoValueIDs": {
        const ids = backend.getUnsyncedCoValueIDs();
        ctx.postMessage({ reqId: req.reqId, type: "result", value: ids });
        break;
      }

      case "close": {
        eraserScheduler?.dispose();
        eraserScheduler = undefined;
        ctx.postMessage({
          reqId: req.reqId,
          type: "result",
          value: undefined,
        });
        break;
      }
    }
  } catch (e) {
    ctx.postMessage({
      reqId: req.reqId,
      type: "error",
      message: e instanceof Error ? e.message : String(e),
    });
  }
};

function handleFireAndForget(
  backend: BfTreeWorkerBackend,
  msg: WorkerFireAndForget,
) {
  switch (msg.method) {
    case "markDeleteAsValid":
      // Delete validation is tracked on the main thread; the worker marks
      // the deletion when it sees a delete session during storeContent.
      break;

    case "enableDeletedCoValuesErasure":
      if (!eraserScheduler) {
        eraserScheduler = new DeletedCoValuesEraserScheduler({
          run: async () => {
            eraserController = new AbortController();
            backend.eraseAllDeletedCoValues(eraserController.signal);
            eraserController = undefined;
            const remaining = backend.getAllCoValuesWaitingForDelete();
            return { hasMore: remaining.length > 0 };
          },
        });
        eraserScheduler.scheduleStartupDrain();
      }
      break;

    case "trackCoValuesSyncState":
      backend.trackCoValuesSyncState(msg.updates);
      break;

    case "stopTrackingSyncState":
      backend.stopTrackingSyncState(msg.id);
      break;

    case "onCoValueUnmounted":
      // Could clean up row-ID caches in the future
      break;
  }
}
