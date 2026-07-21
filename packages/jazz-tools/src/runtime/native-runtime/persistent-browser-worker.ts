import { loadWasmModule } from "../client.js";
import { installWasmTelemetry } from "../sync-telemetry.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  isNativeRowDelta,
  type PersistentBrowserOpfsOwnerRequest,
  type PersistentBrowserSubscriptionFrame,
} from "./persistent-browser-protocol.js";

type OpenMessage = Extract<PersistentBrowserOpfsOwnerRequest, { method: "open" }>;
type WriteMessage = Extract<
  PersistentBrowserOpfsOwnerRequest,
  { method: "insert" | "restore" | "update" | "upsert" | "delete" }
>;

let runtime: NativeRuntimeAdapter | null = null;
let runtimeNamespace: string | null = null;
let workerWasmModule: Awaited<ReturnType<typeof loadWasmModule>> | null = null;
let disposeWorkerTelemetry: (() => void) | null = null;
const pendingWriteTransactionIds = new Set<string>();

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<PersistentBrowserOpfsOwnerRequest>) => void) | null;
  postMessage(message: unknown, transfer?: Transferable[]): void;
};

workerScope.onmessage = (event: MessageEvent<PersistentBrowserOpfsOwnerRequest>) => {
  void handleMessage(event.data);
};

async function handleMessage(message: PersistentBrowserOpfsOwnerRequest): Promise<void> {
  try {
    switch (message.method) {
      case "open": {
        await openRuntime(message);
        postResult(message.id, undefined);
        return;
      }
      case "destroyBrowserStorage": {
        const [runtimeSources, dbName] = message.args;
        const wasmModule = await loadWasmModule(runtimeSources);
        await wasmModule.WasmDb.destroyBrowserStorage(dbName);
        postResult(message.id, undefined);
        return;
      }
      case "insert":
      case "restore":
      case "update":
      case "upsert":
      case "delete": {
        const result = dispatchWrite(message);
        await getRuntime().waitForTransaction(result.transactionId, "local");
        postResult(message.id, result);
        return;
      }
      case "waitForTransaction": {
        const [transactionId, tier] = message.args;
        const result = await getRuntime().waitForTransaction(transactionId, tier);
        postResult(message.id, result);
        return;
      }
      case "beginTransaction": {
        const [kind] = message.args;
        const result = getRuntime().beginTransaction(kind);
        postResult(message.id, result);
        return;
      }
      case "commitTransaction": {
        const [transactionId] = message.args;
        const result = getRuntime().commitTransaction(transactionId);
        postResult(message.id, result);
        return;
      }
      case "rollbackTransaction": {
        const [transactionId] = message.args;
        const result = getRuntime().rollbackTransaction(transactionId);
        postResult(message.id, result);
        return;
      }
      case "query": {
        const result = await getRuntime().query(...message.args);
        postResult(message.id, result);
        return;
      }
      case "createExecutedSubscription": {
        const [ownerHandle, ...subscriptionArgs] = message.args;
        const result = getRuntime().createSubscription(...subscriptionArgs);
        getRuntime().executeSubscription(result, (delta: unknown) => {
          if (delta instanceof Error) {
            workerScope.postMessage({
              subscription: ownerHandle,
              error: { name: delta.name, message: delta.message },
            });
            return;
          }
          const frame = subscriptionFrameFromDelta(delta);
          workerScope.postMessage({ subscription: ownerHandle, frame }, [
            frame.added,
            frame.removed,
            frame.updated,
          ]);
        });
        postResult(message.id, result);
        return;
      }
      case "unsubscribe": {
        const [handle] = message.args;
        getRuntime().unsubscribe(handle);
        postResult(message.id, undefined);
        return;
      }
      case "close": {
        await closeRuntime();
        postResult(message.id, undefined);
        return;
      }
      case "closeForStorageClear": {
        const result = await closeForStorageClear();
        postResult(message.id, result);
        return;
      }
      case "connect": {
        await getRuntime().connect(...message.args);
        postResult(message.id, undefined);
        return;
      }
      case "disconnect": {
        getRuntime().disconnect();
        postResult(message.id, undefined);
        return;
      }
      case "updateAuth": {
        await getRuntime().updateAuth(...message.args);
        postResult(message.id, undefined);
        return;
      }
      case "installTelemetry": {
        const [telemetry] = message.args;
        if (workerWasmModule && !disposeWorkerTelemetry) {
          disposeWorkerTelemetry = installWasmTelemetry({
            wasmModule: workerWasmModule,
            collectorUrl: telemetry.collectorUrl,
            appId: telemetry.appId,
            runtimeThread: "worker",
          });
        }
        postResult(message.id, undefined);
        return;
      }
    }
  } catch (error) {
    postError(message.id, error);
  }
}

function dispatchWrite(message: WriteMessage): { transactionId: string } {
  const runtime = getRuntime();
  let result: { transactionId: string };
  switch (message.method) {
    case "insert": {
      const [table, values, writeContext, objectId] = message.args;
      result = runtime.insert(table, values, writeContext, objectId);
      break;
    }
    case "restore": {
      const [table, objectId, values, writeContext] = message.args;
      result = runtime.restore(table, objectId, values, writeContext);
      break;
    }
    case "update": {
      const [table, objectId, values, writeContext] = message.args;
      result = runtime.update(table, objectId, values, writeContext);
      break;
    }
    case "upsert": {
      const [table, objectId, values, writeContext] = message.args;
      result = runtime.upsert(table, objectId, values, writeContext);
      break;
    }
    case "delete": {
      const [table, objectId, writeContext] = message.args;
      result = runtime.delete(table, objectId, writeContext);
      break;
    }
  }
  pendingWriteTransactionIds.add(result.transactionId);
  return result;
}

async function openRuntime(message: OpenMessage): Promise<void> {
  const [runtimeSources, dbName, schema, node, author] = message.args;
  const wasmModule = await loadWasmModule(runtimeSources);
  workerWasmModule = wasmModule;
  runtimeNamespace = dbName;
  const db = await wasmModule.WasmDb.openBrowser(
    dbName,
    encodeSchema(schema as never),
    openConfig(node, author, 1, true),
  );

  runtime = NativeRuntimeAdapter.fromDb(db as never, schema as never, node, author, 1, true);
  runtime.onAuthFailure((reason: string) => {
    workerScope.postMessage({ event: "authFailure", reason });
  });
}

async function closeForStorageClear(): Promise<string> {
  const namespace = runtimeNamespace;
  if (!namespace) {
    throw new Error("Persistent browser native runtime has no storage namespace");
  }

  await closeRuntime();
  return namespace;
}

async function closeRuntime(): Promise<void> {
  await settlePendingWrites();
  disposeWorkerTelemetry?.();
  disposeWorkerTelemetry = null;
  workerWasmModule = null;
  await runtime?.close?.();
  runtime = null;
  runtimeNamespace = null;
  pendingWriteTransactionIds.clear();
}

async function settlePendingWrites(): Promise<void> {
  if (!runtime) return;
  for (const transactionId of pendingWriteTransactionIds) {
    await runtime.waitForTransaction(transactionId, "local");
    pendingWriteTransactionIds.delete(transactionId);
  }
}

function getRuntime(): NativeRuntimeAdapter {
  if (!runtime) {
    throw new Error("Persistent browser native runtime is not open");
  }
  return runtime;
}

function postResult(id: number, result: unknown): void {
  workerScope.postMessage({ id, ok: true, result });
}

function postError(id: number, error: unknown): void {
  workerScope.postMessage({
    id,
    ok: false,
    error:
      error instanceof Error
        ? { name: error.name, message: error.message }
        : { message: String(error) },
  });
}

function subscriptionFrameFromDelta(delta: unknown): PersistentBrowserSubscriptionFrame {
  if (!isNativeRowDelta(delta)) {
    throw new Error(
      "Persistent browser subscription channel received a non-encoded delta; encoded framing is required",
    );
  }
  const added = transferableBuffer(delta.added);
  const removed = transferableBuffer(delta.removed);
  const updated = transferableBuffer(delta.updated);
  return {
    kind: "native-row-delta",
    reset: delta.reset,
    added,
    removed,
    updated,
    addedCount: delta.addedCount,
    removedCount: delta.removedCount,
    updatedCount: delta.updatedCount,
  };
}

function transferableBuffer(bytes: Uint8Array): ArrayBuffer {
  if (bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength) {
    return bytes.buffer as ArrayBuffer;
  }
  return bytes.slice().buffer;
}
