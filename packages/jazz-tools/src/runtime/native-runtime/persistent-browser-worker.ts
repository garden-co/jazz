import { loadWasmModule } from "../client.js";
import type { MutationResult } from "../client.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  isNativeRowDelta,
  type PersistentBrowserOpfsOwnerRequest,
  type PersistentBrowserSubscriptionFrame,
} from "./persistent-browser-protocol.js";
import { setNamedRowValuesEnumerable } from "./row-values-transport.js";

type OpenMessage = Extract<PersistentBrowserOpfsOwnerRequest, { method: "open" }>;
type WriteMessage = Extract<
  PersistentBrowserOpfsOwnerRequest,
  { method: "insert" | "restore" | "update" | "upsert" | "delete" }
>;

let runtime: NativeRuntimeAdapter | null = null;
const runtimeViews = new Map<number, NativeRuntimeAdapter>();
let nextRuntimeViewId = 1;
let runtimeNamespace: string | null = null;

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<PersistentBrowserOpfsOwnerRequest>) => void) | null;
  postMessage(message: unknown, transfer?: Transferable[]): void;
};

let commandQueue: Promise<void> = Promise.resolve();

workerScope.onmessage = (event: MessageEvent<PersistentBrowserOpfsOwnerRequest>) => {
  const message = event.data;
  if (message.method === "close" || message.method === "closeForStorageClear") {
    void handleMessage(message);
    return;
  }
  // Connection control releases server-tier commands that may already be
  // executing inside this queue. Queuing it behind those commands would make
  // the worker wait for the very control operation it has not yet dispatched.
  if (
    message.method === "disconnect" ||
    (message.method === "connect" && message.control === "reconnect")
  ) {
    void handleMessage(message);
    return;
  }
  commandQueue = commandQueue.then(
    () => handleMessage(message),
    () => handleMessage(message),
  );
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
      case "registerSchema": {
        const [schema] = message.args;
        const viewId = nextRuntimeViewId++;
        runtimeViews.set(viewId, getRuntime().registerSchemaView(schema));
        postResult(message.id, viewId);
        return;
      }
      case "insert":
      case "restore":
      case "update":
      case "upsert":
      case "delete": {
        const result = dispatchWrite(message);
        if (result.kind === "staged") {
          postResult(message.id, result);
          return;
        }
        const batchId = await result.batchId;
        await getRuntime().waitForTransaction(batchId, "local");
        postResult(message.id, { kind: "committed", batchId } satisfies MutationResult);
        return;
      }
      case "waitForTransaction": {
        const [batchId, tier] = message.args;
        const result = await getRuntime().waitForTransaction(batchId, tier);
        postResult(message.id, result);
        return;
      }
      case "beginTransaction": {
        const [kind, id, sessionJson] = message.args;
        const result = getRuntime().beginTransaction(kind, id, sessionJson);
        postResult(message.id, result);
        return;
      }
      case "commitTransaction": {
        const [openBatchId] = message.args;
        const result = await getRuntime().commitTransaction(openBatchId);
        postResult(message.id, result);
        return;
      }
      case "rollbackTransaction": {
        const [openBatchId] = message.args;
        const result = await getRuntime().rollbackTransaction(openBatchId);
        postResult(message.id, result);
        return;
      }
      case "query": {
        const result = await getRuntime(message.viewId).query(...message.args);
        setNamedRowValuesEnumerable(result, true);
        try {
          postResult(message.id, result);
        } finally {
          setNamedRowValuesEnumerable(result, false);
        }
        return;
      }
      case "createExecutedSubscription": {
        const [ownerHandle, ...subscriptionArgs] = message.args;
        const target = getRuntime(message.viewId);
        const result = target.createSubscription(...subscriptionArgs);
        target.executeSubscription(result, (delta: unknown) => {
          if (delta instanceof Error) {
            workerScope.postMessage({
              subscription: ownerHandle,
              error: { name: delta.name, message: delta.message, stack: delta.stack },
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
        getRuntime(message.viewId).unsubscribe(handle);
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
        await getRuntime().disconnect(...message.args);
        postResult(message.id, undefined);
        return;
      }
      case "updateAuth": {
        await getRuntime().updateAuth(...message.args);
        postResult(message.id, undefined);
        return;
      }
    }
  } catch (error) {
    postError(message.id, error);
  }
}

function dispatchWrite(message: WriteMessage): MutationResult {
  const runtime = getRuntime(message.viewId);
  let result: MutationResult;
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
  return result;
}

async function openRuntime(message: OpenMessage): Promise<void> {
  const [runtimeSources, dbName, schema, node, author, initialSyncFlushEvery] = message.args;
  const wasmModule = await loadWasmModule(runtimeSources);
  runtimeNamespace = dbName;
  const db = await wasmModule.WasmDb.openBrowser(
    dbName,
    encodeSchema(schema as never),
    openConfig(node, author, 1, true, initialSyncFlushEvery),
  );

  runtime = NativeRuntimeAdapter.fromDb(db as never, schema as never, node, author, 1, true);
  runtimeViews.clear();
  nextRuntimeViewId = 1;
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
  await runtime?.close?.();
  runtime = null;
  runtimeViews.clear();
  runtimeNamespace = null;
}

function getRuntime(viewId?: number): NativeRuntimeAdapter {
  if (!runtime) {
    throw new Error("Persistent browser native runtime is not open");
  }
  if (viewId === undefined) return runtime;
  const view = runtimeViews.get(viewId);
  if (!view) throw new Error(`Persistent browser runtime view ${viewId} is not registered`);
  return view;
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
        ? { name: error.name, message: error.message, stack: error.stack }
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
