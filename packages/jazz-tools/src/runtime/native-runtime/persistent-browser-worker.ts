import { loadWasmModule } from "../client.js";
import { openConfig } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import type { PersistentBrowserOpfsOwnerRequest } from "./persistent-browser-protocol.js";

type OpenMessage = Extract<PersistentBrowserOpfsOwnerRequest, { method: "open" }>;
type WriteMessage = Extract<
  PersistentBrowserOpfsOwnerRequest,
  { method: "insert" | "restore" | "update" | "upsert" | "delete" }
>;

let runtime: NativeRuntimeAdapter | null = null;
let runtimeNamespace: string | null = null;
let runtimeWasmModule: Awaited<ReturnType<typeof loadWasmModule>> | null = null;
const pendingWriteTransactionIds = new Set<string>();

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<PersistentBrowserOpfsOwnerRequest>) => void) | null;
  postMessage(message: unknown): void;
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
      case "insert":
      case "restore":
      case "update":
      case "upsert":
      case "delete": {
        const result = dispatchWrite(message);
        postResult(message.id, result);
        return;
      }
      case "waitForTransaction": {
        const [transactionId, tier] = message.args;
        const result = await getRuntime().waitForTransaction(transactionId, tier);
        postResult(message.id, result);
        return;
      }
      case "query": {
        const result = await getRuntime().query(...message.args);
        postResult(message.id, result);
        return;
      }
      case "createSubscription": {
        const result = getRuntime().createSubscription(...message.args);
        postResult(message.id, result);
        return;
      }
      case "executeSubscription": {
        const [handle] = message.args;
        getRuntime().executeSubscription(handle, (...args: unknown[]) => {
          workerScope.postMessage({ subscription: handle, args });
        });
        postResult(message.id, undefined);
        return;
      }
      case "unsubscribe": {
        const [handle] = message.args;
        getRuntime().unsubscribe(handle);
        postResult(message.id, undefined);
        return;
      }
      case "clearClientStorage": {
        const result = await clearClientStorage();
        postResult(message.id, result);
        return;
      }
      case "connect": {
        getRuntime().connect(...message.args);
        postResult(message.id, undefined);
        return;
      }
      case "disconnect": {
        getRuntime().disconnect();
        postResult(message.id, undefined);
        return;
      }
      case "updateAuth": {
        getRuntime().updateAuth(...message.args);
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
  runtimeWasmModule = wasmModule;
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

async function clearClientStorage(): Promise<void> {
  const namespace = runtimeNamespace;
  if (!namespace) {
    throw new Error("Persistent browser native runtime has no storage namespace");
  }
  if (!runtimeWasmModule) {
    throw new Error("Persistent browser native runtime has no WASM module");
  }

  await settlePendingWrites();
  await runtime?.close?.();
  runtime = null;
  await runtimeWasmModule.WasmDb.destroyBrowserStorage(namespace);
  runtimeNamespace = null;
  runtimeWasmModule = null;
  pendingWriteTransactionIds.clear();
}

async function settlePendingWrites(): Promise<void> {
  const runtime = getRuntime();
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
