import {
  loadWasmModule,
  type DirectInsertResult,
  type DirectMutationResult,
  type MutationErrorEvent,
} from "../client.js";
import type { InsertValues, Value } from "../../drivers/types.js";
import { openConfig } from "./direct-codec.js";
import { encodeDirectSchema } from "./direct-schema-codec.js";
import { CoreRuntime } from "./runtime.js";
import type { PersistentBrowserWorkerRequest } from "./persistent-browser-runtime.js";

type OpenMessage = Extract<PersistentBrowserWorkerRequest, { method: "open" }>;
type WriteMessage = Extract<
  PersistentBrowserWorkerRequest,
  { method: "insert" | "restore" | "update" | "upsert" | "delete" }
>;

type PersistentBrowserCoreRuntime = {
  insert(
    table: string,
    values: InsertValues,
    writeContext?: string | null,
    objectId?: string | null,
  ): DirectInsertResult;
  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): DirectInsertResult;
  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): DirectMutationResult;
  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): DirectMutationResult;
  delete(table: string, objectId: string, writeContext?: string | null): DirectMutationResult;
  waitForTransaction(transactionId: string, tier: string): Promise<void>;
  query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown>;
  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number;
  executeSubscription(handle: number, onUpdate: (...args: unknown[]) => void): void;
  unsubscribe(handle: number): void;
  close?(): void | Promise<void>;
  connect(url: string, authJson: string): void;
  disconnect(): void;
  updateAuth(authJson: string): void;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  onAuthFailure(callback: (reason: string) => void): void;
};

let runtime: PersistentBrowserCoreRuntime | null = null;

const workerScope = self as unknown as {
  onmessage: ((event: MessageEvent<PersistentBrowserWorkerRequest>) => void) | null;
  postMessage(message: unknown): void;
};

workerScope.onmessage = (event: MessageEvent<PersistentBrowserWorkerRequest>) => {
  void handleMessage(event.data);
};

async function handleMessage(message: PersistentBrowserWorkerRequest): Promise<void> {
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
      case "close": {
        const result = await getRuntime().close?.();
        runtime = null;
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
  switch (message.method) {
    case "insert": {
      const [table, values, writeContext, objectId] = message.args;
      return runtime.insert(table, values, writeContext, objectId);
    }
    case "restore": {
      const [table, objectId, values, writeContext] = message.args;
      return runtime.restore(table, objectId, values, writeContext);
    }
    case "update": {
      const [table, objectId, values, writeContext] = message.args;
      return runtime.update(table, objectId, values, writeContext);
    }
    case "upsert": {
      const [table, objectId, values, writeContext] = message.args;
      return runtime.upsert(table, objectId, values, writeContext);
    }
    case "delete": {
      const [table, objectId, writeContext] = message.args;
      return runtime.delete(table, objectId, writeContext);
    }
  }
}

async function openRuntime(message: OpenMessage): Promise<void> {
  const [runtimeSources, dbName, schema, node, author] = message.args;
  const wasmModule = await loadWasmModule(runtimeSources);
  const db = await wasmModule.WasmDb.openBrowser(
    dbName,
    encodeDirectSchema(schema as never),
    openConfig(node, author, 1, true),
  );

  runtime = CoreRuntime.fromDb(db as never, schema as never, node, author, 1, true);
  runtime.onMutationError((payload: MutationErrorEvent) => {
    workerScope.postMessage({ event: "mutationError", payload });
  });
  runtime.onAuthFailure((reason: string) => {
    workerScope.postMessage({ event: "authFailure", reason });
  });
}

function getRuntime(): PersistentBrowserCoreRuntime {
  if (!runtime) {
    throw new Error("Persistent browser core runtime is not open");
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
