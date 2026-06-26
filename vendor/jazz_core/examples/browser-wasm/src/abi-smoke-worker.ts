import initWasm, { WasmDb } from "../../../jazz-wasm/pkg-web/jazz_core_wasm.js";
import {
  reviveArgs,
  rowsFromDeltaPayload,
  rowsFromPayload,
  type BrowserDbOperation,
  type BrowserDbRequest,
  type BrowserDbResponse,
  type Handle,
  type ObjectKind,
  type SubscriptionHandle,
} from "./abi-smoke-worker-client.js";

type WorkerScope = {
  postMessage(message: BrowserDbResponse): void;
  addEventListener(
    type: "message",
    listener: (event: MessageEvent<BrowserDbRequest>) => void,
  ): void;
  close(): void;
};

type UnknownFunction = (...args: unknown[]) => unknown;

const scope = globalThis as unknown as WorkerScope;
let api: WasmAbiObjectRegistry | undefined;

scope.addEventListener("message", (event) => {
  void handleRequest(event.data).then((response) => {
    scope.postMessage(response);
    if (response.type === "closed") scope.close();
  });
});

async function handleRequest(request: BrowserDbRequest): Promise<BrowserDbResponse> {
  try {
    if (request.type === "load") {
      await initWasm();
      api = new WasmAbiObjectRegistry();
      return { id: request.id, type: "ready" };
    }

    if (request.type === "shutdown") {
      api?.shutdown();
      api = undefined;
      return { id: request.id, type: "closed" };
    }

    if (!api) throw new Error("worker db API is not initialized");
    return {
      id: request.id,
      type: "result",
      result: await api.call(request.operation, ...reviveArgs(request.args)),
    };
  } catch (error) {
    return { id: request.id, type: "error", error: errorMessage(error) };
  }
}

class WasmAbiObjectRegistry {
  private nextId = 1;
  private readonly dbs = new Map<number, unknown>();
  private readonly queries = new Map<number, unknown>();
  private readonly subscriptions = new Map<number, unknown>();
  private readonly transports = new Map<number, unknown>();
  private readonly writes = new Map<number, unknown>();

  shutdown(): void {
    this.dbs.clear();
    this.queries.clear();
    this.subscriptions.clear();
    this.transports.clear();
    this.writes.clear();
  }

  async call(operation: BrowserDbOperation, ...args: unknown[]): Promise<unknown> {
    switch (operation) {
      case "openMemoryDb":
        return this.store(
          "db",
          this.dbs,
          WasmDb.openMemory(args[0] as Uint8Array, args[1] as Uint8Array),
        );
      case "openBrowserDb":
        return this.store(
          "db",
          this.dbs,
          await WasmDb.openBrowser(args[0] as string, args[1] as Uint8Array, args[2] as Uint8Array),
        );
      case "closeDb":
        return this.deleteHandle(this.dbs, args[0]);
      case "prepareQuery":
        return this.store(
          "query",
          this.queries,
          callMethod(this.db(args[0]), "prepareQuery", args[1]),
        );
      case "readAll":
        return rowsFromPayload(callMethod(this.db(args[0]), "all", this.query(args[1]), args[2]));
      case "readOne":
        return rowsFromPayload(callMethod(this.db(args[0]), "one", this.query(args[1]), args[2]));
      case "readAllForIdentity":
        return rowsFromPayload(
          callMethod(this.db(args[0]), "allForIdentity", this.query(args[1]), args[2], args[3]),
        );
      case "subscribe":
        return this.openSubscription(
          callMethod(this.db(args[0]), "subscribe", this.query(args[1]), args[2]),
        );
      case "subscriptionCurrent":
        return this.readSubscription(args[0]);
      case "unsubscribe":
        return this.closeSubscription(args[0]);
      case "canInsertEncoded":
        return callMethod(this.db(args[0]), "canInsertEncoded", args[1], args[2]);
      case "canUpdateEncodedForIdentity":
        return callMethod(
          this.db(args[0]),
          "canUpdateEncodedForIdentity",
          args[1],
          args[2],
          args[3],
          args[4],
        );
      case "insertEncoded":
        return this.store(
          "write",
          this.writes,
          callMethod(this.db(args[0]), "insertEncoded", args[1], args[2]),
        );
      case "insertWithIdEncoded":
        return this.store(
          "write",
          this.writes,
          callMethod(this.db(args[0]), "insertWithIdEncoded", args[1], args[2], args[3]),
        );
      case "updateEncoded":
        return this.store(
          "write",
          this.writes,
          callMethod(this.db(args[0]), "updateEncoded", args[1], args[2], args[3]),
        );
      case "deleteRow":
        return this.store(
          "write",
          this.writes,
          callMethod(this.db(args[0]), "delete", args[1], args[2]),
        );
      case "writeState":
        return callMethod(this.writeObject(args[0]), "writeState");
      case "waitWrite":
        return callMethod(this.writeObject(args[0]), "wait", args[1]);
      case "connectTransport":
        return this.store(
          "transport",
          this.transports,
          callMethod(this.db(args[0]), "connectUpstream"),
        );
      case "transportSendWireFrame":
        return callMethod(this.transport(args[0]), "sendWireFrame", args[1]);
      case "transportRecvWireFrames":
        return Array.from(
          callMethod(this.transport(args[0]), "recvWireFrames") as Iterable<unknown>,
        );
      case "transportTick":
        return { watch_wakes: callMethod(this.transport(args[0]), "tick") };
      case "transportClose":
        return this.closeTransport(args[0]);
      case "destroyBrowserStorage":
        return WasmDb.destroyBrowserStorage(args[0] as string);
      case "release":
        return this.release(args[0]);
      default:
        throw new Error(`unknown browser worker operation ${operation}`);
    }
  }

  private async openSubscription(
    stream: unknown,
  ): Promise<{ subscription: SubscriptionHandle; current: unknown[] }> {
    const reader = (stream as ReadableStream<unknown>).getReader?.();
    if (!reader) throw new Error("WasmDb.subscribe did not return a ReadableStream");
    const subscription = this.store("subscription", this.subscriptions, reader);
    return { subscription, current: rowsFromSubscriptionChunk(await readStreamValue(reader)) };
  }

  private async readSubscription(handle: unknown): Promise<unknown> {
    return rowsFromSubscriptionChunk(
      await readStreamValue(this.subscription(handle) as ReadableStreamDefaultReader<unknown>),
    );
  }

  private closeSubscription(handle: unknown): void {
    const subscription = this.subscription(handle) as {
      cancel?: () => unknown;
      releaseLock?: () => unknown;
    };
    subscription.cancel?.();
    subscription.releaseLock?.();
    this.deleteHandle(this.subscriptions, handle);
  }

  private closeTransport(handle: unknown): unknown {
    const transport = this.transport(handle);
    this.deleteHandle(this.transports, handle);
    return callMethod(transport, "close");
  }

  private release(handle: unknown): void {
    if (!isHandle(handle)) return;
    if (handle.kind === "query") this.queries.delete(handle.id);
    if (handle.kind === "db") this.dbs.delete(handle.id);
    if (handle.kind === "subscription") this.subscriptions.delete(handle.id);
    if (handle.kind === "transport") this.transports.delete(handle.id);
    if (handle.kind === "write") this.writes.delete(handle.id);
  }

  private db(handle: unknown): unknown {
    return getObject(this.dbs, handle, "db");
  }
  private query(handle: unknown): unknown {
    return getObject(this.queries, handle, "query");
  }
  private subscription(handle: unknown): unknown {
    return getObject(this.subscriptions, handle, "subscription");
  }
  private transport(handle: unknown): unknown {
    return getObject(this.transports, handle, "transport");
  }
  private writeObject(handle: unknown): unknown {
    return getObject(this.writes, handle, "write");
  }

  private store<K extends ObjectKind>(
    kind: K,
    map: Map<number, unknown>,
    value: unknown,
  ): { kind: K; id: number } {
    const id = this.nextId++;
    map.set(id, value);
    return { kind, id };
  }

  private deleteHandle(map: Map<number, unknown>, handle: unknown): void {
    if (isHandle(handle)) map.delete(handle.id);
  }
}

async function readStreamValue(reader: ReadableStreamDefaultReader<unknown>): Promise<unknown> {
  const result = await reader.read();
  if (result.done) return [];
  return result.value;
}

function callMethod(target: unknown, method: string, ...args: unknown[]): unknown {
  const fn = (target as Record<string, unknown>)[method];
  if (typeof fn !== "function") throw new Error(`direct object method ${method} is unavailable`);
  return (fn as UnknownFunction).apply(target, args);
}

function getObject(map: Map<number, unknown>, handle: unknown, kind: string): unknown {
  if (!isHandle(handle)) throw new Error(`expected ${kind} handle`);
  const value = map.get(handle.id);
  if (!value) throw new Error(`unknown ${kind} handle ${handle.id}`);
  return value;
}

function isHandle(value: unknown): value is Handle {
  return typeof value === "object" && value !== null && "kind" in value && "id" in value;
}

function rowsFromSubscriptionChunk(value: unknown): unknown[] {
  if (typeof value !== "object" || value === null) return rowsFromPayload(value);
  const chunk = value as Record<string, unknown>;
  if (chunk.type === "snapshot") return rowsFromPayload(chunk.rows);
  if (chunk.type === "delta") return rowsFromDeltaPayload(chunk.delta);
  if (chunk.type === "closed") return [];
  return rowsFromPayload(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
