import {
  PostcardReader,
  PostcardWriter,
  assertBytes,
  openConfig,
  queryFromTable,
  readAbiRowBatch,
  type AbiRowBatch,
} from "./core-codec.js";

export { PostcardWriter, openConfig, queryFromTable };

export type ObjectKind = "db" | "query" | "subscription" | "write" | "transport";
export type ObjectHandle<K extends ObjectKind = ObjectKind> = { kind: K; id: number };
export type DbHandle = ObjectHandle<"db">;
export type QueryHandle = ObjectHandle<"query">;
export type SubscriptionHandle = ObjectHandle<"subscription">;
export type WriteHandle = ObjectHandle<"write">;
export type TransportHandle = ObjectHandle<"transport">;
export type Handle = ObjectHandle;

export type WriteState = {
  fate: string;
  durability: string;
  rejection?: unknown;
};

export type SubscriptionSnapshot = {
  subscription: SubscriptionHandle;
  current: AbiRowBatch[];
};

export type BrowserDbOperation =
  | "openMemoryDb"
  | "openBrowserDb"
  | "closeDb"
  | "prepareQuery"
  | "readAll"
  | "readOne"
  | "readAllForIdentity"
  | "subscribe"
  | "subscriptionCurrent"
  | "unsubscribe"
  | "canInsertEncoded"
  | "canUpdateEncodedForIdentity"
  | "insertEncoded"
  | "insertWithIdEncoded"
  | "updateEncoded"
  | "deleteRow"
  | "writeState"
  | "waitWrite"
  | "connectTransport"
  | "transportSendWireFrame"
  | "transportRecvWireFrames"
  | "transportTick"
  | "transportClose"
  | "destroyBrowserStorage"
  | "release";

export type BrowserDbRequest =
  | { id: number; type: "load" }
  | { id: number; type: "call"; operation: BrowserDbOperation; args: unknown[] }
  | { id: number; type: "shutdown" };

export type BrowserDbResponse =
  | { id: number; type: "ready" }
  | { id: number; type: "result"; result: unknown }
  | { id: number; type: "closed" }
  | { id: number; type: "error"; error: string };

type MessageTarget = {
  postMessage(message: BrowserDbRequest): void;
};

type MessageEvent = {
  data: BrowserDbResponse;
};

type MessageEndpoint = MessageTarget & {
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
};

type PendingRequest = {
  resolve: (response: BrowserDbResponse) => void;
  reject: (error: Error) => void;
  timeout?: ReturnType<typeof setTimeout>;
};

const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export class BrowserWasmAbiSmokeClient {
  private nextMessageId = 1;
  private readonly pending = new Map<number, PendingRequest>();

  constructor(
    private readonly endpoint: MessageEndpoint,
    private readonly requestTimeoutMs = DEFAULT_REQUEST_TIMEOUT_MS,
  ) {
    this.endpoint.addEventListener("message", (event) => {
      this.handleResponse(event.data);
    });
  }

  async init(): Promise<void> {
    const response = await this.request({ id: this.takeMessageId(), type: "load" });
    if (response.type !== "ready")
      throw new Error(`browser db worker init failed: ${JSON.stringify(response)}`);
  }

  openMemoryDb(schema: Uint8Array, config: Uint8Array): Promise<DbHandle> {
    return this.call("openMemoryDb", schema, config) as Promise<DbHandle>;
  }

  openBrowserDb(namespace: string, schema: Uint8Array, config: Uint8Array): Promise<DbHandle> {
    return this.call("openBrowserDb", namespace, schema, config) as Promise<DbHandle>;
  }

  closeDb(db: DbHandle): Promise<void> {
    return this.callVoid("closeDb", db);
  }

  prepareQuery(db: DbHandle, queryBytes: Uint8Array): Promise<QueryHandle> {
    return this.call("prepareQuery", db, queryBytes) as Promise<QueryHandle>;
  }

  readAll(db: DbHandle, query: QueryHandle, opts?: unknown): Promise<AbiRowBatch[]> {
    return this.call("readAll", db, query, opts) as Promise<AbiRowBatch[]>;
  }

  readOne(db: DbHandle, query: QueryHandle, opts?: unknown): Promise<AbiRowBatch[]> {
    return this.call("readOne", db, query, opts) as Promise<AbiRowBatch[]>;
  }

  readAllForIdentity(
    db: DbHandle,
    query: QueryHandle,
    identity: Uint8Array,
    opts?: unknown,
  ): Promise<AbiRowBatch[]> {
    return this.call("readAllForIdentity", db, query, identity, opts) as Promise<AbiRowBatch[]>;
  }

  subscribe(db: DbHandle, query: QueryHandle, opts?: unknown): Promise<SubscriptionSnapshot> {
    return this.call("subscribe", db, query, opts) as Promise<SubscriptionSnapshot>;
  }

  subscriptionCurrent(subscription: SubscriptionHandle): Promise<AbiRowBatch[]> {
    return this.call("subscriptionCurrent", subscription) as Promise<AbiRowBatch[]>;
  }

  unsubscribe(subscription: SubscriptionHandle): Promise<void> {
    return this.callVoid("unsubscribe", subscription);
  }

  canInsertEncoded(db: DbHandle, table: string, cells: Uint8Array): Promise<boolean> {
    return this.call("canInsertEncoded", db, table, cells) as Promise<boolean>;
  }

  canUpdateEncodedForIdentity(
    db: DbHandle,
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    identity: Uint8Array,
  ): Promise<boolean> {
    return this.call(
      "canUpdateEncodedForIdentity",
      db,
      table,
      rowId,
      patch,
      identity,
    ) as Promise<boolean>;
  }

  insertEncoded(db: DbHandle, table: string, cells: Uint8Array): Promise<WriteHandle> {
    return this.call("insertEncoded", db, table, cells) as Promise<WriteHandle>;
  }

  insertWithIdEncoded(
    db: DbHandle,
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
  ): Promise<WriteHandle> {
    return this.call("insertWithIdEncoded", db, table, rowId, cells) as Promise<WriteHandle>;
  }

  updateEncoded(
    db: DbHandle,
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
  ): Promise<WriteHandle> {
    return this.call("updateEncoded", db, table, rowId, patch) as Promise<WriteHandle>;
  }

  deleteRow(db: DbHandle, table: string, rowId: Uint8Array): Promise<WriteHandle> {
    return this.call("deleteRow", db, table, rowId) as Promise<WriteHandle>;
  }

  writeState(write: WriteHandle): Promise<WriteState> {
    return this.call("writeState", write) as Promise<WriteState>;
  }

  waitWrite(write: WriteHandle, tier: string): Promise<void> {
    return this.callVoid("waitWrite", write, tier);
  }

  connectTransport(
    db: DbHandle,
    direction = "upstream",
    hints = new Uint8Array(),
  ): Promise<TransportHandle> {
    return this.call("connectTransport", db, direction, hints) as Promise<TransportHandle>;
  }

  transportSendWireFrame(transport: TransportHandle, frame: Uint8Array): Promise<void> {
    return this.callVoid("transportSendWireFrame", transport, frame);
  }

  transportRecvWireFrames(
    transport: TransportHandle,
    budget: { max_frames: number; max_bytes: number },
  ): Promise<Uint8Array[]> {
    return this.call("transportRecvWireFrames", transport, budget) as Promise<Uint8Array[]>;
  }

  transportTick(transport: TransportHandle): Promise<{ watch_wakes?: number } | undefined> {
    return this.call("transportTick", transport) as Promise<{ watch_wakes?: number } | undefined>;
  }

  transportClose(transport: TransportHandle): Promise<void> {
    return this.callVoid("transportClose", transport);
  }

  destroyBrowserStorage(namespace: string): Promise<void> {
    return this.callVoid("destroyBrowserStorage", namespace);
  }

  release(handle: Handle): Promise<void> {
    return this.callVoid("release", handle);
  }

  async shutdown(): Promise<void> {
    try {
      const response = await this.request({ id: this.takeMessageId(), type: "shutdown" });
      if (response.type !== "closed")
        throw new Error(`browser db worker shutdown failed: ${JSON.stringify(response)}`);
    } finally {
      this.rejectPending(new Error("browser db worker shutdown"));
    }
  }

  handleResponse(response: BrowserDbResponse): void {
    const pending = this.takePending(response.id);
    if (!pending) return;
    pending.resolve(response);
  }

  rejectPending(error: Error): void {
    for (const [id, pending] of this.pending) {
      this.pending.delete(id);
      if (pending.timeout) clearTimeout(pending.timeout);
      pending.reject(error);
    }
  }

  private async callVoid(operation: BrowserDbOperation, ...args: unknown[]): Promise<void> {
    await this.call(operation, ...args);
  }

  private async call(operation: BrowserDbOperation, ...args: unknown[]): Promise<unknown> {
    const response = await this.request({
      id: this.takeMessageId(),
      type: "call",
      operation,
      args,
    });
    if (response.type === "error") throw new Error(response.error);
    if (response.type !== "result")
      throw new Error(`unexpected browser db response ${JSON.stringify(response)}`);
    return response.result;
  }

  private request(request: BrowserDbRequest): Promise<BrowserDbResponse> {
    return new Promise((resolve, reject) => {
      const pending: PendingRequest = { resolve, reject };
      if (this.requestTimeoutMs > 0) {
        pending.timeout = setTimeout(() => {
          if (this.takePending(request.id)) {
            reject(
              new Error(
                `browser db request ${request.id} timed out after ${this.requestTimeoutMs}ms`,
              ),
            );
          }
        }, this.requestTimeoutMs);
      }
      this.pending.set(request.id, pending);
      try {
        this.endpoint.postMessage(request);
      } catch (error) {
        this.takePending(request.id);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  private takePending(id: number): PendingRequest | undefined {
    const pending = this.pending.get(id);
    if (!pending) return undefined;
    this.pending.delete(id);
    if (pending.timeout) clearTimeout(pending.timeout);
    return pending;
  }

  private takeMessageId(): number {
    return this.nextMessageId++;
  }
}

export function rowsFromPayload(value: unknown): AbiRowBatch[] {
  return new PostcardReader(assertBytes(value, "rows payload")).readVec(readAbiRowBatch);
}

export function rowsFromDeltaPayload(value: unknown): AbiRowBatch[] {
  const reader = new PostcardReader(assertBytes(value, "delta payload"));
  return [...reader.readVec(readAbiRowBatch), ...reader.readVec(readAbiRowBatch)];
}

export function normalizeBytes(value: unknown): Uint8Array {
  return assertBytes(value, "payload");
}

export function reviveArgs(args: unknown[]): unknown[] {
  return args.map(reviveBytes);
}

export function reviveBytes(value: unknown): unknown {
  if (value instanceof Uint8Array) return value;
  if (isPayloadBytes(value)) return normalizeBytes(value.payload);
  if (Array.isArray(value)) return value.map(reviveBytes);
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(
      value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
    );
  }
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, reviveBytes(entry)]),
    );
  }
  return value;
}

function isPayloadBytes(value: unknown): value is { type: "payload"; payload: unknown } {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { type?: unknown }).type === "payload" &&
    "payload" in value
  );
}
