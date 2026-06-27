import type {
  DirectInsertResult,
  DirectMutationResult,
  MutationErrorEvent,
  Runtime,
} from "../client.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { InsertValues, Value, WasmSchema } from "../../drivers/types.js";
import { encodeCellsForPatch, encodeCellsForRow, formatUuid, parseUuid } from "./runtime.js";

type PendingCall = {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
};

type WorkerResponse =
  | { id: number; ok: true; result: unknown }
  | { id: number; ok: false; error: { name?: string; message?: string } }
  | { subscription: number; args: unknown[] }
  | { event: "mutationError"; payload: MutationErrorEvent }
  | { event: "authFailure"; reason: string };

type OpenRequest = {
  id: number;
  method: "open";
  args: [
    runtimeSources: RuntimeSourcesConfig | undefined,
    dbName: string,
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
  ];
};

type WriteRequest =
  | {
      id: number;
      method: "insert";
      args: [
        table: string,
        values: InsertValues,
        writeContext: string | null | undefined,
        objectId: string,
      ];
    }
  | {
      id: number;
      method: "restore";
      args: [
        table: string,
        objectId: string,
        values: InsertValues,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "update";
      args: [
        table: string,
        objectId: string,
        values: Record<string, Value>,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "upsert";
      args: [
        table: string,
        objectId: string,
        values: InsertValues,
        writeContext: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "delete";
      args: [table: string, objectId: string, writeContext: string | null | undefined];
    };

type WorkerRequest =
  | OpenRequest
  | WriteRequest
  | {
      id: number;
      method: "waitForTransaction";
      args: [transactionId: string, tier: string];
    }
  | {
      id: number;
      method: "query";
      args: [
        queryJson: string,
        sessionJson: string | null | undefined,
        tier: string | null | undefined,
        optionsJson: string | null | undefined,
      ];
    }
  | {
      id: number;
      method: "createSubscription";
      args: [
        queryJson: string,
        sessionJson: string | null | undefined,
        tier: string | null | undefined,
        optionsJson: string | null | undefined,
      ];
    }
  | { id: number; method: "executeSubscription"; args: [handle: number] }
  | { id: number; method: "unsubscribe"; args: [handle: number] }
  | { id: number; method: "close"; args: [] }
  | { id: number; method: "clearClientStorage"; args: [] }
  | { id: number; method: "connect"; args: [url: string, authJson: string] }
  | { id: number; method: "disconnect"; args: [] }
  | { id: number; method: "updateAuth"; args: [authJson: string] };

type WorkerMethod = WorkerRequest["method"];
type RequestForMethod<Method extends WorkerMethod> = Extract<WorkerRequest, { method: Method }>;
type RequestArgs<Method extends WorkerMethod> = RequestForMethod<Method>["args"];

export type { WorkerRequest as PersistentBrowserWorkerRequest };

export class PersistentBrowserRuntime implements Runtime {
  private readonly worker: Worker;
  private readonly pending = new Map<number, PendingCall>();
  private readonly writes = new Map<string, Promise<unknown>>();
  private readonly subscriptions = new Map<number, Function>();
  private readonly remoteSubscriptions = new Map<number, Promise<number>>();
  private readonly subscriptionLocalHandles = new Map<number, number>();
  private mutationErrorCallback: ((event: MutationErrorEvent) => void) | undefined;
  private authFailureCallback: ((reason: string) => void) | undefined;
  private nextCallId = 1;
  private nextSubscriptionId = 1;
  private closed = false;
  private readonly opened: Promise<void>;

  constructor(
    runtimeSources: RuntimeSourcesConfig | undefined,
    private readonly schema: WasmSchema,
    dbName: string,
    private readonly node: Uint8Array,
    private readonly author: Uint8Array,
  ) {
    this.worker = new Worker(new URL("./persistent-browser-worker.js", import.meta.url), {
      type: "module",
    });
    this.worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      this.handleWorkerMessage(event.data);
    };
    this.worker.onerror = (event) => {
      this.rejectAll(new Error(event.message));
    };
    this.opened = this.call("open", runtimeSources, dbName, schema, node, author).then(
      () => undefined,
    );
  }

  insert(
    table: string,
    values: InsertValues,
    writeContext?: string | null,
    objectId?: string | null,
  ): DirectInsertResult {
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "insert", table, values, writeContext, formatUuid(rowId));
    return {
      id: formatUuid(rowId),
      values: valuesForRow(this.schema, table, values),
      transactionId,
    };
  }

  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): DirectInsertResult {
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "restore", table, objectId, values, writeContext);
    return { id: objectId, values: valuesForRow(this.schema, table, values), transactionId };
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): DirectMutationResult {
    encodeCellsForPatch(tableDefinition(this.schema, table), values);
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "update", table, objectId, values, writeContext);
    return { transactionId };
  }

  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): DirectMutationResult {
    encodeCellsForRow(tableDefinition(this.schema, table), values);
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "upsert", table, objectId, values, writeContext);
    return { transactionId };
  }

  delete(table: string, objectId: string, writeContext?: string | null): DirectMutationResult {
    tableDefinition(this.schema, table);
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "delete", table, objectId, writeContext);
    return { transactionId };
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    this.mutationErrorCallback = callback;
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    await this.opened;
    await this.writes.get(transactionId);
    await this.call("waitForTransaction", transactionId, tier);
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    await this.opened;
    return this.call("query", queryJson, sessionJson, tier, optionsJson);
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    const localHandle = this.nextSubscriptionId++;
    const remoteHandle = this.opened.then(
      () =>
        this.call(
          "createSubscription",
          queryJson,
          sessionJson,
          tier,
          optionsJson,
        ) as Promise<number>,
    );
    void remoteHandle.then((remote) => {
      this.subscriptionLocalHandles.set(remote, localHandle);
    });
    this.remoteSubscriptions.set(localHandle, remoteHandle);
    return localHandle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    this.subscriptions.set(handle, onUpdate);
    const remoteHandle = this.remoteSubscriptions.get(handle);
    if (!remoteHandle) return;
    void remoteHandle.then((remote) => this.call("executeSubscription", remote));
  }

  unsubscribe(handle: number): void {
    this.subscriptions.delete(handle);
    const remoteHandle = this.remoteSubscriptions.get(handle);
    this.remoteSubscriptions.delete(handle);
    if (remoteHandle) void remoteHandle.then((remote) => this.call("unsubscribe", remote));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    try {
      await this.call("close");
    } finally {
      this.closed = true;
      this.worker.terminate();
      this.rejectAll(new Error("Persistent browser core runtime closed"));
    }
  }

  async clearClientStorage(): Promise<void> {
    if (this.closed) return;
    try {
      await this.call("clearClientStorage");
    } finally {
      this.closed = true;
      this.worker.terminate();
      this.rejectAll(new Error("Persistent browser core runtime storage was cleared"));
    }
  }

  connect(url: string, authJson: string): void {
    this.fireAndForget("connect", url, authJson);
  }

  disconnect(): void {
    this.fireAndForget("disconnect");
  }

  updateAuth(authJson: string): void {
    this.fireAndForget("updateAuth", authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  private writeId(): string {
    return `worker-write-${this.nextCallId++}`;
  }

  private call<Method extends WorkerMethod>(
    method: Method,
    ...args: RequestArgs<Method>
  ): Promise<unknown> {
    return this.send(method, args);
  }

  private send(method: WorkerMethod, args: readonly unknown[]): Promise<unknown> {
    if (this.closed && method !== "close") {
      return Promise.reject(new Error("Persistent browser core runtime is closed"));
    }
    const id = this.nextCallId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, method, args } as WorkerRequest);
    });
  }

  private fireAndForget<Method extends WorkerMethod>(
    method: Method,
    ...args: RequestArgs<Method>
  ): void {
    if (this.closed) return;
    void this.opened
      .then(() => {
        if (!this.closed) return this.send(method, args);
      })
      .catch(() => undefined);
  }

  private queueWrite<Method extends WriteRequest["method"]>(
    transactionId: string,
    method: Method,
    ...args: RequestArgs<Method>
  ): void {
    // Public writes stay synchronous for React/local state ergonomics; the
    // worker owns OPFS durability and settles the returned write handle.
    const write = this.opened.then(() => this.send(method, args));
    this.writes.set(transactionId, write);
    void write.catch(() => undefined);
  }

  private handleWorkerMessage(message: WorkerResponse): void {
    if ("event" in message) {
      try {
        if (message.event === "mutationError") {
          this.mutationErrorCallback?.(message.payload);
        } else {
          this.authFailureCallback?.(message.reason);
        }
      } catch (error) {
        setTimeout(() => {
          throw error;
        }, 0);
      }
      return;
    }
    if ("subscription" in message) {
      const callback = this.subscriptions.get(
        this.subscriptionLocalHandles.get(message.subscription) ?? message.subscription,
      );
      callback?.(...message.args);
      return;
    }
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    if (message.ok) {
      pending.resolve(message.result);
    } else {
      pending.reject(new Error(message.error.message ?? "Persistent browser worker call failed"));
    }
  }

  private rejectAll(error: Error): void {
    for (const pending of this.pending.values()) {
      pending.reject(error);
    }
    this.pending.clear();
  }
}

function valuesForRow(schema: WasmSchema, table: string, values: InsertValues): Value[] {
  const definition = tableDefinition(schema, table);
  encodeCellsForRow(definition, values);
  return definition.columns.map(
    (column) => values[column.name] ?? column.default ?? { type: "Null" },
  );
}

function tableDefinition(schema: WasmSchema, table: string): WasmSchema[string] {
  const definition = schema[table];
  if (!definition) throw new Error(`unknown table ${table}`);
  return definition;
}
