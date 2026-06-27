import type { InsertResult, MutationResult, Runtime } from "../client.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { InsertValues, Value, WasmSchema } from "../../drivers/types.js";
import type {
  PersistentBrowserOpfsOwnerRequest,
  PersistentBrowserRequestArgs,
  PersistentBrowserWorkerMethod,
  PersistentBrowserWriteRequest,
} from "./persistent-browser-protocol.js";
import {
  encodeCellsForPatch,
  encodeCellsForRow,
  formatUuid,
  parseUuid,
} from "./native-runtime-adapter.js";

type PendingCall = {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
};

type WorkerResponse =
  | { id: number; ok: true; result: unknown }
  | { id: number; ok: false; error: { name?: string; message?: string } }
  | { subscription: number; args: unknown[] }
  | { event: "authFailure"; reason: string };

export type { PersistentBrowserOpfsOwnerRequest } from "./persistent-browser-protocol.js";

export class PersistentBrowserOpfsRuntime implements Runtime {
  private readonly worker: Worker;
  private readonly pending = new Map<number, PendingCall>();
  // Runtime writes are synchronous, but the worker owns the NativeRuntimeAdapter that can
  // produce the real core transaction id. These ids are pending handles
  // that are only valid for waitForTransaction translation below.
  private readonly writes = new Map<string, Promise<string>>();
  private readonly subscriptions = new Map<number, Function>();
  private readonly remoteSubscriptions = new Map<number, Promise<number>>();
  private readonly subscriptionLocalHandles = new Map<number, number>();
  private authFailureCallback: ((reason: string) => void) | undefined;
  private nextCallId = 1;
  private nextSubscriptionId = 1;
  private closed = false;
  private closing = false;
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
      if (
        this.closing ||
        this.closed ||
        event.message.includes("Persistent browser native runtime closed")
      ) {
        this.resolveAll();
        return;
      }
      this.rejectAll(new Error(event.message));
    };
    this.opened = this.send("open", [runtimeSources, dbName, schema, node, author]).then(
      () => undefined,
    );
  }

  insert(
    table: string,
    values: InsertValues,
    writeContext?: string | null,
    objectId?: string | null,
  ): InsertResult {
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
  ): InsertResult {
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "restore", table, objectId, values, writeContext);
    return { id: objectId, values: valuesForRow(this.schema, table, values), transactionId };
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): MutationResult {
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
  ): MutationResult {
    encodeCellsForRow(tableDefinition(this.schema, table), values);
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "upsert", table, objectId, values, writeContext);
    return { transactionId };
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    tableDefinition(this.schema, table);
    const transactionId = this.writeId();
    this.queueWrite(transactionId, "delete", table, objectId, writeContext);
    return { transactionId };
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    await this.opened;
    const pendingWrite = this.writes.get(transactionId);
    const workerTransactionId = pendingWrite ? await pendingWrite : transactionId;
    await this.send("waitForTransaction", [workerTransactionId, tier]);
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    await this.opened;
    return this.send("query", [queryJson, sessionJson, tier, optionsJson]);
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
        this.send("createSubscription", [
          queryJson,
          sessionJson,
          tier,
          optionsJson,
        ]) as Promise<number>,
    );
    void remoteHandle
      .then((remote) => {
        this.subscriptionLocalHandles.set(remote, localHandle);
      })
      .catch(ignoreExpectedShutdown);
    void remoteHandle.catch(ignoreExpectedShutdown);
    this.remoteSubscriptions.set(localHandle, remoteHandle);
    return localHandle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    this.subscriptions.set(handle, onUpdate);
    const remoteHandle = this.remoteSubscriptions.get(handle);
    if (!remoteHandle) return;
    void remoteHandle
      .then((remote) => this.send("executeSubscription", [remote]))
      .catch(ignoreExpectedShutdown);
  }

  unsubscribe(handle: number): void {
    this.subscriptions.delete(handle);
    const remoteHandle = this.remoteSubscriptions.get(handle);
    this.remoteSubscriptions.delete(handle);
    if (remoteHandle) {
      void remoteHandle
        .then((remote) => this.send("unsubscribe", [remote]))
        .catch(ignoreExpectedShutdown);
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closing = true;
    this.closed = true;
    this.closing = false;
    this.worker.terminate();
    this.resolveAll();
  }

  async clearClientStorage(): Promise<void> {
    if (this.closed) return;
    this.closing = true;
    try {
      await this.send("clearClientStorage", []);
    } catch (error) {
      if (!isExpectedShutdownError(error)) throw error;
    } finally {
      this.closed = true;
      this.closing = false;
      this.worker.terminate();
      this.resolveAll();
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
    return `pending-worker-write-${this.nextCallId++}`;
  }

  private send<Method extends PersistentBrowserWorkerMethod>(
    method: Method,
    args: PersistentBrowserRequestArgs<Method>,
  ): Promise<unknown> {
    if (this.closed) {
      return Promise.reject(new Error("Persistent browser native runtime is closed"));
    }
    const id = this.nextCallId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, method, args } as PersistentBrowserOpfsOwnerRequest);
    });
  }

  private fireAndForget<Method extends PersistentBrowserWorkerMethod>(
    method: Method,
    ...args: PersistentBrowserRequestArgs<Method>
  ): void {
    if (this.closed) return;
    void this.opened
      .then(() => {
        if (!this.closed) return this.send(method, args);
      })
      .catch(() => undefined);
  }

  private queueWrite<Method extends PersistentBrowserWriteRequest["method"]>(
    transactionId: string,
    method: Method,
    ...args: PersistentBrowserRequestArgs<Method>
  ): void {
    // The worker owns the real NativeRuntimeAdapter, so durability waits must use the
    // worker's transaction id. The public Runtime API is synchronous, so the
    // result returned from insert/update/etc. is only a pending handle.
    const write = this.opened.then(async () => {
      const result = (await this.send(method, args)) as { transactionId: string };
      if (!result || typeof result.transactionId !== "string") {
        throw new Error("Persistent browser worker write did not return a transaction id");
      }
      return result.transactionId;
    });
    this.writes.set(transactionId, write);
    void write.catch(() => undefined);
  }

  private handleWorkerMessage(message: WorkerResponse): void {
    if ("event" in message) {
      try {
        this.authFailureCallback?.(message.reason);
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

  private resolveAll(): void {
    for (const pending of this.pending.values()) {
      pending.resolve(undefined);
    }
    this.pending.clear();
  }
}

function ignoreExpectedShutdown(error: unknown): void {
  if (isExpectedShutdownError(error)) {
    return;
  }
  setTimeout(() => {
    throw error;
  }, 0);
}

function isExpectedShutdownError(error: unknown): boolean {
  return error instanceof Error && error.message.includes("Persistent browser native runtime");
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
