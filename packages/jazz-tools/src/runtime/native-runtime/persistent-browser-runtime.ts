import type { InsertResult, MutationResult, Runtime, TransactionKind } from "../client.js";
import type { NativeRowDelta } from "../../drivers/types.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { InsertValues, Value, WasmSchema } from "../../drivers/types.js";
import type {
  PersistentBrowserSubscriptionMessage,
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
  | PersistentBrowserSubscriptionMessage
  | { event: "authFailure"; reason: string };

type CompletedTxState = "committed" | "rolled_back";

export type { PersistentBrowserOpfsOwnerRequest } from "./persistent-browser-protocol.js";

export class PersistentBrowserOpfsRuntime implements Runtime {
  private readonly worker: Worker;
  private readonly pending = new Map<number, PendingCall>();
  // Runtime writes are synchronous, but the worker owns the NativeRuntimeAdapter that can
  // produce the real core transaction id. These ids are pending handles
  // that are only valid for waitForTransaction translation below.
  private readonly writes = new Map<string, Promise<string>>();
  private readonly settledWrites = new Map<string, Map<string, Promise<void>>>();
  private readonly transactionRemoteIds = new Map<string, Promise<string>>();
  private readonly transactionWrites = new Map<string, Promise<string>[]>();
  private readonly completedTxs = new Map<string, CompletedTxState>();
  private readonly readOnlyCommittedTxs = new Set<string>();
  private readonly commitErrors = new Map<string, Error>();
  private readonly subscriptions = new Map<number, Function>();
  private readonly remoteSubscriptions = new Map<number, Promise<number>>();
  private authFailureCallback: ((reason: string) => void) | undefined;
  private connectionReady: Promise<unknown> | null = null;
  private pagehideAbort: AbortController | null = null;
  private nextCallId = 1;
  private nextSubscriptionId = 1;
  private closed = false;
  private closing = false;
  private readonly opened: Promise<void>;

  constructor(
    private readonly runtimeSources: RuntimeSourcesConfig | undefined,
    private readonly schema: WasmSchema,
    private readonly dbName: string,
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
    if (typeof window !== "undefined") {
      this.pagehideAbort = new AbortController();
      window.addEventListener(
        "pagehide",
        () => {
          void this.close();
        },
        { signal: this.pagehideAbort.signal },
      );
    }
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
    try {
      encodeCellsForRow(tableDefinition(this.schema, table), values);
    } catch (error) {
      throw new Error(normalizeWriteSetupMessage(errorMessage(error)));
    }
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
    if (tier === "edge" || tier === "global") {
      await this.connectionReady;
    }
    const pendingWrite = this.writes.get(transactionId);
    const workerTransactionId = pendingWrite ? await pendingWrite : transactionId;
    const commitError = this.commitErrors.get(transactionId);
    if (commitError) {
      throw commitError;
    }
    if (this.readOnlyCommittedTxs.has(transactionId)) {
      return;
    }
    const wait = this.send("waitForTransaction", [workerTransactionId, tier]).then(() => undefined);
    let waits = this.settledWrites.get(transactionId);
    if (!waits) {
      waits = new Map();
      this.settledWrites.set(transactionId, waits);
    }
    waits.set(tier, wait);
    await wait;
  }

  beginTransaction(kind: TransactionKind): string {
    const transactionId = `pending-worker-tx-${this.nextCallId++}`;
    const workerTransactionId = this.opened.then(
      () => this.send("beginTransaction", [kind]) as Promise<string>,
    );
    this.writes.set(transactionId, workerTransactionId);
    this.transactionRemoteIds.set(transactionId, workerTransactionId);
    void workerTransactionId.catch(() => undefined);
    return transactionId;
  }

  commitTransaction(transactionId: string): void {
    if (this.completedTxs.has(transactionId)) {
      throw new Error(commitTransactionMessage(transactionId, this.completedTxs));
    }
    const workerTransactionId = this.writes.get(transactionId);
    if (!workerTransactionId) {
      throw new Error(commitTransactionMessage(transactionId, this.completedTxs));
    }
    const transactionWrites = this.transactionWrites.get(transactionId) ?? [];
    if (transactionWrites.length === 0) {
      this.readOnlyCommittedTxs.add(transactionId);
    }
    const committed = workerTransactionId.then(async (remote) => {
      await Promise.all(transactionWrites);
      this.transactionWrites.delete(transactionId);
      this.transactionRemoteIds.delete(transactionId);
      try {
        await this.send("commitTransaction", [remote]);
      } catch (error) {
        this.commitErrors.set(
          transactionId,
          error instanceof Error ? error : new Error(String(error)),
        );
      }
      return remote;
    });
    this.writes.set(transactionId, committed);
    this.completedTxs.set(transactionId, "committed");
    void committed.catch(() => undefined);
  }

  rollbackTransaction(transactionId: string): void {
    if (this.completedTxs.has(transactionId)) {
      throw new Error(rollbackTransactionMessage(transactionId, this.completedTxs));
    }
    const workerTransactionId = this.writes.get(transactionId);
    if (!workerTransactionId) {
      throw new Error(rollbackTransactionMessage(transactionId, this.completedTxs));
    }
    this.transactionWrites.delete(transactionId);
    this.transactionRemoteIds.delete(transactionId);
    void workerTransactionId
      .then((remote) => this.send("rollbackTransaction", [remote]))
      .catch(ignoreExpectedShutdown);
    this.writes.delete(transactionId);
    this.completedTxs.set(transactionId, "rolled_back");
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    const readFence = this.captureReadFence(optionsJson);
    await this.opened;
    this.assertReadTransactionOpen(optionsJson);
    await this.settleReadFence(readFence);
    const translatedOptionsJson = await this.prepareReadOptions(optionsJson);
    if (requiresServerPropagation(tier, optionsJson)) {
      await this.connectionReady;
      await this.settleServerWaitsForRead(tier);
    }
    return this.send("query", [queryJson, sessionJson, tier, translatedOptionsJson]);
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    const localHandle = this.nextSubscriptionId++;
    const readFence = this.captureReadFence(optionsJson);
    const remoteHandle = this.opened.then(async () => {
      this.assertReadTransactionOpen(optionsJson);
      await this.settleReadFence(readFence);
      const translatedOptionsJson = await this.prepareReadOptions(optionsJson);
      if (requiresServerPropagation(tier, optionsJson)) {
        await this.connectionReady;
        await this.settleServerWaitsForRead(tier);
      }
      return this.send(
        "createExecutedSubscription",
        [localHandle, queryJson, sessionJson, tier, translatedOptionsJson],
        {
          query: queryJson,
          debugName: subscriptionDebugName(queryJson),
        },
      ) as Promise<number>;
    });
    void remoteHandle.catch(ignoreExpectedShutdown);
    this.remoteSubscriptions.set(localHandle, remoteHandle);
    return localHandle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    this.subscriptions.set(handle, onUpdate);
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
    try {
      await this.opened;
      await Promise.allSettled(this.writes.values());
      await this.send("close", []);
    } finally {
      this.closed = true;
      this.closing = false;
      this.pagehideAbort?.abort();
      this.pagehideAbort = null;
      this.worker.terminate();
      this.resolveAll();
    }
  }

  async clearClientStorage(): Promise<void> {
    if (this.closed) return;
    this.closing = true;
    let namespace = this.dbName;
    try {
      await this.opened;
      await Promise.allSettled(this.writes.values());
      namespace = (await this.send("closeForStorageClear", [])) as string;
    } catch (error) {
      if (!isExpectedShutdownError(error)) throw error;
    } finally {
      this.closed = true;
      this.closing = false;
      this.pagehideAbort?.abort();
      this.pagehideAbort = null;
      this.worker.terminate();
      this.resolveAll();
    }
    await destroyBrowserStorage(this.runtimeSources, namespace);
  }

  connect(url: string, authJson: string): void {
    this.connectionReady = this.opened.then(() => {
      if (this.closed) return undefined;
      return this.send("connect", [url, authJson]);
    });
    void this.connectionReady.catch(ignoreExpectedShutdown);
  }

  disconnect(): Promise<void> {
    this.connectionReady = null;
    if (this.closed) return Promise.resolve();
    return this.opened
      .then(() => {
        if (this.closed) return undefined;
        return this.send("disconnect", []);
      })
      .then(() => undefined);
  }

  updateAuth(authJson: string): void {
    this.connectionReady = this.opened.then(() => {
      if (this.closed) return undefined;
      return this.send("updateAuth", [authJson]);
    });
    void this.connectionReady.catch(ignoreExpectedShutdown);
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
    metadata?: Partial<PersistentBrowserOpfsOwnerRequest>,
  ): Promise<unknown> {
    if (this.closed) {
      return Promise.reject(new Error("Persistent browser native runtime is closed"));
    }
    const id = this.nextCallId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({
        id,
        method,
        args,
        ...metadata,
      } as PersistentBrowserOpfsOwnerRequest);
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
    const batchId = this.batchIdFromWriteArgs(method, args);
    if (batchId && this.completedTxs.has(batchId)) {
      throw new Error(
        `${writeOperationName(method)} failed: WriteError("${txStateMessage(batchId, this.completedTxs)}")`,
      );
    }
    // The worker owns the real NativeRuntimeAdapter, so durability waits must use the
    // worker's transaction id. The public Runtime API is synchronous, so the
    // result returned from insert/update/etc. is only a pending handle.
    const write = this.opened.then(async () => {
      if (batchId && this.completedTxs.get(batchId) === "rolled_back") {
        return batchId;
      }
      const translatedArgs = (await this.translateWriteArgs(
        method,
        args,
      )) as PersistentBrowserRequestArgs<Method>;
      const result = (await this.send(method, translatedArgs)) as { transactionId: string };
      if (!result || typeof result.transactionId !== "string") {
        throw new Error("Persistent browser worker write did not return a transaction id");
      }
      return result.transactionId;
    });
    this.writes.set(transactionId, write);
    if (batchId) {
      const writes = this.transactionWrites.get(batchId) ?? [];
      writes.push(write);
      this.transactionWrites.set(batchId, writes);
    }
    void write.catch(() => undefined);
  }

  private batchIdFromWriteArgs<Method extends PersistentBrowserWriteRequest["method"]>(
    method: Method,
    args: PersistentBrowserRequestArgs<Method>,
  ): string | undefined {
    const writeContextIndex = method === "delete" ? 2 : method === "insert" ? 2 : 3;
    const writeContext = (args as unknown[])[writeContextIndex] as string | null | undefined;
    if (!writeContext) return undefined;
    try {
      const parsed = JSON.parse(writeContext) as { batch_id?: unknown };
      return typeof parsed.batch_id === "string" ? parsed.batch_id : undefined;
    } catch {
      return undefined;
    }
  }

  private async translateWriteArgs<Method extends PersistentBrowserWriteRequest["method"]>(
    method: Method,
    args: PersistentBrowserRequestArgs<Method>,
  ): Promise<PersistentBrowserRequestArgs<Method>> {
    const mutable = [...args] as unknown[];
    const writeContextIndex = method === "delete" ? 2 : method === "insert" ? 2 : 3;
    mutable[writeContextIndex] = await this.translateWriteContext(
      mutable[writeContextIndex] as string | null | undefined,
    );
    return mutable as PersistentBrowserRequestArgs<Method>;
  }

  private async translateWriteContext(
    writeContext: string | null | undefined,
  ): Promise<string | null | undefined> {
    if (!writeContext) return writeContext;
    let parsed: { batch_id?: unknown };
    try {
      parsed = JSON.parse(writeContext) as { batch_id?: unknown };
    } catch {
      return writeContext;
    }
    if (typeof parsed.batch_id !== "string") return writeContext;
    const workerTransactionId = this.transactionRemoteIds.get(parsed.batch_id);
    if (!workerTransactionId) return writeContext;
    return JSON.stringify({ ...parsed, batch_id: await workerTransactionId });
  }

  private async prepareReadOptions(
    optionsJson: string | null | undefined,
  ): Promise<string | null | undefined> {
    if (!optionsJson) return optionsJson;
    let parsed: { transaction_batch_id?: unknown };
    try {
      parsed = JSON.parse(optionsJson) as { transaction_batch_id?: unknown };
    } catch {
      return optionsJson;
    }
    if (typeof parsed.transaction_batch_id !== "string") return optionsJson;
    const workerTransactionId = this.transactionRemoteIds.get(parsed.transaction_batch_id);
    if (!workerTransactionId) return optionsJson;
    return JSON.stringify({ ...parsed, transaction_batch_id: await workerTransactionId });
  }

  private captureReadFence(optionsJson: string | null | undefined): Promise<unknown>[] {
    const transactionId = transactionIdFromReadOptions(optionsJson);
    if (transactionId) {
      return [...(this.transactionWrites.get(transactionId) ?? [])];
    }
    return [...this.writes.values()];
  }

  private async settleReadFence(fence: readonly Promise<unknown>[]): Promise<void> {
    await Promise.all(fence);
  }

  private async settleServerWaitsForRead(tier: string | null | undefined): Promise<void> {
    if (tier !== "edge" && tier !== "global") return;
    const waits: Promise<void>[] = [];
    for (const writeWaits of this.settledWrites.values()) {
      const globalWait = writeWaits.get("global");
      if (globalWait) {
        waits.push(globalWait);
        continue;
      }
      const edgeWait = writeWaits.get("edge");
      if (edgeWait) waits.push(edgeWait);
    }
    await Promise.all(waits);
  }

  private assertReadTransactionOpen(optionsJson: string | null | undefined): void {
    const transactionId = transactionIdFromReadOptions(optionsJson);
    if (!transactionId || !this.completedTxs.has(transactionId)) return;
    throw new Error(
      `Query setup failed: Write error: ${txStateMessage(transactionId, this.completedTxs)}`,
    );
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
      const callback = this.subscriptions.get(message.subscription);
      if ("error" in message) {
        callback?.(
          new Error(message.error.message ?? "Persistent browser subscription failed"),
          null,
        );
      } else {
        callback?.(nativeDeltaFromFrame(message));
      }
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

function nativeDeltaFromFrame(
  message: Extract<PersistentBrowserSubscriptionMessage, { frame: unknown }>,
): NativeRowDelta {
  if (message.frame.kind !== "native-row-delta") {
    throw new Error(`Unknown persistent browser subscription frame ${message.frame.kind}`);
  }
  return {
    __jazzNativeRowDelta: true,
    reset: message.frame.reset,
    added: new Uint8Array(message.frame.added),
    removed: new Uint8Array(message.frame.removed),
    updated: new Uint8Array(message.frame.updated),
    addedCount: message.frame.addedCount,
    removedCount: message.frame.removedCount,
    updatedCount: message.frame.updatedCount,
  };
}

function subscriptionDebugName(queryJson: string): string {
  try {
    const query = JSON.parse(queryJson) as {
      table?: unknown;
      relation_ir?: { table?: unknown };
      debugName?: unknown;
    };
    if (typeof query.debugName === "string" && query.debugName.trim()) {
      return query.debugName;
    }
    const table = typeof query.table === "string" ? query.table : query.relation_ir?.table;
    if (typeof table === "string" && table.trim()) return table;
  } catch {
    // Fall through to the bounded raw query label below.
  }
  return queryJson.length > 120 ? `${queryJson.slice(0, 117)}...` : queryJson;
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

function destroyBrowserStorage(
  runtimeSources: RuntimeSourcesConfig | undefined,
  dbName: string,
): Promise<void> {
  const worker = new Worker(new URL("./persistent-browser-worker.js", import.meta.url), {
    type: "module",
  });
  const id = 1;

  return new Promise((resolve, reject) => {
    const finish = (complete: () => void) => {
      worker.terminate();
      complete();
    };

    worker.onmessage = (event: MessageEvent<WorkerResponse>) => {
      const message = event.data;
      if (!("id" in message) || message.id !== id) return;
      if (message.ok) {
        finish(resolve);
      } else {
        finish(() =>
          reject(new Error(message.error.message ?? "Persistent browser storage destroy failed")),
        );
      }
    };
    worker.onerror = (event) => {
      finish(() => reject(new Error(event.message)));
    };
    worker.postMessage({
      id,
      method: "destroyBrowserStorage",
      args: [runtimeSources, dbName],
    } satisfies PersistentBrowserOpfsOwnerRequest);
  });
}

function requiresServerPropagation(tier?: string | null, optionsJson?: string | null): boolean {
  if (tier === "edge" || tier === "global") return true;
  if (optionsJson == null) return false;
  try {
    const options = JSON.parse(optionsJson) as { propagation?: unknown };
    return options.propagation === "full";
  } catch {
    return false;
  }
}

function transactionIdFromReadOptions(optionsJson: string | null | undefined): string | undefined {
  if (!optionsJson) return undefined;
  try {
    const parsed = JSON.parse(optionsJson) as { transaction_batch_id?: unknown };
    return typeof parsed.transaction_batch_id === "string"
      ? parsed.transaction_batch_id
      : undefined;
  } catch {
    return undefined;
  }
}

function normalizeWriteSetupMessage(message: string): string {
  const missingRequiredColumn = /^missing required column ([A-Za-z_$][\w$]*)$/.exec(message);
  if (missingRequiredColumn) {
    return `missing required field \`${missingRequiredColumn[1]}\``;
  }
  return message;
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (error && typeof error === "object") {
    const message = (error as { message?: unknown }).message;
    if (typeof message === "string" && message.trim()) return message;
  }
  return String(error);
}

function txStateMessage(
  transactionId: string,
  completedTxs: Map<string, CompletedTxState>,
): string {
  if (completedTxs.get(transactionId) === "committed") {
    return `transaction ${transactionId} is already committed`;
  }
  return `transaction ${transactionId} has already been completed or was never opened`;
}

function commitTransactionMessage(
  transactionId: string,
  completedTxs: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(transactionId, completedTxs);
  return completedTxs.get(transactionId) === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

function rollbackTransactionMessage(
  transactionId: string,
  completedTxs: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(transactionId, completedTxs);
  return completedTxs.get(transactionId) === "committed"
    ? `Write error: ${message}`
    : `Rollback transaction failed: Write error: ${message}`;
}

function writeOperationName(method: PersistentBrowserWriteRequest["method"]): string {
  switch (method) {
    case "insert":
    case "restore":
      return "Insert";
    case "update":
    case "upsert":
      return "Update";
    case "delete":
      return "Delete";
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
