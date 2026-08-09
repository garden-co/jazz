import type {
  BatchId,
  InsertResult,
  MutationResult,
  OpenBatchId,
  Runtime,
  TransactionKind,
} from "../client.js";
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
import { setNamedRowValuesEnumerable } from "./row-values-transport.js";

type PendingCall = {
  resolve: (value: unknown) => void;
  reject: (error: unknown) => void;
};

type WorkerResponse =
  | { id: number; ok: true; result: unknown }
  | { id: number; ok: false; error: { name?: string; message?: string; stack?: string } }
  | PersistentBrowserSubscriptionMessage
  | { event: "authFailure"; reason: string };

type CompletedTxState = "committed" | "rolled_back";

type ConnectionGate = {
  promise: Promise<void>;
  resolve: () => void;
  reject: (error: unknown) => void;
};

export type { PersistentBrowserOpfsOwnerRequest } from "./persistent-browser-protocol.js";

export class PersistentBrowserOpfsRuntime implements Runtime {
  private readonly worker: Worker;
  private readonly pending = new Map<number, PendingCall>();
  // Runtime writes are synchronous, but the worker owns the NativeRuntimeAdapter that can
  // produce the real core transaction id. These ids are pending handles
  // that are only valid for waitForTransaction translation below.
  private readonly pendingWrites = new Set<Promise<unknown>>();
  private readonly settledWrites = new Map<string, Map<string, Promise<void>>>();
  private readonly transactionWrites = new Map<string, Promise<unknown>[]>();
  private readonly completedTxs = new Map<string, CompletedTxState>();
  private readonly committingTxs = new Set<OpenBatchId>();
  private readonly rollingBackTxs = new Set<OpenBatchId>();
  private readonly subscriptions = new Map<number, Function>();
  private readonly remoteSubscriptions = new Map<number, Promise<number>>();
  // A public subscription is synchronous at this boundary, while the worker
  // registration is asynchronous. Preserve the caller's program order: a
  // write issued immediately after subscribe must not overtake registration
  // and become invisible to its maintained view.
  private subscriptionRegistration: Promise<void> = Promise.resolve();
  private authFailureCallback: ((reason: string) => void) | undefined;
  // Server-tier operations capture this gate while intentionally disconnected.
  // Reconnect resolves that same gate, so outstanding operations survive instead
  // of observing a replacement promise that can never settle.
  // Before an explicit disconnect there is no reconnect barrier. This preserves
  // local-only runtimes, which never call connect(), while disconnect() below
  // installs the unresolved gate that server-tier work must await.
  private connectionReady = connectionGate(true);
  private waitingForReconnect = false;
  private pagehideAbort: AbortController | null = null;
  private nextCallId = 1;
  private nextSubscriptionId = 1;
  private commandTail: Promise<void> = Promise.resolve();
  private closed = false;
  private closing = false;
  private readonly opened: Promise<void>;

  constructor(
    private readonly runtimeSources: RuntimeSourcesConfig | undefined,
    private readonly schema: WasmSchema,
    private readonly dbName: string,
    private readonly node: Uint8Array,
    private readonly author: Uint8Array,
    private readonly initialSyncFlushEvery: number | undefined = 512,
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
      this.rejectAll(
        new Error(
          `Persistent browser worker error: ${JSON.stringify({
            message: event.message,
            filename: event.filename,
            lineno: event.lineno,
            colno: event.colno,
          })}`,
        ),
      );
    };
    this.opened = this.send("open", [
      runtimeSources,
      dbName,
      schema,
      node,
      author,
      initialSyncFlushEvery,
    ]).then(() => undefined);
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
    const receipt = this.queueWrite("insert", table, values, writeContext, formatUuid(rowId));
    return {
      id: formatUuid(rowId),
      values: valuesForRow(this.schema, table, values),
      ...receipt,
    };
  }

  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): InsertResult {
    const receipt = this.queueWrite("restore", table, objectId, values, writeContext);
    return { id: objectId, values: valuesForRow(this.schema, table, values), ...receipt };
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): MutationResult {
    encodeCellsForPatch(tableDefinition(this.schema, table), values);
    return this.queueWrite("update", table, objectId, values, writeContext);
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
    return this.queueWrite("upsert", table, objectId, values, writeContext);
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    tableDefinition(this.schema, table);
    return this.queueWrite("delete", table, objectId, writeContext);
  }

  waitForTransaction(batchId: BatchId, tier: string): Promise<void> {
    const wait = this.enqueueCommand(async () => {
      await this.opened;
      if (tier === "edge" || tier === "global") await this.connectionReady.promise;
      await this.send("waitForTransaction", [batchId, tier]);
    });
    let waits = this.settledWrites.get(batchId);
    if (!waits) {
      waits = new Map();
      this.settledWrites.set(batchId, waits);
    }
    waits.set(tier, wait);
    return wait;
  }

  beginTransaction(kind: TransactionKind, id: OpenBatchId): OpenBatchId {
    const begun = this.enqueueCommand(
      () => this.send("beginTransaction", [kind, id]) as Promise<OpenBatchId>,
    );
    void begun.catch(ignoreExpectedShutdown);
    return id;
  }

  commitTransaction(openBatchId: OpenBatchId): Promise<BatchId> {
    if (this.rollingBackTxs.has(openBatchId)) {
      throw new Error(`Commit transaction failed: batch ${openBatchId} is already rolling back`);
    }
    if (this.committingTxs.has(openBatchId)) {
      throw new Error(`Commit transaction failed: batch ${openBatchId} is already committing`);
    }
    if (this.completedTxs.has(openBatchId)) {
      throw new Error(commitTransactionMessage(openBatchId, this.completedTxs));
    }
    const transactionWrites = this.transactionWrites.get(openBatchId) ?? [];
    this.committingTxs.add(openBatchId);
    return this.enqueueCommand(async () => {
      await Promise.all(transactionWrites);
      return this.send("commitTransaction", [openBatchId]) as Promise<BatchId>;
    }).then(
      (batchId) => {
        this.committingTxs.delete(openBatchId);
        this.transactionWrites.delete(openBatchId);
        this.completedTxs.set(openBatchId, "committed");
        return batchId;
      },
      (error) => {
        this.committingTxs.delete(openBatchId);
        throw error;
      },
    );
  }

  rollbackTransaction(openBatchId: OpenBatchId): Promise<boolean> {
    if (this.committingTxs.has(openBatchId)) {
      throw new Error(`Rollback transaction failed: batch ${openBatchId} is already committing`);
    }
    if (this.rollingBackTxs.has(openBatchId)) {
      throw new Error(`Rollback transaction failed: batch ${openBatchId} is already rolling back`);
    }
    if (this.completedTxs.has(openBatchId)) {
      throw new Error(rollbackTransactionMessage(openBatchId, this.completedTxs));
    }
    this.rollingBackTxs.add(openBatchId);
    return this.enqueueCommand(
      () => this.send("rollbackTransaction", [openBatchId]) as Promise<boolean>,
    ).then(
      (rolledBack) => {
        this.rollingBackTxs.delete(openBatchId);
        this.transactionWrites.delete(openBatchId);
        this.completedTxs.set(openBatchId, "rolled_back");
        return rolledBack;
      },
      (error) => {
        this.rollingBackTxs.delete(openBatchId);
        throw error;
      },
    );
  }

  query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    const readFence = this.captureReadFence(optionsJson);
    return this.enqueueCommand(async () => {
      await this.opened;
      this.assertReadTransactionOpen(optionsJson);
      await this.settleReadFence(readFence);
      if (requiresServerPropagation(tier, optionsJson)) {
        await this.connectionReady.promise;
        await this.settleServerWaitsForRead(tier);
      }
      return this.send("query", [queryJson, sessionJson, tier, optionsJson]);
    });
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    const localHandle = this.nextSubscriptionId++;
    const readFence = this.captureReadFence(optionsJson);
    const remoteHandle = this.enqueueCommand(async () => {
      this.assertReadTransactionOpen(optionsJson);
      await this.settleReadFence(readFence);
      if (requiresServerPropagation(tier, optionsJson)) {
        await this.connectionReady.promise;
        await this.settleServerWaitsForRead(tier);
      }
      return this.send(
        "createExecutedSubscription",
        [localHandle, queryJson, sessionJson, tier, optionsJson],
        {
          query: queryJson,
          debugName: subscriptionDebugName(queryJson),
        },
      ) as Promise<number>;
    });
    void remoteHandle.catch(ignoreExpectedShutdown);
    this.remoteSubscriptions.set(localHandle, remoteHandle);
    this.subscriptionRegistration = this.subscriptionRegistration
      .catch(() => undefined)
      .then(() => remoteHandle.then(() => undefined).catch(() => undefined));
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
      void this.enqueueCommand(async () => {
        const remote = await remoteHandle;
        await this.send("unsubscribe", [remote]);
      }).catch(ignoreExpectedShutdown);
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closing = true;
    this.rejectConnectionWaiters();
    try {
      await this.enqueueCommand(async () => {
        await this.opened;
        await Promise.allSettled(this.pendingWrites);
        await this.send("close", []);
      });
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
    this.rejectConnectionWaiters();
    let namespace = this.dbName;
    try {
      namespace = await this.enqueueCommand(async () => {
        await this.opened;
        await Promise.allSettled(this.pendingWrites);
        return this.send("closeForStorageClear", []) as Promise<string>;
      });
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
    if (this.closing || this.closed) return;
    const gate = this.waitingForReconnect ? this.connectionReady : connectionGate();
    this.connectionReady = gate;
    const connected = this.opened.then(() => {
      if (this.closed) return undefined;
      return this.send("connect", [url, authJson]);
    });
    void connected.then(
      () => {
        if (this.connectionReady === gate) this.waitingForReconnect = false;
        gate.resolve();
      },
      (error) => {
        if (this.connectionReady === gate) this.waitingForReconnect = false;
        gate.reject(error);
      },
    );
    void connected.catch(ignoreExpectedShutdown);
  }

  disconnect(options?: { rejectWaiters?: boolean }): Promise<void> {
    if (this.closing || this.closed) return Promise.resolve();
    if (!this.waitingForReconnect) this.connectionReady = connectionGate();
    this.waitingForReconnect = true;
    return this.opened
      .then(() => {
        if (this.closed) return undefined;
        return this.send("disconnect", [options]);
      })
      .then(() => undefined);
  }

  updateAuth(authJson: string): void {
    if (this.closing || this.closed) return;
    // Updating credentials cannot reconnect an explicitly disconnected worker:
    // without a server endpoint the worker treats updateAuth as a no-op. Keep the
    // reconnect gate parked until connect() supplies an endpoint.
    const gate = this.waitingForReconnect ? undefined : connectionGate();
    if (gate) this.connectionReady = gate;
    const updated = this.opened.then(() => {
      if (this.closed) return undefined;
      return this.send("updateAuth", [authJson]);
    });
    if (gate) {
      void updated.then(
        () => {
          if (this.connectionReady === gate) this.waitingForReconnect = false;
          gate.resolve();
        },
        (error) => {
          if (this.connectionReady === gate) this.waitingForReconnect = false;
          gate.reject(error);
        },
      );
    }
    void updated.catch(ignoreExpectedShutdown);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  private rejectConnectionWaiters(): void {
    this.waitingForReconnect = false;
    this.connectionReady.reject(new Error("Persistent browser native runtime is closed"));
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

  private enqueueCommand<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.commandTail.then(operation);
    this.commandTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private fireAndForget<Method extends PersistentBrowserWorkerMethod>(
    method: Method,
    ...args: PersistentBrowserRequestArgs<Method>
  ): void {
    if (this.closed) return;
    void this.enqueueCommand(() =>
      this.opened.then(() => {
        if (!this.closed) return this.send(method, args);
      }),
    ).catch(() => undefined);
  }

  private queueWrite<Method extends PersistentBrowserWriteRequest["method"]>(
    method: Method,
    ...args: PersistentBrowserRequestArgs<Method>
  ): MutationResult {
    // Capture the registration frontier at the public call boundary. A
    // subscription created after this write must not retroactively delay it.
    const registrationBeforeWrite = this.subscriptionRegistration;
    const batchId = this.batchIdFromWriteArgs(method, args);
    if (
      batchId &&
      (this.committingTxs.has(batchId as OpenBatchId) ||
        this.rollingBackTxs.has(batchId as OpenBatchId))
    ) {
      throw new Error(`${writeOperationName(method)} failed: batch ${batchId} is completing`);
    }
    if (batchId && this.completedTxs.has(batchId)) {
      throw new Error(
        `${writeOperationName(method)} failed: WriteError("${txStateMessage(batchId, this.completedTxs)}")`,
      );
    }
    const write = this.enqueueCommand(async () => {
      await registrationBeforeWrite;
      if (batchId && this.completedTxs.get(batchId) === "rolled_back") {
        return { kind: "staged", openBatchId: batchId as OpenBatchId } satisfies MutationResult;
      }
      return (await this.send(method, args)) as MutationResult;
    });
    this.pendingWrites.add(write);
    void write.finally(() => this.pendingWrites.delete(write)).catch(() => undefined);
    if (batchId) {
      const writes = this.transactionWrites.get(batchId) ?? [];
      writes.push(write);
      this.transactionWrites.set(batchId, writes);
      void write.catch(() => undefined);
      return { kind: "staged", openBatchId: batchId as OpenBatchId };
    }
    void write.catch(() => undefined);
    return {
      kind: "committed",
      batchId: write.then((result) => {
        if (result.kind !== "committed") {
          throw new Error(`Worker returned staged batch ${result.openBatchId} for ordinary write`);
        }
        return result.batchId;
      }),
    };
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

  private captureReadFence(optionsJson: string | null | undefined): Promise<unknown>[] {
    const openBatchId = openBatchIdFromReadOptions(optionsJson);
    if (openBatchId) {
      return [...(this.transactionWrites.get(openBatchId) ?? [])];
    }
    return [...this.pendingWrites];
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
    const openBatchId = openBatchIdFromReadOptions(optionsJson);
    if (!openBatchId || !this.completedTxs.has(openBatchId)) return;
    throw new Error(
      `Query setup failed: Write error: ${txStateMessage(openBatchId, this.completedTxs)}`,
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
        const error = new Error(message.error.message ?? "Persistent browser subscription failed");
        if (message.error.stack) error.stack = message.error.stack;
        callback?.(error, null);
      } else {
        callback?.(nativeDeltaFromFrame(message));
      }
      return;
    }
    const pending = this.pending.get(message.id);
    if (!pending) return;
    this.pending.delete(message.id);
    if (message.ok) {
      setNamedRowValuesEnumerable(message.result, false);
      pending.resolve(message.result);
    } else {
      const error = new Error(message.error.message ?? "Persistent browser worker call failed");
      if (message.error.stack) error.stack = message.error.stack;
      pending.reject(error);
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

function connectionGate(resolved = false): ConnectionGate {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  // A gate may have no current consumers (for example, connect/updateAuth). Mark
  // its rejection handled without changing the promise observed by later waiters.
  void promise.catch(() => undefined);
  if (resolved) resolve();
  return { promise, resolve, reject };
}

function requiresServerPropagation(tier?: string | null, _optionsJson?: string | null): boolean {
  return tier === "edge" || tier === "global";
}

function openBatchIdFromReadOptions(
  optionsJson: string | null | undefined,
): OpenBatchId | undefined {
  if (!optionsJson) return undefined;
  try {
    const parsed = JSON.parse(optionsJson) as { transaction_batch_id?: unknown };
    return typeof parsed.transaction_batch_id === "string"
      ? (parsed.transaction_batch_id as OpenBatchId)
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
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  if (completedBatches.get(openBatchId) === "committed") {
    return `open batch ${openBatchId} is already committed`;
  }
  return `open batch ${openBatchId} has already been completed or was never opened`;
}

function commitTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId) === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

function rollbackTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTxState>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId) === "committed"
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
