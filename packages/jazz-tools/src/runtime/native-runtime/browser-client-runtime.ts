import type {
  InsertResult,
  MutationErrorEvent,
  MutationResult,
  OpenTransactionId,
  PermissionAdvice,
  Row,
  Runtime,
  RuntimeWriteWaitOptions,
  StreamingInsertResult,
  StreamingMutationKind,
  StreamingValueSource,
  TransactionalRuntime,
  TransactionKind,
  TxId,
} from "../client.js";
import type { Session } from "../context.js";
import type {
  InsertValues,
  RuntimeSubscriptionDelta,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "../select-projection.js";
import { isProvenanceMagicColumn } from "../../magic-columns.js";
import { runtimeRandomBytes } from "../runtime-entropy.js";
import {
  encodeCellsForPatch,
  encodeCellsForRow,
  formatUuid,
  parseUuid,
} from "./native-runtime-adapter.js";
import { deserializeBrowserRelayError } from "./browser-worker-protocol.js";
import {
  CLIENT_BINDING_VERSION,
  deltaFromPort,
  queryRowsFromPort,
  transactionInContext,
  type ClientBindingArgs,
  type ClientBindingCall,
  type ClientBindingEvent,
  type ClientBindingMethod,
  type ClientBindingRequest,
  type ClientBindingResult,
} from "./browser-client-binding-protocol.js";

/**
 * Application Runtime backed by one admitted worker client, with no local Db.
 * The containing browser connection owns admission, liveness and auth. This
 * object never retries a write whose response was lost.
 */
export class BrowserClientRuntime implements TransactionalRuntime {
  private nextId = 1;
  private readonly activityListeners = new Set<() => void>();

  /** The control connection uses this to probe a worker with pending app work. */
  hasActiveOperations(): boolean {
    return this.pending.size > 0 || this.subscriptions.size > 0;
  }
  onActivity(listener: () => void): () => void {
    this.activityListeners.add(listener);
    return () => this.activityListeners.delete(listener);
  }
  flushLocalSettlements(): Promise<void> {
    return this.call("flushLocal", []);
  }
  private closed: Error | null = null;
  private closing: Promise<void> | null = null;
  private readonly pending = new Map<
    number,
    { resolve(value: unknown): void; reject(error: Error): void }
  >();
  private readonly subscriptions = new Map<
    number,
    {
      args: Parameters<Runtime["createSubscription"]>;
      callback?: (value: RuntimeSubscriptionDelta | Error) => void;
      started: boolean;
    }
  >();
  private readonly transactions = new Map<OpenTransactionId, { error?: Error; active: boolean }>();
  private readonly mutationErrors = new Set<(event: MutationErrorEvent) => void>();
  private readonly authFailures = new Set<(reason: string) => void>();

  constructor(
    private readonly schema: WasmSchema,
    private readonly port: MessagePort,
    private readonly transport?: Pick<Runtime, "connect" | "disconnect" | "updateAuth">,
  ) {
    port.addEventListener("message", this.onMessage);
    port.addEventListener("messageerror", this.onMessageError);
    port.start();
  }

  previewInsert(table: string, values: InsertValues, objectId?: string): Row {
    const definition = this.table(table);
    const id = formatUuid(objectId ? parseUuid(objectId) : runtimeRandomBytes(16));
    return {
      id,
      values: definition.columns
        .filter(
          (column) =>
            !column.name.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX) &&
            !isProvenanceMagicColumn(column.name),
        )
        .map((column) => values[column.name] ?? column.default ?? { type: "Null" }),
    };
  }

  insert(
    table: string,
    values: InsertValues,
    context?: string | null,
    objectId?: string | null,
  ): InsertResult {
    this.assertOpen();
    encodeCellsForRow(this.table(table), values);
    const row = this.previewInsert(table, values, objectId ?? undefined);
    return {
      ...row,
      ...this.mutation(context, () => this.call("insert", [table, values, context, row.id])),
    };
  }
  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    context?: string | null,
  ): InsertResult {
    this.assertOpen();
    encodeCellsForRow(this.table(table), values);
    const row = this.previewInsert(table, values, objectId);
    return {
      ...row,
      ...this.mutation(context, () => this.call("restore", [table, objectId, values, context])),
    };
  }
  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    context?: string | null,
  ): MutationResult {
    this.assertOpen();
    parseUuid(objectId);
    encodeCellsForPatch(this.table(table), values);
    return this.mutation(context, () => this.call("update", [table, objectId, values, context]));
  }
  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    context?: string | null,
  ): MutationResult {
    this.assertOpen();
    parseUuid(objectId);
    encodeCellsForRow(this.table(table), values);
    return this.mutation(context, () => this.call("upsert", [table, objectId, values, context]));
  }
  delete(table: string, objectId: string, context?: string | null): MutationResult {
    this.assertOpen();
    parseUuid(objectId);
    this.table(table);
    return this.mutation(context, () => this.call("delete", [table, objectId, context]));
  }
  updateLargeValues(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    descriptors: readonly unknown[],
    context?: string | null,
  ): MutationResult {
    this.assertOpen();
    parseUuid(objectId);
    encodeCellsForPatch(this.table(table), values);
    return this.mutation(context, () =>
      this.call("updateLargeValues", [table, objectId, values, descriptors, context]),
    );
  }

  async streamingMutation(
    kind: StreamingMutationKind,
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    context?: string | null,
    objectId?: string | null,
  ): Promise<StreamingInsertResult> {
    this.assertOpen();
    if (transactionInContext(context))
      throw new Error("Streaming mutations are not supported inside a transaction");
    const iterator = streamChunks(source);
    // A transferred stream retains demand/backpressure; do not collect the
    // input into an unbounded byte array before crossing the port.
    const stream = new ReadableStream<Uint8Array | string>(
      {
        async pull(controller) {
          try {
            const item = await iterator.next();
            if (item.done) controller.close();
            else controller.enqueue(item.value);
          } catch (error) {
            controller.error(error);
          }
        },
        async cancel(reason) {
          await iterator.return?.(reason);
        },
      },
      { highWaterMark: 0 },
    );
    try {
      return await this.call(
        "streamingMutation",
        [kind, table, values, column, stream, context, objectId],
        [stream],
      );
    } catch (error) {
      // A failed post did not transfer ownership of the source to the worker.
      if (!stream.locked) await stream.cancel(error).catch(() => undefined);
      throw error;
    }
  }

  beginTransaction(
    kind: TransactionKind,
    id: OpenTransactionId,
    sessionJson?: string | null,
  ): OpenTransactionId {
    this.assertOpen();
    if (this.transactions.has(id)) throw new Error("Transaction has already been opened");
    const state = { active: true } as { error?: Error; active: boolean };
    this.transactions.set(id, state);
    void this.call("beginTransaction", [kind, id, sessionJson]).catch((error) => {
      state.error ??= asError(error);
    });
    return id;
  }
  commitTransaction(id: OpenTransactionId): Promise<TxId> {
    this.requireTransaction(id).active = false;
    // The host admits this after all preceding staged messages. A failed
    // staging request is latched there even if its reply has not arrived here.
    return this.call("commitTransaction", [id]);
  }
  async rollbackTransaction(id: OpenTransactionId): Promise<boolean> {
    this.assertOpen();
    const state = this.transactions.get(id);
    if (!state) throw new Error("Unknown transaction");
    state.active = false;
    return this.call("rollbackTransaction", [id]);
  }

  async waitForTransaction(
    id: TxId | Promise<TxId>,
    tier: string,
    options: RuntimeWriteWaitOptions = {},
  ): Promise<void> {
    // Observe readiness immediately, while the deferred commit id is pending.
    const readiness = options.ready ?? Promise.resolve();
    void readiness.catch(() => undefined);
    const resolvedId = await id;
    const settlement = this.call("waitForTransaction", [
      resolvedId,
      tier,
      { observeOnly: options.observeOnly },
    ]);
    await Promise.all([settlement, readiness]);
  }

  async query(...args: Parameters<Runtime["query"]>): Promise<unknown> {
    this.assertOpen();
    const id = transactionInContext(args[3]);
    if (id) this.requireTransaction(id as OpenTransactionId);
    return queryRowsFromPort(await this.call("query", args));
  }
  createSubscription(...args: Parameters<Runtime["createSubscription"]>): number {
    this.assertOpen();
    const id = this.nextId++;
    this.subscriptions.set(id, { args, started: false });
    return id;
  }
  executeSubscription(
    handle: number,
    callback: (value: RuntimeSubscriptionDelta | Error) => void,
  ): void {
    this.assertOpen();
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    if (subscription.started) throw new Error("Subscription has already been activated");
    subscription.started = true;
    subscription.callback = callback;
    // Handles may be created well before activation and activated out of
    // order; use a fresh request sequence while retaining the local handle.
    const id = this.nextId++;
    this.activeSubscriptions.set(id, handle);
    this.subscriptionRequests.set(handle, id);
    this.send({ type: "client-subscribe", id, args: subscription.args });
  }
  private readonly activeSubscriptions = new Map<number, number>();
  private readonly subscriptionRequests = new Map<number, number>();
  unsubscribe(handle: number): void {
    this.subscriptions.delete(handle);
    const id = this.subscriptionRequests.get(handle);
    this.subscriptionRequests.delete(handle);
    if (id === undefined) return;
    this.activeSubscriptions.delete(id);
    if (!this.closed)
      void this.request({ type: "client-unsubscribe", handle: id }).catch(() => undefined);
  }

  canInsertLocally(): PermissionAdvice {
    return "unknown";
  }
  canReadLocally(): PermissionAdvice {
    return "unknown";
  }
  canUpdateLocally(): PermissionAdvice {
    return "unknown";
  }
  canDeleteLocally(): PermissionAdvice {
    return "unknown";
  }
  requestInsertPermissionAdvice(
    table: string,
    values: InsertValues,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return this.call("requestInsertPermissionAdvice", [table, values, session]);
  }
  requestReadPermissionAdvice(
    table: string,
    id: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return this.call("requestReadPermissionAdvice", [table, id, session]);
  }
  requestUpdatePermissionAdvice(
    table: string,
    id: string,
    values: Record<string, Value>,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return this.call("requestUpdatePermissionAdvice", [table, id, values, session]);
  }
  requestDeletePermissionAdvice(
    table: string,
    id: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return this.call("requestDeletePermissionAdvice", [table, id, session]);
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    this.mutationErrors.add(callback);
  }
  reportRemoteMutationError(event: MutationErrorEvent): void {
    for (const callback of this.mutationErrors) callback(event);
  }
  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailures.add(callback);
  }
  reportAuthFailure(reason: string): void {
    for (const callback of this.authFailures) callback(reason);
  }
  connect(url: string, auth: string): void {
    if (!this.transport) throw new Error("The admitting host owns the worker transport");
    this.transport.connect(url, auth);
  }
  disconnect(options?: { rejectWaiters?: boolean }): Promise<void> {
    return this.transport?.disconnect(options) ?? Promise.resolve();
  }
  updateAuth(auth: string): void {
    this.transport?.updateAuth(auth);
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    if (this.closed) return Promise.resolve();
    const acknowledgement = this.request({ type: "client-close" });
    this.closing = acknowledgement
      .then(() => undefined)
      .finally(() => this.fail(new Error("Client binding is closed")));
    return this.closing;
  }
  discard(): void {
    if (this.closed) return;
    try {
      this.send({ type: "client-close", id: this.nextId++ });
    } finally {
      this.fail(new Error("Client binding was discarded; pending write outcomes are unknown"));
    }
  }
  /** The containing connection calls this on a confirmed port/runtime failure. */
  fail(error: Error): void {
    if (this.closed) return;
    this.closed = error;
    this.port.removeEventListener("message", this.onMessage);
    this.port.removeEventListener("messageerror", this.onMessageError);
    this.port.close();
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
    for (const subscription of this.subscriptions.values()) subscription.callback?.(error);
    this.subscriptions.clear();
    this.transactions.clear();
    this.activeSubscriptions.clear();
    this.subscriptionRequests.clear();
  }

  private mutation(
    context: string | null | undefined,
    run: () => Promise<MutationResult>,
  ): MutationResult {
    const id = transactionInContext(context) as OpenTransactionId | undefined;
    const state = id ? this.requireTransaction(id) : undefined;
    const result = run();
    if (id && state) {
      void result.catch((error) => {
        state.error ??= asError(error);
      });
      return { kind: "staged", openTransactionId: id };
    }
    const txId = result.then((value) => {
      if (value.kind !== "committed") throw new Error("Direct mutation returned a staged receipt");
      return value.txId;
    });
    // Deferred receipt errors belong to the WriteHandle, not an ambient
    // unhandled-rejection event before the caller has installed wait().
    void txId.catch(() => undefined);
    return { kind: "committed", txId };
  }
  private table(name: string) {
    const definition = this.schema[name];
    if (!definition) throw new Error(`Unknown table ${name}`);
    return definition;
  }
  private requireTransaction(id: OpenTransactionId): { error?: Error; active: boolean } {
    this.assertOpen();
    const state = this.transactions.get(id);
    if (!state || !state.active) throw new Error("Transaction is not open");
    if (state.error) throw state.error;
    return state;
  }
  private assertOpen(): void {
    if (this.closed) throw this.closed;
    if (this.closing) throw new Error("Client binding is closing");
  }
  private call<M extends ClientBindingMethod>(
    method: M,
    args: ClientBindingArgs<M>,
    transfer: Transferable[] = [],
  ): Promise<ClientBindingResult<M>> {
    return this.request(
      { type: "client-call", call: { method, args } as ClientBindingCall },
      transfer,
    ) as Promise<ClientBindingResult<M>>;
  }
  private request(message: RequestWithoutId, transfer: Transferable[] = []): Promise<unknown> {
    this.assertOpen();
    const id = this.nextId++;
    const promise = new Promise<unknown>((resolve, reject) =>
      this.pending.set(id, { resolve, reject }),
    );
    this.send({ ...message, id }, transfer);
    return promise;
  }
  private send(message: RequestWithoutVersion, transfer: Transferable[] = []): void {
    try {
      this.port.postMessage({ ...message, version: CLIENT_BINDING_VERSION }, transfer);
      for (const listener of this.activityListeners) listener();
    } catch (error) {
      this.fail(asError(error));
    }
  }
  private readonly onMessage = (event: MessageEvent<ClientBindingEvent>): void => {
    if (this.closed) return;
    const message = event.data;
    if (!message || message.version !== CLIENT_BINDING_VERSION) {
      this.fail(new Error("Invalid client binding response version"));
      return;
    }
    const error = message.error && deserializeBrowserRelayError(message.error);
    if (message.type === "client-failed") {
      this.fail(error ?? new Error("Worker revoked the client binding"));
    } else if (message.type === "client-delta") {
      const handle = this.activeSubscriptions.get(message.id);
      const subscription = handle === undefined ? undefined : this.subscriptions.get(handle);
      if (!subscription) return;
      if (error) subscription.callback?.(error);
      else if (message.value) subscription.callback?.(deltaFromPort(message.value));
    } else if (message.type === "client-result") {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      if (error) pending.reject(error);
      else pending.resolve(message.value);
    } else this.fail(new Error("Unknown client binding response"));
  };
  private readonly onMessageError = (): void =>
    this.fail(new Error("Client binding message error; pending write outcomes are unknown"));
}

type WithoutVersion<T> = T extends unknown ? Omit<T, "version"> : never;
type WithoutId<T> = T extends unknown ? Omit<T, "id"> : never;
type RequestWithoutVersion = WithoutVersion<ClientBindingRequest>;
type RequestWithoutId = WithoutId<RequestWithoutVersion>;
function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

async function* streamChunks(source: StreamingValueSource): AsyncGenerator<Uint8Array | string> {
  if (!("getReader" in source)) {
    yield* source;
    return;
  }
  const reader = source.getReader();
  let completed = false;
  try {
    for (;;) {
      const next = await reader.read();
      if (next.done) {
        completed = true;
        return;
      }
      yield next.value;
    }
  } finally {
    try {
      if (!completed) await reader.cancel();
    } finally {
      reader.releaseLock();
    }
  }
}
