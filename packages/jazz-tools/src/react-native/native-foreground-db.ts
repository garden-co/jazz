type ForegroundMutationKind = "insert" | "update" | "upsert" | "delete" | "restore";
type ForegroundMutationOptions = {
  rowId?: Uint8Array;
  branch?: unknown;
  head?: unknown;
  base?: unknown;
  updatedAtMs?: number;
  author?: Uint8Array;
};

type ForegroundCommand =
  | "tick"
  | "close"
  | { type: "disconnectNativeUpstream" }
  | { type: "reconnectNativeUpstream" }
  | { type: "nativeConnectionStatus" }
  | { type: "nativeSessionMetadata" }
  | { type: "prepareQuery"; query: Uint8Array }
  | { type: "all"; query: number }
  | {
      type: "allWithOptions" | "allRelationSnapshotWithOptions";
      query: number;
      optionsJson: string;
      transaction?: number;
    }
  | { type: "subscribe"; query: number }
  | { type: "drainSubscription"; subscription: number }
  | { type: "unsubscribe"; subscription: number }
  | { type: "poll"; operation: number }
  | { type: "cancel"; operation: number }
  | { type: "beginTransaction"; kind: "mergeable" | "exclusive" }
  | {
      type: "insert";
      transaction: number;
      table: string;
      cells: Uint8Array;
      rowId?: Uint8Array;
    }
  | {
      type: "update";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
    }
  | {
      type: "upsert";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
    }
  | { type: "delete"; transaction: number; table: string; rowId: Uint8Array }
  | { type: "commitTransaction"; transaction: number }
  | { type: "rollbackTransaction"; transaction: number }
  | { type: "subscribeWithOptions"; query: number; optionsJson: string }
  | { type: "waitForTransaction"; txId: Uint8Array; tier: string }
  | {
      type: "stageMutation";
      transaction: number;
      mutation: ForegroundMutationKind;
      table: string;
      rowId?: Uint8Array;
      cells: Uint8Array;
      optionsJson: string;
    };

type ForegroundEvent =
  | {
      type: "delta";
      reset: boolean;
      settled: boolean;
      tier: string;
      delta: Uint8Array;
      terminalOperations?: unknown[];
    }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

type NativeConnectionStatus = {
  type: "nativeConnectionStatus";
  configured: boolean;
  explicitlyOffline: boolean;
  connected: boolean;
};

type ForegroundResponse =
  | { type: "nativeSessionMetadata"; issuer: string; userId: string }
  | NativeConnectionStatus
  | { type: "ticked" }
  | { type: "preparedQuery"; query: number }
  | { type: "rows"; rows: Uint8Array }
  | { type: "subscribed"; subscription: number }
  | { type: "subscriptionEvents"; events: ForegroundEvent[] }
  | { type: "unsubscribed"; closed: boolean }
  | { type: "closed"; closed: boolean }
  | { type: "pending"; operation: number }
  | { type: "operationError"; reason: string }
  | { type: "cancelled"; cancelled: boolean }
  | { type: "transactionOpened"; transaction: number }
  | { type: "inserted"; rowId: Uint8Array }
  | { type: "mutationStaged" }
  | { type: "transactionCommitted"; txId: Uint8Array }
  | { type: "transactionRolledBack"; rolledBack: boolean }
  | { type: "transactionSettled"; txId: Uint8Array };

export type NativeForegroundRuntime = {
  execute(command: Uint8Array): Uint8Array;
  tick(): void;
  setTickScheduler?(callback: (urgency: string) => void): void;
  close(): boolean;
};

export type NativeForegroundModule = {
  installNativeForegroundRuntime(): NativeForegroundFactory;
  encodeNativeForegroundCommand(command: ForegroundCommand): Uint8Array;
  decodeNativeForegroundResponse(bytes: Uint8Array): ForegroundResponse;
};

export type NativeForegroundFactory = {
  readonly abiVersion: number;
  openAttached(capability: Uint8Array): NativeForegroundRuntime;
};

export const REACT_NATIVE_UNSUPPORTED_ERROR =
  "React Native native foreground does not support this operation yet";

/**
 * Narrow NativeDb consumer for the first JSI foreground slice.
 *
 * Rows and subscription deltas stay in the ordinary binding byte format, so
 * NativeRuntimeAdapter remains the sole schema-aware decoder. This is not a
 * second object-shaped database facade and intentionally grows only as the
 * shared Rust command ABI grows.
 */
/**
 * Thin `NativeDb` adapter over the native foreground command ABI.
 *
 * This deliberately contains no row, query, or mutation interpretation: the
 * normal `NativeRuntimeAdapter` owns the public API and existing codecs. A
 * direct mutation is represented by one ordinary native mergeable transaction;
 * explicit public transactions retain their native transaction handle until
 * commit/rollback.
 */
export class NativeForegroundDb {
  private closed = false;
  private readonly transactions = new Map<string, NativeForegroundTransaction>();

  constructor(
    private readonly runtime: NativeForegroundRuntime,
    private readonly commands: NativeForegroundModule,
  ) {}

  setTickScheduler(callback: (first: Error | string | null, second?: string) => void): void {
    if (!this.runtime.setTickScheduler) {
      throw new Error(
        "React Native native foreground runtime does not expose owner wake scheduling",
      );
    }
    this.runtime.setTickScheduler((urgency) => {
      // Native invalidation clears the JSI registration first, but a
      // CallInvoker task may already have crossed into JS when close races
      // teardown. Do not let that stale task re-enter the adapter after its
      // foreground facade has become unusable.
      if (!this.closed) callback(urgency);
    });
  }

  onMutationError(_callback: unknown): void {}

  prepareQuery(query: Uint8Array): object {
    const response = this.execute({ type: "prepareQuery", query });
    if (response.type !== "preparedQuery") return unexpected("prepareQuery", response.type);
    return { nativeForegroundQuery: response.query };
  }

  all(
    query: object,
    opts: unknown,
    openTransactionId?: string,
  ): Uint8Array | { poll(): Uint8Array | null } {
    const transaction =
      openTransactionId === undefined
        ? undefined
        : this.openTransaction(openTransactionId, "read").handle;
    // An attached foreground is an ordinary peer of the native relay. One
    // bounded relay turn admits already-persisted rows before materializing a
    // local read; without it a newly opened foreground can only observe rows
    // after some unrelated caller happens to tick the host.
    this.tick();
    const response = this.execute({
      type: "allWithOptions",
      query: queryHandle(query),
      optionsJson: JSON.stringify(opts ?? {}),
      transaction,
    });
    if (response.type === "rows") return response.rows;
    if (response.type === "pending") return this.pendingRows(response.operation);
    return unexpected("all", response.type);
  }

  allAsync(
    query: object,
    opts: unknown,
    openTransactionId?: string,
  ): Uint8Array | { poll(): Uint8Array | null } {
    return this.all(query, opts, openTransactionId);
  }

  allRelationSnapshot(
    query: object,
    opts: unknown,
    openTransactionId?: string,
  ): Uint8Array | { poll(): Uint8Array | null } {
    const transaction =
      openTransactionId === undefined
        ? undefined
        : this.openTransaction(openTransactionId, "read").handle;
    this.tick();
    const response = this.execute({
      type: "allRelationSnapshotWithOptions",
      query: queryHandle(query),
      optionsJson: JSON.stringify(opts ?? {}),
      transaction,
    });
    if (response.type === "rows") return response.rows;
    if (response.type === "pending") return this.pendingRows(response.operation);
    return unexpected("allRelationSnapshot", response.type);
  }

  allForIdentity(): never {
    return unsupported("trusted-serving reads");
  }

  allRelationQuery(): never {
    return unsupported("relation terminal reads");
  }

  allRelationQueryForIdentity(): never {
    return unsupported("trusted-serving relation reads");
  }

  subscribe(query: object, opts: unknown): NativeForegroundSubscription {
    this.tick();
    const response = this.execute({
      type: "subscribeWithOptions",
      query: queryHandle(query),
      optionsJson: JSON.stringify(opts),
    });
    if (response.type !== "subscribed") return unexpected("subscribe", response.type);
    return new NativeForegroundSubscription(response.subscription, this);
  }

  tick(): void {
    this.assertOpen();
    this.runtime.tick();
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    this.transactions.clear();
    try {
      const response = this.executeAllowClosed("close");
      if (response.type !== "closed") return unexpected("close", response.type);
      return response.closed;
    } finally {
      // The command owns logical Db closure. The HostObject close releases the
      // JSI handle and is deliberately safe to call after that transition.
      // It must also run when command execution fails (for example, because a
      // platform logout revoked the capability just before this call).
      this.runtime.close();
    }
  }

  nativeSessionMetadata(): { issuer: string; userId: string } {
    const response = this.execute({ type: "nativeSessionMetadata" });
    if (response.type !== "nativeSessionMetadata")
      return unexpected("nativeSessionMetadata", response.type);
    return response;
  }

  disconnectNativeUpstream(): void {
    const response = this.execute({ type: "disconnectNativeUpstream" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("disconnectNativeUpstream", response.type);
  }

  reconnectNativeUpstream(): void {
    const response = this.execute({ type: "reconnectNativeUpstream" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("reconnectNativeUpstream", response.type);
  }

  nativeConnectionStatus(): NativeConnectionStatus {
    const response = this.execute({ type: "nativeConnectionStatus" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("nativeConnectionStatus", response.type);
    return response;
  }

  rejectAuthUpdate(): never {
    throw new Error(
      "React Native native foreground cannot rotate authentication in place; keep the existing admitted session or revoke the old native capability and create a new Db for the newly admitted session",
    );
  }

  drain(subscription: number, operation?: number): unknown[] | { pendingOperation: number } {
    const response =
      operation === undefined
        ? this.execute({ type: "drainSubscription", subscription })
        : this.execute({ type: "poll", operation });
    if (response.type === "pending") return { pendingOperation: response.operation };
    if (response.type === "operationError") {
      throw new Error(`React Native native foreground subscription failed: ${response.reason}`);
    }
    if (response.type !== "subscriptionEvents")
      return unexpected("drainSubscription", response.type);
    return response.events.map((event) => {
      if (event.type === "delta") return event;
      if (event.type === "closed") return event;
      return {
        type: "rejected",
        reason: { type: "ServerFailure", code: event.reason },
      };
    });
  }

  unsubscribe(subscription: number): boolean {
    if (this.closed) return false;
    const response = this.execute({ type: "unsubscribe", subscription });
    if (response.type !== "unsubscribed") return unexpected("unsubscribe", response.type);
    // Native unsubscribe retires the stream handle synchronously but queues
    // its core cleanup for the following ordinary turn. Drive that turn here:
    // after the JS reader stops polling there may be no unrelated operation
    // to do it for us.
    if (response.closed) this.tick();
    return response.closed;
  }

  async waitForTransaction(txId: Uint8Array, tier: string): Promise<void> {
    if (!["local", "edge", "global"].includes(tier)) {
      throw new Error(`Unsupported write durability tier: ${tier}`);
    }
    let response = this.execute({ type: "waitForTransaction", txId, tier });
    while (response.type === "pending") {
      const operation = response.operation;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      this.tick();
      response = this.execute({ type: "poll", operation });
    }
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "transactionSettled")
      return unexpected("waitForTransaction", response.type);
  }

  private execute(command: ForegroundCommand): ForegroundResponse {
    this.assertOpen();
    return this.executeAllowClosed(command);
  }

  private pendingRows(operation: number): { poll(): Uint8Array | null } {
    return {
      poll: () => {
        this.tick();
        const response = this.execute({ type: "poll", operation });
        if (response.type === "pending") return null;
        if (response.type === "operationError") {
          throw new Error(`React Native native foreground read failed: ${response.reason}`);
        }
        if (response.type !== "rows") return unexpected("poll", response.type);
        return response.rows;
      },
    };
  }

  private executeAllowClosed(command: ForegroundCommand): ForegroundResponse {
    return this.commands.decodeNativeForegroundResponse(
      this.runtime.execute(this.commands.encodeNativeForegroundCommand(command)),
    );
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("React Native native foreground runtime is closed");
  }

  // The admitted native host already owns and validates the schema. A schema
  // view therefore shares this foreground rather than opening a second store.
  registerSchema(): NativeForegroundDb {
    return this;
  }

  beginTransaction(openTransactionId: string, kind: "mergeable" | "exclusive"): void {
    this.assertOpen();
    if (this.transactions.has(openTransactionId)) {
      throw new Error(
        `React Native native foreground transaction ${openTransactionId} is already open`,
      );
    }
    const response = this.execute({ type: "beginTransaction", kind });
    if (response.type !== "transactionOpened") return unexpected("beginTransaction", response.type);
    this.transactions.set(openTransactionId, {
      handle: response.transaction,
      kind,
      closed: false,
    });
  }

  commitTransaction(openTransactionId: string): NativeForegroundWrite {
    const transaction = this.openTransaction(openTransactionId, "commit");
    const response = this.execute({
      type: "commitTransaction",
      transaction: transaction.handle,
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "transactionCommitted")
      return unexpected("commitTransaction", response.type);
    this.transactions.delete(openTransactionId);
    transaction.closed = true;
    return nativeWrite(this, response.txId);
  }

  rollbackTransaction(openTransactionId: string): void {
    const transaction = this.openTransaction(openTransactionId, "rollback");
    const response = this.execute({
      type: "rollbackTransaction",
      transaction: transaction.handle,
    });
    if (response.type !== "transactionRolledBack")
      return unexpected("rollbackTransaction", response.type);
    this.transactions.delete(openTransactionId);
    transaction.closed = true;
  }

  attachMergeableTx(openTransactionId: string): NativeForegroundTx {
    const transaction = this.transaction(openTransactionId, "mergeable");
    return new NativeForegroundTx(this, transaction);
  }

  attachExclusiveTx(openTransactionId: string): NativeForegroundTx {
    const transaction = this.transaction(openTransactionId, "exclusive");
    return new NativeForegroundTx(this, transaction);
  }

  insertEncoded(
    table: string,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.withOneMutation("insert", (tx) =>
      this.stageMutation(tx, "insert", table, options?.rowId, cells, options),
    );
  }

  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.withOneMutation("update", (tx) =>
      this.stageMutation(tx, "update", table, rowId, patch, options),
    );
  }

  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.withOneMutation("upsert", (tx) =>
      this.stageMutation(tx, "upsert", table, rowId, cells, options),
    );
  }

  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.withOneMutation("restore", (tx) =>
      this.stageMutation(tx, "restore", table, rowId, cells, options),
    );
  }

  deleteEncoded(
    table: string,
    rowId: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.withOneMutation("delete", (tx) =>
      this.stageMutation(tx, "delete", table, rowId, new Uint8Array(), options),
    );
  }

  mergeableTx(): never {
    return unsupported("detached mergeable transaction handles");
  }
  connectUpstream(): never {
    return unsupported("JavaScript upstream transport");
  }

  private withOneMutation(
    operation: string,
    stage: (transaction: NativeForegroundTransaction) => Uint8Array,
  ): NativeForegroundWrite {
    const response = this.execute({
      type: "beginTransaction",
      kind: "mergeable",
    });
    if (response.type !== "transactionOpened")
      return unexpected(`${operation}.begin`, response.type);
    const transaction: NativeForegroundTransaction = {
      handle: response.transaction,
      kind: "mergeable",
      closed: false,
    };
    try {
      const rowId = stage(transaction);
      const committed = this.execute({
        type: "commitTransaction",
        transaction: transaction.handle,
      });
      if (committed.type === "operationError") throw new Error(committed.reason);
      if (committed.type !== "transactionCommitted") {
        return unexpected(`${operation}.commit`, committed.type);
      }
      transaction.closed = true;
      return nativeWrite(this, committed.txId, rowId);
    } catch (error) {
      if (!transaction.closed) {
        try {
          this.execute({
            type: "rollbackTransaction",
            transaction: transaction.handle,
          });
        } catch {
          // The original command failure is the useful error. The native host
          // retires remaining open transactions during foreground close.
        }
      }
      throw error;
    }
  }

  stageMutation(
    transaction: NativeForegroundTransaction,
    mutation: ForegroundMutationKind,
    table: string,
    rowId: Uint8Array | undefined,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): Uint8Array {
    if (transaction.closed) throw new Error("React Native native foreground transaction is closed");
    if (mutation === "upsert" && options && "branch" in options) {
      throw new Error(
        "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view",
      );
    }
    if (
      options?.updatedAtMs !== undefined &&
      (!Number.isSafeInteger(options.updatedAtMs) || options.updatedAtMs < 0)
    ) {
      throw new Error("updatedAtMs must be a non-negative safe integer");
    }
    // Identity belongs to the admitted native capability, never the command.
    const { rowId: _rowId, author: _author, ...wireOptions } = options ?? {};
    const response = this.execute({
      type: "stageMutation",
      transaction: transaction.handle,
      mutation,
      table,
      rowId,
      cells,
      optionsJson: JSON.stringify(wireOptions),
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (mutation === "insert" && response.type === "inserted") return response.rowId;
    if (mutation !== "insert" && response.type === "mutationStaged") return rowId!;
    return unexpected(mutation, response.type);
  }

  private transaction(id: string, kind: "mergeable" | "exclusive"): NativeForegroundTransaction {
    const transaction = this.transactions.get(id);
    if (!transaction || transaction.closed || transaction.kind !== kind) {
      throw new Error(`React Native native foreground has no open ${kind} transaction ${id}`);
    }
    return transaction;
  }

  private openTransaction(id: string, operation: string): NativeForegroundTransaction {
    const transaction = this.transactions.get(id);
    if (!transaction || transaction.closed) {
      throw new Error(
        `React Native native foreground cannot ${operation} unknown transaction ${id}`,
      );
    }
    return transaction;
  }
}

type NativeForegroundTransaction = {
  handle: number;
  kind: "mergeable" | "exclusive";
  closed: boolean;
};

type NativeForegroundWrite = {
  /** Public write identity. Native binding spellings are normalized here. */
  readonly txId: string;
  readonly payload: Uint8Array;
  readonly rowId: Uint8Array;
  wait(tier: string): Promise<void>;
  writeState(): unknown;
  close(): boolean;
};

class NativeForegroundTx {
  constructor(
    private readonly db: NativeForegroundDb,
    private readonly transaction: NativeForegroundTransaction,
  ) {}

  commit(): NativeForegroundWrite {
    throw new Error("React Native native foreground transactions are committed by their owning Db");
  }

  rollback(): void {
    throw new Error(
      "React Native native foreground transactions are rolled back by their owning Db",
    );
  }

  close(): boolean {
    return false;
  }

  insertEncoded(table: string, cells: Uint8Array, options?: ForegroundMutationOptions): Uint8Array {
    return this.db.stageMutation(this.transaction, "insert", table, options?.rowId, cells, options);
  }
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.db.stageMutation(this.transaction, "update", table, rowId, cells, options);
  }
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.db.stageMutation(this.transaction, "upsert", table, rowId, cells, options);
  }
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.db.stageMutation(this.transaction, "restore", table, rowId, cells, options);
  }
  deleteEncoded(table: string, rowId: Uint8Array, options?: ForegroundMutationOptions): void {
    this.db.stageMutation(this.transaction, "delete", table, rowId, new Uint8Array(), options);
  }
}

function nativeWrite(
  db: NativeForegroundDb,
  txId: Uint8Array<ArrayBufferLike>,
  rowId: Uint8Array<ArrayBufferLike> = new Uint8Array(16),
): NativeForegroundWrite {
  const id = formatUuid(txId);
  let closed = false;
  return {
    txId: id,
    payload: new Uint8Array(),
    rowId: rowId.slice(),
    async wait(tier: string): Promise<void> {
      await db.waitForTransaction(txId, tier);
    },
    writeState: () => ({ state: "committed" }),
    close: () => {
      if (closed) return false;
      closed = true;
      return true;
    },
  };
}

class NativeForegroundSubscription {
  private closed = false;
  private pendingOperation: number | undefined;

  constructor(
    private readonly handle: number,
    private readonly db: NativeForegroundDb,
  ) {}

  readAll(): unknown[] | { retryAfterMs(): number } {
    if (this.closed) return [];
    // There is no native-to-JS wake callback in this first slice. Polling must
    // therefore also drive one fair relay turn before observing the stream;
    // Drain alone only consumes events which some other caller has advanced.
    this.db.tick();
    const events = this.db.drain(this.handle, this.pendingOperation);
    if (!Array.isArray(events)) {
      this.pendingOperation = events.pendingOperation;
      return { retryAfterMs: () => 0 };
    }
    this.pendingOperation = undefined;
    // The first command slice has no native-to-JS wake callback yet. Keep the
    // normal NativeRuntimeAdapter subscription reader suspended on its
    // existing retry contract so later native deltas are observable without
    // a second subscription implementation. This polling seam is deliberately
    // capability-gated and can become a wake-driven pending batch later.
    return events.length === 0 ? { retryAfterMs: () => 50 } : events;
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    return this.db.unsubscribe(this.handle);
  }
}

function queryHandle(query: object): number {
  const handle = (query as { nativeForegroundQuery?: unknown }).nativeForegroundQuery;
  if (!Number.isSafeInteger(handle) || (handle as number) < 0) {
    throw new Error("React Native native foreground received an invalid prepared query handle");
  }
  return handle as number;
}

function assertLocalReadOptions(opts: unknown): void {
  if (!opts || typeof opts !== "object") throw new Error(REACT_NATIVE_UNSUPPORTED_ERROR);
  const entries = Object.entries(opts as Record<string, unknown>);
  if (entries.some(([key, value]) => key !== "tier" || value !== "local")) {
    throw new Error(
      `${REACT_NATIVE_UNSUPPORTED_ERROR}; remote tiers, historical views, and include-deleted reads are not implemented`,
    );
  }
}

function unsupported(operation: string): never {
  throw new Error(`${REACT_NATIVE_UNSUPPORTED_ERROR}; ${operation} is unavailable`);
}

function unexpected(operation: string, response: string): never {
  throw new Error(
    `React Native native foreground ${operation} returned unexpected ${response} response`,
  );
}

function formatUuid(bytes: Uint8Array<ArrayBufferLike>): string {
  if (bytes.byteLength !== 16)
    throw new Error("React Native native foreground returned malformed transaction id");
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
