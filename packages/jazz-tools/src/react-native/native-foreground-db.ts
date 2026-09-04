type ForegroundCommand =
  | "tick"
  | "close"
  | { type: "prepareQuery"; query: Uint8Array }
  | { type: "all"; query: number }
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
  | { type: "rollbackTransaction"; transaction: number };

type ForegroundEvent =
  | {
      type: "delta";
      reset: boolean;
      settled: boolean;
      tier: string;
      delta: Uint8Array;
    }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

type ForegroundResponse =
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
  | { type: "transactionRolledBack"; rolledBack: boolean };

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
    // ABI V1 cannot select a transaction snapshot for an all command. Fail
    // before ticking or issuing a read rather than silently reading outside it.
    if (openTransactionId !== undefined) return unsupported("transaction reads");
    assertLocalReadOptions(opts);
    // An attached foreground is an ordinary peer of the native relay. Advance
    // one bounded relay turn before materializing its current LocalFirst
    // snapshot. This progresses queued peer work but does not promise relay
    // coverage: rows not yet delivered to this foreground remain absent until
    // a later owner wake updates its local knowledge.
    this.tick();
    const response = this.execute({ type: "all", query: queryHandle(query) });
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
    assertLocalReadOptions(opts);
    this.tick();
    const response = this.execute({
      type: "subscribe",
      query: queryHandle(query),
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
    if (response.type !== "transactionCommitted")
      return unexpected("commitTransaction", response.type);
    this.transactions.delete(openTransactionId);
    transaction.closed = true;
    return nativeWrite(response.txId);
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
    options?: { rowId?: Uint8Array },
  ): NativeForegroundWrite {
    return this.withOneMutation("insert", (transaction) => {
      const response = this.execute({
        type: "insert",
        transaction: transaction.handle,
        table,
        cells,
        rowId: options?.rowId,
      });
      if (response.type !== "inserted") return unexpected("insert", response.type);
      return response.rowId;
    });
  }

  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): NativeForegroundWrite {
    return this.withOneMutation("update", (transaction) => {
      const response = this.execute({
        type: "update",
        transaction: transaction.handle,
        table,
        rowId,
        patch,
      });
      if (response.type !== "mutationStaged") return unexpected("update", response.type);
      return rowId;
    });
  }

  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): NativeForegroundWrite {
    return this.withOneMutation("upsert", (transaction) => {
      const response = this.execute({
        type: "upsert",
        transaction: transaction.handle,
        table,
        rowId,
        cells,
      });
      if (response.type !== "mutationStaged") return unexpected("upsert", response.type);
      return rowId;
    });
  }

  deleteEncoded(table: string, rowId: Uint8Array): NativeForegroundWrite {
    return this.withOneMutation("delete", (transaction) => {
      const response = this.execute({
        type: "delete",
        transaction: transaction.handle,
        table,
        rowId,
      });
      if (response.type !== "mutationStaged") return unexpected("delete", response.type);
      return rowId;
    });
  }

  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): NativeForegroundWrite {
    // The current shared command ABI has no restore discriminant. Upsert is
    // not equivalent for a tombstoned row, so make the limitation explicit
    // rather than silently changing restore semantics.
    void table;
    void rowId;
    void cells;
    return unsupported("restore");
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
      if (committed.type !== "transactionCommitted") {
        return unexpected(`${operation}.commit`, committed.type);
      }
      transaction.closed = true;
      return nativeWrite(committed.txId, rowId);
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

  stageInsert(
    transaction: NativeForegroundTransaction,
    table: string,
    cells: Uint8Array,
    rowId?: Uint8Array,
  ): Uint8Array {
    const response = this.execute({
      type: "insert",
      transaction: transaction.handle,
      table,
      cells,
      rowId,
    });
    if (response.type !== "inserted") return unexpected("insert", response.type);
    return response.rowId;
  }

  stageUpdate(
    transaction: NativeForegroundTransaction,
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
  ): void {
    const response = this.execute({
      type: "update",
      transaction: transaction.handle,
      table,
      rowId,
      patch,
    });
    if (response.type !== "mutationStaged") return unexpected("update", response.type);
  }

  stageUpsert(
    transaction: NativeForegroundTransaction,
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
  ): void {
    const response = this.execute({
      type: "upsert",
      transaction: transaction.handle,
      table,
      rowId,
      cells,
    });
    if (response.type !== "mutationStaged") return unexpected("upsert", response.type);
  }

  stageDelete(transaction: NativeForegroundTransaction, table: string, rowId: Uint8Array): void {
    const response = this.execute({
      type: "delete",
      transaction: transaction.handle,
      table,
      rowId,
    });
    if (response.type !== "mutationStaged") return unexpected("delete", response.type);
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

  insertEncoded(table: string, cells: Uint8Array, options?: { rowId?: Uint8Array }): Uint8Array {
    return this.db.stageInsert(this.transaction, table, cells, options?.rowId);
  }

  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void {
    this.db.stageUpdate(this.transaction, table, rowId, patch);
  }

  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void {
    this.db.stageUpsert(this.transaction, table, rowId, cells);
  }

  deleteEncoded(table: string, rowId: Uint8Array): void {
    this.db.stageDelete(this.transaction, table, rowId);
  }

  restoreEncoded(): never {
    return unsupported("restore");
  }
}

function nativeWrite(
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
      if (tier !== "local") {
        throw new Error(
          `React Native native foreground only confirms local writes; ${tier} settlement is not implemented`,
        );
      }
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
