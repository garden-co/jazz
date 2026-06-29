import type {
  ColumnDescriptor,
  ColumnType,
  InsertValues,
  SubscriptionWireDelta,
  TablePolicies,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type { InsertResult, MutationResult, Runtime, TransactionKind } from "../client.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryWithPredicates,
  readNativeRowBatch,
  readNativeSubscriptionDelta,
  writeValueType,
  type NativeRowBatch,
  type NativeRemovedRow,
  type QueryLiteral,
  type QueryOrder,
  type QueryPredicate,
  type QueryPredicateOp,
  type DescriptorField,
  type ValueType,
} from "./native-codec.js";
import { columnTypeToValueType, columnValueType, encodeSchema } from "./schema-codec.js";
import { WebSocketCarrier, wireAuthFailureReason } from "./websocket.js";
import { createRecord, decodeRecordValue } from "./native-row-codec.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "../select-projection.js";

export { encodeSchema } from "./schema-codec.js";

type NativeDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): NativeDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): NativeDb;
};

type NativeDb = {
  all(query: PreparedQuery, opts: unknown): Uint8Array;
  allForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
  setIdentityClaims?(author: Uint8Array, claims: Record<string, unknown> | undefined | null): void;
  attachQuery?(query: PreparedQuery, opts: unknown): unknown;
  attachQueryForIdentity?(query: PreparedQuery, author: Uint8Array, opts: unknown): unknown;
  queryAttachmentIsCovered?(attachment: unknown): boolean;
  detachQuery?(attachment: unknown): void;
  prepareQuery(query: Uint8Array): PreparedQuery;
  subscribe?(query: PreparedQuery, opts: unknown): ReadableStream<unknown> | Subscription;
  subscribeForIdentity?(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): ReadableStream<unknown> | Subscription;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  insertWithIdEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  restoreEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): Write;
  updateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
  ): Write;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  upsertEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): Write;
  delete(table: string, rowId: Uint8Array): Write;
  deleteForIdentity(table: string, rowId: Uint8Array, author: Uint8Array): Write;
  mergeableTx(): Tx;
  mergeableTxForIdentity?(author: Uint8Array): Tx;
  exclusiveTx?(): Tx;
  setTickScheduler(
    callback:
      | ((urgency: "immediate" | "deferred") => void)
      | ((error: Error | null, urgency: string) => void),
  ): void;
  connectUpstream(): Transport;
  tick(): void;
  close?(): void;
  free?(): void;
};

type PreparedQuery = object;

type Subscription = {
  readAll(): unknown[];
  drain?(): unknown[];
  close?(): boolean;
};

type Write = {
  payload: Uint8Array;
  wait(tier: string): void;
  writeState(): unknown;
  nextWriteStateChange(): Promise<void>;
  close?(): boolean;
};

type Tx = {
  commit(): Write;
  rollback(): void;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  delete(table: string, rowId: Uint8Array): void;
};

export type Transport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
};

type PendingTx = {
  kind: TransactionKind;
  tx?: Tx;
  identity?: Uint8Array;
  writes: Array<{ table: string; rowId: Uint8Array }>;
};

type CompletedTx = {
  kind: TransactionKind;
  state: "committed" | "rolled_back";
};

type RuntimeSession = {
  user_id: string;
  claims: Record<string, unknown>;
  identity: Uint8Array;
};

type SubscriptionState = {
  sources: SubscriptionSourceState[];
  rows: RowState[];
  opened: boolean;
  callback?: Function;
  cancelled: boolean;
};

type SubscriptionSourceState = {
  source: ReadableStreamDefaultReader<unknown> | Subscription;
  reading: boolean;
};

type RowState = {
  table: string;
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function openPersistentDb(
  Runtime: NativeDbConstructor,
  dataPath: string,
  schema: Uint8Array,
  config: Uint8Array,
): NativeDb {
  if (!Runtime.openPersistent) {
    throw new Error("Native runtime does not expose persistent storage");
  }
  return Runtime.openPersistent(dataPath, schema, config);
}

export class NativeRuntimeAdapter implements Runtime {
  private readonly db: NativeDb;
  private readonly schemaBytes: Uint8Array;
  private readonly configBytes: Uint8Array;
  private readonly peerIdentity: Uint8Array;
  private readonly schemaHash: string;
  private readonly preparedQueries = new Map<string, PreparedQuery>();
  private readonly pendingTxs = new Map<string, PendingTx>();
  private readonly completedTxs = new Map<string, CompletedTx>();
  private readonly writes = new Map<string, Write>();
  private readonly serverPumpObservedWrites = new WeakSet<Write>();
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private authFailureCallback: ((reason: string) => void) | null = null;
  private serverTransport: Transport | null = null;
  private serverCarrier: WebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<WebSocketCarrier> | null = null;
  private serverTransportError: Error | null = null;
  private serverTransportErrorWaiters: Array<(error: Error) => void> = [];
  private serverEndpointUrl: string | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private coreTickScheduled = false;
  private coreTickRunning = false;
  private coreTickAgain = false;
  private serverPumpScheduled = false;
  private serverPumpAgain = false;
  private closed = false;
  private nextTransactionId = 1;
  private nextSubscriptionId = 1;

  static fromDb(
    db: NativeDb,
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
  ): NativeRuntimeAdapter {
    return new NativeRuntimeAdapter(null, schema, node, author, sourceId, historyComplete, { db });
  }

  constructor(
    Runtime: NativeDbConstructor | null,
    private readonly schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
    opts?: { persistentPath?: string; db?: NativeDb },
  ) {
    this.schemaBytes = encodeSchema(schema);
    this.configBytes = openConfig(node, author, sourceId, historyComplete);
    this.peerIdentity = author;
    this.schemaHash = serializeRuntimeSchema(schema);
    if (opts?.db) {
      this.db = opts.db;
    } else if (opts?.persistentPath) {
      if (!Runtime) {
        throw new Error("Native runtime constructor required for persistent storage");
      }
      this.db = openPersistentDb(Runtime, opts.persistentPath, this.schemaBytes, this.configBytes);
    } else {
      if (!Runtime) {
        throw new Error("Native runtime constructor required for memory storage");
      }
      this.db = Runtime.openMemory(this.schemaBytes, this.configBytes);
    }
    if (typeof this.db.setTickScheduler !== "function") {
      throw new Error("Native runtime requires db.setTickScheduler");
    }
    this.db.setTickScheduler(((first: Error | string | null, second?: string) => {
      const urgency = typeof first === "string" ? first : second;
      if (urgency === "immediate" || urgency === "deferred") {
        this.scheduleCoreWake(urgency);
      }
    }) as (error: Error | null, urgency: string) => void);
  }

  connectUpstreamPeer(): Transport {
    return this.db.connectUpstream();
  }

  close(): void {
    this.closed = true;
    for (const subscription of this.subscriptions.values()) {
      for (const source of subscription.sources) {
        closeSubscriptionSource(source.source);
      }
    }
    for (const write of this.writes.values()) {
      write.close?.();
    }
    this.subscriptions.clear();
    this.pendingTxs.clear();
    this.completedTxs.clear();
    this.writes.clear();
    this.queuedServerFrames.length = 0;
    this.serverTransport?.close();
    this.serverTransport = null;
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.db.close?.();
    this.db.free?.();
  }

  insert(
    table: string,
    values: InsertValues,
    _writeContext?: string | null,
    objectId?: string | null,
  ): InsertResult {
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const cells = encodeCellsForRow(this.table(table), values);
    const writeSession = sessionFromWriteContext(_writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const tx = this.currentTx(_writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).insertWithIdEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(_writeContext) ?? "", writeIdentity);
    }
    const write = writeOrNormalizeRejection("Insert", () =>
      writeIdentity
        ? this.db.insertWithIdEncodedForIdentity(table, rowId, cells, writeIdentity)
        : this.db.insertWithIdEncoded(table, rowId, cells),
    );
    return this.finishInsert(table, rowId, write, writeIdentity);
  }

  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): InsertResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).restoreEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(writeContext) ?? "", writeIdentity);
    }
    const write = writeOrNormalizeRejection("Insert", () =>
      writeIdentity
        ? this.db.restoreEncodedForIdentity(table, rowId, cells, writeIdentity)
        : this.db.restoreEncoded(table, rowId, cells),
    );
    return this.finishInsert(table, rowId, write, writeIdentity);
  }

  update(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    writeContext?: string | null,
  ): MutationResult {
    const rowId = parseUuid(objectId);
    const patch = encodeCellsForPatch(this.table(table), values);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).updateEncoded(table, rowId, patch);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Update", () =>
      writeIdentity
        ? this.db.updateEncodedForIdentity(table, rowId, patch, writeIdentity)
        : this.db.updateEncoded(table, rowId, patch),
    );
    return this.finishMutation(write);
  }

  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    writeContext?: string | null,
  ): MutationResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).upsertEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Insert", () =>
      writeIdentity
        ? this.db.upsertEncodedForIdentity(table, rowId, cells, writeIdentity)
        : this.db.upsertEncoded(table, rowId, cells),
    );
    return this.finishMutation(write);
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    this.table(table);
    const rowId = parseUuid(objectId);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const tx = this.currentTx(writeContext, "Delete");
    if (tx) {
      this.txForWrite(tx, writeIdentity).delete(table, rowId);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Delete", () =>
      writeIdentity
        ? this.db.deleteForIdentity(table, rowId, writeIdentity)
        : this.db.delete(table, rowId),
    );
    return this.finishMutation(write);
  }

  beginTransaction(kind: TransactionKind): string {
    const id = `tx-${this.nextTransactionId++}`;
    this.pendingTxs.set(id, { kind, writes: [] });
    return id;
  }

  commitTransaction(transactionId: string): void {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) {
      throw new Error(commitTransactionMessage(transactionId, this.completedTxs));
    }
    const write = (pending.tx ?? this.txForKind(pending.kind)).commit();
    this.writes.set(transactionId, write);
    this.pendingTxs.delete(transactionId);
    this.completedTxs.set(transactionId, { kind: pending.kind, state: "committed" });
    this.pumpSubscriptions();
    this.observeWriteForServerPump(write);
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    const write = this.writes.get(transactionId);
    if (!write) return;
    for (;;) {
      this.throwServerTransportErrorForTier(tier);
      try {
        this.pumpServerTransport();
        this.throwServerTransportErrorForTier(tier);
        write.wait(tier);
        this.pumpSubscriptions();
        return;
      } catch (error) {
        const rejected = rejectedWaitError(transactionId, error);
        if (rejected) throw rejected;
        if (!isPendingWaitError(error)) throw error;
        this.pumpSubscriptions();
        const change = write.nextWriteStateChange();
        try {
          this.pumpServerTransport();
          this.throwServerTransportErrorForTier(tier);
          write.wait(tier);
          this.pumpSubscriptions();
          return;
        } catch (secondError) {
          const secondRejected = rejectedWaitError(transactionId, secondError);
          if (secondRejected) throw secondRejected;
          if (!isPendingWaitError(secondError)) throw secondError;
        }
        const transportError = this.waitForServerTransportError(tier);
        await (transportError ? Promise.race([change, transportError]) : change);
      }
    }
  }

  rollbackTransaction(transactionId: string): boolean {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) {
      throw new Error(rollbackTransactionMessage(transactionId, this.completedTxs));
    }
    pending.tx?.rollback();
    this.pendingTxs.delete(transactionId);
    this.completedTxs.set(transactionId, { kind: pending.kind, state: "rolled_back" });
    return true;
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    assertSupportedReadOptions(tier, optionsJson);
    assertTransactionReadOpen(optionsJson, this.pendingTxs, this.completedTxs);
    const query = this.prepareQuery(queryJson);
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    const opts = readOptions(tier, queryIncludesDeleted(queryJson), optionsJson);
    const attachment = await this.attachQueryIfNeeded(tier, optionsJson, query, session);
    try {
      const rows = session
        ? this.db.allForIdentity(query, session.identity, opts)
        : this.db.all(query, opts);
      return rowsFromBatches(readRowBatches(rows), this.schema);
    } finally {
      if (attachment !== undefined) this.db.detachQuery?.(attachment);
    }
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    assertSupportedReadOptions(tier, optionsJson);
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    if (!this.db.subscribe) {
      throw new Error("Native runtime does not support subscriptions");
    }
    if (session && !this.db.subscribeForIdentity) {
      throw new Error("Native runtime does not support session-scoped subscriptions");
    }
    const handle = this.nextSubscriptionId++;
    const opts = readOptions(tier, false, optionsJson);
    const identity = session?.identity;
    const query = this.prepareQuery(queryJson);
    let nativeSubscription: ReadableStream<unknown> | Subscription;
    try {
      nativeSubscription = identity
        ? this.db.subscribeForIdentity!(query, identity, opts)
        : this.db.subscribe!(query, opts);
    } catch (error) {
      throw new Error(`Core subscribe failed for ${queryJson}: ${errorMessage(error)}`);
    }
    this.subscriptions.set(handle, {
      sources: [{ source: subscriptionSource(nativeSubscription), reading: false }],
      rows: [],
      opened: false,
      cancelled: false,
    });
    return handle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate;
    if (subscription.opened) {
      subscription.callback(nativeDeltaFromRows(subscription.rows, []));
    }
    this.startSubscriptionReader(handle, subscription);
  }

  unsubscribe(handle: number): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.cancelled = true;
    for (const source of subscription.sources) {
      closeSubscriptionSource(source.source);
    }
    this.subscriptions.delete(handle);
  }

  connect(url: string, authJson: string): Promise<void> {
    this.disconnect();
    this.serverTransportError = null;
    this.serverEndpointUrl = url;
    const transport = this.db.connectUpstream();
    this.serverTransport = transport;
    const carrier = new WebSocketCarrier({
      endpointUrl: url,
      peerIdentity: this.peerIdentity,
      authJson,
      onFrame: (frame) => {
        transport.sendWireFrame(frame);
        this.scheduleServerPump();
      },
      onError: (error) => {
        this.handleServerTransportError(error);
        const reason = wireAuthFailureReason(error);
        if (reason) this.authFailureCallback?.(reason);
      },
    });
    this.serverCarrier = carrier;
    this.serverCarrierPromise = carrier.ready().then(() => {
      this.flushQueuedServerFrames(carrier);
      this.pumpServerTransport();
      return carrier;
    });
    this.serverCarrierPromise.catch((error) => {
      this.handleServerTransportError(error);
    });
    this.scheduleServerPump();
    return this.serverCarrierPromise.then(() => undefined);
  }

  disconnect(): void {
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverTransportError = null;
    this.resolveServerTransportErrorWaiters(new Error("server transport disconnected"));
    this.serverTransport?.close();
    this.serverTransport = null;
    this.serverEndpointUrl = null;
    this.queuedServerFrames.length = 0;
    this.serverPumpScheduled = false;
    this.serverPumpAgain = false;
  }

  updateAuth(authJson: string): Promise<void> | void {
    if (!this.serverEndpointUrl) return;
    return this.connect(this.serverEndpointUrl, authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  private finishInsert(
    table: string,
    rowId: Uint8Array,
    write: Write,
    identity?: Uint8Array,
  ): InsertResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.observeWriteForServerPump(write);
    return this.resultForRow(table, rowId, transactionId, identity);
  }

  private finishMutation(write: Write): MutationResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.observeWriteForServerPump(write);
    return { transactionId };
  }

  private observeWriteForServerPump(write: Write): void {
    if (this.serverPumpObservedWrites.has(write)) return;
    this.serverPumpObservedWrites.add(write);
    this.scheduleServerPump();

    const pumpUntilSettled = async () => {
      for (;;) {
        if (this.closed) return;
        try {
          write.wait("edge");
          this.scheduleServerPump();
          return;
        } catch (error) {
          if (!isPendingWaitError(error)) return;
        }

        try {
          await write.nextWriteStateChange();
        } catch {
          return;
        }
        this.scheduleServerPump();
      }
    };

    void pumpUntilSettled();
  }

  private resultForRow(
    table: string,
    rowId: Uint8Array,
    transactionId: string,
    identity?: Uint8Array,
  ): InsertResult {
    const row = this.readRow(table, rowId, identity);
    return { id: formatUuid(rowId), values: row?.values ?? [], transactionId };
  }

  private readRow(table: string, rowId: Uint8Array, identity?: Uint8Array): RowState | undefined {
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = identity
      ? this.db.allForIdentity(query, identity, readOptions())
      : this.db.all(query, readOptions());
    return rowsFromBatches(readRowBatches(rows), this.schema).find(
      (row) => row.table === table && row.id === formatUuid(rowId),
    );
  }

  private prepareQuery(queryJson: string): PreparedQuery {
    const queryBytes = encodeQueryJson(queryJson, this.schema);
    const key = bytesKey(queryBytes);
    let query = this.preparedQueries.get(key);
    if (!query) {
      try {
        query = this.db.prepareQuery(queryBytes);
      } catch (error) {
        throw new Error(`Core prepareQuery failed for ${queryJson}: ${errorMessage(error)}`);
      }
      this.preparedQueries.set(key, query);
    }
    return query;
  }

  private async attachQueryIfNeeded(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: PreparedQuery,
    session: RuntimeSession | null,
  ): Promise<unknown | undefined> {
    if (tier == null || tier === "local") return;
    const options = optionsJson == null ? {} : (JSON.parse(optionsJson) as Record<string, unknown>);
    if (options.propagation != null && options.propagation !== "full") return;
    if (!this.serverTransport) return;
    if (!this.db.attachQuery) return;
    const opts = readOptions(tier, false, optionsJson);
    let attachment: unknown;
    if (session) {
      if (!this.db.attachQueryForIdentity) {
        throw new Error("Native runtime does not support session-scoped query coverage");
      }
      attachment = this.db.attachQueryForIdentity(query, session.identity, opts);
    } else {
      attachment = this.db.attachQuery(query, opts);
    }
    if (!this.db.queryAttachmentIsCovered) return attachment;
    await this.waitForQueryCoverage(
      attachment,
      query,
      readOptions(tier, false, optionsJson),
      session?.identity,
    );
    return attachment;
  }

  private applySessionClaims(session: RuntimeSession | null | undefined): void {
    if (!session || !this.db.setIdentityClaims) return;
    this.db.setIdentityClaims(session.identity, session.claims);
  }

  private async waitForQueryCoverage(
    attachment: unknown,
    query: PreparedQuery,
    opts: unknown,
    identity?: Uint8Array,
  ): Promise<void> {
    const deadline = Date.now() + 15_000;
    const tier = (opts as { tier?: string }).tier ?? "";
    while (Date.now() < deadline) {
      this.throwServerTransportErrorForTier(tier);
      this.pumpServerTransport();
      this.throwServerTransportErrorForTier(tier);
      if (this.db.queryAttachmentIsCovered) {
        if (this.db.queryAttachmentIsCovered(attachment)) return;
      }
      try {
        if (identity) {
          this.db.allForIdentity(query, identity, opts);
        } else {
          this.db.all(query, opts);
        }
        if (!this.db.queryAttachmentIsCovered) return;
      } catch (error) {
        if (!isPendingCoverageError(error)) throw error;
      }
      const transportError = this.waitForServerTransportError(tier);
      await (transportError ? Promise.race([sleep(10), transportError]) : sleep(10));
    }
    this.scheduleServerPump();
    throw new Error("Timed out waiting for edge query coverage");
  }

  private table(table: string): { columns: ColumnDescriptor[]; policies?: TablePolicies } {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }

  private currentTx(
    writeContext: string | null | undefined,
    operation: "Insert" | "Delete",
  ): PendingTx | undefined {
    const id = txIdFromContext(writeContext);
    if (!id) return undefined;
    const pending = this.pendingTxs.get(id);
    if (pending) return pending;
    throw new Error(`${operation} failed: WriteError("${txStateMessage(id, this.completedTxs)}")`);
  }

  private txForWrite(pending: PendingTx, identity: Uint8Array | undefined): Tx {
    if (pending.kind === "exclusive") {
      if (identity) {
        throw new Error(
          "Native runtime cannot perform session-scoped exclusive transaction writes: " +
            "the native runtime exclusive transaction API has no identity-aware staging methods.",
        );
      }
      if (!pending.tx) {
        pending.tx = this.exclusiveTx();
      }
      return pending.tx;
    }
    if (pending.identity && (!identity || !sameBytes(pending.identity, identity))) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    if (identity && pending.tx && !pending.identity) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    if (!pending.tx) {
      pending.identity = identity;
      pending.tx = identity ? this.mergeableTxForIdentity(identity) : this.db.mergeableTx();
    }
    return pending.tx;
  }

  private txForKind(kind: TransactionKind): Tx {
    return kind === "exclusive" ? this.exclusiveTx() : this.db.mergeableTx();
  }

  private exclusiveTx(): Tx {
    if (!this.db.exclusiveTx) {
      throw new Error(
        "Native runtime cannot perform exclusive transaction writes: " +
          "the native runtime exclusive transaction API is unavailable.",
      );
    }
    return this.db.exclusiveTx();
  }

  private mergeableTxForIdentity(identity: Uint8Array): Tx {
    if (!this.db.mergeableTxForIdentity) {
      throw new Error(
        "Native runtime cannot perform session-scoped transaction writes: " +
          "the native runtime mergeable transaction API has no identity-aware staging methods.",
      );
    }
    return this.db.mergeableTxForIdentity(identity);
  }

  private pumpSubscriptions(): void {
    for (const [handle, subscription] of this.subscriptions) {
      this.startSubscriptionReader(handle, subscription);
    }
  }

  private scheduleCoreWake(urgency: "immediate" | "deferred"): void {
    if (this.closed) return;
    if (urgency === "immediate") {
      this.scheduleCoreTick();
      return;
    }
    queueMicrotask(() => {
      this.scheduleCoreTick();
    });
  }

  private scheduleCoreTick(): void {
    if (this.closed) return;
    if (this.coreTickRunning) {
      this.coreTickAgain = true;
      return;
    }
    if (this.coreTickScheduled) return;
    this.coreTickScheduled = true;
    queueMicrotask(() => {
      this.coreTickScheduled = false;
      this.runCoreTick();
    });
  }

  private runCoreTick(): void {
    if (this.closed || this.coreTickRunning) return;
    this.coreTickRunning = true;
    try {
      this.db.tick();
      this.pumpSubscriptions();
      this.scheduleServerPump();
    } finally {
      this.coreTickRunning = false;
    }
    if (this.coreTickAgain) {
      this.coreTickAgain = false;
      this.scheduleCoreTick();
    }
  }

  private startSubscriptionReader(handle: number, subscription: SubscriptionState): void {
    if (subscription.cancelled) return;
    for (const source of subscription.sources) {
      if (!isReadableSubscriptionReader(source.source)) {
        this.drainNativeSubscription(handle, subscription, source);
        continue;
      }
      if (source.reading) continue;
      source.reading = true;
      void this.readSubscription(handle, subscription, source);
    }
  }

  private async readSubscription(
    handle: number,
    subscription: SubscriptionState,
    source: SubscriptionSourceState,
  ): Promise<void> {
    if (!isReadableSubscriptionReader(source.source)) return;
    try {
      while (!subscription.cancelled && this.subscriptions.get(handle) === subscription) {
        const next = await source.source.read();
        if (next.done || subscription.cancelled) return;
        void this.applySubscriptionChunk(subscription, next.value).catch((error: unknown) => {
          subscription.cancelled = true;
          console.error("Core subscription failed", error);
        });
      }
    } finally {
      source.reading = false;
    }
  }

  private drainNativeSubscription(
    handle: number,
    subscription: SubscriptionState,
    source: SubscriptionSourceState,
  ): void {
    if (isReadableSubscriptionReader(source.source)) return;
    for (const event of source.source.readAll()) {
      if (subscription.cancelled || this.subscriptions.get(handle) !== subscription) return;
      void this.applySubscriptionChunk(subscription, event).catch((error: unknown) => {
        subscription.cancelled = true;
        console.error("Core subscription failed", error);
      });
    }
  }

  private async applySubscriptionChunk(
    subscription: SubscriptionState,
    value: unknown,
  ): Promise<void> {
    const chunk = normalizeSubscriptionChunk(value);
    if (chunk.type === "closed") {
      subscription.cancelled = true;
      return;
    }
    const previousRows = subscription.rows;
    if (chunk.type === "snapshot") {
      subscription.rows = rowsFromBatches(chunk.rows, this.schema);
      subscription.opened = true;
    } else {
      subscription.rows = applySubscriptionDelta(subscription.rows, chunk.delta, this.schema);
    }
    subscription.callback?.(nativeDeltaFromRows(subscription.rows, previousRows));
  }

  private scheduleServerPump(): void {
    if (this.closed || !this.serverTransport || this.serverPumpScheduled) return;
    this.serverPumpScheduled = true;
    queueMicrotask(() => {
      this.serverPumpScheduled = false;
      if (this.closed) return;
      this.pumpServerTransport();
      if (this.serverPumpAgain) {
        this.serverPumpAgain = false;
        this.scheduleServerPump();
      }
    });
  }

  private pumpServerTransport(): void {
    const transport = this.serverTransport;
    if (this.closed || !transport) return;
    for (let round = 0; round < 32; round += 1) {
      transport.tick();
      const frames = normalizeTransportFrames(transport.recvWireFrames());
      if (frames.length > 0) {
        this.sendServerFrames(frames);
      }
      this.pumpSubscriptions();
      if (frames.length === 0) {
        return;
      }
    }
    this.serverPumpAgain = true;
  }

  private sendServerFrames(frames: Uint8Array[]): void {
    const carrier = this.serverCarrier;
    if (!carrier) {
      this.queuedServerFrames.push(...frames);
      return;
    }
    void carrier.sendBatch(frames).catch((error) => {
      this.handleServerTransportError(error);
    });
  }

  private flushQueuedServerFrames(carrier: WebSocketCarrier): void {
    if (this.queuedServerFrames.length === 0 || carrier !== this.serverCarrier) return;
    const frames = this.queuedServerFrames.splice(0);
    void carrier.sendBatch(frames).catch((error) => {
      this.handleServerTransportError(error);
    });
  }

  private handleServerTransportError(error: unknown): void {
    const message = errorMessage(error);
    if (this.serverTransportError && message === "websocket closed") return;
    this.serverTransportError = error instanceof Error ? error : new Error(message);
    this.resolveServerTransportErrorWaiters(this.serverTransportError);
  }

  private throwServerTransportErrorForTier(tier: string): void {
    if ((tier === "edge" || tier === "global") && this.serverTransportError) {
      throw this.serverTransportError;
    }
  }

  private waitForServerTransportError(tier: string): Promise<never> | null {
    if (tier !== "edge" && tier !== "global") return null;
    if (this.serverTransportError) return Promise.reject(this.serverTransportError);
    return new Promise((_, reject) => {
      this.serverTransportErrorWaiters.push(reject);
    });
  }

  private resolveServerTransportErrorWaiters(error: Error): void {
    const waiters = this.serverTransportErrorWaiters.splice(0);
    for (const reject of waiters) reject(error);
  }
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.filter(
    (frame): frame is Uint8Array =>
      ArrayBuffer.isView(frame) && frame.constructor.name === "Uint8Array",
  );
}

function writeId(write: Write, writes: Map<string, Write>): string {
  const id = `tx-${writes.size + 1}`;
  writes.set(id, write);
  return id;
}

function txIdFromContext(writeContext?: string | null): string | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { batch_id?: unknown };
    return typeof parsed.batch_id === "string" ? parsed.batch_id : undefined;
  } catch {
    return undefined;
  }
}

function sessionFromWriteContext(writeContext?: string | null): RuntimeSession | null {
  if (!writeContext) return null;
  try {
    const parsed = JSON.parse(writeContext) as {
      user_id?: unknown;
      attribution?: unknown;
      session?: { user_id?: unknown; claims?: unknown };
    };
    const userId =
      typeof parsed.user_id === "string"
        ? parsed.user_id
        : typeof parsed.session?.user_id === "string"
          ? parsed.session.user_id
          : parsed.attribution === SYSTEM_AUTHOR_ID
            ? SYSTEM_AUTHOR_ID
            : undefined;
    if (!userId) return null;
    const claims = sessionClaims(userId, parsed.session?.claims);
    return { user_id: userId, claims, identity: authorBytesForSubject(userId) };
  } catch {
    return null;
  }
}

function txStateMessage(transactionId: string, completedTxs: Map<string, CompletedTx>): string {
  const completed = completedTxs.get(transactionId);
  if (completed?.state === "committed") {
    return `transaction ${transactionId} is already committed`;
  }
  return `transaction ${transactionId} has already been completed or was never opened`;
}

function commitTransactionMessage(
  transactionId: string,
  completedTxs: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(transactionId, completedTxs);
  return completedTxs.get(transactionId)?.state === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

function rollbackTransactionMessage(
  transactionId: string,
  completedTxs: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(transactionId, completedTxs);
  return completedTxs.get(transactionId)?.state === "committed"
    ? `Write error: ${message}`
    : `Rollback transaction failed: Write error: ${message}`;
}

function assertTransactionReadOpen(
  optionsJson: string | null | undefined,
  pendingTxs: Map<string, PendingTx>,
  completedTxs: Map<string, CompletedTx>,
): void {
  const transactionId = txIdFromOptions(optionsJson);
  if (!transactionId || pendingTxs.has(transactionId)) return;
  throw new Error(
    `Query setup failed: Write error: ${txStateMessage(transactionId, completedTxs)}`,
  );
}

function txIdFromOptions(optionsJson?: string | null): string | undefined {
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

function readOptions(
  tier?: string | null,
  includeDeleted = false,
  optionsJson?: string | null,
): unknown {
  const options = optionsJson == null ? ({} as Record<string, unknown>) : JSON.parse(optionsJson);
  const readOptions: Record<string, unknown> = { tier: tier ?? "local" };
  if (includeDeleted) readOptions.include_deleted = true;
  if (options.propagation === "local-only") readOptions.propagation = "local_only";
  if (options.propagation === "full") readOptions.propagation = "full";
  return readOptions;
}

function assertSupportedReadOptions(tier?: string | null, optionsJson?: string | null): void {
  if (tier != null && !["local", "edge", "global"].includes(tier)) {
    throw new Error(`Native runtime received unsupported read tier '${tier}'`);
  }
  if (optionsJson != null) readSupportedReadOptions(optionsJson);
}

function readSession(sessionJson?: string | null): RuntimeSession | null {
  if (sessionJson == null) return null;
  const parsed = JSON.parse(sessionJson) as { user_id?: unknown; claims?: unknown };
  if (typeof parsed.user_id !== "string") {
    throw new Error("Native runtime session is missing user_id");
  }
  return {
    user_id: parsed.user_id,
    claims: sessionClaims(parsed.user_id, parsed.claims),
    identity: authorBytesForSubject(parsed.user_id),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sessionClaims(userId: string, rawClaims: unknown): Record<string, unknown> {
  return {
    ...(isRecord(rawClaims) ? rawClaims : {}),
    user_id: userId,
    userId,
  };
}

function closeSubscriptionSource(source: SubscriptionSourceState["source"]): void {
  if ("close" in source && typeof source.close === "function") {
    source.close();
    return;
  }
  if ("cancel" in source && typeof source.cancel === "function") {
    void source.cancel().catch(() => {});
  }
}

function readSupportedReadOptions(optionsJson: string): void {
  const parsed = JSON.parse(optionsJson) as Record<string, unknown>;
  const propagation = parsed.propagation;
  if (propagation != null && propagation !== "full" && propagation !== "local-only") {
    throw new Error(
      `Native runtime does not support read propagation '${String(propagation)}' yet`,
    );
  }
}

function queryIncludesDeleted(queryJson: string): boolean {
  try {
    return (JSON.parse(queryJson) as { include_deleted?: unknown }).include_deleted === true;
  } catch {
    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isPendingWaitError(error: unknown): boolean {
  const message = errorMessage(error);
  return (
    message.includes("NotObserved") ||
    message.includes("has not been accepted at requested tier") ||
    message.includes("has not reached requested tier")
  );
}

function isPendingCoverageError(error: unknown): boolean {
  const message = errorMessage(error);
  return (
    message.includes("NotCovered") ||
    message.includes("not covered") ||
    message.includes("has not reached requested tier")
  );
}

function rejectedWaitError(
  transactionId: string,
  error: unknown,
): { kind: "rejected"; transactionId: string; code: string; reason: string } | null {
  const message = errorMessage(error);
  if (!message.includes("WriteRejected")) return null;
  return {
    kind: "rejected",
    transactionId,
    code: rejectionCode(message),
    reason: rejectionReason(message),
  };
}

function writeOrNormalizeRejection<T>(
  operation: "Insert" | "Update" | "Delete",
  write: () => T,
): T {
  try {
    return write();
  } catch (error) {
    const message = errorMessage(error);
    if (message.includes("WriteRejected")) {
      const reason = rejectionReason(message);
      throw new Error(`${operation} failed: WriteError("${reason}")`);
    }
    throw error;
  }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (error && typeof error === "object") {
    const message = (error as { message?: unknown }).message;
    if (typeof message === "string" && message.trim()) return message;
    try {
      return JSON.stringify(error);
    } catch {
      return Object.prototype.toString.call(error);
    }
  }
  return String(error);
}

function rejectionCode(message: string): string {
  if (message.includes("AuthorizationDenied")) return "permission_denied";
  if (message.includes("ExclusiveConflict")) return "exclusive_conflict";
  if (message.includes("CausalityViolation")) return "causality_violation";
  if (message.includes("ClientClockTooFarAhead")) return "client_clock_too_far_ahead";
  if (message.includes("Cascade")) return "cascade_rejected";
  return "write_rejected";
}

function rejectionReason(message: string): string {
  if (message.includes("AuthorizationDenied")) return "Write rejected by server authorization";
  return message.replace(/^.*WriteRejected:?\s*/, "") || "Write rejected";
}

function encodeQueryJson(queryJson: string, schema: WasmSchema): Uint8Array {
  const parsed = JSON.parse(queryJson) as {
    conditions?: unknown;
    table?: unknown;
    limit?: unknown;
    relation_ir?: unknown;
    offset?: unknown;
    select?: unknown;
    select_columns?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Native runtime only supports table queries in this slice");
  }
  const encoded = encodeSimpleRelationQuery(parsed.table, parsed, schema);
  return queryWithPredicates(
    parsed.table,
    encoded.predicates,
    encoded.hasPostFilter
      ? {}
      : {
          limit: readLimitIfPresent(parsed.limit ?? encoded.limit),
          offset: encoded.offset,
          orderBy: encoded.orderBy,
          select: readSelectColumns(parsed.select_columns ?? parsed.select ?? encoded.select),
        },
  );
}

function unsupportedRelationQueryError(operator?: string): Error {
  const detail = operator
    ? ` Relation IR operator "${operator}" requires a relation-tree lowerer or native relation query API; the TS native runtime can currently lower only TableScan plus Filter/OrderBy/Offset/Limit into flat native predicates.`
    : " The TS native runtime can currently lower only TableScan plus Filter/OrderBy/Offset/Limit into flat native predicates.";
  return new Error(`Native runtime cannot lower this relation IR.${detail}`);
}

function encodeSimpleRelationQuery(
  table: string,
  query: {
    conditions?: unknown;
    relation_ir?: unknown;
    limit?: unknown;
    offset?: unknown;
  },
  schema: WasmSchema,
): {
  predicates: QueryPredicate[];
  hasPostFilter: boolean;
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} {
  const unwrapped = unwrapSimpleQuery(table, query);
  if (!unwrapped) throw unsupportedRelationQueryError(relationOperator(query.relation_ir));
  const rootPredicates = readFlatConditions(query.conditions);
  if (!rootPredicates) throw unsupportedRelationQueryError();
  return {
    hasPostFilter: false,
    limit: unwrapped.limit,
    offset: unwrapped.offset,
    orderBy: unwrapped.orderBy,
    select: unwrapped.select,
    predicates: unwrapped.predicates
      .concat(rootPredicates)
      .map((filter) => coerceQueryPredicate(table, filter, schema)),
  };
}

function relationOperator(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const record = value as Record<string, unknown>;
  for (const operator of ["Join", "Project", "Gather", "Union"]) {
    if (operator in record) return operator;
  }
  for (const operator of ["Limit", "Offset", "OrderBy", "Filter"]) {
    const child = record[operator];
    if (child && typeof child === "object") {
      const input = (child as { input?: unknown }).input;
      const nested = relationOperator(input);
      if (nested) return nested;
    }
  }
  return undefined;
}

function coerceQueryPredicate(
  table: string,
  filter: QueryPredicate,
  schema: WasmSchema,
): QueryPredicate {
  if (filter.op === "In") {
    return {
      ...filter,
      values: filter.values.map((value) => coerceQueryLiteral(table, filter.column, value, schema)),
    };
  }
  if (filter.op === "IsNull" || filter.op === "IsNotNull") return filter;
  return {
    ...filter,
    value: coerceQueryLiteral(table, filter.column, filter.value, schema),
  };
}

function unwrapSimpleQuery(
  table: string,
  query: {
    relation_ir?: unknown;
    limit?: unknown;
    offset?: unknown;
  },
): {
  predicates: QueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} | null {
  if (query.relation_ir == null) return { predicates: [], offset: 0, orderBy: [] };
  return unwrapSimpleRelation(table, query.relation_ir);
}

function unwrapSimpleRelation(
  table: string,
  relationIr: unknown,
): {
  predicates: QueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} | null {
  if (relationIr == null) return { predicates: [], offset: 0, orderBy: [] };
  if (typeof relationIr !== "object") return null;
  const relation = relationIr as Record<string, unknown>;
  const tableScan = relation.TableScan;
  if (
    tableScan &&
    typeof tableScan === "object" &&
    (tableScan as { table?: unknown }).table === table
  ) {
    return { predicates: [], offset: 0, orderBy: [] };
  }
  const limit = relation.Limit;
  if (limit && typeof limit === "object") {
    const limitRecord = limit as { input?: unknown; limit?: unknown };
    const input = unwrapSimpleRelation(table, limitRecord.input);
    if (!input) return null;
    return { ...input, limit: readLimit(limitRecord.limit) };
  }
  const offset = relation.Offset;
  if (offset && typeof offset === "object") {
    const offsetRecord = offset as { input?: unknown; offset?: unknown };
    const input = unwrapSimpleRelation(table, offsetRecord.input);
    if (!input) return null;
    return { ...input, offset: readOffset(offsetRecord.offset) };
  }
  const orderBy = relation.OrderBy;
  if (orderBy && typeof orderBy === "object") {
    const orderByRecord = orderBy as { input?: unknown; terms?: unknown };
    const input = unwrapSimpleRelation(table, orderByRecord.input);
    const terms = readOrderByTerms(orderByRecord.terms);
    if (!input || !terms) return null;
    return { ...input, orderBy: input.orderBy.concat(terms) };
  }
  const project = relation.Project;
  if (project && typeof project === "object") {
    const projectRecord = project as { input?: unknown; columns?: unknown };
    const input = unwrapSimpleRelation(table, projectRecord.input);
    const columns = readProjectColumns(projectRecord.columns);
    if (!input || !columns) return null;
    return { ...input, select: columns };
  }
  const filter = relation.Filter;
  if (!filter || typeof filter !== "object") return null;
  const filterRecord = filter as { input?: unknown; predicate?: unknown };
  const input = unwrapSimpleRelation(table, filterRecord.input);
  if (!input) return null;
  const predicates = predicateToFilters(filterRecord.predicate);
  return predicates ? { ...input, predicates: input.predicates.concat(predicates) } : null;
}

function readProjectColumns(value: unknown): string[] | null {
  if (!Array.isArray(value)) return null;
  const columns: string[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== "object") return null;
    const record = entry as { alias?: unknown; expr?: unknown; source?: unknown };
    const expr = record.expr ?? record.source;
    if (!expr || typeof expr !== "object") return null;
    const column = readColumnProjectExpr(expr);
    if (!column) return null;
    if (record.alias != null && record.alias !== column) return null;
    columns.push(column);
  }
  return columns;
}

function readColumnProjectExpr(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const record = value as { Column?: unknown; column?: unknown };
  if (record.Column != null) return readColumnRef(record.Column);
  if (record.column != null) return readColumnRef(record);
  return null;
}

function readSelectColumns(value: unknown): string[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedRelationQueryError();
  if (!value.every((column): column is string => typeof column === "string")) {
    throw unsupportedRelationQueryError();
  }
  return value;
}

function readOrderByTerms(value: unknown): QueryOrder[] | null {
  if (!Array.isArray(value)) return null;
  const terms: QueryOrder[] = [];
  for (const term of value) {
    if (!term || typeof term !== "object") return null;
    const record = term as { column?: unknown; direction?: unknown };
    const column = readColumnRef(record.column);
    if (!column || (record.direction !== "Asc" && record.direction !== "Desc")) return null;
    terms.push({ column, direction: record.direction });
  }
  return terms;
}

function coerceQueryLiteral(
  table: string,
  column: string,
  value: QueryLiteral,
  schema: WasmSchema,
): QueryLiteral {
  if (value.type === "Array") {
    const elementType =
      column === "id"
        ? { type: "Uuid" as const }
        : schema[table]?.columns.find((entry) => entry.name === column)?.column_type;
    const elementColumnType = elementType?.type === "Array" ? elementType.element : elementType;
    return {
      type: "Array",
      value: value.value.map((entry) =>
        coerceLiteralForColumnType(entry, elementColumnType, false),
      ),
    };
  }
  const columnType =
    column === "id"
      ? ({ type: "Uuid" } as const)
      : schema[table]?.columns.find((entry) => entry.name === column)?.column_type;
  const coerced = coerceLiteralForColumnType(value, columnType, true);
  return coerced;
}

function coerceLiteralForColumnType(
  value: QueryLiteral,
  columnType: ColumnType | undefined,
  allowNullable: boolean,
): QueryLiteral {
  if (value.type === "Nullable") {
    return allowNullable && value.value
      ? { type: "Nullable", value: coerceLiteralForColumnType(value.value, columnType, false) }
      : value;
  }
  if (columnType?.type === "Uuid" && value.type === "Text" && isUuidString(value.value)) {
    return { type: "Uuid", value: value.value };
  }
  if (columnType?.type === "Bytea" && value.type === "Array") {
    return { type: "Bytea", value: Uint8Array.from(value.value.map(readByteLiteral)) };
  }
  if (columnType?.type === "Array" && value.type === "Array") {
    return {
      type: "Array",
      value: value.value.map((entry) =>
        coerceLiteralForColumnType(entry, columnType.element, false),
      ),
    };
  }
  return value;
}

function readByteLiteral(value: QueryLiteral): number {
  if (value.type !== "Integer" || value.value < 0 || value.value > 255) {
    throw new Error("Bytea values must contain integers in range 0..255");
  }
  return value.value;
}

function predicateToFilters(predicate: unknown): QueryPredicate[] | null {
  if (predicate === "True") return [];
  if (predicate === "False") return [{ column: "id", op: "In", values: [] }];
  if (!predicate || typeof predicate !== "object") return null;
  const record = predicate as Record<string, unknown>;
  if (Array.isArray(record.And)) {
    const filters: QueryPredicate[] = [];
    for (const child of record.And) {
      const childFilters = predicateToFilters(child);
      if (!childFilters) return null;
      filters.push(...childFilters);
    }
    return filters;
  }
  if (Array.isArray(record.Or)) return null;
  if (record.Not) return null;
  const isNull = record.IsNull;
  if (isNull && typeof isNull === "object") {
    const column = readColumnRef((isNull as { column?: unknown }).column);
    return column ? [{ column, op: "IsNull" }] : null;
  }
  const isNotNull = record.IsNotNull;
  if (isNotNull && typeof isNotNull === "object") {
    const column = readColumnRef((isNotNull as { column?: unknown }).column);
    return column ? [{ column, op: "IsNotNull" }] : null;
  }
  const contains = record.Contains;
  if (contains && typeof contains === "object") {
    const containsRecord = contains as { left?: unknown; right?: unknown };
    const column = readColumnRef(containsRecord.left);
    const value = readLiteral(containsRecord.right);
    return column && value ? [{ column, op: "Contains", value }] : null;
  }
  const inPredicate = record.In;
  if (inPredicate && typeof inPredicate === "object") {
    const inRecord = inPredicate as { left?: unknown; values?: unknown };
    const column = readColumnRef(inRecord.left);
    if (!column || !Array.isArray(inRecord.values)) return null;
    const values = inRecord.values.map(readLiteral);
    return values.every((value): value is QueryLiteral => value != null)
      ? [{ column, op: "In", values }]
      : null;
  }
  const cmp = record.Cmp;
  if (!cmp || typeof cmp !== "object") return null;
  const cmpRecord = cmp as { left?: unknown; op?: unknown; right?: unknown };
  const op = readPredicateOp(cmpRecord.op);
  if (!op) return null;
  const column = readColumnRef(cmpRecord.left);
  const value = readLiteral(cmpRecord.right);
  return column && value ? [{ column, op: op as QueryPredicateOp, value }] : null;
}

function readFlatConditions(conditions: unknown): QueryPredicate[] | null {
  if (conditions == null) return [];
  if (!Array.isArray(conditions)) return null;
  const predicates: QueryPredicate[] = [];
  for (const condition of conditions) {
    if (!condition || typeof condition !== "object") return null;
    const record = condition as { column?: unknown; op?: unknown; value?: unknown };
    if (typeof record.column !== "string" || typeof record.op !== "string") return null;
    const column = record.column.split(".").at(-1) ?? record.column;
    switch (record.op) {
      case "eq":
        if (record.value === null) {
          predicates.push({ column, op: "IsNull" });
        } else {
          predicates.push({ column, op: "Eq", value: valueToQueryLiteral(record.value) });
        }
        break;
      case "ne":
        if (record.value === null) {
          predicates.push({ column, op: "IsNotNull" });
        } else {
          predicates.push({ column, op: "Ne", value: valueToQueryLiteral(record.value) });
        }
        break;
      case "gt":
        predicates.push({ column, op: "Gt", value: valueToQueryLiteral(record.value) });
        break;
      case "gte":
        predicates.push({ column, op: "Gte", value: valueToQueryLiteral(record.value) });
        break;
      case "lt":
        predicates.push({ column, op: "Lt", value: valueToQueryLiteral(record.value) });
        break;
      case "lte":
        predicates.push({ column, op: "Lte", value: valueToQueryLiteral(record.value) });
        break;
      case "contains":
        predicates.push({ column, op: "Contains", value: valueToQueryLiteral(record.value) });
        break;
      case "isNull":
        if (typeof record.value !== "boolean") return null;
        predicates.push({ column, op: record.value ? "IsNull" : "IsNotNull" });
        break;
      case "in":
        if (!Array.isArray(record.value)) return null;
        predicates.push({
          column,
          op: "In",
          values: record.value.map(valueToQueryLiteral),
        });
        break;
      default:
        return null;
    }
  }
  return predicates;
}

function valueToQueryLiteral(value: unknown): QueryLiteral {
  if (value === null || value === undefined) return { type: "Nullable", value: null };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number" && Number.isSafeInteger(value)) return { type: "Integer", value };
  if (typeof value === "string")
    return isUuidString(value) ? { type: "Uuid", value } : { type: "Text", value };
  if (value instanceof Uint8Array) return { type: "Bytea", value };
  if (Array.isArray(value)) return { type: "Array", value: value.map(valueToQueryLiteral) };
  throw unsupportedRelationQueryError();
}

function readPredicateOp(value: unknown): QueryPredicateOp | null {
  switch (value) {
    case "Eq":
    case "Ne":
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
      return value;
    case "Ge":
      return "Gte";
    case "Le":
      return "Lte";
    default:
      return null;
  }
}

function readColumnRef(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const column = (value as { column?: unknown }).column;
  if (typeof column !== "string") return null;
  return column.split(".").at(-1) ?? column;
}

function readLiteral(value: unknown): QueryLiteral | null {
  if (!value || typeof value !== "object" || !("Literal" in value)) return null;
  const literal = (value as { Literal?: unknown }).Literal;
  if (!literal || typeof literal !== "object") return null;
  const record = literal as { type?: unknown; value?: unknown };
  if (record.type === "Boolean" && typeof record.value === "boolean") {
    return { type: "Boolean", value: record.value };
  }
  if (
    (record.type === "Integer" || record.type === "BigInt" || record.type === "Timestamp") &&
    typeof record.value === "number" &&
    Number.isSafeInteger(record.value)
  ) {
    return { type: "Integer", value: record.value };
  }
  if (record.type === "Bytea" && Array.isArray(record.value)) {
    return { type: "Bytea", value: Uint8Array.from(record.value.map(Number)) };
  }
  if (record.type === "Null") {
    return { type: "Nullable", value: null };
  }
  if (record.type === "Array" && Array.isArray(record.value)) {
    const values = record.value.map((entry) => readLiteral({ Literal: entry }));
    if (values.every((entry): entry is QueryLiteral => entry != null)) {
      return { type: "Array", value: values };
    }
  }
  if (record.type === "Uuid" && typeof record.value === "string") {
    return { type: "Uuid", value: record.value };
  }
  if ((record.type === "Text" || record.type === "Enum") && typeof record.value === "string") {
    return { type: "Text", value: record.value };
  }
  return null;
}

function readLimit(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("query limit must be a non-negative safe integer");
  }
  return value;
}

function readLimitIfPresent(value: unknown): number | undefined {
  return value == null ? undefined : readLimit(value);
}

function readOffset(value: unknown): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new Error("query offset must be a non-negative safe integer");
  }
  return value;
}

function isUuidString(value: string): boolean {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    value,
  );
}

export function encodeCellsForRow(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  row: InsertValues,
): Uint8Array {
  return encodeCells(definition.columns, (column) => row[column.name], true);
}

export function encodeCellsForPatch(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  patch: Record<string, Value>,
): Uint8Array {
  const columns = definition.columns.filter((column) => Object.hasOwn(patch, column.name));
  return encodeCells(columns, (column) => patch[column.name], false);
}

function encodeCells(
  columns: ColumnDescriptor[],
  valueFor: (column: ColumnDescriptor) => Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const descriptor = [...columns]
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((column) => ({ name: column.name, valueType: columnValueType(column), column }));
  const values = descriptor.map(({ column }) =>
    encodeValue(column, valueFor(column), requireMissingDefaults),
  );
  const writer = new PostcardWriter();
  writer.vec((field, index) => {
    field.some((name) => name.string(descriptor[index]!.name));
    writeValueType(field, descriptor[index]!.valueType);
  }, descriptor.length);
  writer.bytes(createRecord(descriptor, values));
  return writer.finish();
}

function encodeValue(
  column: ColumnDescriptor,
  value: Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const resolved = value ?? column.default;
  if (!resolved || resolved.type === "Null") {
    if (column.nullable) return encodeNullValue(columnValueType(column));
    if (column.column_type.type === "Array") {
      return encodeNonNullValue(column.column_type, { type: "Array", value: [] });
    }
    if (requireMissingDefaults) throw new Error(`missing required column ${column.name}`);
    return new Uint8Array();
  }
  const bytes = encodeNonNullValue(column.column_type, resolved);
  return column.nullable ? concatBytes([Uint8Array.of(1), bytes]) : bytes;
}

function encodeNonNullValue(type: ColumnType, value: Value): Uint8Array {
  const view = new DataView(new ArrayBuffer(8));
  switch (type.type) {
    case "Boolean":
      return Uint8Array.of(value.type === "Boolean" && value.value ? 1 : 0);
    case "Integer":
      view.setUint32(0, encodeSignedI32ForCore(expectI32(value, "Integer")), true);
      return new Uint8Array(view.buffer, 0, 4);
    case "BigInt":
    case "Timestamp":
      view.setBigUint64(0, BigInt(expectNumber(value, type.type)), true);
      return new Uint8Array(view.buffer);
    case "Double":
      view.setFloat64(0, expectNumber(value, "Double"), true);
      return new Uint8Array(view.buffer);
    case "Text":
    case "Json":
    case "Enum":
      return textEncoder.encode(expectString(value, type.type));
    case "Uuid":
      return parseUuid(expectString(value, "Uuid"));
    case "Bytea":
      if (value.type !== "Bytea") throw new Error("expected Bytea value");
      return value.value;
    case "Array":
      return encodeArrayValue(type.element, value);
    case "Row":
      throw new Error(`Native runtime does not encode ${type.type} values yet`);
  }
}

function encodeArrayValue(elementType: ColumnType, value: Value): Uint8Array {
  if (value.type !== "Array") throw new Error("expected Array value");
  const encoded = value.value.map((item) => encodeNonNullValue(elementType, item));
  const elementWidth = fixedValueSize(columnTypeToValueType(elementType));
  if (elementWidth != null) return concatBytes(encoded);

  const offsets = new PostcardWriter();
  let nextOffset = 4 + Math.max(0, encoded.length - 1) * 4;
  for (const chunk of encoded.slice(0, -1)) {
    nextOffset += chunk.length;
    offsets.u32Le(nextOffset);
  }
  return concatBytes([u32Le(encoded.length), offsets.finish(), ...encoded]);
}

function u32Le(value: number): Uint8Array {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setUint32(0, value, true);
  return bytes;
}

function encodeNullValue(valueType: ValueType): Uint8Array {
  const width = fixedValueSize(valueType);
  return width == null ? Uint8Array.of(0) : new Uint8Array(width);
}

function fixedValueSize(valueType: ValueType): number | undefined {
  switch (valueType.tag) {
    case 0:
    case 5:
    case 9:
      return 1;
    case 1:
      return 2;
    case 2:
      return 4;
    case 3:
    case 4:
      return 8;
    case 8:
      return 16;
    case 10: {
      const members = valueType.members ?? (valueType.inner ? [valueType.inner] : []);
      return members.reduce<number | undefined>((total, member) => {
        if (total == null) return undefined;
        const memberSize = fixedValueSize(member);
        return memberSize == null ? undefined : total + memberSize;
      }, 0);
    }
    case 12: {
      const innerSize = valueType.inner ? fixedValueSize(valueType.inner) : undefined;
      return innerSize == null ? undefined : innerSize + 1;
    }
    default:
      return undefined;
  }
}

function expectNumber(value: Value, type: string): number {
  if (
    (value.type === "Integer" ||
      value.type === "BigInt" ||
      value.type === "Double" ||
      value.type === "Timestamp") &&
    typeof value.value === "number"
  ) {
    return value.value;
  }
  throw new Error(`expected ${type} value`);
}

function expectI32(value: Value, type: string): number {
  const number = expectNumber(value, type);
  if (!Number.isSafeInteger(number) || number < -0x80000000 || number > 0x7fffffff) {
    throw new Error(`${type} value must be a signed 32-bit integer`);
  }
  return number;
}

function encodeSignedI32ForCore(value: number): number {
  return (value ^ 0x80000000) >>> 0;
}

function decodeSignedI32FromCore(value: number): number {
  return (value ^ 0x80000000) | 0;
}

function expectString(value: Value, type: string): string {
  if ((value.type === "Text" || value.type === "Uuid") && typeof value.value === "string") {
    return value.value;
  }
  throw new Error(`expected ${type} value`);
}

function readRowBatches(payload: Uint8Array): NativeRowBatch[] {
  return new PostcardReader(payload).readVec(readNativeRowBatch);
}

function rowsFromBatches(batches: NativeRowBatch[], schema: WasmSchema): RowState[] {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => {
      const decoded = batch.descriptor
        .map((field, index) => ({ field, index, name: publicFieldName(field.name ?? "") }))
        .filter(({ field }) => field.name && !isInternalField(field.name))
        .map(({ field, index, name }) => ({
          name,
          value: decodeField(batch.table, field, batch.descriptor, row.raw, index, schema),
        }));
      const valuesByColumn = new Map(decoded.map(({ name, value }) => [name, value]));
      return withValuesByColumn(
        {
          table: batch.table,
          id: formatUuid(row.rowId),
          values: decoded
            .filter(({ name }) => !isHiddenIncludeColumn(name))
            .map(({ value }) => value),
        },
        valuesByColumn,
      );
    }),
  );
}

function withValuesByColumn(row: RowState, valuesByColumn: Map<string, Value>): RowState {
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

function applySubscriptionDelta(
  currentRows: RowState[],
  delta: { added: NativeRowBatch[]; updated: NativeRowBatch[]; removed: NativeRemovedRow[] },
  schema: WasmSchema,
): RowState[] {
  const rowsByKey = new Map(currentRows.map((row) => [rowKey(row.table, row.id), row]));
  for (const removed of delta.removed) {
    rowsByKey.delete(rowKey(removed.table, formatUuid(removed.rowId)));
  }
  for (const row of rowsFromBatches(delta.added, schema).concat(
    rowsFromBatches(delta.updated, schema),
  )) {
    rowsByKey.set(rowKey(row.table, row.id), row);
  }
  return Array.from(rowsByKey.values());
}

function rowKey(table: string, id: string): string {
  return `${table}\0${id}`;
}

function decodeField(
  table: string,
  field: DescriptorField,
  descriptor: DescriptorField[],
  raw: Uint8Array,
  index: number,
  schema: WasmSchema,
): Value {
  const column = schema[table]?.columns.find(
    (candidate) => candidate.name === publicFieldName(field.name ?? ""),
  );
  const type = column?.column_type;
  const bytes = decodeRecordValue(descriptor, raw, index);
  if (bytes == null) return { type: "Null" };
  if (!type) return { type: "Bytea", value: bytes };
  return decodeBytes(type, bytes);
}

function decodeBytes(type: ColumnType, bytes: Uint8Array): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: decodeSignedI32FromCore(view.getUint32(0, true)) };
    case "BigInt":
      return { type: "BigInt", value: Number(view.getBigUint64(0, true)) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Timestamp":
      return { type: "Timestamp", value: Number(view.getBigUint64(0, true)) };
    case "Text":
    case "Json":
    case "Enum":
      return { type: "Text", value: textDecoder.decode(bytes) };
    case "Uuid":
      return { type: "Uuid", value: formatUuid(bytes) };
    case "Bytea":
      return { type: "Bytea", value: bytes.slice() };
    case "Array":
      return { type: "Array", value: decodeArrayBytes(type.element, bytes) };
    case "Row":
      return { type: "Bytea", value: bytes.slice() };
  }
}

function decodeArrayBytes(elementType: ColumnType, bytes: Uint8Array): Value[] {
  const elementWidth = fixedValueSize(columnTypeToValueType(elementType));
  if (elementWidth != null) {
    if (elementWidth === 0) return [];
    if (bytes.length % elementWidth !== 0) {
      throw new Error(`invalid fixed-width array byte length ${bytes.length}`);
    }
    const values: Value[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementWidth) {
      values.push(decodeBytes(elementType, bytes.subarray(offset, offset + elementWidth)));
    }
    return values;
  }

  if (bytes.length < 4) {
    throw new Error("invalid variable-width array byte length");
  }

  const length = readU32Le(bytes, 0);
  const offsetTableEnd = 4 + Math.max(0, length - 1) * 4;
  if (offsetTableEnd > bytes.length) {
    throw new Error("invalid variable-width array offset table");
  }

  const values: Value[] = [];
  for (let index = 0; index < length; index += 1) {
    const start = index === 0 ? offsetTableEnd : readU32Le(bytes, 4 + (index - 1) * 4);
    const end = index === length - 1 ? bytes.length : readU32Le(bytes, 4 + index * 4);
    if (start > end || end > bytes.length) {
      throw new Error("invalid variable-width array element offset");
    }
    values.push(decodeBytes(elementType, bytes.subarray(start, end)));
  }
  return values;
}

function normalizeSubscriptionChunk(chunk: unknown):
  | { type: "snapshot"; rows: NativeRowBatch[]; settled?: boolean }
  | {
      type: "delta";
      delta: { added: NativeRowBatch[]; updated: NativeRowBatch[]; removed: NativeRemovedRow[] };
      settled?: boolean;
    }
  | { type: "closed" } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as { type?: unknown; rows?: unknown; delta?: unknown; settled?: unknown };
  if (record.type === "closed" || record.type === "Closed") {
    return { type: "closed" };
  }
  if (record.type === "snapshot" || record.type === "Snapshot") {
    return {
      type: "snapshot",
      rows: readRowBatches(assertBytes(record.rows, "subscription rows")),
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
    };
  }
  if (record.type === "delta" || record.type === "Delta") {
    return {
      type: "delta",
      delta: readNativeSubscriptionDelta(
        new PostcardReader(assertBytes(record.delta, "subscription delta")),
      ),
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
    };
  }
  throw new Error("unknown subscription chunk");
}

function subscriptionSource(
  subscription: ReadableStream<unknown> | Subscription,
): ReadableStreamDefaultReader<unknown> | Subscription {
  const maybeReadable = subscription as Partial<ReadableStream<unknown>>;
  if (typeof maybeReadable.getReader === "function") {
    return maybeReadable.getReader();
  }
  return subscription as Subscription;
}

function isReadableSubscriptionReader(
  source: ReadableStreamDefaultReader<unknown> | Subscription,
): source is ReadableStreamDefaultReader<unknown> {
  return "read" in source && typeof source.read === "function";
}

function nativeDeltaFromRows(
  rows: RowState[],
  previousRows: RowState[] = [],
): SubscriptionWireDelta {
  const previousByKey = new Map(
    previousRows.map((row, index) => [rowKey(row.table, row.id), { row, index }]),
  );
  const nextKeys = new Set<string>();
  const delta: SubscriptionWireDelta = [];

  rows.forEach((row, index) => {
    const key = rowKey(row.table, row.id);
    nextKeys.add(key);
    const previous = previousByKey.get(key);
    if (!previous) {
      delta.push({
        kind: 0,
        id: row.id,
        index,
        row: { id: row.id, values: row.values },
      });
      return;
    }
    if (previous.index !== index || !rowValuesEqual(previous.row.values, row.values)) {
      delta.push({
        kind: 2,
        id: row.id,
        index,
        row: { id: row.id, values: row.values },
      });
    }
  });

  previousRows.forEach((row, index) => {
    if (!nextKeys.has(rowKey(row.table, row.id))) {
      delta.push({ kind: 1, id: row.id, index });
    }
  });

  return delta;
}

function rowValuesEqual(left: Value[], right: Value[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => valueEqual(value, right[index]));
}

function valueEqual(left: Value, right: Value | undefined): boolean {
  if (!right || left.type !== right.type) return false;
  switch (left.type) {
    case "Bytea":
      return right.type === "Bytea" && bytesEqual(left.value, right.value);
    case "Array":
      return right.type === "Array" && rowValuesEqual(left.value, right.value);
    case "Null":
      return right.type === "Null";
    case "Boolean":
    case "Text":
    case "Uuid":
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
    case "Row":
      return "value" in right && left.value === right.value;
  }
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  return left.every((byte, index) => byte === right[index]);
}

export function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function uuidBytes(value: string): Uint8Array | null {
  try {
    return parseUuid(value);
  } catch {
    return null;
  }
}

function authorBytesForSubject(subject: string): Uint8Array {
  return uuidBytes(subject) ?? deterministicBytes(`session:${subject}:author`);
}

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}

export function formatUuid(bytes: Uint8Array): string {
  const hex = Array.from(bytes.subarray(0, 16), (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function readU32Le(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset]! |
    (bytes[offset + 1]! << 8) |
    (bytes[offset + 2]! << 16) |
    (bytes[offset + 3]! << 24)
  );
}

function bytesKey(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function publicFieldName(name: string): string {
  return name.startsWith("user_") ? name.slice("user_".length) : name;
}

function isInternalField(name?: string): boolean {
  return name === "row_uuid" || name === "tx_node_id" || name === "tx_time";
}

function isHiddenIncludeColumn(name: string): boolean {
  return name.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX);
}

function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`expected ${label} bytes`);
}
