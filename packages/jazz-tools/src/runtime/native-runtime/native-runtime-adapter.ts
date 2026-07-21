import type {
  ColumnDescriptor,
  ColumnType,
  InsertValues,
  NativeRowDelta,
  TablePolicies,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type { InsertResult, MutationResult, Runtime, TransactionKind } from "../client.js";
import type { Session } from "../context.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryWithPredicates,
  readNativeRowBatch,
  readNativeRelationSubscriptionDelta,
  readNativeRelationSubscriptionSnapshot,
  readNativeSubscriptionDelta,
  writeValueType,
  type NativeSubscriptionDelta,
  type NativeRelationSubscriptionSnapshot,
  type NativeRelationSubscriptionDelta,
  type NativeRelationSubscriptionEdge,
  type NativeRowBatch,
  type NativeRemovedRow,
  type QueryArraySubquery,
  type QueryLiteral,
  type QueryOrder,
  type QueryPredicate,
  type QueryPredicateOp,
  type ValueType,
} from "./native-codec.js";
import { columnTypeToValueType, columnValueType, encodeSchema } from "./schema-codec.js";
import { WebSocketCarrier, wireAuthFailureReason } from "./websocket.js";
import {
  createNativeRowValueEncoder,
  createRecord,
  createRecordValueDecoder,
} from "./native-row-codec.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX, hiddenIncludeColumnName } from "../select-projection.js";
import {
  isPermissionIntrospectionColumn,
  isProvenanceMagicColumn,
  magicColumnType,
} from "../../magic-columns.js";

export { encodeSchema } from "./schema-codec.js";

const SERVER_PUMP_DEBOUNCE_MS = 16;

type NativeDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): NativeDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): NativeDb;
};

type NativeDb = {
  all(query: PreparedQuery, opts: unknown): Uint8Array;
  allForIdentity(query: PreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
  allRelationQuery?(queryJson: string, opts: unknown): Uint8Array;
  allRelationQueryForIdentity?(queryJson: string, author: Uint8Array, opts: unknown): Uint8Array;
  allRelationSnapshot?(query: PreparedQuery, opts: unknown): Uint8Array;
  allRelationSnapshotForIdentity?(
    query: PreparedQuery,
    author: Uint8Array,
    opts: unknown,
  ): Uint8Array;
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
  subscribeRelationQuery?(queryJson: string, opts: unknown): ReadableStream<unknown> | Subscription;
  subscribeRelationQueryForIdentity?(
    queryJson: string,
    author: Uint8Array,
    opts: unknown,
  ): ReadableStream<unknown> | Subscription;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  insertWithIdEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  restoreEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): Write;
  updateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): Write;
  upsertEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): Write;
  deleteForIdentity(
    table: string,
    rowId: Uint8Array,
    author: Uint8Array,
    updatedAtMs?: number | null,
  ): Write;
  canInsertEncoded?(table: string, cells: Uint8Array): boolean;
  canInsertEncodedForIdentity?(table: string, cells: Uint8Array, author: Uint8Array): boolean;
  canReadForIdentity?(table: string, rowId: Uint8Array, author: Uint8Array): boolean;
  canUpdateEncodedForIdentity?(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
  ): boolean;
  canDeleteForIdentity?(table: string, rowId: Uint8Array, author: Uint8Array): boolean;
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
  insertWithIdEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  restoreEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  updateEncoded(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  upsertEncoded(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    updatedAtMs?: number | null,
  ): void;
  delete(table: string, rowId: Uint8Array, updatedAtMs?: number | null): void;
};

export type Transport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  sendWireFrames?(frames: readonly Uint8Array[]): void;
  tick(): number;
};

type PendingTx = {
  kind: TransactionKind;
  tx?: Tx;
  identity?: Uint8Array;
  writes: PendingTxWrite[];
};

type PendingTxWrite = {
  table: string;
  rowId: Uint8Array;
  baseRow?: RowState;
  row?: RowState;
  deleted?: boolean;
};

type CompletedTx = {
  kind: TransactionKind;
  state: "committed" | "rolled_back";
};

type ServerTransportErrorWaiter = {
  active: boolean;
  reject: (error: Error) => void;
};

type RuntimeSession = {
  user_id: string;
  claims: Record<string, unknown>;
  identity: Uint8Array;
};

type SubscriptionState = {
  sources: SubscriptionSourceState[];
  queryJson: string;
  query: PreparedQuery | null;
  identity?: Uint8Array;
  rows: RowState[];
  rowIndexByKey: Map<string, number>;
  visibleRows: RowState[];
  relationRows: RowState[];
  relationRootCount: number;
  relationEdges: NativeRelationSubscriptionEdge[];
  relationMaterialization: RelationMaterializationSpec;
  outputColumns: SubscriptionOutputColumns | null;
  snapshotRefresh: boolean;
  session: RuntimeSession | null;
  opts: unknown;
  opened: boolean;
  visibleOpened: boolean;
  deferredVisiblePublication: boolean;
  deferredVisibleReset: boolean;
  callback?: Function;
  cancelled: boolean;
};

type SubscriptionOutputColumns = {
  rootTable: string;
  rootColumns: readonly ColumnDescriptor[];
};

type RelationMaterializationSpec = {
  rootTable: string | null;
  arraySubqueries: QueryArraySubquery[];
  clientLimit: number | null;
  clientOffset: number;
};

type SubscriptionSourceState = {
  source: ReadableStreamDefaultReader<unknown> | Subscription;
  reading: boolean;
};

export type RowState = {
  table: string;
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
};

type NativeRowFieldPlan = {
  name: string;
  index: number;
  type?: ColumnType;
  includeInValues: boolean;
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
  private serverTransportErrorWaiters: ServerTransportErrorWaiter[] = [];
  private serverEndpointUrl: string | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private readonly pendingInboundServerFrames: Uint8Array[] = [];
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
    this.clearServerTransportErrorWaiters();
    this.queuedServerFrames.length = 0;
    this.pendingInboundServerFrames.length = 0;
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
    const updatedAtMs = effectiveUpdatedAtMs(_writeContext);
    const tx = this.currentTx(_writeContext, "Insert");
    if (tx) {
      const baseRow = this.baseRowForExclusiveWrite(tx, table, rowId, writeIdentity);
      this.txForWrite(tx, writeIdentity).insertWithIdEncoded(table, rowId, cells, updatedAtMs);
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, baseRow, row });
      return {
        id: row.id,
        values: row.values,
        transactionId: txIdFromContext(_writeContext) ?? "",
      };
    }
    const write = writeOrNormalizeRejection("Insert", () =>
      writeIdentity
        ? this.db.insertWithIdEncodedForIdentity(table, rowId, cells, writeIdentity, updatedAtMs)
        : this.db.insertWithIdEncoded(table, rowId, cells, updatedAtMs),
    );
    return this.finishInsert(table, rowId, values, write);
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
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Restore");
    if (tx) {
      const baseRow = this.baseRowForExclusiveWrite(tx, table, rowId, writeIdentity);
      this.txForWrite(tx, writeIdentity).restoreEncoded(table, rowId, cells, updatedAtMs);
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, baseRow, row });
      return { id: row.id, values: row.values, transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Restore", () =>
      writeIdentity
        ? this.db.restoreEncodedForIdentity(table, rowId, cells, writeIdentity, updatedAtMs)
        : this.db.restoreEncoded(table, rowId, cells, updatedAtMs),
    );
    return this.finishInsert(table, rowId, values, write);
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
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Update");
    if (tx) {
      const baseRow = this.baseRowForExclusiveWrite(tx, table, rowId, writeIdentity);
      this.txForWrite(tx, writeIdentity).updateEncoded(table, rowId, patch, updatedAtMs);
      tx.writes.push({
        table,
        rowId,
        baseRow,
        row: this.mergeRowState(table, rowId, values, tx, writeIdentity),
      });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Update", () =>
      writeIdentity
        ? this.db.updateEncodedForIdentity(table, rowId, patch, writeIdentity, updatedAtMs)
        : this.db.updateEncoded(table, rowId, patch, updatedAtMs),
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
    const definition = this.table(table);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Upsert");
    const existing = tx
      ? (this.pendingRow(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId))
      : this.readRow(table, rowId, writeIdentity);
    let cells: Uint8Array;
    try {
      cells = existing
        ? encodeCellsForPatch(definition, values)
        : encodeCellsForRow(definition, values, table);
    } catch (error) {
      throw writeError("Upsert", normalizeWriteSetupMessage(errorMessage(error)));
    }
    if (tx) {
      const baseRow = this.baseRowForExclusiveWrite(tx, table, rowId, writeIdentity);
      this.txForWrite(tx, writeIdentity).upsertEncoded(table, rowId, cells, updatedAtMs);
      tx.writes.push({
        table,
        rowId,
        baseRow,
        row: existing
          ? this.mergeRowState(table, rowId, values, tx, writeIdentity)
          : this.rowStateFromValues(table, rowId, values),
      });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Upsert", () =>
      writeIdentity
        ? this.db.upsertEncodedForIdentity(table, rowId, cells, writeIdentity, updatedAtMs)
        : this.db.upsertEncoded(table, rowId, cells, updatedAtMs),
    );
    return this.finishMutation(write);
  }

  delete(table: string, objectId: string, writeContext?: string | null): MutationResult {
    this.table(table);
    const rowId = parseUuid(objectId);
    const writeSession = sessionFromWriteContext(writeContext);
    this.applySessionClaims(writeSession);
    const writeIdentity = writeSession?.identity;
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Delete");
    if (tx) {
      const baseRow = this.baseRowForExclusiveWrite(tx, table, rowId, writeIdentity);
      this.txForWrite(tx, writeIdentity).delete(table, rowId, updatedAtMs);
      tx.writes.push({ table, rowId, baseRow, deleted: true });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = writeOrNormalizeRejection("Delete", () =>
      writeIdentity
        ? this.db.deleteForIdentity(table, rowId, writeIdentity, updatedAtMs)
        : this.db.delete(table, rowId, updatedAtMs),
    );
    return this.finishMutation(write);
  }

  canInsert(table: string, values: InsertValues, session?: Session): boolean {
    const cells = encodeCellsForRow(this.table(table), values, table);
    const runtimeSession = runtimeSessionFromPublicSession(session);
    this.applySessionClaims(runtimeSession);
    const identity = runtimeSession?.identity ?? this.peerIdentity ?? parseUuid(SYSTEM_AUTHOR_ID);
    if (this.db.canInsertEncodedForIdentity) {
      return this.db.canInsertEncodedForIdentity(table, cells, identity);
    }
    if (!runtimeSession && this.db.canInsertEncoded) {
      return this.db.canInsertEncoded(table, cells);
    }
    throw new Error("Runtime does not support write-policy dry-run insert checks.");
  }

  canRead(table: string, objectId: string, session?: Session): boolean {
    this.table(table);
    const rowId = parseUuid(objectId);
    const runtimeSession = runtimeSessionFromPublicSession(session);
    this.applySessionClaims(runtimeSession);
    const identity = runtimeSession?.identity ?? this.peerIdentity ?? parseUuid(SYSTEM_AUTHOR_ID);
    if (!this.db.canReadForIdentity) {
      throw new Error("Runtime does not support read-policy dry-run checks.");
    }
    return this.db.canReadForIdentity(table, rowId, identity);
  }

  canUpdate(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): boolean {
    const rowId = parseUuid(objectId);
    const patch = encodeCellsForPatch(this.table(table), values);
    const runtimeSession = runtimeSessionFromPublicSession(session);
    this.applySessionClaims(runtimeSession);
    const identity = runtimeSession?.identity ?? this.peerIdentity ?? parseUuid(SYSTEM_AUTHOR_ID);
    if (!this.db.canUpdateEncodedForIdentity) {
      throw new Error("Runtime does not support write-policy dry-run update checks.");
    }
    return this.db.canUpdateEncodedForIdentity(table, rowId, patch, identity);
  }

  canDelete(table: string, objectId: string, session?: Session): boolean {
    this.table(table);
    const rowId = parseUuid(objectId);
    const runtimeSession = runtimeSessionFromPublicSession(session);
    this.applySessionClaims(runtimeSession);
    const identity = runtimeSession?.identity ?? this.peerIdentity ?? parseUuid(SYSTEM_AUTHOR_ID);
    if (!this.db.canDeleteForIdentity) {
      throw new Error("Runtime does not support write-policy dry-run delete checks.");
    }
    return this.db.canDeleteForIdentity(table, rowId, identity);
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
    this.pendingTxs.delete(transactionId);
    this.completedTxs.set(transactionId, { kind: pending.kind, state: "committed" });
    if (!pending.tx) {
      this.pumpSubscriptions();
      return;
    }
    this.rejectMovedExclusiveParents(pending);
    const write = pending.tx.commit();
    this.writes.set(transactionId, write);
    this.pumpSubscriptions();
    this.observeWriteForBoundaryEffects(write);
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
        this.refreshOpenedPlainSubscriptions();
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
          this.refreshOpenedPlainSubscriptions();
          return;
        } catch (secondError) {
          const secondRejected = rejectedWaitError(transactionId, secondError);
          if (secondRejected) throw secondRejected;
          if (!isPendingWaitError(secondError)) throw secondError;
        }
        const transportError = this.waitForServerTransportError(tier);
        try {
          await (transportError ? Promise.race([change, transportError.promise]) : change);
        } finally {
          transportError?.cancel();
        }
        // `change` can resolve as a microtask without real progress. Yield a
        // macrotask before re-polling so timers and socket frames — the events
        // that actually advance settlement — can run; a hot microtask loop
        // here starves them and wedges the whole worker.
        await new Promise((resolve) => setTimeout(resolve, 0));
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
    const outcome = await this.queryTransportOutcome(queryJson, sessionJson, tier, optionsJson, {
      preferEncoded: false,
    });
    return outcome.kind === "rows"
      ? outcome.rows
      : decodeEncodedQueryResult(outcome, this.schema, queryJson);
  }

  /**
   * @internal Worker-boundary variant of {@link query}: returns the engine's
   * encoded result payload whenever no adapter-side post-processing (edge
   * refresh, transaction read overlay) was applied, so a message transport can
   * move one transferable buffer instead of structured-cloning a decoded row
   * tree. Decode with {@link decodeEncodedQueryResult} — a pure function of
   * (payload, schema, queryJson) — on the receiving side.
   */
  async queryForTransport(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<QueryTransportOutcome> {
    return this.queryTransportOutcome(queryJson, sessionJson, tier, optionsJson, {
      preferEncoded: true,
    });
  }

  private async queryTransportOutcome(
    queryJson: string,
    sessionJson: string | null | undefined,
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    { preferEncoded }: { preferEncoded: boolean },
  ): Promise<QueryTransportOutcome> {
    assertSupportedReadOptions(tier, optionsJson);
    assertTransactionReadOpen(optionsJson, this.pendingTxs, this.completedTxs);
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    assertNoUnsupportedPermissionIntrospection(queryJson);
    const coreQueryJson = addNestedOuterColumns(queryJson);
    const opts = readOptions(tier, queryIncludesDeleted(coreQueryJson), optionsJson);
    if (queryUsesNativeRelationApi(coreQueryJson)) {
      if (session) {
        if (!this.db.allRelationQueryForIdentity) {
          throw new Error("Native runtime does not support session-scoped relation queries");
        }
        const payload = this.db.allRelationQueryForIdentity(coreQueryJson, session.identity, opts);
        return preferEncoded
          ? { kind: "encoded_row_batches", payload }
          : { kind: "rows", rows: rowsFromBatches(readRowBatches(payload), this.schema) };
      }
      if (!this.db.allRelationQuery) {
        throw new Error("Native runtime does not support relation queries");
      }
      const payload = this.db.allRelationQuery(coreQueryJson, opts);
      return preferEncoded
        ? { kind: "encoded_row_batches", payload }
        : { kind: "rows", rows: rowsFromBatches(readRowBatches(payload), this.schema) };
    }
    const query = this.prepareQuery(coreQueryJson);
    const attachment = await this.attachQueryIfNeeded(tier, optionsJson, query, session);
    this.attachLocalReadCoverageInBackground(tier, optionsJson, query, session);
    try {
      if (queryHasArraySubqueries(coreQueryJson)) {
        if (session) {
          if (!this.db.allRelationSnapshotForIdentity) {
            throw new Error("Native runtime does not support session-scoped relation snapshots");
          }
          const payload = this.db.allRelationSnapshotForIdentity(query, session.identity, opts);
          return preferEncoded
            ? { kind: "encoded_relation_snapshot", payload }
            : {
                kind: "rows",
                rows: rowsFromRelationSnapshot(
                  readRelationSnapshot(payload),
                  this.schema,
                  relationMaterializationSpec(coreQueryJson, this.schema),
                ),
              };
        }
        if (!this.db.allRelationSnapshot) {
          throw new Error("Native runtime does not support relation snapshots");
        }
        const payload = this.db.allRelationSnapshot(query, opts);
        return preferEncoded
          ? { kind: "encoded_relation_snapshot", payload }
          : {
              kind: "rows",
              rows: rowsFromRelationSnapshot(
                readRelationSnapshot(payload),
                this.schema,
                relationMaterializationSpec(coreQueryJson, this.schema),
              ),
            };
      }
      // The plain path can ship encoded only when the adapter adds nothing on
      // top of the engine result: an edge/global read may refresh and re-query,
      // and an open transaction read overlays pending writes — both require
      // decoded rows here.
      if (
        preferEncoded &&
        tier !== "edge" &&
        tier !== "global" &&
        txIdFromOptions(optionsJson) === undefined
      ) {
        const payload = session
          ? this.db.allForIdentity(query, session.identity, opts)
          : this.db.all(query, opts);
        return { kind: "encoded_row_batches", payload };
      }
      let rows = session
        ? this.db.allForIdentity(query, session.identity, opts)
        : this.db.all(query, opts);
      let rowStates = rowsFromBatches(readRowBatches(rows), this.schema);
      if ((tier === "edge" || tier === "global") && rowStates.length > 0) {
        await this.refreshRowsFromEdge(session, rowStates);
        rows = session
          ? this.db.allForIdentity(query, session.identity, opts)
          : this.db.all(query, opts);
        rowStates = rowsFromBatches(readRowBatches(rows), this.schema);
      }
      return {
        kind: "rows",
        rows: this.applyTransactionReadOverlay(rowStates, coreQueryJson, optionsJson),
      };
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
    if (queryIncludesDeleted(queryJson)) {
      throw new Error("Native runtime does not support include_deleted subscriptions yet");
    }
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    assertNoUnsupportedPermissionIntrospection(queryJson);
    const usesNativeRelationApi = queryUsesNativeRelationApi(queryJson);
    if (usesNativeRelationApi) {
      if (!this.db.subscribeRelationQuery) {
        throw new Error("Native runtime does not support relation query subscriptions");
      }
      if (session && !this.db.subscribeRelationQueryForIdentity) {
        throw new Error(
          "Native runtime does not support session-scoped relation query subscriptions",
        );
      }
    } else if (!this.db.subscribe) {
      throw new Error("Native runtime does not support subscriptions");
    }
    if (!usesNativeRelationApi && session && !this.db.subscribeForIdentity) {
      throw new Error("Native runtime does not support session-scoped subscriptions");
    }
    const handle = this.nextSubscriptionId++;
    const opts = readOptions(tier, false, optionsJson);
    const identity = session?.identity;
    let nativeSubscription: ReadableStream<unknown> | Subscription;
    try {
      if (usesNativeRelationApi) {
        nativeSubscription = identity
          ? this.db.subscribeRelationQueryForIdentity!(queryJson, identity, opts)
          : this.db.subscribeRelationQuery!(queryJson, opts);
      } else {
        const query = this.prepareQuery(queryJson);
        nativeSubscription = identity
          ? this.db.subscribeForIdentity!(query, identity, opts)
          : this.db.subscribe!(query, opts);
      }
    } catch (error) {
      throw new Error(`Core subscribe failed for ${queryJson}: ${errorMessage(error)}`);
    }
    const snapshotRefresh = !usesNativeRelationApi && typeof this.db.all === "function";
    this.subscriptions.set(handle, {
      sources: [{ source: subscriptionSource(nativeSubscription), reading: false }],
      queryJson,
      query: usesNativeRelationApi ? null : this.prepareQuery(queryJson),
      identity,
      rows: [],
      rowIndexByKey: new Map(),
      visibleRows: [],
      relationRows: [],
      relationRootCount: 0,
      relationEdges: [],
      relationMaterialization: usesNativeRelationApi
        ? emptyRelationMaterializationSpec()
        : relationMaterializationSpec(queryJson, this.schema),
      outputColumns: usesNativeRelationApi
        ? null
        : subscriptionOutputColumns(queryJson, this.schema),
      snapshotRefresh,
      session,
      opts,
      opened: false,
      visibleOpened: false,
      deferredVisiblePublication: false,
      deferredVisibleReset: false,
      cancelled: false,
    });
    return handle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate;
    if (subscription.visibleOpened) {
      subscription.callback(
        nativeResetDeltaFromRows(subscription.visibleRows, this.schema, subscription.outputColumns),
      );
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

  connect(url: string, authJson: string): void {
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
        this.pendingInboundServerFrames.push(frame);
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
  }

  disconnect(options: { rejectWaiters?: boolean } = {}): void {
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverTransportError = null;
    if (options.rejectWaiters ?? true) {
      this.resolveServerTransportErrorWaiters(new Error("server transport disconnected"));
    } else {
      this.clearServerTransportErrorWaiters();
    }
    this.serverTransport?.close();
    this.serverTransport = null;
    this.serverEndpointUrl = null;
    this.queuedServerFrames.length = 0;
    this.pendingInboundServerFrames.length = 0;
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
    values: InsertValues,
    write: Write,
  ): InsertResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.observeWriteForBoundaryEffects(write);
    const row = this.rowStateFromValues(table, rowId, values);
    return { id: row.id, values: row.values, transactionId };
  }

  private finishMutation(write: Write): MutationResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.observeWriteForBoundaryEffects(write);
    return { transactionId };
  }

  private observeWriteForBoundaryEffects(write: Write): void {
    if (this.serverPumpObservedWrites.has(write)) return;
    this.serverPumpObservedWrites.add(write);
    this.scheduleServerPump();

    const pumpUntilLocalVisible = async () => {
      for (;;) {
        if (this.closed) return;
        try {
          write.wait("local");
          this.pumpSubscriptions();
          this.refreshOpenedPlainSubscriptions();
          return;
        } catch (error) {
          if (!isPendingWaitError(error)) return;
        }

        try {
          await write.nextWriteStateChange();
        } catch {
          return;
        }
      }
    };

    const pumpUntilSettled = async () => {
      for (;;) {
        if (this.closed) return;
        try {
          write.wait("edge");
          this.pumpSubscriptions();
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
        // Write-state progression can make local maintained subscriptions
        // observe inserts/updates/deletes even when the app fire-and-forgets
        // the write handle. Keep subscription pumping paired with the server
        // pump here so write acks are not the only path that drains changes.
        this.pumpSubscriptions();
        this.refreshOpenedPlainSubscriptions();
        this.scheduleServerPump();
      }
    };

    void pumpUntilLocalVisible();
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
    if (!identity) return this.readRowForWriteMerge(table, rowId);
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = this.db.allForIdentity(query, identity, readOptions());
    return rowsFromBatches(readRowBatches(rows), this.schema).find(
      (row) => row.table === table && row.id === formatUuid(rowId),
    );
  }

  private readRowForWriteMerge(table: string, rowId: Uint8Array): RowState | undefined {
    const exactReader = (
      this.db as { localCurrentRow?: (table: string, rowId: Uint8Array) => Uint8Array }
    ).localCurrentRow;
    if (exactReader) {
      const rows = rowsFromBatches(
        readRowBatches(exactReader.call(this.db, table, rowId)),
        this.schema,
      );
      return rows[0];
    }
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = this.db.all(query, readOptions());
    return rowsFromBatches(readRowBatches(rows), this.schema).find(
      (row) => row.table === table && row.id === formatUuid(rowId),
    );
  }

  private rowStateFromValues(
    table: string,
    rowId: Uint8Array,
    values: Record<string, Value>,
  ): RowState {
    const visibleColumns = this.table(table).columns.filter(
      (column) => !isInternalField(column.name) && !isHiddenIncludeColumn(column.name),
    );
    const valuesByColumn = new Map<string, Value>();
    for (const column of visibleColumns) {
      valuesByColumn.set(column.name, values[column.name] ?? column.default ?? { type: "Null" });
    }
    return withValuesByColumn(
      {
        table,
        id: formatUuid(rowId),
        values: visibleColumns
          .filter((column) => !isProvenanceMagicColumn(column.name))
          .map((column) => valuesByColumn.get(column.name)!),
      },
      valuesByColumn,
    );
  }

  private mergeRowState(
    table: string,
    rowId: Uint8Array,
    patch: Record<string, Value>,
    tx: PendingTx,
    _identity?: Uint8Array,
  ): RowState {
    const current = this.pendingRow(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId);
    const merged: Record<string, Value> = {};
    for (const column of this.table(table).columns) {
      const existing = current?.valuesByColumn?.get(column.name);
      if (existing !== undefined) merged[column.name] = existing;
    }
    Object.assign(merged, patch);
    return this.rowStateFromValues(table, rowId, merged);
  }

  private pendingRow(tx: PendingTx, table: string, rowId: Uint8Array): RowState | undefined {
    const id = formatUuid(rowId);
    for (let index = tx.writes.length - 1; index >= 0; index -= 1) {
      const write = tx.writes[index]!;
      if (write.table !== table || formatUuid(write.rowId) !== id) continue;
      return write.deleted ? undefined : write.row;
    }
    return undefined;
  }

  private applyTransactionReadOverlay(
    committedRows: RowState[],
    queryJson: string,
    optionsJson?: string | null,
  ): RowState[] {
    const transactionId = txIdFromOptions(optionsJson);
    if (!transactionId) return committedRows;
    const tx = this.pendingTxs.get(transactionId);
    if (!tx) return committedRows;

    let query: {
      table?: unknown;
      conditions?: unknown;
      orderBy?: unknown;
      limit?: unknown;
      offset?: unknown;
    };
    try {
      query = JSON.parse(queryJson) as typeof query;
    } catch {
      return committedRows;
    }
    if (typeof query.table !== "string") return committedRows;

    const rowsById = new Map(committedRows.map((row) => [row.id, row]));
    for (const write of tx.writes) {
      if (write.table !== query.table) continue;
      const id = formatUuid(write.rowId);
      if (write.deleted) {
        rowsById.delete(id);
      } else if (write.row) {
        rowsById.set(id, write.row);
      }
    }

    let rows = [...rowsById.values()].filter((row) => this.rowMatchesQuery(row, query));
    if (Array.isArray(query.orderBy) && query.orderBy.length > 0) {
      rows = this.sortRowsForQuery(rows, query.orderBy);
    }
    const offset = typeof query.offset === "number" && query.offset > 0 ? query.offset : 0;
    const limit = typeof query.limit === "number" && query.limit >= 0 ? query.limit : undefined;
    return limit === undefined ? rows.slice(offset) : rows.slice(offset, offset + limit);
  }

  private rowMatchesQuery(row: RowState, query: { conditions?: unknown }): boolean {
    if (!Array.isArray(query.conditions)) return true;
    return query.conditions.every((condition) => {
      if (!condition || typeof condition !== "object") return false;
      const { column, op, value } = condition as {
        column?: unknown;
        op?: unknown;
        value?: unknown;
      };
      if (typeof column !== "string" || op !== "eq") return false;
      const actual = column === "id" ? uuidValue(row.id) : row.valuesByColumn?.get(column);
      return actual !== undefined && queryValueEqual(actual, value);
    });
  }

  private sortRowsForQuery(rows: RowState[], orderBy: unknown[]): RowState[] {
    const clauses = orderBy.filter(
      (clause): clause is [string, "asc" | "desc"] =>
        Array.isArray(clause) &&
        typeof clause[0] === "string" &&
        (clause[1] === "asc" || clause[1] === "desc"),
    );
    return [...rows].sort((left, right) => {
      for (const [column, direction] of clauses) {
        const comparison = compareQueryValues(
          column === "id" ? uuidValue(left.id) : left.valuesByColumn?.get(column),
          column === "id" ? uuidValue(right.id) : right.valuesByColumn?.get(column),
        );
        if (comparison !== 0) return direction === "desc" ? -comparison : comparison;
      }
      return left.id.localeCompare(right.id);
    });
  }

  private refreshOpenedPlainSubscriptions(): void {
    for (const subscription of this.subscriptions.values()) {
      if (subscription.cancelled || !subscription.opened || !subscription.callback) continue;

      const previousRows = subscription.rows;
      const nextRows =
        subscription.query === null
          ? this.refreshNativeRelationSubscriptionRows(subscription)
          : subscription.relationMaterialization.arraySubqueries.length > 0
            ? this.refreshRelationSubscriptionRows(subscription)
            : this.refreshPlainSubscriptionRows(subscription);
      const delta = nativeDeltaFromRows(
        nextRows,
        previousRows,
        this.schema,
        subscription.outputColumns,
      );
      if (!nativeDeltaHasChanges(delta)) continue;
      subscription.rows = nextRows;
      subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
      subscription.callback(delta);
    }
  }

  private refreshPlainSubscriptionRows(subscription: SubscriptionState): RowState[] {
    if (!subscription.query) return [];
    const rowsBytes = subscription.identity
      ? this.db.allForIdentity(subscription.query, subscription.identity, subscription.opts)
      : this.db.all(subscription.query, subscription.opts);
    return rowsFromBatches(readRowBatches(rowsBytes), this.schema);
  }

  private warnedOnce = new Set<string>();
  private warnOnce(key: string, message: string): void {
    if (this.warnedOnce.has(key)) return;
    this.warnedOnce.add(key);
    console.warn(`[jazz native-runtime] ${message}`);
  }

  private async refreshRowsFromEdge(
    session: RuntimeSession | null,
    rows: RowState[],
  ): Promise<void> {
    if (!this.serverTransport || rows.length === 0) return;
    const seen = new Set<string>();
    for (const row of rows) {
      const key = rowKey(row.table, row.id);
      if (seen.has(key)) continue;
      seen.add(key);
      const queryJson = JSON.stringify({
        table: row.table,
        conditions: [{ column: "id", op: "eq", value: row.id }],
        array_subqueries: [],
      });
      const query = this.prepareQuery(queryJson);
      let attachment: unknown;
      try {
        attachment = await this.attachQueryIfNeeded("edge", null, query, session);
        const opts = readOptions("edge", false, null);
        if (session) {
          this.db.allForIdentity(query, session.identity, opts);
        } else {
          this.db.all(query, opts);
        }
      } finally {
        if (attachment !== undefined) this.db.detachQuery?.(attachment);
      }
    }
  }

  private refreshNativeRelationSubscriptionRows(subscription: SubscriptionState): RowState[] {
    if (
      !this.db.allRelationQuery ||
      (subscription.identity && !this.db.allRelationQueryForIdentity)
    ) {
      return [];
    }
    const rowsBytes = subscription.identity
      ? this.db.allRelationQueryForIdentity!(
          subscription.queryJson,
          subscription.identity,
          subscription.opts,
        )
      : this.db.allRelationQuery(subscription.queryJson, subscription.opts);
    return rowsFromBatches(readRowBatches(rowsBytes), this.schema);
  }

  private refreshRelationSubscriptionRows(subscription: SubscriptionState): RowState[] {
    if (!subscription.query) return [];
    if (
      !this.db.allRelationSnapshot ||
      (subscription.identity && !this.db.allRelationSnapshotForIdentity)
    ) {
      return this.refreshPlainSubscriptionRows(subscription);
    }
    const payload = subscription.identity
      ? this.db.allRelationSnapshotForIdentity!(
          subscription.query,
          subscription.identity,
          subscription.opts,
        )
      : this.db.allRelationSnapshot(subscription.query, subscription.opts);
    const snapshot = readRelationSnapshot(payload);
    subscription.relationRows = rowsFromBatches(snapshot.rows, this.schema);
    subscription.relationRootCount = snapshot.rootCount;
    subscription.relationEdges = snapshot.edges;
    return materializeRelationRows(
      subscription.relationRows,
      subscription.relationEdges,
      subscription.relationRootCount,
      subscription.relationMaterialization,
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

  private attachLocalReadCoverageInBackground(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: PreparedQuery,
    session: RuntimeSession | null,
  ): void {
    if (tier != null && tier !== "local") return;
    if (!readPropagationIsFull(optionsJson)) return;
    if (!this.serverTransport || !this.db.attachQuery) return;

    const refresh = async () => {
      await this.serverCarrierPromise;
      const edgeOptionsJson = JSON.stringify({ propagation: "full" });
      const attachment = await this.attachQueryIfNeeded("edge", edgeOptionsJson, query, session);
      if (attachment !== undefined) this.db.detachQuery?.(attachment);
    };

    void refresh().catch((error: unknown) => {
      if (this.closed) return;
      if (error instanceof Error && error.message === "Timed out waiting for edge query coverage") {
        return;
      }
      this.handleServerTransportError(error);
    });
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
      try {
        await (transportError ? Promise.race([sleep(10), transportError.promise]) : sleep(10));
      } finally {
        transportError?.cancel();
      }
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
    operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
  ): PendingTx | undefined {
    const id = txIdFromContext(writeContext);
    if (!id) return undefined;
    const pending = this.pendingTxs.get(id);
    if (pending) return pending;
    throw new Error(`${operation} failed: WriteError("${txStateMessage(id, this.completedTxs)}")`);
  }

  private txForWrite(pending: PendingTx, identity: Uint8Array | undefined): Tx {
    if (pending.kind === "exclusive") {
      if (identity && schemaHasPolicies(this.schema)) {
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

  private baseRowForExclusiveWrite(
    tx: PendingTx,
    table: string,
    rowId: Uint8Array,
    identity: Uint8Array | undefined,
  ): RowState | undefined {
    if (tx.kind !== "exclusive") return undefined;
    const id = formatUuid(rowId);
    for (const write of tx.writes) {
      if (write.table === table && formatUuid(write.rowId) === id) {
        return write.baseRow;
      }
    }
    return this.readRow(table, rowId, identity);
  }

  private rejectMovedExclusiveParents(tx: PendingTx): void {
    if (tx.kind !== "exclusive") return;
    for (const write of tx.writes) {
      const current = this.readRowForWriteMerge(write.table, write.rowId);
      if (rowsMatchForExclusiveParent(this.schema, write.table, current, write.baseRow)) {
        continue;
      }
      throw new Error(
        "(transaction_conflict): row visible parent changed since transaction write was staged",
      );
    }
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
    if (chunk.type === "snapshot") {
      const previousRows = subscription.rows;
      const wasOpened = subscription.opened;
      subscription.relationRows = rowsFromBatches(chunk.snapshot.rows, this.schema);
      subscription.relationRootCount = chunk.snapshot.rootCount;
      subscription.relationEdges = chunk.snapshot.edges;
      subscription.rows = materializeRelationRows(
        subscription.relationRows,
        subscription.relationEdges,
        subscription.relationRootCount,
        subscription.relationMaterialization,
      );
      subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
      subscription.opened = true;
      this.publishSubscriptionRows(
        subscription,
        wasOpened
          ? nativeDeltaFromRows(
              subscription.rows,
              previousRows,
              this.schema,
              subscription.outputColumns,
            )
          : nativeResetDeltaFromRows(subscription.rows, this.schema, subscription.outputColumns),
        chunk.settled,
        !wasOpened,
      );
    } else if (
      chunk.relationSnapshot &&
      subscription.relationMaterialization.arraySubqueries.length > 0
    ) {
      const previousRows = subscription.rows;
      subscription.relationRows = rowsFromBatches(chunk.relationSnapshot.rows, this.schema);
      subscription.relationRootCount = chunk.relationSnapshot.rootCount;
      subscription.relationEdges = chunk.relationSnapshot.edges;
      subscription.rows = materializeRelationRows(
        subscription.relationRows,
        subscription.relationEdges,
        subscription.relationRootCount,
        subscription.relationMaterialization,
      );
      subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
      this.publishSubscriptionRows(
        subscription,
        nativeDeltaFromRows(
          subscription.rows,
          previousRows,
          this.schema,
          subscription.outputColumns,
        ),
        chunk.settled,
        false,
      );
    } else {
      if (chunk.reset) {
        subscription.rows = [];
        subscription.rowIndexByKey = new Map();
      }
      if (chunk.relationDelta && subscription.relationMaterialization.arraySubqueries.length > 0) {
        const previousRows = subscription.rows;
        applyRelationSubscriptionDelta(
          subscription,
          chunk.delta,
          chunk.relationDelta,
          this.schema,
          chunk.reset === true,
        );
        subscription.rows = materializeRelationRows(
          subscription.relationRows,
          subscription.relationEdges,
          subscription.relationRootCount,
          subscription.relationMaterialization,
        );
        subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
        subscription.opened = true;
        const wireDelta = chunk.reset
          ? nativeResetDeltaFromRows(subscription.rows, this.schema, subscription.outputColumns)
          : nativeDeltaFromRows(
              subscription.rows,
              previousRows,
              this.schema,
              subscription.outputColumns,
            );
        this.publishSubscriptionRows(subscription, wireDelta, chunk.settled, chunk.reset === true);
      } else if (subscription.snapshotRefresh) {
        const previousRows = subscription.rows;
        // Guarded so the argument never evaluates without a server transport:
        // memory-backed chunks carry a different updated-payload shape and the
        // callers swallow rejections, which silently killed delivery. The
        // refresh is an optimization: any failure (including payload-shape
        // decode errors on individual subscriptions) must degrade to a plain
        // snapshot refresh, never kill delivery for that subscription.
        // Scope: post-open update convergence only. During initial ingest
        // (reset chunks / not-yet-opened subscriptions) the refresh would fire
        // per-row edge queries across the whole snapshot — a multi-second cold
        // open tax with no correctness benefit.
        if (this.serverTransport && subscription.opened && !chunk.reset) {
          try {
            await this.refreshRowsFromEdge(
              subscription.session,
              rowsFromBatches(chunk.delta.updated, this.schema),
            );
          } catch (error) {
            this.warnOnce(
              "edge-refresh-decode",
              `edge refresh skipped for a subscription chunk: ${String(error)}`,
            );
          }
        }
        subscription.rows = this.refreshPlainSubscriptionRows(subscription);
        subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
        subscription.opened = true;
        const wireDelta = chunk.reset
          ? nativeResetDeltaFromRows(subscription.rows, this.schema, subscription.outputColumns)
          : nativeDeltaFromRows(
              subscription.rows,
              previousRows,
              this.schema,
              subscription.outputColumns,
            );
        this.publishSubscriptionRows(subscription, wireDelta, chunk.settled, chunk.reset === true);
      } else {
        const applied = applySubscriptionDeltaWithWireDelta(
          subscription.rows,
          subscription.rowIndexByKey,
          chunk.delta,
          this.schema,
          chunk.reset === true,
        );
        subscription.rows = applied.rows;
        subscription.rowIndexByKey = applied.rowIndexByKey;
        subscription.opened = true;
        this.publishSubscriptionRows(
          subscription,
          applied.wireDelta,
          chunk.settled,
          chunk.reset === true,
        );
      }
    }
  }

  private publishSubscriptionRows(
    subscription: SubscriptionState,
    wireDelta: NativeRowDelta,
    settled: boolean | undefined,
    reset: boolean,
  ): void {
    if (this.subscriptionCallbacksAreSettledGated(subscription) && settled === false) {
      subscription.deferredVisiblePublication = true;
      subscription.deferredVisibleReset ||= reset;
      return;
    }

    let visibleDelta = wireDelta;
    if (
      subscription.deferredVisiblePublication ||
      subscription.deferredVisibleReset ||
      !subscription.visibleOpened
    ) {
      visibleDelta =
        subscription.deferredVisibleReset || !subscription.visibleOpened
          ? nativeResetDeltaFromRows(subscription.rows, this.schema, subscription.outputColumns)
          : nativeDeltaFromRows(
              subscription.rows,
              subscription.visibleRows,
              this.schema,
              subscription.outputColumns,
            );
    }

    subscription.callback?.(visibleDelta);
    subscription.visibleRows = [...subscription.rows];
    subscription.visibleOpened = true;
    subscription.deferredVisiblePublication = false;
    subscription.deferredVisibleReset = false;
  }

  private subscriptionCallbacksAreSettledGated(subscription: SubscriptionState): boolean {
    return (subscription.opts as { tier?: unknown }).tier === "global";
  }

  private scheduleServerPump(): void {
    if (this.closed || !this.serverTransport || this.serverPumpScheduled) return;
    this.serverPumpScheduled = true;
    setTimeout(() => {
      this.serverPumpScheduled = false;
      if (this.closed) return;
      this.pumpServerTransport();
      if (this.serverPumpAgain) {
        this.serverPumpAgain = false;
        this.scheduleServerPump();
      }
    }, SERVER_PUMP_DEBOUNCE_MS);
  }

  private pumpServerTransport(): void {
    const transport = this.serverTransport;
    if (this.closed || !transport) return;
    this.drainPendingInboundServerFrames(transport);
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

  private drainPendingInboundServerFrames(transport: Transport): void {
    if (this.pendingInboundServerFrames.length === 0) return;
    const frames = this.pendingInboundServerFrames.splice(0);
    if (transport.sendWireFrames) {
      transport.sendWireFrames(frames);
      return;
    }
    for (const frame of frames) transport.sendWireFrame(frame);
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
    this.failActiveSubscriptions(this.serverTransportError);
    this.resolveServerTransportErrorWaiters(this.serverTransportError);
  }

  private failActiveSubscriptions(error: Error): void {
    for (const subscription of this.subscriptions.values()) {
      if (subscription.cancelled) continue;
      subscription.cancelled = true;
      for (const source of subscription.sources) {
        closeSubscriptionSource(source.source);
      }
      try {
        subscription.callback?.(error, null);
      } catch (callbackError) {
        setTimeout(() => {
          throw callbackError;
        }, 0);
      }
    }
  }

  private throwServerTransportErrorForTier(tier: string): void {
    if ((tier === "edge" || tier === "global") && this.serverTransportError) {
      throw this.serverTransportError;
    }
  }

  private waitForServerTransportError(
    tier: string,
  ): { promise: Promise<never>; cancel: () => void } | null {
    if (tier !== "edge" && tier !== "global") return null;
    if (this.serverTransportError) {
      return {
        promise: Promise.reject(this.serverTransportError),
        cancel: () => {},
      };
    }
    const waiter: ServerTransportErrorWaiter = {
      active: true,
      reject: () => {},
    };
    const promise = new Promise<never>((_, reject) => {
      waiter.reject = reject;
    });
    this.serverTransportErrorWaiters.push(waiter);
    return {
      promise,
      cancel: () => {
        waiter.active = false;
      },
    };
  }

  private resolveServerTransportErrorWaiters(error: Error): void {
    const waiters = this.serverTransportErrorWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.active) waiter.reject(error);
    }
  }

  private clearServerTransportErrorWaiters(): void {
    for (const waiter of this.serverTransportErrorWaiters) {
      waiter.active = false;
    }
    this.serverTransportErrorWaiters.length = 0;
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

function updatedAtMsFromWriteContext(writeContext?: string | null): number | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { updated_at?: unknown };
    if (typeof parsed.updated_at !== "number") return undefined;
    return Math.trunc(parsed.updated_at / 1_000);
  } catch {
    return undefined;
  }
}

function effectiveUpdatedAtMs(writeContext?: string | null): number | null {
  return updatedAtMsFromWriteContext(writeContext) ?? Date.now();
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
  if (options.propagate === false && options.propagation == null) {
    readOptions.propagation = "local_only";
  }
  if (options.propagation === "local-only") readOptions.propagation = "local_only";
  if (options.propagation === "full") readOptions.propagation = "full";
  return readOptions;
}

function readPropagationIsFull(optionsJson?: string | null): boolean {
  if (optionsJson == null) return true;
  try {
    const options = JSON.parse(optionsJson) as { propagation?: unknown; propagate?: unknown };
    if (options.propagation != null) return options.propagation === "full";
    return options.propagate !== false;
  } catch {
    return true;
  }
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

function runtimeSessionFromPublicSession(session: Session | undefined): RuntimeSession | null {
  if (!session) return null;
  return {
    user_id: session.user_id,
    claims: sessionClaims(session.user_id, session.claims),
    identity: authorBytesForSubject(session.user_id),
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
  if (parsed.read_view != null || parsed.readView != null) {
    throw new Error("Native runtime does not support non-default read_view yet");
  }
  const propagation = parsed.propagation;
  if (propagation != null && propagation !== "full" && propagation !== "local-only") {
    throw new Error(
      `Native runtime does not support read propagation '${String(propagation)}' yet`,
    );
  }
  const propagate = parsed.propagate;
  if (propagate != null && typeof propagate !== "boolean") {
    throw new Error(`Native runtime does not support read propagate '${String(propagate)}' yet`);
  }
}

function queryIncludesDeleted(queryJson: string): boolean {
  try {
    return (JSON.parse(queryJson) as { include_deleted?: unknown }).include_deleted === true;
  } catch {
    return false;
  }
}

function queryHasArraySubqueries(queryJson: string): boolean {
  try {
    const value = (JSON.parse(queryJson) as { array_subqueries?: unknown }).array_subqueries;
    return Array.isArray(value) && value.length > 0;
  } catch {
    return false;
  }
}

function queryUsesNativeRelationApi(queryJson: string): boolean {
  try {
    const relationIr = (JSON.parse(queryJson) as { relation_ir?: unknown }).relation_ir;
    return relationIrContainsNativeOperator(relationIr);
  } catch {
    return false;
  }
}

function relationIrContainsNativeOperator(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(relationIrContainsNativeOperator);
  const record = value as Record<string, unknown>;
  if ("Join" in record || "Gather" in record || "Union" in record) return true;
  return Object.values(record).some(relationIrContainsNativeOperator);
}

function assertNoUnsupportedPermissionIntrospection(queryJson: string): void {
  if (!queryContainsPermissionIntrospection(queryJson)) return;
  throw new Error(
    "Native runtime does not support permission-introspection query columns or predicates " +
      "($canRead) until unified policy lowering exists.",
  );
}

function queryContainsPermissionIntrospection(queryJson: string): boolean {
  const parsed = JSON.parse(queryJson) as {
    conditions?: unknown;
    relation_ir?: unknown;
    select?: unknown;
    select_columns?: unknown;
    array_subqueries?: unknown;
  };
  return (
    selectedColumnsContainPermissionIntrospection(parsed.select_columns ?? parsed.select) ||
    flatConditionsContainPermissionIntrospection(parsed.conditions) ||
    relationIrContainsPermissionPredicate(parsed.relation_ir) ||
    relationIrContainsPermissionProjection(parsed.relation_ir) ||
    arraySubqueriesContainPermissionIntrospection(parsed.array_subqueries)
  );
}

function relationIrContainsPermissionPredicate(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(relationIrContainsPermissionPredicate);
  const record = value as Record<string, unknown>;
  const column =
    readMagicPredicateColumn(record.Cmp) ??
    readMagicPredicateColumn(record.In) ??
    readMagicPredicateColumn(record.IsNull) ??
    readMagicPredicateColumn(record.IsNotNull) ??
    readMagicPredicateColumn(record.Contains);
  if (column && isPermissionIntrospectionColumn(column)) return true;
  return Object.values(record).some(relationIrContainsPermissionPredicate);
}

function relationIrContainsPermissionProjection(value: unknown): boolean {
  if (!value || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(relationIrContainsPermissionProjection);
  const record = value as Record<string, unknown>;
  const project = record.Project;
  if (project && typeof project === "object") {
    const columns = (project as { columns?: unknown }).columns;
    if (Array.isArray(columns)) {
      for (const entry of columns) {
        if (!entry || typeof entry !== "object") continue;
        const projection = entry as { expr?: unknown; source?: unknown };
        const column = readProjectedColumnRef(projection.expr ?? projection.source);
        if (column && isPermissionIntrospectionColumn(column)) return true;
      }
    }
  }
  return Object.values(record).some(relationIrContainsPermissionProjection);
}

function readMagicPredicateColumn(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const record = value as { left?: unknown; column?: unknown };
  return readColumnRef(record.left ?? record.column);
}

function readProjectedColumnRef(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  if ("Column" in value) {
    return readColumnRef((value as { Column?: unknown }).Column);
  }
  return readColumnRef(value);
}

function selectedColumnsContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some(
    (column) =>
      typeof column === "string" && isPermissionIntrospectionColumn(unqualifiedColumn(column)),
  );
}

function flatConditionsContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const column = (entry as { column?: unknown }).column;
    return typeof column === "string" && isPermissionIntrospectionColumn(unqualifiedColumn(column));
  });
}

function arraySubqueriesContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const record = entry as {
      filters?: unknown;
      nested_arrays?: unknown;
      select?: unknown;
      select_columns?: unknown;
    };
    return (
      selectedColumnsContainPermissionIntrospection(record.select_columns ?? record.select) ||
      arrayFiltersContainPermissionIntrospection(record.filters) ||
      arraySubqueriesContainPermissionIntrospection(record.nested_arrays)
    );
  });
}

function arrayFiltersContainPermissionIntrospection(value: unknown): boolean {
  if (!Array.isArray(value)) return false;
  return value.some((entry) => {
    if (!entry || typeof entry !== "object") return false;
    const record = entry as Record<string, unknown>;
    for (const key of ["Eq", "Ne", "Gt", "Ge", "Lt", "Le", "IsNull", "IsNotNull", "Contains"]) {
      const predicate = record[key];
      if (!predicate || typeof predicate !== "object") continue;
      const column = (predicate as { column?: unknown }).column;
      if (
        typeof column === "string" &&
        isPermissionIntrospectionColumn(unqualifiedColumn(column))
      ) {
        return true;
      }
    }
    return false;
  });
}

function unqualifiedColumn(column: string): string {
  return column.split(".").at(-1) ?? column;
}

function emptyRelationMaterializationSpec(): RelationMaterializationSpec {
  return {
    rootTable: null,
    arraySubqueries: [],
    clientLimit: null,
    clientOffset: 0,
  };
}

function relationMaterializationSpec(
  queryJson: string,
  schema: WasmSchema,
): RelationMaterializationSpec {
  const parsed = JSON.parse(queryJson) as {
    table?: unknown;
    array_subqueries?: unknown;
    __jazz_client_limit?: unknown;
    __jazz_client_offset?: unknown;
  };
  if (typeof parsed.table !== "string") {
    return emptyRelationMaterializationSpec();
  }
  return {
    rootTable: parsed.table,
    arraySubqueries: readQueryArraySubqueries(parsed.array_subqueries, parsed.table, schema) ?? [],
    clientLimit: parsed.__jazz_client_limit == null ? null : readLimit(parsed.__jazz_client_limit),
    clientOffset: parsed.__jazz_client_offset == null ? 0 : readOffset(parsed.__jazz_client_offset),
  };
}

function subscriptionOutputColumns(
  queryJson: string,
  schema: WasmSchema,
): SubscriptionOutputColumns {
  const parsed = JSON.parse(queryJson) as {
    table?: unknown;
    select?: unknown;
    select_columns?: unknown;
    array_subqueries?: unknown;
    relation_ir?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Native runtime only supports table queries in this slice");
  }
  return {
    rootTable: parsed.table,
    rootColumns: outputColumnsForTable(
      parsed.table,
      schema,
      readSelectColumns(parsed.select_columns ?? parsed.select),
      readQueryArraySubqueries(parsed.array_subqueries, parsed.table, schema) ?? [],
    ),
  };
}

function outputColumnsForTable(
  table: string,
  schema: WasmSchema,
  select: string[] | undefined,
  arraySubqueries: readonly QueryArraySubquery[],
): ColumnDescriptor[] {
  const tableSchema = schema[table];
  if (!tableSchema) throw new Error(`missing schema for subscription table ${table}`);
  const selected = select ?? tableSchema.columns.map((column) => column.name);
  const columns = selected
    .map((columnName) => tableSchema.columns.find((column) => column.name === columnName))
    .filter((column): column is ColumnDescriptor => column !== undefined);

  for (const subquery of arraySubqueries) {
    const childColumns = outputColumnsForTable(
      subquery.table,
      schema,
      subquery.select,
      subquery.nestedArrays ?? [],
    );
    columns.push({
      name: subquery.columnName,
      column_type: {
        type: "Array",
        element: { type: "Row", columns: childColumns },
      },
      nullable: false,
    });
  }

  return columns;
}

function addNestedOuterColumns(queryJson: string): string {
  const parsed = JSON.parse(queryJson) as { array_subqueries?: unknown };
  addNestedOuterColumnsToSubqueries(parsed.array_subqueries);
  return JSON.stringify(parsed);
}

function addNestedOuterColumnsToSubqueries(subqueries: unknown): void {
  if (!Array.isArray(subqueries)) return;
  for (const entry of subqueries) {
    if (!entry || typeof entry !== "object") continue;
    const record = entry as {
      select_columns?: unknown;
      nested_arrays?: unknown;
    };
    if (Array.isArray(record.select_columns) && Array.isArray(record.nested_arrays)) {
      for (const nested of record.nested_arrays) {
        if (!nested || typeof nested !== "object") continue;
        const outerColumn = (nested as { outer_column?: unknown }).outer_column;
        if (typeof outerColumn !== "string") continue;
        const column = outerColumn.split(".").at(-1) ?? outerColumn;
        if (!record.select_columns.includes(column)) {
          record.select_columns.push(column);
        }
      }
    }
    addNestedOuterColumnsToSubqueries(record.nested_arrays);
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
  operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
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

function writeError(
  operation: "Insert" | "Restore" | "Update" | "Upsert" | "Delete",
  reason: string,
): Error {
  return new Error(`${operation} failed: WriteError("${reason.replaceAll('"', '\\"')}")`);
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
    try {
      return JSON.stringify(error);
    } catch {
      return Object.prototype.toString.call(error);
    }
  }
  return String(error);
}

function schemaHasPolicies(schema: WasmSchema): boolean {
  return Object.values(schema).some((table) => table.policies !== undefined);
}

function rowsMatchForExclusiveParent(
  schema: WasmSchema,
  table: string,
  current: RowState | undefined,
  base: RowState | undefined,
): boolean {
  if (!current || !base) return current === base;
  if (current.id !== base.id || current.table !== base.table) return false;
  const columns = schema[table]?.columns ?? [];
  for (const column of columns) {
    if (isInternalField(column.name) || isHiddenIncludeColumn(column.name)) continue;
    if (
      !valueEqual(
        current.valuesByColumn?.get(column.name) ?? { type: "Null" },
        base.valuesByColumn?.get(column.name) ?? { type: "Null" },
      )
    ) {
      return false;
    }
  }
  return true;
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
    array_subqueries?: unknown;
    conditions?: unknown;
    table?: unknown;
    limit?: unknown;
    relation_ir?: unknown;
    offset?: unknown;
    order_by?: unknown;
    orderBy?: unknown;
    select?: unknown;
    select_columns?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Native runtime only supports table queries in this slice");
  }
  const encoded = encodeSimpleRelationQuery(parsed.table, parsed, schema);
  return queryWithPredicates(parsed.table, encoded.predicates, {
    limit: readLimitIfPresent(parsed.limit ?? encoded.limit),
    offset: readOffset(parsed.offset ?? encoded.offset),
    orderBy: encoded.orderBy.concat(readRootOrderBy(parsed.order_by ?? parsed.orderBy)),
    select: readSelectColumns(parsed.select_columns ?? parsed.select ?? encoded.select),
    arraySubqueries: readQueryArraySubqueries(parsed.array_subqueries, parsed.table, schema),
  });
}

function unsupportedQueryEncodingError(context?: string): Error {
  const suffix = context ? ` (${context})` : "";
  return new Error(`Native runtime cannot encode this query shape${suffix}.`);
}

function unsupportedRelationQueryError(operator?: string): Error {
  const detail = operator
    ? ` Relation IR operator "${operator}" requires a relation-tree lowerer or native relation query API; the TS native runtime can currently lower only TableScan plus Filter/Project/OrderBy/Offset/Limit into flat native predicates.`
    : " The TS native runtime can currently lower only TableScan plus Filter/Project/OrderBy/Offset/Limit into flat native predicates.";
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
  limit?: number;
  offset: number;
  orderBy: QueryOrder[];
  select?: string[];
} {
  const unwrapped = unwrapSimpleQuery(table, query);
  if (!unwrapped) throw unsupportedRelationQueryError(relationOperator(query.relation_ir));
  const rootPredicates = readFlatConditions(query.conditions);
  if (!rootPredicates) throw unsupportedQueryEncodingError();
  return {
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

function unwrapSimpleQuery(
  table: string,
  query: {
    relation_ir?: unknown;
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

function coerceQueryPredicate(
  table: string,
  filter: QueryPredicate,
  schema: WasmSchema,
): QueryPredicate {
  if (filter.op === "All" || filter.op === "Any") {
    return {
      op: filter.op,
      predicates: filter.predicates.map((predicate) =>
        coerceQueryPredicate(table, predicate, schema),
      ),
    };
  }
  if (filter.op === "In") {
    const columnType =
      filter.column === "id"
        ? ({ type: "Uuid" } as const)
        : schema[table]?.columns.find((entry) => entry.name === filter.column)?.column_type;
    if (columnType?.type === "Array") {
      return {
        op: "Any",
        predicates: filter.values.map((value) =>
          value.type === "Array"
            ? {
                column: filter.column,
                op: "Eq",
                value: coerceQueryLiteral(table, filter.column, value, schema),
              }
            : {
                column: filter.column,
                op: "Contains",
                value: coerceLiteralForColumnType(value, columnType.element, false),
              },
        ),
      };
    }
    return {
      ...filter,
      values: filter.values.map((value) => coerceQueryLiteral(table, filter.column, value, schema)),
    };
  }
  if (filter.op === "Contains") {
    const columnType =
      filter.column === "id"
        ? ({ type: "Uuid" } as const)
        : schema[table]?.columns.find((entry) => entry.name === filter.column)?.column_type;
    return {
      ...filter,
      value:
        columnType?.type === "Array"
          ? coerceLiteralForColumnType(filter.value, columnType.element, false)
          : coerceQueryLiteral(table, filter.column, filter.value, schema),
    };
  }
  if (filter.op === "IsNull" || filter.op === "IsNotNull") return filter;
  if (isQueryPredicateCmp(filter)) {
    return {
      ...filter,
      value: coerceQueryLiteral(table, filter.column, filter.value, schema),
    };
  }
  throw new Error(`unsupported query predicate ${JSON.stringify(filter)}`);
}

function isQueryPredicateCmp(
  predicate: QueryPredicate,
): predicate is Extract<QueryPredicate, { op: QueryPredicateOp }> {
  return (
    predicate.op === "Eq" ||
    predicate.op === "Ne" ||
    predicate.op === "Gt" ||
    predicate.op === "Gte" ||
    predicate.op === "Lt" ||
    predicate.op === "Lte"
  );
}

function readSelectColumns(value: unknown): string[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError();
  if (!value.every((column): column is string => typeof column === "string")) {
    throw unsupportedQueryEncodingError();
  }
  return value;
}

function readRootOrderBy(value: unknown): QueryOrder[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("order_by");
  return value.map((entry) => {
    if (Array.isArray(entry) && entry.length === 2 && typeof entry[0] === "string") {
      if (entry[1] !== "asc" && entry[1] !== "desc") {
        throw unsupportedQueryEncodingError("order_by");
      }
      return { column: entry[0], direction: entry[1] === "asc" ? "Asc" : "Desc" };
    }
    if (!entry || typeof entry !== "object") {
      throw unsupportedQueryEncodingError("order_by");
    }
    const record = entry as { column?: unknown; direction?: unknown };
    if (typeof record.column !== "string") {
      throw unsupportedQueryEncodingError("order_by");
    }
    if (record.direction !== "Asc" && record.direction !== "Desc") {
      throw unsupportedQueryEncodingError("order_by");
    }
    return { column: record.column, direction: record.direction };
  });
}

function readQueryArraySubqueries(
  value: unknown,
  parentTable: string,
  schema: WasmSchema,
): QueryArraySubquery[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries");
  return value.map((entry) => readQueryArraySubquery(entry, parentTable, schema));
}

function readQueryArraySubquery(
  value: unknown,
  parentTable: string,
  schema: WasmSchema,
): QueryArraySubquery {
  if (!value || typeof value !== "object") throw unsupportedQueryEncodingError("array_subqueries");
  const record = value as {
    column_name?: unknown;
    table?: unknown;
    inner_column?: unknown;
    outer_column?: unknown;
    filters?: unknown;
    joins?: unknown;
    select_columns?: unknown;
    order_by?: unknown;
    limit?: unknown;
    requirement?: unknown;
    nested_arrays?: unknown;
  };
  if (
    typeof record.column_name !== "string" ||
    typeof record.table !== "string" ||
    typeof record.inner_column !== "string" ||
    typeof record.outer_column !== "string"
  ) {
    throw unsupportedQueryEncodingError("array_subqueries");
  }
  if (Array.isArray(record.joins) && record.joins.length > 0) {
    throw unsupportedQueryEncodingError("array_subqueries.joins");
  }
  const filters = readArraySubqueryFilters(record.filters, record.table, schema);
  const select = readSelectColumns(record.select_columns);
  const orderBy = readArraySubqueryOrder(record.order_by);
  const nestedArrays = readQueryArraySubqueries(record.nested_arrays, record.table, schema) ?? [];
  return {
    columnName: record.column_name,
    table: record.table,
    innerColumn: record.inner_column,
    outerColumn: stripParentQualifier(record.outer_column, parentTable),
    filters,
    select,
    orderBy,
    limit: record.limit == null ? null : readLimit(record.limit),
    requirement: readArraySubqueryRequirement(record.requirement),
    nestedArrays,
  };
}

function readArraySubqueryFilters(
  value: unknown,
  table: string,
  schema: WasmSchema,
): QueryPredicate[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries.filters");
  const filters: QueryPredicate[] = [];
  for (const entry of value) {
    const next = arraySubqueryFilterToPredicates(entry);
    if (!next) throw unsupportedQueryEncodingError("array_subqueries.filters");
    filters.push(...next.map((filter) => coerceQueryPredicate(table, filter, schema)));
  }
  return filters;
}

function arraySubqueryFilterToPredicates(value: unknown): QueryPredicate[] | null {
  if (!value || typeof value !== "object") return null;
  const record = value as Record<string, unknown>;
  for (const [key, op] of [
    ["Eq", "Eq"],
    ["Ne", "Ne"],
    ["Gt", "Gt"],
    ["Ge", "Gte"],
    ["Lt", "Lt"],
    ["Le", "Lte"],
  ] as const) {
    const entry = record[key];
    if (entry && typeof entry === "object") {
      const { column, value: literal } = entry as { column?: unknown; value?: unknown };
      return typeof column === "string"
        ? [{ column, op, value: valueToQueryLiteral(literal) }]
        : null;
    }
  }
  const isNull = record.IsNull;
  if (isNull && typeof isNull === "object") {
    const column = (isNull as { column?: unknown }).column;
    return typeof column === "string" ? [{ column, op: "IsNull" }] : null;
  }
  const isNotNull = record.IsNotNull;
  if (isNotNull && typeof isNotNull === "object") {
    const column = (isNotNull as { column?: unknown }).column;
    return typeof column === "string" ? [{ column, op: "IsNotNull" }] : null;
  }
  const contains = record.Contains;
  if (contains && typeof contains === "object") {
    const { column, value: literal } = contains as { column?: unknown; value?: unknown };
    return typeof column === "string"
      ? [{ column, op: "Contains", value: valueToQueryLiteral(literal) }]
      : null;
  }
  return null;
}

function readArraySubqueryOrder(value: unknown): QueryOrder[] {
  if (value == null) return [];
  if (!Array.isArray(value)) throw unsupportedQueryEncodingError("array_subqueries.order_by");
  return value.map((entry) => {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
      throw unsupportedQueryEncodingError("array_subqueries.order_by");
    }
    if (entry[1] !== "Ascending" && entry[1] !== "Descending") {
      throw unsupportedQueryEncodingError("array_subqueries.order_by");
    }
    return { column: entry[0], direction: entry[1] === "Ascending" ? "Asc" : "Desc" };
  });
}

function readArraySubqueryRequirement(value: unknown): QueryArraySubquery["requirement"] {
  if (value == null || value === "Optional") return "Optional";
  if (value === "AtLeastOne" || value === "MatchCorrelationCardinality") return value;
  throw unsupportedQueryEncodingError("array_subqueries.requirement");
}

function stripParentQualifier(column: string, parentTable: string): string {
  const prefix = `${parentTable}.`;
  return column.startsWith(prefix) ? column.slice(prefix.length) : column;
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
  const columnType =
    column === "id"
      ? ({ type: "Uuid" } as const)
      : (magicColumnType(column) ??
        schema[table]?.columns.find((entry) => entry.name === column)?.column_type);
  if (columnType?.type === "Bytea" && value.type === "Array") {
    return { type: "Bytea", value: Uint8Array.from(value.value.map(readByteLiteral)) };
  }
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
  if (columnType?.type === "Text" && value.type === "Uuid") {
    return { type: "Text", value: value.value };
  }
  if (columnType?.type === "Double" && value.type === "Integer") {
    return { type: "Double", value: value.value };
  }
  if (columnType?.type === "BigInt" && value.type === "Integer") {
    return { type: "BigInt", value: BigInt(value.value) };
  }
  if (columnType?.type === "Timestamp" && value.type === "Integer") {
    return { type: "Timestamp", value: value.value };
  }
  if (columnType?.type === "Timestamp" && value.type === "Text") {
    const time = Date.parse(value.value);
    if (Number.isFinite(time)) {
      return { type: "Timestamp", value: time };
    }
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
  return column && value ? [{ column, op, value }] : null;
}

function valueToQueryLiteral(value: unknown): QueryLiteral {
  if (value === null || value === undefined) return { type: "Nullable", value: null };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number" && Number.isSafeInteger(value)) return { type: "Integer", value };
  if (typeof value === "number" && Number.isFinite(value)) return { type: "Double", value };
  if (typeof value === "bigint") return { type: "BigInt", value };
  if (typeof value === "string")
    return isUuidString(value) ? { type: "Uuid", value } : { type: "Text", value };
  if (value instanceof Uint8Array) return { type: "Bytea", value };
  if (Array.isArray(value)) return { type: "Array", value: value.map(valueToQueryLiteral) };
  throw unsupportedQueryEncodingError();
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
    record.type === "Integer" &&
    typeof record.value === "number" &&
    Number.isSafeInteger(record.value)
  ) {
    return { type: "Integer", value: record.value };
  }
  if (
    record.type === "BigInt" &&
    (typeof record.value === "bigint" ||
      (typeof record.value === "number" && Number.isSafeInteger(record.value)))
  ) {
    return { type: "BigInt", value: BigInt(record.value) };
  }
  if (
    record.type === "Timestamp" &&
    typeof record.value === "number" &&
    Number.isSafeInteger(record.value)
  ) {
    return { type: "Timestamp", value: record.value };
  }
  if (
    record.type === "Double" &&
    typeof record.value === "number" &&
    Number.isFinite(record.value)
  ) {
    return { type: "Double", value: record.value };
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
  if (value == null) return 0;
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
  table?: string,
): Uint8Array {
  assertRequiredRowColumnsPresent(definition.columns, row, table);
  const columns = definition.columns.filter(
    (column) =>
      Object.hasOwn(row, column.name) ||
      (column.column_type.type === "Array" && column.default == null),
  );
  return encodeCells(columns, (column) => row[column.name], true);
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

function assertRequiredRowColumnsPresent(
  columns: ColumnDescriptor[],
  row: InsertValues,
  table?: string,
): void {
  for (const column of columns) {
    const value = row[column.name] ?? column.default;
    if (value && value.type !== "Null") continue;
    if (column.nullable || column.column_type.type === "Array") continue;
    throw new Error(
      table
        ? `encoding error: missing required field \`${column.name}\` on table \`${table}\``
        : `missing required column ${column.name}`,
    );
  }
}

function encodeValue(
  column: ColumnDescriptor,
  value: Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const resolved = value;
  if (!resolved || resolved.type === "Null") {
    if (column.nullable) return encodeNullValue(columnValueType(column));
    if (column.column_type.type === "Array") {
      return encodeNonNullValue(column.column_type, { type: "Array", value: [] });
    }
    if (requireMissingDefaults && column.default == null) {
      throw new Error(`missing required column ${column.name}`);
    }
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
    case "Timestamp":
      view.setBigUint64(0, BigInt(expectNumber(value, type.type)), true);
      return new Uint8Array(view.buffer);
    case "BigInt":
      if (value.type !== "BigInt") throw new Error("expected BigInt value");
      view.setBigInt64(0, BigInt(value.value), true);
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
    case 13:
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
    (value.type === "Integer" || value.type === "Double" || value.type === "Timestamp") &&
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

/**
 * Encoded query result crossing a message transport (see
 * {@link NativeRuntimeAdapter.queryForTransport}). The payload stays in the
 * engine's postcard encoding so it can ride a transfer list.
 */
export type EncodedQueryResult =
  | { kind: "encoded_row_batches"; payload: Uint8Array }
  | { kind: "encoded_relation_snapshot"; payload: Uint8Array };

export type QueryTransportOutcome = EncodedQueryResult | { kind: "rows"; rows: unknown };

/**
 * Decode a transported encoded query result. Pure function of its inputs, so
 * both sides of a worker boundary produce identical rows from the same
 * payload, schema, and original query JSON.
 */
export function decodeEncodedQueryResult(
  result: EncodedQueryResult,
  schema: WasmSchema,
  queryJson: string,
): RowState[] {
  if (result.kind === "encoded_row_batches") {
    return rowsFromBatches(readRowBatches(result.payload), schema);
  }
  const coreQueryJson = addNestedOuterColumns(queryJson);
  return rowsFromRelationSnapshot(
    readRelationSnapshot(result.payload),
    schema,
    relationMaterializationSpec(coreQueryJson, schema),
  );
}

function readRowBatches(payload: Uint8Array): NativeRowBatch[] {
  return new PostcardReader(payload).readVec(readNativeRowBatch);
}

function readRelationSnapshot(payload: Uint8Array): NativeRelationSubscriptionSnapshot {
  return readNativeRelationSubscriptionSnapshot(new PostcardReader(payload));
}

function rowsFromBatches(batches: NativeRowBatch[], schema: WasmSchema): RowState[] {
  const rows: RowState[] = [];
  for (const batch of batches) {
    const fieldPlans = nativeRowFieldPlans(batch, schema);
    const decodeRecord = createRecordValueDecoder(batch.descriptor);
    for (const row of batch.rows) {
      const values: Value[] = [];
      const valuesByColumn = new Map<string, Value>();

      for (const field of fieldPlans) {
        const value = decodePlannedField(field, decodeRecord, row.raw);
        valuesByColumn.set(field.name, value);
        if (field.includeInValues) {
          values.push(value);
        }
      }

      rows.push(
        withValuesByColumn(
          {
            table: batch.table,
            id: formatUuid(row.rowId),
            values,
          },
          valuesByColumn,
        ),
      );
    }
  }
  return rows;
}

function nativeRowFieldPlans(batch: NativeRowBatch, schema: WasmSchema): NativeRowFieldPlan[] {
  const columns = schema[batch.table]?.columns ?? [];
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  const plans: NativeRowFieldPlan[] = [];

  for (let index = 0; index < batch.descriptor.length; index += 1) {
    const fieldName = batch.descriptor[index]?.name;
    if (!fieldName || isInternalField(fieldName)) continue;

    const name = publicFieldName(fieldName);
    const type =
      name === "$createdBy" || name === "$updatedBy"
        ? ({ type: "Uuid" } as const)
        : (magicColumnType(name) ?? columnsByName.get(name)?.column_type);
    plans.push({
      name,
      index,
      type,
      includeInValues: !isHiddenIncludeColumn(name) && !isProvenanceMagicColumn(name),
    });
  }

  return plans;
}

function rowsFromRelationSnapshot(
  snapshot: NativeRelationSubscriptionSnapshot,
  schema: WasmSchema,
  materialization: RelationMaterializationSpec,
): RowState[] {
  const rows = stripRelationSnapshotMetadata(rowsFromBatches(snapshot.rows, schema), schema);
  return materializeRelationRows(rows, snapshot.edges, snapshot.rootCount, materialization);
}

const RELATION_SNAPSHOT_METADATA_FIELDS = new Set([
  "table",
  "layer",
  "schema_version",
  "parents",
  "created_by",
  "created_at",
  "updated_by",
  "updated_at",
]);

function stripRelationSnapshotMetadata(rows: RowState[], schema: WasmSchema): RowState[] {
  return rows.map((row) => {
    if (!row.valuesByColumn) return row;
    const schemaColumns = new Set((schema[row.table]?.columns ?? []).map((column) => column.name));
    const valuesByColumn = new Map(row.valuesByColumn);
    const metadataValues = new Set<Value>();
    for (const field of RELATION_SNAPSHOT_METADATA_FIELDS) {
      if (schemaColumns.has(field)) continue;
      const value = valuesByColumn.get(field);
      if (!value) continue;
      metadataValues.add(value);
      valuesByColumn.delete(field);
    }
    if (metadataValues.size === 0) return row;
    return withValuesByColumn(
      {
        ...row,
        values: row.values.filter((value) => !metadataValues.has(value)),
      },
      valuesByColumn,
    );
  });
}

function materializeRelationRows(
  rows: RowState[],
  edges: NativeRelationSubscriptionEdge[],
  rootCount: number,
  materialization: RelationMaterializationSpec,
): RowState[] {
  const rowsByKey = new Map(rows.map((row) => [rowKey(row.table, row.id), row]));
  const childRowsBySourceAndRelation = new Map<string, RowState[]>();
  for (const edge of edges) {
    const targetKey = rowKey(edge.targetTable, formatUuid(edge.targetRowId));
    const child = rowsByKey.get(targetKey);
    if (!child) continue;
    const key = relationKey(edge.sourceTable, formatUuid(edge.sourceRowId), edge.relation);
    const children = childRowsBySourceAndRelation.get(key) ?? [];
    children.push(child);
    childRowsBySourceAndRelation.set(key, children);
  }
  const materialized = new Map<string, RowState>();
  const rootCandidates =
    materialization.rootTable === null
      ? rows.slice(0, rootCount)
      : rows.filter((row) => row.table === materialization.rootTable).slice(0, rootCount);
  let roots = rootCandidates
    .map((row) =>
      materializeRelationRow(
        row,
        materialization.arraySubqueries,
        childRowsBySourceAndRelation,
        materialized,
      ),
    )
    .filter((result) => result.satisfiesRequirements)
    .map((result) => result.row);
  if (materialization.clientOffset > 0 || materialization.clientLimit !== null) {
    const offset = materialization.clientOffset;
    const limit = materialization.clientLimit ?? roots.length;
    roots = roots.slice(offset, offset + limit);
  }
  return roots;
}

function materializeRelationRow(
  row: RowState,
  subqueries: readonly QueryArraySubquery[],
  childRowsBySourceAndRelation: Map<string, RowState[]>,
  materialized: Map<string, RowState>,
): { row: RowState; satisfiesRequirements: boolean } {
  const rowKeyValue = rowKey(row.table, row.id);
  const cached = materialized.get(rowKeyValue);
  if (cached) return { row: cached, satisfiesRequirements: true };
  materialized.set(rowKeyValue, row);
  const valuesByColumn = new Map(row.valuesByColumn ?? []);
  const relationValues: Value[] = [];
  const relationPayloadValues = new Set<Value>();
  let satisfiesRequirements = true;
  for (const subquery of subqueries) {
    const key = relationKey(row.table, row.id, subquery.columnName);
    const children = childRowsBySourceAndRelation.get(key) ?? [];
    const childResults = children.map((child) =>
      materializeRelationRow(
        child,
        subquery.nestedArrays ?? [],
        childRowsBySourceAndRelation,
        materialized,
      ),
    );
    const materializedChildren = childResults
      .filter((child) => child.satisfiesRequirements)
      .map((child) => child.row);
    if (!arraySubqueryRequirementSatisfied(row, subquery, materializedChildren.length)) {
      satisfiesRequirements = false;
    }
    const value: Value = {
      type: "Array",
      value: materializedChildren.map((child) => ({
        type: "Row",
        value: rowValueWithValuesByColumn(child),
      })),
    } as Value;
    const publicRelation = publicIncludeRelationName(subquery.columnName);
    const hiddenRelation = hiddenIncludeColumnName(publicRelation);
    for (const name of [subquery.columnName, publicRelation, hiddenRelation]) {
      const payload = valuesByColumn.get(name);
      if (payload) relationPayloadValues.add(payload);
    }
    valuesByColumn.set(subquery.columnName, value);
    if (publicRelation !== subquery.columnName) {
      valuesByColumn.set(publicRelation, value);
    }
    valuesByColumn.set(hiddenRelation, value);
    relationValues.push(value);
  }
  const baseValues =
    relationPayloadValues.size === 0
      ? row.values
      : row.values.filter((value) => !relationPayloadValues.has(value));
  const next = withValuesByColumn(
    {
      ...row,
      values: baseValues.concat(relationValues),
    },
    valuesByColumn,
  );
  materialized.set(rowKeyValue, next);
  return { row: next, satisfiesRequirements };
}

function arraySubqueryRequirementSatisfied(
  row: RowState,
  subquery: QueryArraySubquery,
  childCount: number,
): boolean {
  switch (subquery.requirement ?? "Optional") {
    case "Optional":
      return true;
    case "AtLeastOne":
      return childCount > 0;
    case "MatchCorrelationCardinality":
      return childCount === correlationCardinality(row, subquery.outerColumn);
  }
}

function correlationCardinality(row: RowState, column: string): number {
  const valuesByColumn = row.valuesByColumn ?? new Map<string, Value>();
  const value = valuesByColumn.get(stripParentQualifier(column, row.table));
  if (!value) return 0;
  if (value.type === "Array") return value.value.length;
  if (value.type === "Null") return 0;
  return 1;
}

function publicIncludeRelationName(relation: string): string {
  return relation.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX)
    ? relation.slice(HIDDEN_INCLUDE_COLUMN_PREFIX.length)
    : relation;
}

function rowValueWithValuesByColumn(row: RowState): {
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
} {
  const value: {
    id: string;
    values: Value[];
    valuesByColumn?: Map<string, Value>;
    table?: string;
  } = {
    id: row.id,
    values: row.values,
  };
  Object.defineProperty(value, "table", {
    value: row.table,
    enumerable: false,
    configurable: true,
  });
  if (row.valuesByColumn) {
    Object.defineProperty(value, "valuesByColumn", {
      value: row.valuesByColumn,
      enumerable: false,
      configurable: true,
    });
  }
  return value;
}

function withValuesByColumn(row: RowState, valuesByColumn: Map<string, Value>): RowState {
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

function applySubscriptionDeltaWithWireDelta(
  currentRows: RowState[],
  currentIndexByKey: Map<string, number>,
  delta: { added: NativeRowBatch[]; updated: NativeRowBatch[]; removed: NativeRemovedRow[] },
  schema: WasmSchema,
  reset = false,
): { rows: RowState[]; rowIndexByKey: Map<string, number>; wireDelta: NativeRowDelta } {
  const rowsByKey = reset
    ? new Map<string, RowState>()
    : new Map(currentRows.map((row) => [rowKey(row.table, row.id), row]));
  const removedEntries: Array<{ id: string; index: number }> = [];

  for (const removed of delta.removed) {
    const id = formatUuid(removed.rowId);
    const key = rowKey(removed.table, id);
    removedEntries.push({ id, index: currentIndexByKey.get(key) ?? 0 });
    rowsByKey.delete(key);
  }

  const addedRows = rowsFromBatches(delta.added, schema);
  const updatedRows = rowsFromBatches(delta.updated, schema);
  for (const row of addedRows.concat(updatedRows)) {
    rowsByKey.set(rowKey(row.table, row.id), row);
  }

  const rows = Array.from(rowsByKey.values());
  const rowIndexByKey = indexRowsByKey(rows);
  return {
    rows,
    rowIndexByKey,
    wireDelta: {
      ...nativeDeltaFromChanges(addedRows, updatedRows, removedEntries, rowIndexByKey, schema),
      ...(reset ? { reset: true } : {}),
    },
  };
}

function applyRelationSubscriptionDelta(
  subscription: SubscriptionState,
  rootDelta: NativeSubscriptionDelta,
  relationDelta: NativeRelationSubscriptionDelta,
  schema: WasmSchema,
  reset: boolean,
): void {
  if (reset) {
    subscription.relationRows = [];
    subscription.relationEdges = [];
    subscription.relationRootCount = 0;
  }

  const rootRemoved = new Set(
    rootDelta.removed.map((row) => rowKey(row.table, formatUuid(row.rowId))),
  );
  if (rootRemoved.size > 0) {
    let index = 0;
    while (index < subscription.relationRootCount) {
      const row = subscription.relationRows[index];
      if (row && rootRemoved.has(rowKey(row.table, row.id))) {
        subscription.relationRows.splice(index, 1);
        subscription.relationRootCount -= 1;
      } else {
        index += 1;
      }
    }
  }

  const relatedRemoved = new Set(
    relationDelta.removed.map((row) => rowKey(row.table, formatUuid(row.rowId))),
  );
  if (relatedRemoved.size > 0) {
    subscription.relationRows = subscription.relationRows.filter((row, index) => {
      if (index < subscription.relationRootCount) return true;
      return !relatedRemoved.has(rowKey(row.table, row.id));
    });
  }

  const rootUpdates = rowsFromBatches(rootDelta.updated, schema);
  for (const row of rootUpdates) {
    const position = subscription.relationRows
      .slice(0, subscription.relationRootCount)
      .findIndex((current) => rowKey(current.table, current.id) === rowKey(row.table, row.id));
    if (position >= 0) {
      subscription.relationRows[position] = row;
    }
  }

  const rootAdds = rowsFromBatches(rootDelta.added, schema);
  for (const row of rootAdds) {
    const existing = subscription.relationRows
      .slice(0, subscription.relationRootCount)
      .findIndex((current) => rowKey(current.table, current.id) === rowKey(row.table, row.id));
    if (existing >= 0) {
      subscription.relationRows[existing] = row;
    } else {
      subscription.relationRows.splice(subscription.relationRootCount, 0, row);
      subscription.relationRootCount += 1;
    }
  }

  const relationRows = rowsFromBatches(relationDelta.added, schema).concat(
    rowsFromBatches(relationDelta.updated, schema),
  );
  for (const row of relationRows) {
    const key = rowKey(row.table, row.id);
    const rootPosition = subscription.relationRows
      .slice(0, subscription.relationRootCount)
      .findIndex((current) => rowKey(current.table, current.id) === key);
    if (rootPosition >= 0) {
      subscription.relationRows[rootPosition] = row;
      continue;
    }
    const relatedPosition = subscription.relationRows
      .slice(subscription.relationRootCount)
      .findIndex((current) => rowKey(current.table, current.id) === key);
    if (relatedPosition >= 0) {
      subscription.relationRows[subscription.relationRootCount + relatedPosition] = row;
    } else {
      subscription.relationRows.push(row);
    }
  }

  const removedEdges = new Set(relationDelta.removedEdges.map(relationEdgeKey));
  subscription.relationEdges = subscription.relationEdges.filter(
    (edge) => !removedEdges.has(relationEdgeKey(edge)),
  );
  for (const edge of relationDelta.addedEdges) {
    const key = relationEdgeKey(edge);
    if (!subscription.relationEdges.some((current) => relationEdgeKey(current) === key)) {
      subscription.relationEdges.push(edge);
    }
  }

  const referenced = new Set(
    subscription.relationEdges.map((edge) =>
      rowKey(edge.targetTable, formatUuid(edge.targetRowId)),
    ),
  );
  subscription.relationRows = subscription.relationRows.filter((row, index) => {
    if (index < subscription.relationRootCount) return true;
    return referenced.has(rowKey(row.table, row.id));
  });
}

function indexRowsByKey(rows: RowState[]): Map<string, number> {
  const index = new Map<string, number>();
  rows.forEach((row, rowIndex) => {
    index.set(rowKey(row.table, row.id), rowIndex);
  });
  return index;
}

function rowKey(table: string, id: string): string {
  return `${table}\0${id}`;
}

function relationKey(table: string, id: string, relation: string): string {
  return `${table}\0${id}\0${relation}`;
}

function relationEdgeKey(edge: NativeRelationSubscriptionEdge): string {
  return `${edge.sourceTable}\0${formatUuid(edge.sourceRowId)}\0${edge.relation}\0${edge.targetTable}\0${formatUuid(edge.targetRowId)}`;
}

function decodePlannedField(
  field: NativeRowFieldPlan,
  decodeRecord: (raw: Uint8Array, logicalIndex: number) => Uint8Array | null,
  raw: Uint8Array,
): Value {
  const bytes = decodeRecord(raw, field.index);
  if (bytes == null) return { type: "Null" };
  if (!field.type) return { type: "Bytea", value: bytes };
  return decodeBytes(field.type, bytes, field.name);
}

function decodeBytes(type: ColumnType, bytes: Uint8Array, fieldName?: string): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: decodeSignedI32FromCore(view.getUint32(0, true)) };
    case "BigInt":
      return { type: "BigInt", value: view.getBigInt64(0, true) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Timestamp":
      return {
        type: "Timestamp",
        value:
          Number(view.getBigUint64(0, true)) *
          (fieldName && isProvenanceMagicColumn(fieldName) ? 1_000 : 1),
      };
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
  | { type: "snapshot"; snapshot: NativeRelationSubscriptionSnapshot; settled?: boolean }
  | {
      type: "delta";
      reset?: boolean;
      delta: { added: NativeRowBatch[]; updated: NativeRowBatch[]; removed: NativeRemovedRow[] };
      relationDelta?: NativeRelationSubscriptionDelta;
      relationSnapshot?: NativeRelationSubscriptionSnapshot;
      settled?: boolean;
    }
  | { type: "closed" } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as {
    type?: unknown;
    rows?: unknown;
    delta?: unknown;
    relation_delta?: unknown;
    relation_snapshot?: unknown;
    reset?: unknown;
    settled?: unknown;
  };
  if (record.type === "closed" || record.type === "Closed") {
    return { type: "closed" };
  }
  if (record.type === "snapshot" || record.type === "Snapshot") {
    return {
      type: "snapshot",
      snapshot: readRelationSnapshot(assertBytes(record.rows, "subscription rows")),
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
    };
  }
  if (record.type === "delta" || record.type === "Delta") {
    return {
      type: "delta",
      reset: record.reset === true,
      delta: readNativeSubscriptionDelta(
        new PostcardReader(assertBytes(record.delta, "subscription delta")),
      ),
      relationDelta:
        record.relation_delta === undefined
          ? undefined
          : readNativeRelationSubscriptionDelta(
              new PostcardReader(assertBytes(record.relation_delta, "relation subscription delta")),
            ),
      relationSnapshot:
        record.relation_snapshot === undefined
          ? undefined
          : readRelationSnapshot(
              assertBytes(record.relation_snapshot, "relation subscription snapshot"),
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
  schema?: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): NativeRowDelta {
  const previousByKey = new Map(
    previousRows.map((row, index) => [rowKey(row.table, row.id), { row, index }]),
  );
  const nextKeys = new Set<string>();
  const added: RowState[] = [];
  const updated: RowState[] = [];
  const removed: Array<{ id: string; index: number }> = [];
  const rowIndexByKey = indexRowsByKey(rows);

  rows.forEach((row, index) => {
    const key = rowKey(row.table, row.id);
    nextKeys.add(key);
    const previous = previousByKey.get(key);
    if (!previous) {
      added.push(row);
      return;
    }
    if (previous.index !== index || !rowValuesEqual(previous.row.values, row.values)) {
      updated.push(row);
    }
  });

  previousRows.forEach((row, index) => {
    if (!nextKeys.has(rowKey(row.table, row.id))) {
      removed.push({ id: row.id, index });
    }
  });

  return nativeDeltaFromChanges(added, updated, removed, rowIndexByKey, schema, outputColumns);
}

function nativeResetDeltaFromRows(
  rows: RowState[],
  schema: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): NativeRowDelta {
  return {
    ...nativeDeltaFromChanges(rows, [], [], indexRowsByKey(rows), schema, outputColumns),
    reset: true,
  };
}

function nativeDeltaFromChanges(
  added: RowState[],
  updated: RowState[],
  removed: Array<{ id: string; index: number }>,
  rowIndexByKey: Map<string, number>,
  schema?: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null = null,
): NativeRowDelta {
  return {
    __jazzNativeRowDelta: true,
    added: encodeNativeRows(added, rowIndexByKey, schema, false, outputColumns),
    removed: encodeNativeRemoves(removed),
    updated: encodeNativeRows(updated, rowIndexByKey, schema, true, outputColumns),
    addedCount: added.length,
    removedCount: removed.length,
    updatedCount: updated.length,
  };
}

function nativeDeltaHasChanges(delta: NativeRowDelta): boolean {
  return delta.addedCount > 0 || delta.updatedCount > 0 || delta.removedCount > 0;
}

function encodeNativeRows(
  rows: RowState[],
  rowIndexByKey: Map<string, number>,
  schema: WasmSchema | undefined,
  updated = false,
  outputColumns: SubscriptionOutputColumns | null = null,
): Uint8Array {
  const chunks: Uint8Array[] = [];
  const encodersByColumns = new Map<
    readonly ColumnDescriptor[],
    (values: readonly Value[]) => Uint8Array
  >();
  for (const row of rows) {
    const columns =
      outputColumns && row.table === outputColumns.rootTable
        ? outputColumns.rootColumns
        : schema?.[row.table]?.columns;
    if (!columns) {
      throw new Error(`missing schema for subscription row table ${row.table}`);
    }
    let encodeRow = encodersByColumns.get(columns);
    if (!encodeRow) {
      encodeRow = createNativeRowValueEncoder(columns);
      encodersByColumns.set(columns, encodeRow);
    }
    const raw = encodeRow(valuesForNativeFrame(row, columns));
    chunks.push(
      requiredUuidBytes(row.id),
      u32Le(rowIndexByKey.get(rowKey(row.table, row.id)) ?? 0),
    );
    if (updated) chunks.push(Uint8Array.of(1));
    chunks.push(u32Le(raw.byteLength), raw);
  }
  return concatBytes(chunks);
}

function valuesForNativeFrame(row: RowState, columns: readonly ColumnDescriptor[]): Value[] {
  if (!row.valuesByColumn) {
    return row.values.slice(0, columns.length);
  }
  const values: Value[] = [];
  values.length = columns.length;
  for (let index = 0; index < columns.length; index += 1) {
    const column = columns[index]!;
    values[index] =
      row.valuesByColumn.get(column.name) ??
      (column.column_type.type === "Array" ? { type: "Array", value: [] } : { type: "Null" });
  }
  return values;
}

function encodeNativeRemoves(removed: Array<{ id: string; index: number }>): Uint8Array {
  return concatBytes(removed.flatMap((row) => [requiredUuidBytes(row.id), u32Le(row.index)]));
}

function requiredUuidBytes(id: string): Uint8Array {
  const hex = id.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid UUID ${id}`);
  return Uint8Array.from(hex.match(/../g)!.map((byte) => Number.parseInt(byte, 16)));
}

function rowValuesEqual(left: Value[], right: Value[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => valueEqual(value, right[index]));
}

function uuidValue(value: string): Value {
  return { type: "Uuid", value };
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

function queryValueEqual(left: Value, right: unknown): boolean {
  if (right && typeof right === "object" && "type" in right) {
    return valueEqual(left, right as Value);
  }
  switch (left.type) {
    case "Boolean":
    case "Text":
    case "Uuid":
    case "Integer":
    case "BigInt":
    case "Double":
    case "Timestamp":
    case "Row":
      return left.value === right;
    case "Null":
      return right === null || right === undefined;
    case "Bytea":
      return Array.isArray(right)
        ? bytesEqual(left.value, Uint8Array.from(right as number[]))
        : right instanceof Uint8Array && bytesEqual(left.value, right);
    case "Array":
      return Array.isArray(right) && left.value.length === right.length;
  }
}

function compareQueryValues(left: Value | undefined, right: Value | undefined): number {
  if (left === undefined && right === undefined) return 0;
  if (left === undefined) return -1;
  if (right === undefined) return 1;
  if (left.type !== right.type) return left.type.localeCompare(right.type);
  switch (left.type) {
    case "Boolean":
      return Number(left.value) - Number((right as typeof left).value);
    case "Text":
    case "Uuid":
    case "Timestamp":
    case "Row":
      return String(left.value).localeCompare(String((right as typeof left).value));
    case "Integer":
    case "Double":
      return Number(left.value) - Number((right as typeof left).value);
    case "BigInt": {
      const leftValue = BigInt(left.value);
      const rightValue = BigInt((right as typeof left).value);
      return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0;
    }
    case "Null":
      return 0;
    case "Bytea":
      return compareBytes(left.value, (right as typeof left).value);
    case "Array":
      return left.value.length - (right as typeof left).value.length;
  }
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  return left.every((byte, index) => byte === right[index]);
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const diff = left[index]! - right[index]!;
    if (diff !== 0) return diff;
  }
  return left.length - right.length;
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
