import type {
  ColumnDescriptor,
  ColumnType,
  InsertValues,
  NativeRowDelta,
  NativeTerminalOperation,
  NativeTerminalRootLayout,
  TablePolicies,
  Value,
  WasmSchema,
} from "../../drivers/types.js";
import { serializeRuntimeSchema } from "../../drivers/schema-wire.js";
import type {
  BatchId,
  InsertResult,
  MutationErrorEvent,
  MutationResult,
  OpenBatchId,
  PermissionAdvice,
  Runtime,
  TransactionKind,
} from "../client.js";
import type { Session } from "../context.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryWithPredicates,
  readNativeRowBatch,
  readNativeRelationSubscriptionSnapshot,
  readNativeSubscriptionDelta,
  type NativeRelationSubscriptionSnapshot,
  type NativeRowBatch,
  type NativeSubscriptionDelta,
  type QueryArraySubquery,
  type DescriptorField,
  type QueryLiteral,
  type QueryOrder,
  type QueryPredicate,
  type QueryPredicateOp,
  type ValueType,
} from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { nativeRowFieldPlanCacheKey, valueTypeCacheKey } from "./native-row-descriptor-key.js";
import { WebSocketCarrier, type WebSocketNegotiation, wireAuthFailureReason } from "./websocket.js";
import {
  createNativeRowValueEncoder,
  createRecord,
  createRecordValueDecoder,
  decodeNativeRowValues,
  encodeNativeColumnValue,
  encodeNativeNullValue,
  encodeU32Le,
  logicalStorageColumns,
  nativeFixedValueSize,
  storageColumnTypeToValueType,
  storageColumnValueType,
  writeDescriptor,
} from "./native-row-codec.js";
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "../select-projection.js";
import {
  isPermissionIntrospectionColumn,
  isProvenanceMagicColumn,
  magicColumnType,
} from "../../magic-columns.js";

export { encodeSchema } from "./schema-codec.js";

const SERVER_PUMP_DEBOUNCE_MS = 16;

type ReadAuthorizationHost = "client-local" | "trusted-serving";

type NativeDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): NativeDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): NativeDb;
};

type NativeDb = {
  registerSchema(schema: Uint8Array): NativeDb;
  beginTransaction(openBatchId: string, kind: TransactionKind, author?: Uint8Array): void;
  commitTransaction(openBatchId: string, kind?: TransactionKind): Write;
  rollbackTransaction(openBatchId: string): void;
  attachMergeableTx(openBatchId: string): Tx;
  attachExclusiveTx?(openBatchId: string): Tx;
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
  requestInsertPermissionAdviceEncoded?(
    table: string,
    cells: Uint8Array,
  ): NativePermissionAdviceRequest;
  requestReadPermissionAdvice?(table: string, rowId: Uint8Array): NativePermissionAdviceRequest;
  requestUpdatePermissionAdviceEncoded?(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
  ): NativePermissionAdviceRequest;
  requestDeletePermissionAdvice?(table: string, rowId: Uint8Array): NativePermissionAdviceRequest;
  mergeableTx(openBatchId: OpenBatchId): Tx;
  mergeableTxForIdentity?(openBatchId: OpenBatchId, author: Uint8Array): Tx;
  exclusiveTx?(openBatchId: OpenBatchId): Tx;
  allInTransaction?(query: PreparedQuery, tx: Tx, opts: unknown): Uint8Array;
  allInTransactionForIdentity?(
    query: PreparedQuery,
    tx: Tx,
    author: Uint8Array,
    opts: unknown,
  ): Uint8Array;
  setTickScheduler(
    callback:
      | ((urgency: "immediate" | "deferred") => void)
      | ((error: Error | null, urgency: string) => void),
  ): void;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  connectUpstream(): Transport;
  connectUpstreamWithSession?(
    protocolVersion: number,
    features: number,
    remoteNode: Uint8Array,
    remoteEpoch: bigint,
    localNode: Uint8Array,
    localEpoch: bigint,
  ): Transport;
  tick(): void;
  close?(): void;
  free?(): void;
};

type NativePermissionAdviceRequest = {
  readonly promise: Promise<PermissionAdvice>;
  cancel(): void;
};

type PreparedQuery = object;

type Subscription = {
  readAll(): unknown[];
  drain?(): unknown[];
  close?(): boolean;
};

type Write = {
  readonly batchId: string;
  payload: Uint8Array;
  wait(tier: string): Promise<void>;
  writeState(): unknown;
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
  id: OpenBatchId;
  kind: TransactionKind;
  txByView: Map<NativeRuntimeAdapter, Tx>;
  identity?: Uint8Array;
  writes: PendingTxWrite[];
};

type PendingTxWrite = {
  table: string;
  rowId: Uint8Array;
  row?: RowState;
  deleted?: boolean;
};

type CompletedTx = {
  kind: TransactionKind;
  state: "committed" | "rolled_back";
};

type TransactionOwnerState = {
  pendingTxs: Map<string, PendingTx>;
  completedTxs: Map<string, CompletedTx>;
  writes: Map<string, Write>;
};

type ServerTransportErrorWaiter = {
  active: boolean;
  reject: (error: Error) => void;
};

type ServerTransportWorkWaiter = {
  active: boolean;
  resolve: () => void;
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
  packedResetBatches: NativeRowBatch[] | null;
  packedResetRows: NativeRowDelta | null;
  visibleRows: RowState[];
  visiblePackedResetRows: NativeRowDelta | null;
  outputColumns: SubscriptionOutputColumns | null;
  session: RuntimeSession | null;
  opts: unknown;
  opened: boolean;
  visibleOpened: boolean;
  deferredVisiblePublication: boolean;
  deferredVisibleReset: boolean;
  deferredTerminalOperations: NativeTerminalOperation[];
  deferredTerminalLayouts: NativeTerminalRootLayout[];
  deferredPlaceholderChunks: number;
  deferredPlaceholderRows: number;
  deferredPlaceholderBytes: number;
  callback?: Function;
  cancelled: boolean;
};

type SubscriptionOutputColumns = {
  rootTable: string;
  rootColumns: readonly ColumnDescriptor[];
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
  resultKey?: string;
  resultKeyBytes?: Uint8Array;
};

type NativeRowFieldPlan = {
  name: string;
  index: number;
  type?: ColumnType;
  storageType: ValueType;
  includeInValues: boolean;
};

const textDecoder = new TextDecoder();
const byteHex = Array.from({ length: 256 }, (_, byte) => byte.toString(16).padStart(2, "0"));
const nativeRowFieldPlanCache = new WeakMap<WasmSchema, Map<string, NativeRowFieldPlan[]>>();
const MAX_DEFERRED_PLACEHOLDER_CHUNKS = 16;
const MAX_DEFERRED_PLACEHOLDER_ROWS = 4_096;
const MAX_DEFERRED_PLACEHOLDER_BYTES = 4 * 1024 * 1024;

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
  private readonly transactionOwner: TransactionOwnerState;
  private readonly pendingTxs: Map<string, PendingTx>;
  private readonly completedTxs: Map<string, CompletedTx>;
  private readonly writes: Map<string, Write>;
  private readonly ownerRuntime: NativeRuntimeAdapter;
  private readonly readAuthorizationHost: ReadAuthorizationHost;
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private authFailureCallback: ((reason: string) => void) | null = null;
  private serverTransport: Transport | null = null;
  private serverCarrier: WebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<WebSocketCarrier> | null = null;
  private serverTransportError: Error | null = null;
  private serverTransportErrorWaiters: ServerTransportErrorWaiter[] = [];
  private serverTransportWorkEpoch = 0;
  private serverTransportWorkWaiters: ServerTransportWorkWaiter[] = [];
  private nextServerConnectionEpoch = 1n;
  private serverEndpointUrl: string | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private readonly pendingInboundServerFrames: Uint8Array[] = [];
  private coreTickScheduled = false;
  private coreTickRunning = false;
  private coreTickAgain = false;
  private serverPumpScheduled = false;
  private serverPumpAgain = false;
  private closed = false;
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

  registerSchemaView(schema: WasmSchema): NativeRuntimeAdapter {
    return new NativeRuntimeAdapter(null, schema, this.node, this.peerIdentity, 1, true, {
      db: this.db.registerSchema(encodeSchema(schema)),
      owner: this,
    });
  }

  constructor(
    Runtime: NativeDbConstructor | null,
    private readonly schema: WasmSchema,
    private readonly node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
    opts?: {
      persistentPath?: string;
      db?: NativeDb;
      initialSyncFlushEvery?: number;
      readAuthorizationHost?: ReadAuthorizationHost;
      owner?: NativeRuntimeAdapter;
    },
  ) {
    this.ownerRuntime = opts?.owner?.ownerRuntime ?? this;
    this.readAuthorizationHost =
      opts?.owner?.readAuthorizationHost ?? opts?.readAuthorizationHost ?? "client-local";
    this.transactionOwner = opts?.owner?.transactionOwner ?? {
      pendingTxs: new Map(),
      completedTxs: new Map(),
      writes: new Map(),
    };
    this.pendingTxs = this.transactionOwner.pendingTxs;
    this.completedTxs = this.transactionOwner.completedTxs;
    this.writes = this.transactionOwner.writes;
    this.schemaBytes = encodeSchema(schema);
    this.configBytes = openConfig(
      node,
      author,
      sourceId,
      historyComplete,
      opts?.initialSyncFlushEvery,
    );
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
    if (opts?.owner) return;
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
    if (this.closed) return;
    this.closed = true;
    for (const subscription of this.subscriptions.values()) {
      for (const source of subscription.sources) {
        closeSubscriptionSource(source.source);
      }
    }
    if (this !== this.ownerRuntime) {
      this.subscriptions.clear();
      this.db.free?.();
      return;
    }
    for (const write of this.writes.values()) {
      write.close?.();
    }
    this.subscriptions.clear();
    this.pendingTxs.clear();
    this.completedTxs.clear();
    this.writes.clear();
    this.clearServerTransportErrorWaiters();
    this.resolveServerTransportWorkWaiters();
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
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const updatedAtMs = effectiveUpdatedAtMs(_writeContext);
    const tx = this.currentTx(_writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).insertWithIdEncoded(table, rowId, cells, updatedAtMs);
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, row });
      return {
        id: row.id,
        values: row.values,
        kind: "staged",
        openBatchId: txIdFromContext(_writeContext)!,
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
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Restore");
    if (tx) {
      this.txForWrite(tx, writeIdentity).restoreEncoded(table, rowId, cells, updatedAtMs);
      const row = this.rowStateFromValues(table, rowId, values);
      tx.writes.push({ table, rowId, row });
      return {
        id: row.id,
        values: row.values,
        kind: "staged",
        openBatchId: txIdFromContext(writeContext)!,
      };
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
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Update");
    if (tx) {
      this.txForWrite(tx, writeIdentity).updateEncoded(table, rowId, patch, updatedAtMs);
      tx.writes.push({
        table,
        rowId,
        row: this.mergeRowState(table, rowId, values, tx, writeIdentity),
      });
      return { kind: "staged", openBatchId: txIdFromContext(writeContext)! };
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
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Upsert");
    const existing = tx
      ? (this.stagedRowForWriteMerge(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId))
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
      this.txForWrite(tx, writeIdentity).upsertEncoded(table, rowId, cells, updatedAtMs);
      tx.writes.push({
        table,
        rowId,
        row: existing
          ? this.mergeRowState(table, rowId, values, tx, writeIdentity)
          : this.rowStateFromValues(table, rowId, values),
      });
      return { kind: "staged", openBatchId: txIdFromContext(writeContext)! };
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
    const writeIdentity = this.trustedWriteIdentity(writeSession);
    const updatedAtMs = effectiveUpdatedAtMs(writeContext);
    const tx = this.currentTx(writeContext, "Delete");
    if (tx) {
      this.txForWrite(tx, writeIdentity).delete(table, rowId, updatedAtMs);
      tx.writes.push({ table, rowId, deleted: true });
      return { kind: "staged", openBatchId: txIdFromContext(writeContext)! };
    }
    const write = writeOrNormalizeRejection("Delete", () =>
      writeIdentity
        ? this.db.deleteForIdentity(table, rowId, writeIdentity, updatedAtMs)
        : this.db.delete(table, rowId, updatedAtMs),
    );
    return this.finishMutation(write);
  }

  canInsertLocally(table: string, values: InsertValues, session?: Session): PermissionAdvice {
    void table;
    void values;
    void session;
    return "unknown";
  }

  canReadLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    void table;
    void objectId;
    void session;
    return "unknown";
  }

  canUpdateLocally(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice {
    void table;
    void objectId;
    void values;
    void session;
    return "unknown";
  }

  canDeleteLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    void table;
    void objectId;
    void session;
    return "unknown";
  }

  requestInsertPermissionAdvice(
    table: string,
    values: InsertValues,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestInsertPermissionAdviceEncoded;
    if (!request) return Promise.resolve("unknown");
    const cells = encodeCellsForRow(this.table(table), values, table);
    return this.withPermissionAdviceTimeout(() => request.call(this.db, table, cells));
  }

  requestReadPermissionAdvice(
    table: string,
    objectId: string,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestReadPermissionAdvice;
    if (!request) return Promise.resolve("unknown");
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId)),
    );
  }

  requestUpdatePermissionAdvice(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestUpdatePermissionAdviceEncoded;
    if (!request) return Promise.resolve("unknown");
    const patch = encodeCellsForPatch(this.table(table), values);
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId), patch),
    );
  }

  requestDeletePermissionAdvice(
    table: string,
    objectId: string,
    _session?: Session,
  ): Promise<PermissionAdvice> {
    const request = this.db.requestDeletePermissionAdvice;
    if (!request) return Promise.resolve("unknown");
    return this.withPermissionAdviceTimeout(() =>
      request.call(this.db, table, parseUuid(objectId)),
    );
  }

  beginTransaction(
    kind: TransactionKind,
    id: OpenBatchId,
    sessionJson?: string | null,
  ): OpenBatchId {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.beginTransaction(kind, id, sessionJson);
    }
    if (this.pendingTxs.has(id) || this.completedTxs.has(id)) {
      throw new Error(`Begin transaction failed: batch ${id} has already been opened`);
    }
    const session = sessionFromWriteContext(sessionJson);
    this.applySessionClaims(session);
    const identity = kind === "mergeable" ? this.trustedWriteIdentity(session) : undefined;
    this.db.beginTransaction(id, kind, identity);
    this.pendingTxs.set(id, { id, kind, identity, writes: [], txByView: new Map() });
    return id;
  }

  commitTransaction(openBatchId: OpenBatchId): Promise<BatchId> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.commitTransaction(openBatchId);
    const pending = this.pendingTxs.get(openBatchId);
    if (!pending) {
      throw new Error(commitTransactionMessage(openBatchId, this.completedTxs));
    }
    if (pending.writes.length === 0 && pending.kind === "mergeable") {
      throw new Error(
        "Commit transaction failed: empty mergeable batch has no committed unit; roll it back instead",
      );
    }
    const write = this.db.commitTransaction(openBatchId, pending.kind);
    this.pendingTxs.delete(openBatchId);
    this.completedTxs.set(openBatchId, { kind: pending.kind, state: "committed" });
    this.pumpSubscriptions();
    this.scheduleServerPump();
    return Promise.resolve(recordWrite(write, this.writes));
  }

  async waitForTransaction(batchId: BatchId | Promise<BatchId>, tier: string): Promise<void> {
    if (this !== this.ownerRuntime) {
      return this.ownerRuntime.waitForTransaction(batchId, tier);
    }
    batchId = await batchId;
    const write = this.writes.get(batchId);
    if (!write) {
      throw new Error(`Wait for batch failed: unknown batch ${batchId}`);
    }
    this.throwServerTransportErrorForTier(tier);
    this.pumpServerTransport();
    this.throwServerTransportErrorForTier(tier);
    const settlement = write.wait(tier);
    const transportError = this.waitForServerTransportError(tier);
    try {
      await (transportError ? Promise.race([settlement, transportError.promise]) : settlement);
      this.pumpSubscriptions();
    } catch (error) {
      const rejected = rejectedWaitError(batchId, error);
      if (rejected) {
        throw rejected;
      }
      throw error;
    } finally {
      transportError?.cancel();
    }
  }

  rollbackTransaction(openBatchId: OpenBatchId): Promise<boolean> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.rollbackTransaction(openBatchId);
    const pending = this.pendingTxs.get(openBatchId);
    if (!pending) {
      throw new Error(rollbackTransactionMessage(openBatchId, this.completedTxs));
    }
    this.db.rollbackTransaction(openBatchId);
    this.pendingTxs.delete(openBatchId);
    this.completedTxs.set(openBatchId, { kind: pending.kind, state: "rolled_back" });
    return Promise.resolve(true);
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    assertSupportedReadOptions(tier, optionsJson);
    assertTransactionReadOpen(optionsJson, this.pendingTxs, this.completedTxs);
    const session = readSession(sessionJson);
    this.applySessionClaims(session);
    assertNoUnsupportedPermissionIntrospection(queryJson);
    const coreQueryJson = addNestedOuterColumns(queryJson);
    const opts = readOptions(tier, queryIncludesDeleted(coreQueryJson), optionsJson);
    if (queryUsesNativeRelationApi(coreQueryJson)) {
      if (this.readAuthorizationHost === "trusted-serving") {
        if (!this.db.allRelationQueryForIdentity) {
          throw new Error("Native runtime does not support trusted-serving relation queries");
        }
        const payload = this.db.allRelationQueryForIdentity(
          coreQueryJson,
          session?.identity ?? this.peerIdentity,
          opts,
        );
        return rowsFromBatches(readRowBatches(payload), this.schema);
      }
      if (!this.db.allRelationQuery) {
        throw new Error("Native runtime does not support relation queries");
      }
      const payload = this.db.allRelationQuery(coreQueryJson, opts);
      return rowsFromBatches(readRowBatches(payload), this.schema);
    }
    const query = this.prepareQuery(coreQueryJson);
    const attachment = await this.attachQueryIfNeeded(tier, optionsJson, query, session);
    this.attachLocalReadCoverageInBackground(tier, optionsJson, query, session);
    try {
      if (queryHasArraySubqueries(coreQueryJson)) {
        if (this.readAuthorizationHost === "trusted-serving") {
          if (!this.db.allRelationSnapshotForIdentity) {
            throw new Error("Native runtime does not support trusted-serving relation snapshots");
          }
          const payload = this.db.allRelationSnapshotForIdentity(
            query,
            session?.identity ?? this.peerIdentity,
            opts,
          );
          return rowsFromRelationSnapshot(
            readRelationSnapshot(payload),
            this.schema,
            subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns,
          );
        }
        if (!this.db.allRelationSnapshot) {
          throw new Error("Native runtime does not support relation snapshots");
        }
        const payload = this.db.allRelationSnapshot(query, opts);
        return rowsFromRelationSnapshot(
          readRelationSnapshot(payload),
          this.schema,
          subscriptionOutputColumns(coreQueryJson, this.schema).rootColumns,
        );
      }
      const pendingTx = pendingTxFromOptions(optionsJson, this.pendingTxs);
      let rows = this.readPlainRows(query, opts, session ?? undefined, pendingTx);
      let rowStates = rowsFromBatches(readRowBatches(rows), this.schema);
      if (!pendingTx && (tier === "edge" || tier === "global") && rowStates.length > 0) {
        await this.refreshRowsFromEdge(session, rowStates);
        rows = this.readPlainRows(query, opts, session ?? undefined, pendingTx);
        rowStates = rowsFromBatches(readRowBatches(rows), this.schema);
      }
      return rowStates;
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
      if (
        this.readAuthorizationHost === "trusted-serving" &&
        !this.db.subscribeRelationQueryForIdentity
      ) {
        throw new Error("Native runtime does not support trusted-serving relation subscriptions");
      }
    } else if (!this.db.subscribe) {
      throw new Error("Native runtime does not support subscriptions");
    }
    if (
      this.readAuthorizationHost === "trusted-serving" &&
      !usesNativeRelationApi &&
      !this.db.subscribeForIdentity
    ) {
      throw new Error("Native runtime does not support trusted-serving subscriptions");
    }
    const handle = this.nextSubscriptionId++;
    const opts = readOptions(tier, false, optionsJson);
    const identity = session?.identity;
    let nativeSubscription: ReadableStream<unknown> | Subscription;
    let preparedQuery: PreparedQuery | null = null;
    try {
      if (usesNativeRelationApi) {
        nativeSubscription =
          this.readAuthorizationHost === "trusted-serving"
            ? this.db.subscribeRelationQueryForIdentity!(
                queryJson,
                identity ?? this.peerIdentity,
                opts,
              )
            : this.db.subscribeRelationQuery!(queryJson, opts);
      } else {
        const query = this.prepareQuery(queryJson);
        preparedQuery = query;
        nativeSubscription =
          this.readAuthorizationHost === "trusted-serving"
            ? this.db.subscribeForIdentity!(query, identity ?? this.peerIdentity, opts)
            : this.db.subscribe!(query, opts);
      }
    } catch (error) {
      const nativeStack = error instanceof Error ? error.stack : undefined;
      throw new Error(
        `Core subscribe failed for ${queryJson}: ${errorMessage(error)}${nativeStack ? `\n${nativeStack}` : ""}`,
      );
    }
    this.subscriptions.set(handle, {
      sources: [{ source: subscriptionSource(nativeSubscription), reading: false }],
      queryJson,
      query: preparedQuery,
      identity,
      rows: [],
      rowIndexByKey: new Map(),
      packedResetBatches: null,
      packedResetRows: null,
      visibleRows: [],
      visiblePackedResetRows: null,
      outputColumns: usesNativeRelationApi
        ? null
        : subscriptionOutputColumns(queryJson, this.schema),
      session,
      opts,
      opened: false,
      visibleOpened: false,
      deferredVisiblePublication: false,
      deferredVisibleReset: false,
      deferredTerminalOperations: [],
      deferredTerminalLayouts: [],
      deferredPlaceholderChunks: 0,
      deferredPlaceholderRows: 0,
      deferredPlaceholderBytes: 0,
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
        subscription.visiblePackedResetRows ??
          nativeResetDeltaFromRows(
            subscription.visibleRows,
            this.schema,
            subscription.outputColumns,
          ),
      );
    }
    this.startSubscriptionReader(handle, subscription);
  }

  unsubscribe(handle: number): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.cancelled = true;
    clearDeferredPlaceholderBuffer(subscription);
    closeSubscriptionSourceState(subscription);
    this.subscriptions.delete(handle);
  }

  connect(url: string, authJson: string): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.connect(url, authJson);
    // A new transport replaces the old one during a temporary reconnect. Server-tier
    // waits are still meaningful across that transition, so only an explicit runtime
    // shutdown is allowed to reject them.
    void this.disconnect({ rejectWaiters: false });
    this.serverTransportError = null;
    this.serverEndpointUrl = url;
    const carrier = new WebSocketCarrier({
      endpointUrl: url,
      peerIdentity: this.peerIdentity,
      authJson,
      onFrame: (frame) => {
        this.pendingInboundServerFrames.push(frame);
        this.notifyServerTransportWork();
        this.scheduleServerPump();
      },
      onError: (error) => {
        this.handleServerTransportError(error);
        const reason = wireAuthFailureReason(error);
        if (reason) this.authFailureCallback?.(reason);
      },
    });
    this.serverCarrier = carrier;
    this.serverCarrierPromise = carrier.ready().then((negotiation) => {
      const transport = this.connectNegotiatedUpstream(negotiation);
      this.serverTransport = transport;
      this.flushQueuedServerFrames(carrier);
      this.pumpServerTransport();
      this.pumpSubscriptions();
      return carrier;
    });
    this.serverCarrierPromise.catch((error) => {
      this.handleServerTransportError(error);
    });
  }

  private connectNegotiatedUpstream(negotiation: WebSocketNegotiation): Transport {
    const authority = negotiation.authority;
    const connectWithSession = this.db.connectUpstreamWithSession;
    if (!authority || !connectWithSession) return this.db.connectUpstream();
    const localEpoch = this.nextServerConnectionEpoch++;
    return connectWithSession.call(
      this.db,
      negotiation.protocolVersion,
      negotiation.features,
      authority.node,
      authority.epoch,
      this.peerIdentity,
      localEpoch,
    );
  }

  disconnect(options: { rejectWaiters?: boolean } = {}): Promise<void> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.disconnect(options);
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverTransportError = null;
    if (options.rejectWaiters ?? true) {
      this.resolveServerTransportErrorWaiters(new Error("server transport disconnected"));
    } else {
      this.clearServerTransportErrorWaiters();
    }
    this.resolveServerTransportWorkWaiters();
    this.serverTransport?.close();
    this.serverTransport = null;
    this.serverEndpointUrl = null;
    this.queuedServerFrames.length = 0;
    this.pendingInboundServerFrames.length = 0;
    this.serverPumpScheduled = false;
    this.serverPumpAgain = false;
    return Promise.resolve();
  }

  updateAuth(authJson: string): Promise<void> | void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.updateAuth(authJson);
    if (!this.serverEndpointUrl) return;
    return this.connect(this.serverEndpointUrl, authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onAuthFailure(callback);
    this.authFailureCallback = callback;
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    if (this !== this.ownerRuntime) return this.ownerRuntime.onMutationError(callback);
    this.db.onMutationError(callback);
  }

  private finishInsert(
    table: string,
    rowId: Uint8Array,
    values: InsertValues,
    write: Write,
  ): InsertResult {
    const batchId = recordWrite(write, this.writes);
    this.pumpSubscriptions();
    this.scheduleServerPump();
    const row = this.rowStateFromValues(table, rowId, values);
    return { id: row.id, values: row.values, kind: "committed", batchId };
  }

  private finishMutation(write: Write): MutationResult {
    const batchId = recordWrite(write, this.writes);
    this.pumpSubscriptions();
    this.scheduleServerPump();
    return { kind: "committed", batchId };
  }

  private resultForRow(
    table: string,
    rowId: Uint8Array,
    receipt: { kind: "committed"; batchId: BatchId } | { kind: "staged"; openBatchId: OpenBatchId },
    identity?: Uint8Array,
  ): InsertResult {
    const row = this.readRow(table, rowId, identity);
    return { id: formatUuid(rowId), values: row?.values ?? [], ...receipt };
  }

  private readRow(table: string, rowId: Uint8Array, identity?: Uint8Array): RowState | undefined {
    if (!identity) return this.readRowForWriteMerge(table, rowId);
    const query = this.prepareQuery(JSON.stringify({ table }));
    const rows = this.readRowsForHost(query, readOptions(), identity);
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
      (column) => !isHiddenIncludeColumn(column.name),
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
    const current =
      this.stagedRowForWriteMerge(tx, table, rowId) ?? this.readRowForWriteMerge(table, rowId);
    const merged: Record<string, Value> = {};
    for (const column of this.table(table).columns) {
      const existing = current?.valuesByColumn?.get(column.name);
      if (existing !== undefined) merged[column.name] = existing;
    }
    Object.assign(merged, patch);
    return this.rowStateFromValues(table, rowId, merged);
  }

  private readPlainRows(
    query: PreparedQuery,
    opts: unknown,
    session: RuntimeSession | undefined,
    pendingTx: PendingTx | undefined,
  ): Uint8Array {
    if (!pendingTx) return this.readRowsForHost(query, opts, session?.identity);
    if (this.readAuthorizationHost === "trusted-serving") {
      if (!this.db.allInTransactionForIdentity) {
        throw new Error("Native runtime does not support trusted-serving transaction reads");
      }
      return this.db.allInTransactionForIdentity(
        query,
        this.txForRead(pendingTx),
        session?.identity ?? this.peerIdentity,
        opts,
      );
    }
    if (!this.db.allInTransaction) {
      throw new Error("Native runtime does not support transaction reads");
    }
    return this.db.allInTransaction(query, this.txForRead(pendingTx), opts);
  }

  /**
   * Client runtimes consume server-scoped settled data locally. Only an
   * explicitly configured serving host may select the policy-enforcing entry
   * point, with a request session supplying its subject when present.
   */
  private readRowsForHost(query: PreparedQuery, opts: unknown, identity?: Uint8Array): Uint8Array {
    return this.readAuthorizationHost === "trusted-serving"
      ? this.db.allForIdentity(query, identity ?? this.peerIdentity, opts)
      : this.db.all(query, opts);
  }

  /**
   * A session supplies the subject for a trusted-serving host; it never turns
   * an ordinary client mutation into a policy-enforcing local admission.
   */
  private trustedWriteIdentity(session: RuntimeSession | null | undefined): Uint8Array | undefined {
    return this.readAuthorizationHost === "trusted-serving"
      ? (session?.identity ?? this.peerIdentity)
      : undefined;
  }

  private stagedRowForWriteMerge(
    tx: PendingTx,
    table: string,
    rowId: Uint8Array,
  ): RowState | undefined {
    const id = formatUuid(rowId);
    for (let index = tx.writes.length - 1; index >= 0; index -= 1) {
      const write = tx.writes[index]!;
      if (write.table !== table || formatUuid(write.rowId) !== id) continue;
      return write.deleted ? undefined : write.row;
    }
    return undefined;
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
        this.readRowsForHost(query, opts, session?.identity);
      } finally {
        if (attachment !== undefined) this.db.detachQuery?.(attachment);
      }
    }
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
    if (this.readAuthorizationHost === "trusted-serving") {
      if (!this.db.attachQueryForIdentity) {
        throw new Error("Native runtime does not support trusted-serving query coverage");
      }
      attachment = this.db.attachQueryForIdentity(
        query,
        session?.identity ?? this.peerIdentity,
        opts,
      );
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
        this.readRowsForHost(query, opts, identity);
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
      return this.txForRead(pending);
    }
    if (pending.identity && (!identity || !sameBytes(pending.identity, identity))) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    if (identity && !pending.identity) {
      throw new Error("Native runtime mergeable transaction cannot mix write identities");
    }
    return this.txForRead(pending);
  }

  private txForRead(pending: PendingTx): Tx {
    const existing = pending.txByView.get(this);
    if (existing) return existing;
    const tx =
      pending.kind === "mergeable"
        ? this.db.attachMergeableTx(pending.id)
        : this.db.attachExclusiveTx?.(pending.id);
    if (!tx) throw new Error("Native runtime does not support attached exclusive batches");
    pending.txByView.set(this, tx);
    return tx;
  }

  private exclusiveTx(id: OpenBatchId): Tx {
    if (!this.db.exclusiveTx) {
      throw new Error(
        "Native runtime cannot perform exclusive transaction writes: " +
          "the native runtime exclusive transaction API is unavailable.",
      );
    }
    return this.db.exclusiveTx(id);
  }

  private mergeableTxForIdentity(id: OpenBatchId, identity: Uint8Array): Tx {
    if (!this.db.mergeableTxForIdentity) {
      throw new Error(
        "Native runtime cannot perform session-scoped transaction writes: " +
          "the native runtime mergeable transaction API has no identity-aware staging methods.",
      );
    }
    return this.db.mergeableTxForIdentity(id, identity);
  }

  private pumpSubscriptions(): void {
    for (const [handle, subscription] of this.subscriptions) {
      this.startSubscriptionReader(handle, subscription);
    }
  }

  private withPermissionAdviceTimeout(
    start: () => NativePermissionAdviceRequest,
  ): Promise<PermissionAdvice> {
    if (this !== this.ownerRuntime) return this.ownerRuntime.withPermissionAdviceTimeout(start);
    if (this.closed || !this.serverTransport || !this.serverCarrier) {
      return Promise.resolve("unknown");
    }
    const request = start();
    return new Promise((resolve) => {
      let settled = false;
      const finish = (advice: PermissionAdvice) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        resolve(advice);
      };
      const timeout = setTimeout(() => {
        request.cancel();
        finish("unknown");
      }, 2_000);
      request.promise.then(finish, () => finish("unknown"));
    });
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
          this.failSubscription(
            subscription,
            error instanceof Error ? error : new Error(String(error)),
          );
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
        this.failSubscription(
          subscription,
          error instanceof Error ? error : new Error(String(error)),
        );
      });
    }
  }

  private async applySubscriptionChunk(
    subscription: SubscriptionState,
    value: unknown,
  ): Promise<void> {
    const chunk = normalizeSubscriptionChunk(value);
    if (chunk.type === "closed") {
      clearDeferredPlaceholderBuffer(subscription);
      closeSubscriptionSourceState(subscription);
      subscription.cancelled = true;
      return;
    }
    if (chunk.type === "rejected") {
      if (chunk.reason.type === "ShapeRegistrationPendingCatalogueAdmission") {
        return;
      }
      this.failSubscription(subscription, subscriptionRejectionError(chunk.reason));
      return;
    }
    if (chunk.type === "snapshot") {
      const previousRows = subscription.rows;
      const wasOpened = subscription.opened;
      subscription.rows = rowsFromRelationSnapshot(
        chunk.snapshot,
        this.schema,
        subscription.outputColumns?.rootColumns,
        "full-record",
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
    } else {
      if (chunk.reset) {
        subscription.rows = [];
        subscription.rowIndexByKey = new Map();
        subscription.packedResetBatches = null;
        subscription.packedResetRows = null;
        clearDeferredPlaceholderBuffer(subscription);
      }
      if (plainResetChunkCanStayPacked(subscription, chunk, this.schema)) {
        const packedResetRows = nativeResetDeltaFromBatches(
          chunk.delta.added,
          subscription.outputColumns,
        );
        subscription.rows = [];
        subscription.rowIndexByKey = new Map();
        subscription.packedResetBatches = chunk.delta.added;
        subscription.packedResetRows = packedResetRows;
        subscription.opened = true;
        packedResetRows.terminalOperations = chunk.terminalOperations;
        packedResetRows.terminalLayouts = chunk.terminalLayouts;
        this.publishSubscriptionRows(subscription, packedResetRows, chunk.settled, true);
      } else {
        materializePackedResetRows(subscription, this.schema);
        let applied;
        try {
          applied = applySubscriptionDeltaWithWireDelta(
            subscription.rows,
            subscription.rowIndexByKey,
            chunk.delta,
            this.schema,
            chunk.reset === true,
            subscription.outputColumns,
          );
        } catch (error) {
          const buffered = applySubscriptionDeltaToState(
            subscription.rows,
            subscription.rowIndexByKey,
            chunk.delta,
            this.schema,
            chunk.reset === true,
            subscription.outputColumns,
          );
          if (
            subscriptionRowsRequireBufferedPublication(
              buffered.rows,
              this.schema,
              subscription.outputColumns,
            )
          ) {
            if (chunk.settled === true) {
              throw new Error(
                "settled relation subscription chunk retained unresolved placeholder rows",
              );
            }
            subscription.rows = buffered.rows;
            subscription.rowIndexByKey = buffered.rowIndexByKey;
            subscription.opened = true;
            this.deferSubscriptionRows(
              subscription,
              chunk.terminalOperations,
              chunk.terminalLayouts,
              chunk.reset === true,
              chunk.delta,
            );
            return;
          }
          throw error;
        }
        subscription.rows = applied.rows;
        subscription.rowIndexByKey = applied.rowIndexByKey;
        subscription.opened = true;
        if (
          subscriptionRowsRequireBufferedPublication(
            subscription.rows,
            this.schema,
            subscription.outputColumns,
          )
        ) {
          if (chunk.settled === true) {
            throw new Error(
              "settled relation subscription chunk retained unresolved placeholder rows",
            );
          }
          this.deferSubscriptionRows(
            subscription,
            chunk.terminalOperations,
            chunk.terminalLayouts,
            chunk.reset === true,
            chunk.delta,
          );
          return;
        }
        applied.wireDelta.terminalOperations = chunk.terminalOperations;
        applied.wireDelta.terminalLayouts = chunk.terminalLayouts;
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
      subscription.deferredTerminalOperations.push(...(wireDelta.terminalOperations ?? []));
      subscription.deferredTerminalLayouts.push(...(wireDelta.terminalLayouts ?? []));
      return;
    }

    let visibleDelta = wireDelta;
    if (
      subscription.deferredVisiblePublication ||
      subscription.deferredVisibleReset ||
      !subscription.visibleOpened
    ) {
      const publishReset = subscription.deferredVisibleReset || !subscription.visibleOpened;
      if (publishReset && subscription.packedResetRows) {
        visibleDelta = subscription.packedResetRows;
      } else if (publishReset) {
        visibleDelta = nativeResetDeltaFromRows(
          subscription.rows,
          this.schema,
          subscription.outputColumns,
        );
      } else {
        visibleDelta = nativeDeltaFromRows(
          subscription.rows,
          subscription.visibleRows,
          this.schema,
          subscription.outputColumns,
        );
      }
    }

    const terminalOperations = [
      ...subscription.deferredTerminalOperations,
      ...(wireDelta.terminalOperations ?? []),
    ];
    const terminalLayouts = [
      ...subscription.deferredTerminalLayouts,
      ...(wireDelta.terminalLayouts ?? []),
    ];
    if (terminalOperations.length > 0) {
      visibleDelta.terminalOperations = terminalOperations;
    }
    if (terminalLayouts.length > 0) visibleDelta.terminalLayouts = terminalLayouts;

    subscription.callback?.(visibleDelta);
    if (visibleDelta === subscription.packedResetRows) {
      subscription.visibleRows = [];
      subscription.visiblePackedResetRows = subscription.packedResetRows;
    } else {
      subscription.visibleRows = [...subscription.rows];
      subscription.visiblePackedResetRows = null;
    }
    subscription.visibleOpened = true;
    clearDeferredPlaceholderBuffer(subscription);
  }

  private subscriptionCallbacksAreSettledGated(subscription: SubscriptionState): boolean {
    return (subscription.opts as { tier?: unknown }).tier === "global";
  }

  private deferSubscriptionRows(
    subscription: SubscriptionState,
    terminalOperations: NativeTerminalOperation[] | undefined,
    terminalLayouts: NativeTerminalRootLayout[] | undefined,
    reset: boolean,
    delta: NativeSubscriptionDelta,
  ): void {
    subscription.deferredVisiblePublication = true;
    subscription.deferredVisibleReset ||= reset;
    subscription.deferredTerminalOperations.push(...(terminalOperations ?? []));
    subscription.deferredTerminalLayouts.push(...(terminalLayouts ?? []));
    subscription.deferredPlaceholderChunks = reset ? 1 : subscription.deferredPlaceholderChunks + 1;
    subscription.deferredPlaceholderRows = subscription.rows.length;
    subscription.deferredPlaceholderBytes = reset
      ? subscriptionDeltaPayloadBytes(delta, terminalOperations)
      : subscription.deferredPlaceholderBytes +
        subscriptionDeltaPayloadBytes(delta, terminalOperations);
    if (
      subscription.deferredPlaceholderChunks > MAX_DEFERRED_PLACEHOLDER_CHUNKS ||
      subscription.deferredPlaceholderRows > MAX_DEFERRED_PLACEHOLDER_ROWS ||
      subscription.deferredPlaceholderBytes > MAX_DEFERRED_PLACEHOLDER_BYTES
    ) {
      throw new Error(
        "relation subscription buffered unresolved placeholder rows beyond bounded limits",
      );
    }
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
      this.failSubscription(subscription, error);
    }
  }

  private failSubscription(subscription: SubscriptionState, error: Error): void {
    if (subscription.cancelled) return;
    subscription.cancelled = true;
    clearDeferredPlaceholderBuffer(subscription);
    closeSubscriptionSourceState(subscription);
    try {
      subscription.callback?.(error, null);
    } catch (callbackError) {
      setTimeout(() => {
        throw callbackError;
      }, 0);
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
        if (!waiter.active) return;
        waiter.active = false;
        const index = this.serverTransportErrorWaiters.indexOf(waiter);
        if (index >= 0) this.serverTransportErrorWaiters.splice(index, 1);
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

  private waitForServerTransportWork(
    tier: string,
    observedEpoch: number,
  ): { promise: Promise<void>; cancel: () => void } | null {
    if (tier !== "edge" && tier !== "global") return null;
    if (
      this.serverTransportWorkEpoch !== observedEpoch ||
      this.pendingInboundServerFrames.length > 0
    ) {
      return { promise: Promise.resolve(), cancel: () => {} };
    }
    const waiter: ServerTransportWorkWaiter = {
      active: true,
      resolve: () => {},
    };
    const promise = new Promise<void>((resolve) => {
      waiter.resolve = resolve;
    });
    this.serverTransportWorkWaiters.push(waiter);
    return {
      promise,
      cancel: () => {
        if (!waiter.active) return;
        waiter.active = false;
        const index = this.serverTransportWorkWaiters.indexOf(waiter);
        if (index >= 0) this.serverTransportWorkWaiters.splice(index, 1);
      },
    };
  }

  private notifyServerTransportWork(): void {
    this.serverTransportWorkEpoch += 1;
    this.resolveServerTransportWorkWaiters();
  }

  private resolveServerTransportWorkWaiters(): void {
    const waiters = this.serverTransportWorkWaiters.splice(0);
    for (const waiter of waiters) {
      if (waiter.active) waiter.resolve();
    }
  }
}

function closeSubscriptionSourceState(subscription: SubscriptionState): void {
  for (const source of subscription.sources) {
    closeSubscriptionSource(source.source);
  }
}

function clearDeferredPlaceholderBuffer(subscription: SubscriptionState): void {
  subscription.deferredVisiblePublication = false;
  subscription.deferredVisibleReset = false;
  subscription.deferredTerminalOperations = [];
  subscription.deferredTerminalLayouts = [];
  subscription.deferredPlaceholderChunks = 0;
  subscription.deferredPlaceholderRows = 0;
  subscription.deferredPlaceholderBytes = 0;
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.filter(
    (frame): frame is Uint8Array =>
      ArrayBuffer.isView(frame) && frame.constructor.name === "Uint8Array",
  );
}

function recordWrite(write: Write, writes: Map<string, Write>): BatchId {
  const id = write.batchId as BatchId;
  writes.set(id, write);
  return id as BatchId;
}

function txIdFromContext(writeContext?: string | null): OpenBatchId | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as { batch_id?: unknown };
    return typeof parsed.batch_id === "string" ? (parsed.batch_id as OpenBatchId) : undefined;
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

function txStateMessage(openBatchId: string, completedBatches: Map<string, CompletedTx>): string {
  const completed = completedBatches.get(openBatchId);
  if (completed?.state === "committed") {
    return `open batch ${openBatchId} is already committed`;
  }
  return `open batch ${openBatchId} has already been completed or was never opened`;
}

function commitTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId)?.state === "committed"
    ? `Write error: ${message}`
    : `Commit transaction failed: Write error: ${message}`;
}

function rollbackTransactionMessage(
  openBatchId: string,
  completedBatches: Map<string, CompletedTx>,
): string {
  const message = txStateMessage(openBatchId, completedBatches);
  return completedBatches.get(openBatchId)?.state === "committed"
    ? `Write error: ${message}`
    : `Rollback transaction failed: Write error: ${message}`;
}

function assertTransactionReadOpen(
  optionsJson: string | null | undefined,
  pendingTxs: Map<string, PendingTx>,
  completedTxs: Map<string, CompletedTx>,
): void {
  const openBatchId = openBatchIdFromOptions(optionsJson);
  if (!openBatchId || pendingTxs.has(openBatchId)) return;
  throw new Error(`Query setup failed: Write error: ${txStateMessage(openBatchId, completedTxs)}`);
}

function pendingTxFromOptions(
  optionsJson: string | null | undefined,
  pendingTxs: Map<string, PendingTx>,
): PendingTx | undefined {
  const openBatchId = openBatchIdFromOptions(optionsJson);
  return openBatchId ? pendingTxs.get(openBatchId) : undefined;
}

function openBatchIdFromOptions(optionsJson?: string | null): OpenBatchId | undefined {
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
  rootTerminal = true,
): ColumnDescriptor[] {
  const tableSchema = schema[table];
  if (!tableSchema) throw new Error(`missing schema for subscription table ${table}`);
  const wildcard = select === undefined;
  const selected = select ?? tableSchema.columns.map((column) => column.name);
  const columns = selected
    .map((columnName) => {
      const declared = tableSchema.columns.find((column) => column.name === columnName);
      if (declared) return wildcard && rootTerminal ? { ...declared, sparse: true } : declared;
      const magicType = magicColumnType(columnName);
      return magicType
        ? ({ name: columnName, column_type: magicType, nullable: false } satisfies ColumnDescriptor)
        : undefined;
    })
    .filter((column): column is ColumnDescriptor => column !== undefined);

  for (const subquery of arraySubqueries) {
    const childColumns = outputColumnsForTable(
      subquery.table,
      schema,
      subquery.select,
      subquery.nestedArrays ?? [],
      false,
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

function isPendingCoverageError(error: unknown): boolean {
  const message = errorMessage(error);
  return (
    message.includes("NotCovered") ||
    message.includes("not covered") ||
    message.includes("has not reached requested tier")
  );
}

function rejectedWaitError(
  batchId: BatchId,
  error: unknown,
): {
  kind: "rejected";
  batchId: BatchId;
  code: string;
  reason: string;
  /** An Error-compatible diagnostic for direct native callers. */
  message: string;
} | null {
  const message = errorMessage(error);
  if (!message.includes("WriteRejected")) return null;
  const rejection: {
    kind: "rejected";
    batchId: BatchId;
    code: string;
    reason: string;
    message: string;
  } = {
    kind: "rejected",
    batchId,
    code: rejectionCode(message),
    reason: rejectionReason(message),
    message,
  };
  // Worker transport intentionally carries only the enumerable structured
  // fields. Native callers also inspect rejected promises like Errors, so
  // retain Rust's diagnostic without widening that transport payload.
  Object.defineProperty(rejection, "message", { enumerable: false });
  return rejection;
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
    offset?: unknown;
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
    offset: readOffset(record.offset),
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
    .map((column) => ({ name: column.name, valueType: storageColumnValueType(column), column }));
  const values = descriptor.map(({ column }) =>
    encodeCellValue(column, valueFor(column), requireMissingDefaults),
  );
  const writer = new PostcardWriter();
  writeDescriptor(writer, descriptor);
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

/**
 * Mutation cells deliberately differ from packed rows only when a field is
 * omitted: inserts can leave a server default unresolved, whereas patches
 * never synthesize a missing field. Every present value is encoded by the row
 * codec so scalar tags, arrays, nested rows, nullable values, and sparse
 * carriers have one binary authority.
 */
function encodeCellValue(
  column: ColumnDescriptor,
  value: Value | undefined,
  requireMissingDefaults: boolean,
): Uint8Array {
  const resolved = value;
  if (!resolved) {
    if (column.nullable) return encodeNativeNullValue(storageColumnValueType(column));
    if (column.column_type.type === "Array") {
      return encodeNativeColumnValue(column, { type: "Array", value: [] });
    }
    if (requireMissingDefaults && column.default == null) {
      throw new Error(`missing required column ${column.name}`);
    }
    return new Uint8Array();
  }
  return encodeNativeColumnValue(column, resolved);
}

function readRowBatches(payload: Uint8Array): NativeRowBatch[] {
  return new PostcardReader(payload).readVec(readNativeRowBatch);
}

function readRelationSnapshot(payload: Uint8Array): NativeRelationSubscriptionSnapshot {
  return readNativeRelationSubscriptionSnapshot(new PostcardReader(payload));
}

export function rowsFromBatches(
  batches: NativeRowBatch[],
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
  nestedRowCarrier: NestedRowCarrier = "full-record",
): RowState[] {
  const rows: RowState[] = [];
  for (const batch of batches) {
    const fieldPlans = nativeRowFieldPlans(batch, schema, projectedColumns);
    const decodeRecord = createRecordValueDecoder(batch.descriptor);
    for (const row of batch.rows) {
      const values: Value[] = [];
      const valuesByColumn = new Map<string, Value>();

      for (const field of fieldPlans) {
        const value = decodePlannedField(field, decodeRecord, row.raw, nestedRowCarrier);
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

function nativeRowFieldPlans(
  batch: NativeRowBatch,
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
): NativeRowFieldPlan[] {
  let cache = nativeRowFieldPlanCache.get(schema);
  if (!cache) {
    cache = new Map();
    nativeRowFieldPlanCache.set(schema, cache);
  }
  const cacheKey = nativeRowFieldPlanCacheKey(batch);
  const cached = projectedColumns ? undefined : cache.get(cacheKey);
  if (cached) return cached;

  const columns = projectedColumns ?? schema[batch.table]?.columns ?? [];
  const columnsByName = new Map(columns.map((column) => [column.name, column]));
  const plans: NativeRowFieldPlan[] = [];

  for (let index = 0; index < batch.descriptor.length; index += 1) {
    const fieldName = batch.descriptor[index]?.name;
    if (!fieldName || isInternalField(fieldName) || isCurrentRowPhysicalField(fieldName)) continue;

    const name = publicFieldName(fieldName);
    const type =
      name === "$createdBy" || name === "$updatedBy"
        ? ({ type: "Uuid" } as const)
        : (magicColumnType(name) ?? columnsByName.get(name)?.column_type);
    plans.push({
      name,
      index,
      type,
      storageType: batch.descriptor[index]!.valueType,
      includeInValues: !isHiddenIncludeColumn(name) && !isProvenanceMagicColumn(name),
    });
  }

  if (!projectedColumns) cache.set(cacheKey, plans);
  return plans;
}

// These fields are provenance retained by settled/materializer read paths.
// They are never Jazz application columns (user columns use the `user_`
// descriptor namespace) and must not cross the public native row boundary.
function isCurrentRowPhysicalField(fieldName: string): boolean {
  return (
    fieldName === "schema_version" || fieldName === "parents" || fieldName === "authored_columns"
  );
}

function rowsFromRelationSnapshot(
  snapshot: NativeRelationSubscriptionSnapshot,
  schema: WasmSchema,
  projectedColumns?: readonly ColumnDescriptor[],
  nestedRowCarrier: NestedRowCarrier = "full-record",
): RowState[] {
  const rows = stripRelationSnapshotMetadata(
    rowsFromBatches(snapshot.rows, schema, projectedColumns, nestedRowCarrier),
    schema,
  );
  return rows.slice(0, snapshot.rootCount);
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

function withValuesByColumn(row: RowState, valuesByColumn: Map<string, Value>): RowState {
  Object.defineProperty(row, "valuesByColumn", {
    value: valuesByColumn,
    enumerable: false,
    configurable: true,
  });
  return row;
}

export function applySubscriptionDeltaWithWireDelta(
  currentRows: RowState[],
  currentIndexByKey: Map<string, number>,
  delta: NativeSubscriptionDelta,
  schema: WasmSchema,
  reset = false,
  outputColumns: SubscriptionOutputColumns | null = null,
): { rows: RowState[]; rowIndexByKey: Map<string, number>; wireDelta: NativeRowDelta } {
  const { addedRows, updatedRows, removedEntries, rows, rowIndexByKey } =
    applySubscriptionDeltaToState(
      currentRows,
      currentIndexByKey,
      delta,
      schema,
      reset,
      outputColumns,
    );
  return {
    rows,
    rowIndexByKey,
    wireDelta: {
      ...nativeDeltaFromChanges(
        addedRows,
        updatedRows,
        removedEntries,
        rowIndexByKey,
        schema,
        outputColumns,
      ),
      ...(reset ? { reset: true } : {}),
    },
  };
}

function applySubscriptionDeltaToState(
  currentRows: RowState[],
  currentIndexByKey: Map<string, number>,
  delta: NativeSubscriptionDelta,
  schema: WasmSchema,
  reset = false,
  outputColumns: SubscriptionOutputColumns | null = null,
): {
  addedRows: RowState[];
  updatedRows: RowState[];
  removedEntries: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }>;
  rows: RowState[];
  rowIndexByKey: Map<string, number>;
} {
  const rowsByKey = reset
    ? new Map<string, RowState>()
    : new Map(currentRows.map((row) => [rowStateKey(row), row]));
  const removedEntries: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }> = [];

  for (const [removedIndex, removed] of delta.removed.entries()) {
    const id = formatUuid(removed.rowId);
    const resultKeyBytes = delta.removedOccurrenceKeys[removedIndex];
    const key = resultKeyBytes
      ? occurrenceStateKey(resultKeyBytes, removed.table, id)
      : rowKey(removed.table, id);
    removedEntries.push({ id, index: currentIndexByKey.get(key) ?? 0, resultKeyBytes });
    rowsByKey.delete(key);
  }

  const projectedColumns = outputColumns?.rootColumns;
  const nestedRowCarrier: NestedRowCarrier = "full-record";
  const addedRows = rowsFromBatches(delta.added, schema, projectedColumns, nestedRowCarrier);
  const updatedRows = rowsFromBatches(delta.updated, schema, projectedColumns, nestedRowCarrier);
  attachOccurrenceKeys(addedRows, delta.addedOccurrenceKeys);
  attachOccurrenceKeys(updatedRows, delta.updatedOccurrenceKeys);
  for (const row of addedRows.concat(updatedRows)) {
    rowsByKey.set(rowStateKey(row), row);
  }

  const rows = Array.from(rowsByKey.values());
  const rowIndexByKey = indexRowsByKey(rows);
  return {
    addedRows,
    updatedRows,
    removedEntries,
    rows,
    rowIndexByKey,
  };
}

function indexRowsByKey(rows: RowState[]): Map<string, number> {
  const index = new Map<string, number>();
  rows.forEach((row, rowIndex) => {
    index.set(rowStateKey(row), rowIndex);
  });
  return index;
}

function attachOccurrenceKeys(rows: RowState[], keys: Uint8Array[]): void {
  if (rows.length !== keys.length)
    throw new Error("subscription occurrence sidecar length mismatch");
  rows.forEach((row, index) => {
    const bytes = keys[index]!;
    row.resultKeyBytes = bytes;
    row.resultKey = publicResultKey(bytes);
  });
}

function occurrenceStateKey(bytes: Uint8Array, table?: string, sourceId?: string): string {
  if (bytes.length === 17 && bytes[0] === 1 && table && sourceId) return rowKey(table, sourceId);
  return `result\0${Array.from(bytes, (byte) => byteHex[byte]).join("")}`;
}

function publicResultKey(bytes: Uint8Array): string {
  if (bytes.length === 17 && bytes[0] === 1) return formatUuid(bytes.subarray(1));
  return `result:${Array.from(bytes, (byte) => byteHex[byte]).join("")}`;
}

function rowStateKey(row: RowState): string {
  return row.resultKeyBytes
    ? occurrenceStateKey(row.resultKeyBytes, row.table, row.id)
    : rowKey(row.table, row.id);
}

function rowKey(table: string, id: string): string {
  return `${table}\0${id}`;
}

function decodePlannedField(
  field: NativeRowFieldPlan,
  decodeRecord: (raw: Uint8Array, logicalIndex: number) => Uint8Array | null,
  raw: Uint8Array,
  nestedRowCarrier: NestedRowCarrier,
): Value {
  const bytes = decodeRecord(raw, field.index);
  if (bytes == null) return { type: "Null" };
  if (!field.type) return { type: "Bytea", value: bytes };
  try {
    return decodeBytes(field.type, bytes, field.name, field.storageType, nestedRowCarrier);
  } catch (error) {
    throw new Error(
      `${String(error)} while decoding ${field.name} as ${field.type.type} from storage tag ${field.storageType.tag} (${bytes.byteLength} bytes)`,
    );
  }
}

function decodeBytes(
  type: ColumnType,
  bytes: Uint8Array,
  fieldName?: string,
  storageType?: ValueType,
  nestedRowCarrier: NestedRowCarrier = "full-record",
): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Integer":
      return { type: "Integer", value: view.getInt32(0, true) };
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
    case "EnumPayload":
      return decodePayloadEnumBytes(type, bytes, storageType, nestedRowCarrier);
    case "Uuid":
      return { type: "Uuid", value: formatUuid(bytes) };
    case "Bytea":
      return { type: "Bytea", value: bytes.slice() };
    case "Array":
      return {
        type: "Array",
        value: decodeArrayBytes(
          type.element,
          bytes,
          arrayElementStorageType(storageType),
          nestedRowCarrier,
        ),
      };
    case "Row":
      return {
        type: "Row",
        value: decodeNestedRowBytes(
          type.columns,
          bytes,
          recordStorageDescriptor(storageType),
          nestedRowCarrier,
        ),
      };
  }
}

function decodePayloadEnumBytes(
  type: Extract<ColumnType, { type: "EnumPayload" }>,
  bytes: Uint8Array,
  storageType: ValueType | undefined,
  nestedRowCarrier: NestedRowCarrier,
): Value {
  if (bytes.byteLength < 4) throw new Error("invalid Enum payload value");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const nameLength = view.getUint32(0, true);
  if (bytes.byteLength < 4 + nameLength) throw new Error("invalid Enum payload case");
  const caseName = textDecoder.decode(bytes.subarray(4, 4 + nameLength));
  const entry = type.cases.find((candidate) => candidate.name === caseName);
  if (!entry) throw new Error("unknown Enum payload case");
  const enumStorage = nonNullableStorageType(storageType);
  const payloadDescriptor =
    enumStorage?.tag === 16
      ? enumStorage.enumSchema?.cases?.find((candidate) => candidate.name === caseName)?.payload
      : undefined;
  if (!payloadDescriptor || payloadDescriptor.length !== entry.fields.length) {
    throw new Error("Enum payload descriptor mismatch");
  }
  const raw = bytes.subarray(4 + nameLength);
  const decodeRecord = createRecordValueDecoder(payloadDescriptor);
  return {
    type: "Enum",
    value: {
      case: caseName,
      values: entry.fields.map((field, index) => {
        const fieldBytes = decodeRecord(raw, index);
        return fieldBytes == null
          ? { type: "Null" }
          : decodeBytes(
              field.column_type,
              fieldBytes,
              field.name,
              payloadDescriptor[index]?.valueType,
              nestedRowCarrier,
            );
      }),
    },
  };
}

function nonNullableStorageType(storageType?: ValueType): ValueType | undefined {
  let current = storageType;
  while (current?.tag === 14) current = current.inner;
  return current;
}

function arrayElementStorageType(storageType?: ValueType): ValueType | undefined {
  const array = nonNullableStorageType(storageType);
  return array?.tag === 13 ? array.inner : undefined;
}

function recordStorageDescriptor(storageType?: ValueType): DescriptorField[] | undefined {
  const record = nonNullableStorageType(storageType);
  return record?.tag === 15 ? record.record : undefined;
}

export type NestedRowCarrier = "full-record" | "keyed-terminal";

export function decodeNestedRowBytes(
  columns: readonly ColumnDescriptor[],
  bytes: Uint8Array,
  descriptor?: DescriptorField[],
  carrier: NestedRowCarrier = "full-record",
): { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } {
  if (descriptor) {
    const keyed = carrier === "keyed-terminal";
    if (keyed && descriptor[0]?.name !== "row_uuid") {
      throw new Error("keyed terminal nested row descriptor must start with row_uuid");
    }
    if (keyed && bytes.byteLength < 16) throw new Error("terminal nested row is missing its key");
    const payloadDescriptor = keyed ? descriptor.slice(1) : descriptor;
    const payload = keyed ? bytes.subarray(16) : bytes;
    const decodeRecord = createRecordValueDecoder(payloadDescriptor);
    const columnsByName = new Map(columns.map((column) => [column.name, column]));
    const valuesByColumn = new Map<string, Value>();
    let id = keyed ? formatUuid(bytes.subarray(0, 16)) : undefined;
    for (let index = 0; index < payloadDescriptor.length; index += 1) {
      const name = payloadDescriptor[index]?.name;
      if (!name) continue;
      const valueBytes = decodeRecord(payload, index);
      if (!keyed && name === "row_uuid" && valueBytes) {
        id = formatUuid(valueBytes);
        continue;
      }
      const column = columnsByName.get(name);
      if (!column) continue;
      valuesByColumn.set(
        name,
        valueBytes == null
          ? { type: "Null" }
          : decodeBytes(
              column.column_type,
              valueBytes,
              name,
              payloadDescriptor[index]?.valueType,
              carrier,
            ),
      );
    }
    const row: { id?: string; values: Value[]; valuesByColumn?: Map<string, Value> } = {
      id,
      values: columns.map(
        (column) =>
          valuesByColumn.get(column.name) ??
          (column.column_type.type === "Array"
            ? ({ type: "Array", value: [] } satisfies Value)
            : ({ type: "Null" } satisfies Value)),
      ),
    };
    Object.defineProperty(row, "valuesByColumn", {
      value: valuesByColumn,
      enumerable: false,
      configurable: true,
    });
    return row;
  }
  if (bytes.byteLength < 5) throw new Error("invalid nested row value");
  const hasId = bytes[0] === 1;
  let offset = 1;
  let id: string | undefined;
  if (hasId) {
    if (bytes.byteLength < 21) throw new Error("invalid nested row id");
    id = formatUuid(bytes.subarray(offset, offset + 16));
    offset += 16;
  }
  const length = readU32Le(bytes, offset);
  offset += 4;
  const raw = bytes.subarray(offset, offset + length);
  if (raw.byteLength !== length) throw new Error("invalid nested row value length");
  return { id, values: decodeNativeRowValues(columns, raw) };
}

function decodeArrayBytes(
  elementType: ColumnType,
  bytes: Uint8Array,
  storageElementType?: ValueType,
  nestedRowCarrier: NestedRowCarrier = "full-record",
): Value[] {
  const elementWidth = nativeFixedValueSize(
    storageElementType ?? storageColumnTypeToValueType(elementType),
  );
  if (elementWidth != null) {
    if (elementWidth === 0) return [];
    if (bytes.length % elementWidth !== 0) {
      throw new Error(`invalid fixed-width array byte length ${bytes.length}`);
    }
    const values: Value[] = [];
    for (let offset = 0; offset < bytes.length; offset += elementWidth) {
      values.push(
        decodeBytes(
          elementType,
          bytes.subarray(offset, offset + elementWidth),
          undefined,
          storageElementType,
          nestedRowCarrier,
        ),
      );
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
    values.push(
      decodeBytes(
        elementType,
        bytes.subarray(start, end),
        undefined,
        storageElementType,
        nestedRowCarrier,
      ),
    );
  }
  return values;
}

function normalizeSubscriptionChunk(chunk: unknown):
  | { type: "snapshot"; snapshot: NativeRelationSubscriptionSnapshot; settled?: boolean }
  | {
      type: "delta";
      reset?: boolean;
      delta: NativeSubscriptionDelta;
      terminalOperations?: NativeTerminalOperation[];
      terminalLayouts?: NativeTerminalRootLayout[];
      settled?: boolean;
    }
  | {
      type: "rejected";
      reason:
        | { type: "UnsupportedShapeCapability"; detail: string }
        | { type: "ServerFailure"; code: string }
        | { type: "ShapeRegistrationPendingCatalogueAdmission" };
    }
  | { type: "closed" } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as {
    type?: unknown;
    rows?: unknown;
    delta?: unknown;
    reason?: unknown;
    reset?: unknown;
    settled?: unknown;
    terminalOperations?: unknown;
    terminalLayouts?: unknown;
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
      terminalOperations: Array.isArray(record.terminalOperations)
        ? (record.terminalOperations as NativeTerminalOperation[])
        : undefined,
      terminalLayouts: Array.isArray(record.terminalLayouts)
        ? (record.terminalLayouts as NativeTerminalRootLayout[])
        : undefined,
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
    };
  }
  if (record.type === "rejected" || record.type === "Rejected") {
    return {
      type: "rejected",
      reason: normalizeSubscriptionRejectionReason(record.reason),
    };
  }
  throw new Error("unknown subscription chunk");
}

function normalizeSubscriptionRejectionReason(
  reason: unknown,
):
  | { type: "UnsupportedShapeCapability"; detail: string }
  | { type: "ServerFailure"; code: string }
  | { type: "ShapeRegistrationPendingCatalogueAdmission" } {
  if (!reason || typeof reason !== "object") {
    throw new Error("expected subscription rejection reason");
  }
  const record = reason as { type?: unknown; detail?: unknown; code?: unknown };
  if (record.type === "UnsupportedShapeCapability" && typeof record.detail === "string") {
    return { type: "UnsupportedShapeCapability", detail: record.detail };
  }
  if (record.type === "ShapeRegistrationPendingCatalogueAdmission") {
    return { type: "ShapeRegistrationPendingCatalogueAdmission" };
  }
  if (record.type === "ServerFailure" && typeof record.code === "string") {
    return { type: "ServerFailure", code: record.code };
  }
  throw new Error("unknown subscription rejection reason");
}

function subscriptionRejectionError(
  reason:
    | { type: "UnsupportedShapeCapability"; detail: string }
    | { type: "ServerFailure"; code: string }
    | { type: "ShapeRegistrationPendingCatalogueAdmission" },
): Error {
  const detail =
    reason.type === "UnsupportedShapeCapability"
      ? reason.detail
      : reason.type === "ServerFailure"
        ? reason.code
        : "catalogue admission pending";
  return new Error(`Subscription rejected: ${reason.type}: ${detail}`);
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
    previousRows.map((row, index) => [rowStateKey(row), { row, index }]),
  );
  const nextKeys = new Set<string>();
  const added: RowState[] = [];
  const updated: RowState[] = [];
  const removed: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }> = [];
  const rowIndexByKey = indexRowsByKey(rows);

  rows.forEach((row, index) => {
    const key = rowStateKey(row);
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
    if (!nextKeys.has(rowStateKey(row))) {
      removed.push({ id: row.id, index, resultKeyBytes: row.resultKeyBytes });
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

function nativeResetDeltaFromBatches(
  batches: NativeRowBatch[],
  outputColumns: SubscriptionOutputColumns | null,
): NativeRowDelta {
  let rowIndex = 0;
  const chunks: Uint8Array[] = [];
  for (const batch of batches) {
    const frameColumns =
      outputColumns && batch.table === outputColumns.rootTable
        ? logicalStorageColumns(outputColumns.rootColumns)
        : null;
    const encodeFrameRow = frameColumns
      ? createRawNativeFrameRowEncoder(batch.descriptor, frameColumns)
      : (raw: Uint8Array) => raw;
    for (const row of batch.rows) {
      const raw = encodeFrameRow(row.raw);
      chunks.push(row.rowId, encodeU32Le(rowIndex), encodeU32Le(raw.byteLength), raw);
      rowIndex += 1;
    }
  }
  return {
    __jazzNativeRowDelta: true,
    added: concatBytes(chunks),
    removed: new Uint8Array(),
    updated: new Uint8Array(),
    addedCount: rowIndex,
    removedCount: 0,
    updatedCount: 0,
    reset: true,
  };
}

function plainResetChunkCanStayPacked(
  subscription: SubscriptionState,
  chunk: {
    reset?: boolean;
    delta: NativeSubscriptionDelta;
  },
  schema: WasmSchema,
): boolean {
  const reset = chunk.reset === true;
  const noUpdated = chunk.delta.updated.length === 0;
  const noRemoved = chunk.delta.removed.length === 0;
  const identityProjection = subscriptionOutputColumnsAreIdentityProjection(
    subscription.outputColumns,
    schema,
  );
  const sourceRowKeysOnly = chunk.delta.addedOccurrenceKeys.every(
    (key) => key.length === 17 && key[0] === 1,
  );
  const canStayPacked = reset && noUpdated && noRemoved && identityProjection && sourceRowKeysOnly;
  return canStayPacked;
}

function subscriptionOutputColumnsAreIdentityProjection(
  outputColumns: SubscriptionOutputColumns | null,
  schema: WasmSchema,
): boolean {
  if (!outputColumns) return true;
  const tableColumns = publicTableColumns(schema[outputColumns.rootTable]?.columns ?? []);
  if (outputColumns.rootColumns.length !== tableColumns.length) return false;
  return outputColumns.rootColumns.every((column, index) => column === tableColumns[index]);
}

function publicTableColumns(columns: readonly ColumnDescriptor[]): ColumnDescriptor[] {
  return columns.filter((column) => !isHiddenIncludeColumn(column.name));
}

function createRawNativeFrameRowEncoder(
  sourceDescriptor: DescriptorField[],
  columns: readonly ColumnDescriptor[],
): (raw: Uint8Array) => Uint8Array {
  if (nativeDescriptorMatchesColumns(sourceDescriptor, columns)) {
    return (raw) => raw;
  }
  const outputDescriptor = columns.map((column) => ({
    name: column.name,
    valueType: storageColumnValueType(column),
  }));
  const decodeRecord = createRecordValueDecoder(sourceDescriptor);
  const sourceIndexesByPublicName = new Map<string, number>();
  for (let index = 0; index < sourceDescriptor.length; index += 1) {
    const fieldName = sourceDescriptor[index]?.name;
    if (!fieldName || isInternalField(fieldName)) continue;
    sourceIndexesByPublicName.set(publicFieldName(fieldName), index);
  }

  return (raw) => {
    const values = columns.map((column) => {
      const sourceIndex = sourceIndexesByPublicName.get(column.name);
      const outputValueType = storageColumnValueType(column);
      if (sourceIndex === undefined) return encodeNativeNullValue(outputValueType);
      const decoded = decodeRecord(raw, sourceIndex);
      if (decoded == null) return encodeNativeNullValue(outputValueType);
      return encodeFrameColumnValue(decoded, outputValueType);
    });
    return createRecord(outputDescriptor, values);
  };
}

function encodeFrameColumnValue(decoded: Uint8Array, outputValueType: ValueType): Uint8Array {
  if (outputValueType.tag !== 14) return decoded;
  const output = new Uint8Array(decoded.length + 1);
  output[0] = 1;
  output.set(decoded, 1);
  return output;
}

function nativeDescriptorMatchesColumns(
  descriptor: readonly DescriptorField[],
  columns: readonly ColumnDescriptor[],
): boolean {
  if (descriptor.length !== columns.length) return false;
  return columns.every((column, index) => {
    const field = descriptor[index];
    return (
      field?.name === column.name &&
      valueTypeCacheKey(field.valueType) === valueTypeCacheKey(storageColumnValueType(column))
    );
  });
}

function materializePackedResetRows(subscription: SubscriptionState, schema: WasmSchema): void {
  if (!subscription.packedResetBatches) return;
  subscription.rows = rowsFromBatches(
    subscription.packedResetBatches,
    schema,
    subscription.outputColumns?.rootColumns,
    "full-record",
  );
  subscription.rowIndexByKey = indexRowsByKey(subscription.rows);
  subscription.packedResetBatches = null;
  subscription.packedResetRows = null;
}

function subscriptionDeltaPayloadBytes(
  delta: NativeSubscriptionDelta,
  terminalOperations?: NativeTerminalOperation[],
): number {
  const rowBytes = delta.added
    .concat(delta.updated)
    .reduce(
      (sum, batch) =>
        sum +
        batch.rows.reduce((rowSum, row) => rowSum + row.raw.byteLength + row.rowId.byteLength, 0),
      0,
    );
  const occurrenceBytes = delta.addedOccurrenceKeys
    .concat(delta.updatedOccurrenceKeys, delta.removedOccurrenceKeys)
    .reduce((sum, key) => sum + key.byteLength, 0);
  const terminalBytes =
    terminalOperations?.reduce(
      (sum, operation) => sum + nativeTerminalOperationBytes(operation),
      0,
    ) ?? 0;
  return rowBytes + occurrenceBytes + terminalBytes;
}

function nativeTerminalOperationBytes(operation: NativeTerminalOperation): number {
  const layoutIdBytes = operation.rootLayoutId?.length ?? operation.rootDescriptor?.length ?? 0;
  const rootKeyBytes = operation.root_key.length;
  const pathBytes = operation.path.reduce((sum, segment) => {
    if ("Collection" in segment) {
      return sum + utf8ByteLength(segment.Collection);
    }
    return sum + segment.Key.length;
  }, 0);
  const editBytes =
    "Insert" in operation.edit
      ? operation.edit.Insert.key.length + operation.edit.Insert.value.length
      : "Update" in operation.edit
        ? operation.edit.Update.key.length + operation.edit.Update.value.length
        : "Remove" in operation.edit
          ? operation.edit.Remove.key.length
          : operation.edit.Move.key.length;
  return layoutIdBytes + rootKeyBytes + pathBytes + editBytes;
}

function utf8ByteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function nativeDeltaFromChanges(
  added: RowState[],
  updated: RowState[],
  removed: Array<{ id: string; index: number; resultKeyBytes?: Uint8Array }>,
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
    addedOccurrenceKeys: added.map((row) => row.resultKeyBytes ?? legacyResultKey(row.id)),
    updatedOccurrenceKeys: updated.map((row) => row.resultKeyBytes ?? legacyResultKey(row.id)),
    removedOccurrenceKeys: removed.map((row) => row.resultKeyBytes ?? legacyResultKey(row.id)),
  };
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
    const physicalColumns =
      outputColumns && row.table === outputColumns.rootTable
        ? outputColumns.rootColumns
        : schema?.[row.table]?.columns;
    const columns = physicalColumns ? logicalStorageColumns(physicalColumns) : undefined;
    if (!columns) {
      throw new Error(`missing schema for subscription row table ${row.table}`);
    }
    let encodeRow = encodersByColumns.get(columns);
    if (!encodeRow) {
      encodeRow = createNativeRowValueEncoder(columns);
      encodersByColumns.set(columns, encodeRow);
    }
    const frameValues = valuesForNativeFrame(row, columns);
    let raw: Uint8Array;
    try {
      raw = encodeRow(frameValues);
    } catch (error) {
      throw new Error(
        `${String(error)} while encoding ${row.table}: ${columns.map((column, index) => `${column.name}:${column.column_type.type}=${frameValues[index]?.type}`).join(", ")}`,
      );
    }
    chunks.push(requiredUuidBytes(row.id), encodeU32Le(rowIndexByKey.get(rowStateKey(row)) ?? 0));
    if (updated) chunks.push(Uint8Array.of(1));
    chunks.push(encodeU32Le(raw.byteLength), raw);
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

function subscriptionRowsRequireBufferedPublication(
  rows: RowState[],
  schema: WasmSchema,
  outputColumns: SubscriptionOutputColumns | null,
): boolean {
  return rows.some((row) => {
    const columns =
      outputColumns && row.table === outputColumns.rootTable
        ? outputColumns.rootColumns
        : schema[row.table]?.columns;
    if (!columns) return false;
    return valuesForNativeFrame(row, logicalStorageColumns(columns)).some(
      (value, index) =>
        value.type === "Null" &&
        logicalStorageColumns(columns)[index]?.nullable === false &&
        logicalStorageColumns(columns)[index]?.column_type.type !== "Array",
    );
  });
}

function encodeNativeRemoves(removed: Array<{ id: string; index: number }>): Uint8Array {
  return concatBytes(removed.flatMap((row) => [requiredUuidBytes(row.id), encodeU32Le(row.index)]));
}

function legacyResultKey(id: string): Uint8Array {
  return Uint8Array.from([1, ...requiredUuidBytes(id)]);
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

function valueEqual(left: Value, right: Value | undefined): boolean {
  if (!right || left.type !== right.type) return false;
  switch (left.type) {
    case "Bytea":
      return right.type === "Bytea" && bytesEqual(left.value, right.value);
    case "Array":
      return right.type === "Array" && rowValuesEqual(left.value, right.value);
    case "Enum":
      return (
        right.type === "Enum" &&
        left.value.case === right.value.case &&
        rowValuesEqual(left.value.values, right.value.values)
      );
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
  return (
    byteHex[bytes[0]!] +
    byteHex[bytes[1]!] +
    byteHex[bytes[2]!] +
    byteHex[bytes[3]!] +
    "-" +
    byteHex[bytes[4]!] +
    byteHex[bytes[5]!] +
    "-" +
    byteHex[bytes[6]!] +
    byteHex[bytes[7]!] +
    "-" +
    byteHex[bytes[8]!] +
    byteHex[bytes[9]!] +
    "-" +
    byteHex[bytes[10]!] +
    byteHex[bytes[11]!] +
    byteHex[bytes[12]!] +
    byteHex[bytes[13]!] +
    byteHex[bytes[14]!] +
    byteHex[bytes[15]!]
  );
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
  return (
    name === "row_uuid" ||
    name === "tx_node_id" ||
    name === "tx_time" ||
    name === "schema_version" ||
    name === "parents" ||
    name === "authored_columns"
  );
}

function isHiddenIncludeColumn(name: string): boolean {
  return name.startsWith(HIDDEN_INCLUDE_COLUMN_PREFIX);
}

function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`expected ${label} bytes`);
}
