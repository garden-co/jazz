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
import type {
  DirectInsertResult,
  DirectMutationResult,
  MutationErrorEvent,
  Runtime,
  TransactionKind,
} from "../client.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryFromTable,
  queryWithEqFilters,
  queryWithPredicates,
  readAbiRowBatch,
  readAbiSubscriptionDelta,
  writeValueType,
  type AbiRowBatch,
  type AbiRemovedRow,
  type DirectQueryLiteral,
  type DirectQueryPredicate,
  type DescriptorField,
  type ValueType,
} from "./direct-codec.js";
import {
  columnTypeToValueType,
  columnValueType,
  encodeDirectSchema,
} from "./direct-schema-codec.js";
import { DirectWebSocketCarrier, directWireAuthFailureReason } from "./direct-websocket.js";
import { createRecord, decodeRecordValue } from "./direct-row-codec.js";

export { encodeDirectSchema } from "./direct-schema-codec.js";

type DirectCoreDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): DirectCoreDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): DirectCoreDb;
};

type DirectCoreDb = {
  all(query: DirectPreparedQuery, opts: unknown): Uint8Array;
  allForIdentity(query: DirectPreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
  propagateQuery?(query: DirectPreparedQuery, opts: unknown): void;
  queryIsCovered?(query: DirectPreparedQuery): boolean;
  prepareQuery(query: Uint8Array): DirectPreparedQuery;
  subscribe?(
    query: DirectPreparedQuery,
    opts: unknown,
  ): ReadableStream<unknown> | DirectSubscription;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  insertWithIdEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): DirectWrite;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  restoreEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): DirectWrite;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): DirectWrite;
  updateEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    author: Uint8Array,
  ): DirectWrite;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): DirectWrite;
  upsertEncodedForIdentity(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    author: Uint8Array,
  ): DirectWrite;
  delete(table: string, rowId: Uint8Array): DirectWrite;
  deleteForIdentity(table: string, rowId: Uint8Array, author: Uint8Array): DirectWrite;
  mergeableTx(): DirectTx;
  connectUpstream(): DirectTransport;
  tick(): void;
  close?(): void;
};

type DirectPreparedQuery = object;

type DirectSubscription = {
  readAll(): unknown[];
  drain?(): unknown[];
  close?(): boolean;
};

type DirectWrite = {
  payload: Uint8Array;
  wait(tier: string): void;
  writeState?(): unknown;
};

type DirectTx = {
  commit(): DirectWrite;
  rollback(): void;
  insertWithIdEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  restoreEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  updateEncoded(table: string, rowId: Uint8Array, patch: Uint8Array): void;
  upsertEncoded(table: string, rowId: Uint8Array, cells: Uint8Array): void;
  delete(table: string, rowId: Uint8Array): void;
};

export type DirectTransport = {
  close(): boolean;
  recvWireFrames(): unknown[];
  sendWireFrame(frame: Uint8Array): void;
  tick(): number;
};

export type DirectOpenPayload = {
  schema: Uint8Array;
  config: Uint8Array;
  peerIdentity: Uint8Array;
};

type PendingTx = {
  kind: TransactionKind;
  tx: DirectTx;
  writes: Array<{ table: string; rowId: Uint8Array }>;
};

type SubscriptionState = {
  source: ReadableStreamDefaultReader<unknown> | DirectSubscription;
  rows: RowState[];
  filters: RowFilter[];
  callback?: Function;
  cancelled: boolean;
  reading: boolean;
};

type RowFilter = { column: string; value: DirectQueryLiteral };

type RowState = {
  table: string;
  id: string;
  values: Value[];
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function openPersistentDirectDb(
  Runtime: DirectCoreDbConstructor,
  dataPath: string,
  schema: Uint8Array,
  config: Uint8Array,
): DirectCoreDb {
  if (!Runtime.openPersistent) {
    throw new Error("Direct core runtime does not expose persistent storage");
  }
  return Runtime.openPersistent(dataPath, schema, config);
}

export class DirectCoreRuntime implements Runtime {
  private readonly db: DirectCoreDb;
  private readonly schemaBytes: Uint8Array;
  private readonly configBytes: Uint8Array;
  private readonly peerIdentity: Uint8Array;
  private readonly schemaHash: string;
  private readonly preparedQueries = new Map<string, DirectPreparedQuery>();
  private readonly pendingTxs = new Map<string, PendingTx>();
  private readonly writes = new Map<string, DirectWrite>();
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private authFailureCallback: ((reason: string) => void) | null = null;
  private serverTransport: DirectTransport | null = null;
  private serverCarrier: DirectWebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<DirectWebSocketCarrier> | null = null;
  private serverEndpointUrl: string | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private readonly syncNeededCallbacks = new Set<() => void>();
  private serverPumpScheduled = false;
  private serverPumpAgain = false;
  private nextTransactionId = 1;
  private nextSubscriptionId = 1;

  constructor(
    Runtime: DirectCoreDbConstructor,
    private readonly schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
    opts?: { persistentPath?: string },
  ) {
    this.schemaBytes = encodeDirectSchema(schema);
    this.configBytes = openConfig(node, author, sourceId, historyComplete);
    this.peerIdentity = author;
    this.schemaHash = serializeRuntimeSchema(schema);
    this.db = opts?.persistentPath
      ? openPersistentDirectDb(Runtime, opts.persistentPath, this.schemaBytes, this.configBytes)
      : Runtime.openMemory(this.schemaBytes, this.configBytes);
  }

  getDirectOpenPayload(): DirectOpenPayload {
    return { schema: this.schemaBytes, config: this.configBytes, peerIdentity: this.peerIdentity };
  }

  connectUpstreamPeer(): DirectTransport {
    return this.db.connectUpstream();
  }

  close(): void {
    this.db.close?.();
  }

  encodeDirectQuery(queryJson: string): Uint8Array {
    return encodeQueryJson(queryJson, this.schema);
  }

  decodeDirectRows(payload: Uint8Array, queryJson: string): RowState[] {
    return filterRows(
      rowsFromBatches(readRowBatches(payload), this.schema),
      queryFiltersFromJson(queryJson, this.schema),
      this.schema,
    );
  }

  onDirectSyncNeeded(callback: () => void): () => void {
    this.syncNeededCallbacks.add(callback);
    return () => {
      this.syncNeededCallbacks.delete(callback);
    };
  }

  insert(
    table: string,
    values: InsertValues,
    _writeContext?: string | null,
    objectId?: string | null,
  ): DirectInsertResult {
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const cells = encodeCellsForRow(this.table(table), values);
    const writeIdentity = identityFromWriteContext(_writeContext);
    const tx = this.currentTx(_writeContext);
    if (tx) {
      assertNoSessionWriteInTx(writeIdentity);
      tx.tx.insertWithIdEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(_writeContext) ?? "");
    }
    const write = directWriteOrThrow("Insert", () =>
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
  ): DirectInsertResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const writeIdentity = identityFromWriteContext(writeContext);
    const tx = this.currentTx(writeContext);
    if (tx) {
      assertNoSessionWriteInTx(writeIdentity);
      tx.tx.restoreEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(writeContext) ?? "");
    }
    const write = directWriteOrThrow("Insert", () =>
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
  ): DirectMutationResult {
    const rowId = parseUuid(objectId);
    const patch = encodeCellsForPatch(this.table(table), values);
    const writeIdentity = identityFromWriteContext(writeContext);
    const tx = this.currentTx(writeContext);
    if (tx) {
      assertNoSessionWriteInTx(writeIdentity);
      tx.tx.updateEncoded(table, rowId, patch);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = directWriteOrThrow("Update", () =>
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
  ): DirectMutationResult {
    const rowId = parseUuid(objectId);
    const cells = encodeCellsForRow(this.table(table), values);
    const writeIdentity = identityFromWriteContext(writeContext);
    const tx = this.currentTx(writeContext);
    if (tx) {
      assertNoSessionWriteInTx(writeIdentity);
      tx.tx.upsertEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = directWriteOrThrow("Insert", () =>
      writeIdentity
        ? this.db.upsertEncodedForIdentity(table, rowId, cells, writeIdentity)
        : this.db.upsertEncoded(table, rowId, cells),
    );
    return this.finishMutation(write);
  }

  delete(table: string, objectId: string, writeContext?: string | null): DirectMutationResult {
    this.table(table);
    const rowId = parseUuid(objectId);
    const writeIdentity = identityFromWriteContext(writeContext);
    const tx = this.currentTx(writeContext);
    if (tx) {
      assertNoSessionWriteInTx(writeIdentity);
      tx.tx.delete(table, rowId);
      tx.writes.push({ table, rowId });
      return { transactionId: txIdFromContext(writeContext) ?? "" };
    }
    const write = directWriteOrThrow("Delete", () =>
      writeIdentity
        ? this.db.deleteForIdentity(table, rowId, writeIdentity)
        : this.db.delete(table, rowId),
    );
    return this.finishMutation(write);
  }

  onMutationError(_callback: (event: MutationErrorEvent) => void): void {
    // Direct core wait() surfaces rejected writes synchronously today. Worker
    // replay of async rejection events is handled above this runtime layer.
  }

  beginTransaction(kind: TransactionKind): string {
    if (kind !== "mergeable") {
      throw new Error("Direct core runtime does not support exclusive transactions yet");
    }
    const id = `tx-${this.nextTransactionId++}`;
    this.pendingTxs.set(id, { kind, tx: this.db.mergeableTx(), writes: [] });
    return id;
  }

  commitTransaction(transactionId: string): void {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) throw new Error(`unknown transaction ${transactionId}`);
    const write = pending.tx.commit();
    this.writes.set(transactionId, write);
    this.pendingTxs.delete(transactionId);
    this.pumpSubscriptions();
    this.notifySyncNeeded();
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    const write = this.writes.get(transactionId);
    if (!write) return;
    for (;;) {
      try {
        this.pumpServerTransport();
        write.wait(tier);
        return;
      } catch (error) {
        const rejected = rejectedWaitError(transactionId, error);
        if (rejected) throw rejected;
        if (!isNotObservedWaitError(error)) throw error;
        this.pumpSubscriptions();
        await sleep(10);
      }
    }
  }

  rollbackTransaction(transactionId: string): boolean {
    const pending = this.pendingTxs.get(transactionId);
    if (!pending) return false;
    pending.tx.rollback();
    this.pendingTxs.delete(transactionId);
    return true;
  }

  async query(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<unknown> {
    assertSupportedReadOptions(tier, optionsJson);
    const query = this.prepareQuery(queryJson);
    const session = readSession(sessionJson);
    const opts = readOptions(tier, queryIncludesDeleted(queryJson));
    await this.propagateQueryIfNeeded(tier, optionsJson, query);
    const rows = session
      ? this.db.allForIdentity(query, parseUuid(session.user_id), opts)
      : this.db.all(query, opts);
    return this.decodeDirectRows(rows, queryJson);
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    assertSupportedReadOptions(tier, optionsJson);
    void readSession(sessionJson);
    if (!this.db.subscribe) {
      throw new Error("Direct core runtime does not support subscriptions");
    }
    const handle = this.nextSubscriptionId++;
    const query = this.prepareQuery(queryJson);
    const source = subscriptionSource(this.db.subscribe(query, readOptions(tier)));
    this.propagateSubscriptionQueryIfNeeded(tier, optionsJson, query);
    this.subscriptions.set(handle, {
      source,
      rows: [],
      filters: queryFiltersFromJson(queryJson, this.schema),
      cancelled: false,
      reading: false,
    });
    this.notifySyncNeeded();
    return handle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate;
    this.startSubscriptionReader(handle, subscription);
    this.notifySyncNeeded();
  }

  unsubscribe(handle: number): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.cancelled = true;
    if (isReadableSubscriptionReader(subscription.source)) {
      void subscription.source.cancel();
    } else {
      subscription.source.close?.();
    }
    this.subscriptions.delete(handle);
  }

  connect(url: string, authJson: string): void {
    this.disconnect();
    this.serverEndpointUrl = url;
    const transport = this.db.connectUpstream();
    this.serverTransport = transport;
    const carrier = new DirectWebSocketCarrier({
      endpointUrl: url,
      peerIdentity: this.peerIdentity,
      authJson,
      onFrame: (frame) => {
        transport.sendWireFrame(frame);
        this.scheduleServerPump();
      },
      onError: (error) => {
        const reason = directWireAuthFailureReason(error);
        if (reason) this.authFailureCallback?.(reason);
      },
    });
    this.serverCarrier = carrier;
    this.serverCarrierPromise = carrier.ready().then(() => {
      this.flushQueuedServerFrames(carrier);
      return carrier;
    });
    this.serverCarrierPromise.catch((error) => {
      this.handleServerTransportError(error);
    });
    this.scheduleServerPump();
  }

  disconnect(): void {
    this.serverCarrier?.close();
    this.serverCarrier = null;
    this.serverCarrierPromise = null;
    this.serverTransport?.close();
    this.serverTransport = null;
    this.serverEndpointUrl = null;
    this.queuedServerFrames.length = 0;
    this.serverPumpScheduled = false;
    this.serverPumpAgain = false;
  }

  updateAuth(authJson: string): void {
    if (!this.serverEndpointUrl) return;
    this.connect(this.serverEndpointUrl, authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    this.authFailureCallback = callback;
  }

  getSchema(): unknown {
    return this.schema;
  }

  getSchemaHash(): string {
    return this.schemaHash;
  }

  private finishInsert(
    table: string,
    rowId: Uint8Array,
    write: DirectWrite,
    identity?: Uint8Array,
  ): DirectInsertResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.notifySyncNeeded();
    return this.resultForRow(table, rowId, transactionId, identity);
  }

  private finishMutation(write: DirectWrite): DirectMutationResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    this.notifySyncNeeded();
    return { transactionId };
  }

  private resultForRow(
    table: string,
    rowId: Uint8Array,
    transactionId: string,
    identity?: Uint8Array,
  ): DirectInsertResult {
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

  private prepareQuery(queryJson: string): DirectPreparedQuery {
    const queryBytes = this.encodeDirectQuery(queryJson);
    const key = bytesKey(queryBytes);
    let query = this.preparedQueries.get(key);
    if (!query) {
      query = this.db.prepareQuery(queryBytes);
      this.preparedQueries.set(key, query);
    }
    return query;
  }

  private async propagateQueryIfNeeded(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: DirectPreparedQuery,
  ): Promise<void> {
    if (tier == null || tier === "local") return;
    const options = optionsJson == null ? {} : (JSON.parse(optionsJson) as Record<string, unknown>);
    if (options.propagation != null && options.propagation !== "full") return;
    if (!this.db.propagateQuery) return;
    this.db.propagateQuery(query, readOptions(tier));
    await this.waitForQueryCoverage(query);
  }

  private propagateSubscriptionQueryIfNeeded(
    tier: string | null | undefined,
    optionsJson: string | null | undefined,
    query: DirectPreparedQuery,
  ): void {
    const options = optionsJson == null ? {} : (JSON.parse(optionsJson) as Record<string, unknown>);
    if (options.propagation != null && options.propagation !== "full") return;
    if (!this.db.propagateQuery) return;
    this.db.propagateQuery(query, readOptions(tier === "local" ? "edge" : tier));
  }

  private async waitForQueryCoverage(query: DirectPreparedQuery): Promise<void> {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      this.pumpServerTransport();
      if (this.db.queryIsCovered?.(query)) return;
      await sleep(10);
    }
    this.scheduleServerPump();
  }

  private table(table: string): { columns: ColumnDescriptor[]; policies?: TablePolicies } {
    const definition = this.schema[table];
    if (!definition) throw new Error(`unknown table ${table}`);
    return definition;
  }

  private currentTx(writeContext?: string | null): PendingTx | undefined {
    const id = txIdFromContext(writeContext);
    return id ? this.pendingTxs.get(id) : undefined;
  }

  private pumpSubscriptions(): void {
    this.db.tick();
    for (const [handle, subscription] of this.subscriptions) {
      this.startSubscriptionReader(handle, subscription);
    }
  }

  private notifySyncNeeded(): void {
    for (const callback of this.syncNeededCallbacks) {
      callback();
    }
    this.scheduleServerPump();
  }

  private startSubscriptionReader(handle: number, subscription: SubscriptionState): void {
    if (subscription.cancelled || subscription.reading || !subscription.callback) return;
    if (!isReadableSubscriptionReader(subscription.source)) {
      this.drainNativeSubscription(handle, subscription);
      return;
    }
    subscription.reading = true;
    void this.readSubscription(handle, subscription);
  }

  private async readSubscription(handle: number, subscription: SubscriptionState): Promise<void> {
    if (!isReadableSubscriptionReader(subscription.source)) return;
    try {
      while (!subscription.cancelled && this.subscriptions.get(handle) === subscription) {
        const next = await subscription.source.read();
        if (next.done || subscription.cancelled) return;
        this.applySubscriptionChunk(subscription, next.value);
      }
    } finally {
      subscription.reading = false;
    }
  }

  private drainNativeSubscription(handle: number, subscription: SubscriptionState): void {
    if (isReadableSubscriptionReader(subscription.source)) return;
    for (const event of subscription.source.readAll()) {
      if (subscription.cancelled || this.subscriptions.get(handle) !== subscription) return;
      this.applySubscriptionChunk(subscription, event);
    }
  }

  private applySubscriptionChunk(subscription: SubscriptionState, value: unknown): void {
    const chunk = normalizeSubscriptionChunk(value);
    if (chunk.type === "closed") {
      subscription.cancelled = true;
      return;
    }
    if (chunk.type === "snapshot") {
      subscription.rows = filterRows(
        rowsFromBatches(chunk.rows, this.schema),
        subscription.filters,
        this.schema,
      );
      subscription.callback?.(nativeDeltaFromRows(subscription.rows));
    } else {
      subscription.rows = applySubscriptionDelta(subscription.rows, chunk.delta, this.schema);
      subscription.rows = filterRows(subscription.rows, subscription.filters, this.schema);
      subscription.callback?.(nativeDeltaFromRows(subscription.rows));
    }
  }

  private scheduleServerPump(): void {
    if (!this.serverTransport || this.serverPumpScheduled) return;
    this.serverPumpScheduled = true;
    queueMicrotask(() => {
      this.serverPumpScheduled = false;
      this.pumpServerTransport();
      if (this.serverPumpAgain) {
        this.serverPumpAgain = false;
        this.scheduleServerPump();
      }
    });
  }

  private pumpServerTransport(): void {
    const transport = this.serverTransport;
    if (!transport) return;
    for (let round = 0; round < 32; round += 1) {
      transport.tick();
      this.db.tick();
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

  private flushQueuedServerFrames(carrier: DirectWebSocketCarrier): void {
    if (this.queuedServerFrames.length === 0 || carrier !== this.serverCarrier) return;
    const frames = this.queuedServerFrames.splice(0);
    void carrier.sendBatch(frames).catch((error) => {
      this.handleServerTransportError(error);
    });
  }

  private handleServerTransportError(error: unknown): void {
    this.authFailureCallback?.(errorMessage(error));
  }
}

function normalizeTransportFrames(frames: unknown[]): Uint8Array[] {
  return frames.filter(
    (frame): frame is Uint8Array =>
      ArrayBuffer.isView(frame) && frame.constructor.name === "Uint8Array",
  );
}

function writeId(write: DirectWrite, writes: Map<string, DirectWrite>): string {
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

function identityFromWriteContext(writeContext?: string | null): Uint8Array | undefined {
  if (!writeContext) return undefined;
  try {
    const parsed = JSON.parse(writeContext) as {
      user_id?: unknown;
      attribution?: unknown;
      session?: { user_id?: unknown };
    };
    const userId =
      typeof parsed.user_id === "string"
        ? parsed.user_id
        : typeof parsed.session?.user_id === "string"
          ? parsed.session.user_id
          : parsed.attribution === SYSTEM_AUTHOR_ID
            ? SYSTEM_AUTHOR_ID
            : undefined;
    return userId ? parseUuid(userId) : undefined;
  } catch {
    return undefined;
  }
}

function assertNoSessionWriteInTx(writeIdentity: Uint8Array | undefined): void {
  if (!writeIdentity) return;
  throw new Error(
    "Direct core runtime cannot perform session-scoped transaction writes: " +
      "the direct core mergeable transaction API has no identity-aware staging methods.",
  );
}

function readOptions(tier?: string | null, includeDeleted = false): unknown {
  return includeDeleted
    ? { tier: tier ?? "local", include_deleted: true }
    : { tier: tier ?? "local" };
}

function assertSupportedReadOptions(tier?: string | null, optionsJson?: string | null): void {
  if (tier != null && !["local", "edge", "global"].includes(tier)) {
    throw new Error(`Direct core runtime received unsupported read tier '${tier}'`);
  }
  if (optionsJson != null) readSupportedReadOptions(optionsJson);
}

function readSession(sessionJson?: string | null): { user_id: string } | null {
  if (sessionJson == null) return null;
  const parsed = JSON.parse(sessionJson) as { user_id?: unknown };
  if (typeof parsed.user_id !== "string") {
    throw new Error("Direct core runtime session is missing user_id");
  }
  return { user_id: parsed.user_id };
}

function readSupportedReadOptions(optionsJson: string): void {
  const parsed = JSON.parse(optionsJson) as Record<string, unknown>;
  const propagation = parsed.propagation;
  if (propagation != null && propagation !== "full") {
    throw new Error(
      `Direct core runtime does not support read propagation '${String(propagation)}' yet`,
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

function isNotObservedWaitError(error: unknown): boolean {
  return errorMessage(error).includes("NotObserved");
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

function directWriteOrThrow<T>(operation: "Insert" | "Update" | "Delete", write: () => T): T {
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
    table?: unknown;
    limit?: unknown;
    relation_ir?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Direct core runtime only supports table queries in this slice");
  }
  const encoded = encodeSimpleRelationQuery(parsed.table, parsed.relation_ir, schema);
  if (encoded) {
    return queryWithPredicates(
      parsed.table,
      encoded.predicates,
      encoded.hasPostFilter ? undefined : readLimitIfPresent(parsed.limit),
    );
  }
  if (parsed.limit != null) {
    return queryWithEqFilters(parsed.table, [], readLimit(parsed.limit));
  }
  return queryFromTable(parsed.table);
}

function encodeSimpleRelationQuery(
  table: string,
  relationIr: unknown,
  schema: WasmSchema,
): { predicates: DirectQueryPredicate[]; hasPostFilter: boolean } | null {
  const unwrapped = unwrapSimpleRelation(table, relationIr);
  if (!unwrapped) return null;
  const unsupportedIdComparator = unwrapped.predicates.find(
    (filter) => filter.column === "id" && filter.op !== "Eq",
  );
  if (unsupportedIdComparator) {
    throw new Error(
      `Direct core runtime does not support '${unsupportedIdComparator.op}' comparisons on id yet`,
    );
  }
  const hasPostFilter = unwrapped.predicates.some((filter) => filter.column === "id");
  return {
    hasPostFilter,
    predicates: unwrapped.predicates
      .filter((filter) => filter.column !== "id")
      .map((filter) => ({
        ...filter,
        value: coerceQueryLiteral(table, filter.column, filter.value, schema),
      })),
  };
}

function coerceQueryLiteral(
  table: string,
  column: string,
  value: DirectQueryLiteral,
  schema: WasmSchema,
): DirectQueryLiteral {
  const columnType =
    column === "id"
      ? { type: "Uuid" }
      : schema[table]?.columns.find((entry) => entry.name === column)?.column_type;
  const coerced =
    columnType?.type === "Uuid" && value.type === "Text" && isUuidString(value.value)
      ? { type: "Uuid" as const, value: value.value }
      : value;
  const nullable =
    column !== "id" && schema[table]?.columns.find((entry) => entry.name === column)?.nullable;
  if (nullable && coerced.type !== "Nullable") {
    return { type: "Nullable", value: coerced };
  }
  return coerced;
}

function unwrapSimpleRelation(
  table: string,
  relationIr: unknown,
): { predicates: DirectQueryPredicate[] } | null {
  if (!relationIr || typeof relationIr !== "object") return { predicates: [] };
  const relation = relationIr as Record<string, unknown>;
  const tableScan = relation.TableScan;
  if (
    tableScan &&
    typeof tableScan === "object" &&
    (tableScan as { table?: unknown }).table === table
  ) {
    return { predicates: [] };
  }
  const limit = relation.Limit;
  if (limit && typeof limit === "object") {
    const limitRecord = limit as { input?: unknown };
    return unwrapSimpleRelation(table, limitRecord.input);
  }
  const filter = relation.Filter;
  if (!filter || typeof filter !== "object") return null;
  const filterRecord = filter as { input?: unknown; predicate?: unknown };
  const input = unwrapSimpleRelation(table, filterRecord.input);
  if (!input) return null;
  const predicates = predicateToFilters(filterRecord.predicate);
  return predicates ? { predicates: input.predicates.concat(predicates) } : null;
}

function predicateToFilters(predicate: unknown): DirectQueryPredicate[] | null {
  if (predicate === "True") return [];
  if (!predicate || typeof predicate !== "object") return null;
  const record = predicate as Record<string, unknown>;
  if (Array.isArray(record.And)) {
    const filters: DirectQueryPredicate[] = [];
    for (const child of record.And) {
      const childFilters = predicateToFilters(child);
      if (!childFilters) return null;
      filters.push(...childFilters);
    }
    return filters;
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

function readPredicateOp(value: unknown): DirectQueryPredicate["op"] | null {
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

function readLiteral(value: unknown): DirectQueryLiteral | null {
  if (!value || typeof value !== "object" || !("Literal" in value)) return null;
  const literal = (value as { Literal?: unknown }).Literal;
  if (!literal || typeof literal !== "object") return null;
  const record = literal as { type?: unknown; value?: unknown };
  if (record.type === "Boolean" && typeof record.value === "boolean") {
    return { type: "Boolean", value: record.value };
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

function queryFiltersFromJson(queryJson: string, schema: WasmSchema): RowFilter[] {
  const parsed = JSON.parse(queryJson) as { table?: unknown; relation_ir?: unknown };
  if (typeof parsed.table !== "string") return [];
  return (
    unwrapSimpleRelation(parsed.table, parsed.relation_ir)
      ?.predicates.filter((filter) => filter.op === "Eq")
      .map((filter) => ({
        ...filter,
        value: coerceQueryLiteral(parsed.table as string, filter.column, filter.value, schema),
      })) ?? []
  );
}

function filterRows(rows: RowState[], filters: RowFilter[], schema: WasmSchema): RowState[] {
  if (filters.length === 0) return rows;
  return rows.filter((row) => filters.every((filter) => rowMatchesFilter(row, filter, schema)));
}

function rowMatchesFilter(row: RowState, filter: RowFilter, schema: WasmSchema): boolean {
  if (filter.column === "id") {
    return literalString(filter.value) === row.id;
  }
  const index =
    schema[row.table]?.columns.findIndex((column) => column.name === filter.column) ?? -1;
  if (index < 0) return false;
  return valueMatchesLiteral(row.values[index], filter.value);
}

function valueMatchesLiteral(value: Value | undefined, literal: DirectQueryLiteral): boolean {
  if (literal.type === "Nullable") {
    if (literal.value == null) return !value || value.type === "Null";
    return valueMatchesLiteral(value, literal.value);
  }
  if (!value || value.type === "Null") return false;
  if (literal.type === "Boolean") {
    return value.type === "Boolean" && value.value === literal.value;
  }
  const expected = literalString(literal);
  return (
    (value.type === "Text" || value.type === "Uuid") &&
    typeof value.value === "string" &&
    value.value === expected
  );
}

function literalString(literal: DirectQueryLiteral): string | null {
  if (literal.type === "Nullable") return literal.value ? literalString(literal.value) : null;
  if (literal.type === "Text" || literal.type === "Uuid") return literal.value;
  return null;
}

function isUuidString(value: string): boolean {
  return /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
    value,
  );
}

function encodeCellsForRow(
  definition: { columns: ColumnDescriptor[]; policies?: TablePolicies },
  row: InsertValues,
): Uint8Array {
  return encodeCells(definition.columns, (column) => row[column.name], true);
}

function encodeCellsForPatch(
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
      view.setUint32(0, expectNumber(value, "Integer"), true);
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
      throw new Error(`Direct core runtime does not encode ${type.type} values yet`);
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
  return width == null ? Uint8Array.of(0) : new Uint8Array(width + 1);
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

function expectString(value: Value, type: string): string {
  if ((value.type === "Text" || value.type === "Uuid") && typeof value.value === "string") {
    return value.value;
  }
  throw new Error(`expected ${type} value`);
}

function readRowBatches(payload: Uint8Array): AbiRowBatch[] {
  return new PostcardReader(payload).readVec(readAbiRowBatch);
}

function rowsFromBatches(batches: AbiRowBatch[], schema: WasmSchema): RowState[] {
  return batches.flatMap((batch) =>
    batch.rows.map((row) => ({
      table: batch.table,
      id: formatUuid(row.rowId),
      values: batch.descriptor
        .map((field, index) => ({ field, index }))
        .filter(({ field }) => field.name && !isInternalField(field.name))
        .map(({ field, index }) =>
          decodeField(batch.table, field, batch.descriptor, row.raw, index, schema),
        ),
    })),
  );
}

function applySubscriptionDelta(
  currentRows: RowState[],
  delta: { added: AbiRowBatch[]; updated: AbiRowBatch[]; removed: AbiRemovedRow[] },
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
      return { type: "Integer", value: view.getUint32(0, true) };
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
  | { type: "snapshot"; rows: AbiRowBatch[] }
  | {
      type: "delta";
      delta: { added: AbiRowBatch[]; updated: AbiRowBatch[]; removed: AbiRemovedRow[] };
    }
  | { type: "closed" } {
  if (!chunk || typeof chunk !== "object") throw new Error("expected subscription chunk");
  const record = chunk as { type?: unknown; rows?: unknown; delta?: unknown };
  if (record.type === "closed" || record.type === "Closed") {
    return { type: "closed" };
  }
  if (record.type === "snapshot" || record.type === "Snapshot") {
    return {
      type: "snapshot",
      rows: readRowBatches(assertBytes(record.rows, "subscription rows")),
    };
  }
  if (record.type === "delta" || record.type === "Delta") {
    return {
      type: "delta",
      delta: readAbiSubscriptionDelta(
        new PostcardReader(assertBytes(record.delta, "subscription delta")),
      ),
    };
  }
  throw new Error("unknown subscription chunk");
}

function subscriptionSource(
  subscription: ReadableStream<unknown> | DirectSubscription,
): ReadableStreamDefaultReader<unknown> | DirectSubscription {
  const maybeReadable = subscription as Partial<ReadableStream<unknown>>;
  if (typeof maybeReadable.getReader === "function") {
    return maybeReadable.getReader();
  }
  return subscription as DirectSubscription;
}

function isReadableSubscriptionReader(
  source: ReadableStreamDefaultReader<unknown> | DirectSubscription,
): source is ReadableStreamDefaultReader<unknown> {
  return "read" in source && typeof source.read === "function";
}

function nativeDeltaFromRows(rows: RowState[]): SubscriptionWireDelta {
  return rows.map((row, index) => ({
    kind: 0,
    id: row.id,
    index,
    row: { id: row.id, values: row.values },
  }));
}

function parseUuid(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid uuid ${value}`);
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i += 1) {
    bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

function formatUuid(bytes: Uint8Array): string {
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

function assertBytes(value: unknown, label: string): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return Uint8Array.from(value);
  throw new Error(`expected ${label} bytes`);
}
