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
import { analyzeRelations, type Relation } from "../../codegen/relation-analyzer.js";
import type {
  DirectInsertResult,
  DirectMutationResult,
  Runtime,
  TransactionKind,
} from "../client.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";
import {
  PostcardReader,
  PostcardWriter,
  openConfig,
  queryWithPredicates,
  readAbiRowBatch,
  readAbiSubscriptionDelta,
  writeValueType,
  type AbiRowBatch,
  type AbiRemovedRow,
  type DirectQueryOrder,
  type DirectQueryLiteral,
  type DirectQueryPredicate,
  type DirectQueryPredicateOp,
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
import { HIDDEN_INCLUDE_COLUMN_PREFIX } from "../select-projection.js";

export { encodeDirectSchema } from "./direct-schema-codec.js";

type CoreDbConstructor = {
  openMemory(schema: Uint8Array, config: Uint8Array): CoreDb;
  openPersistent?(dataPath: string, schema: Uint8Array, config: Uint8Array): CoreDb;
};

type CoreDb = {
  all(query: DirectPreparedQuery, opts: unknown): Uint8Array;
  allForIdentity(query: DirectPreparedQuery, author: Uint8Array, opts: unknown): Uint8Array;
  propagateQuery?(query: DirectPreparedQuery, opts: unknown): void;
  queryIsCovered?(query: DirectPreparedQuery): boolean;
  prepareQuery(query: Uint8Array): DirectPreparedQuery;
  subscribe?(
    query: DirectPreparedQuery,
    opts: unknown,
  ): ReadableStream<unknown> | DirectSubscription;
  subscribeForIdentity?(
    query: DirectPreparedQuery,
    author: Uint8Array,
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
  mergeableTxForIdentity?(author: Uint8Array): DirectTx;
  exclusiveTx?(): DirectTx;
  setTickScheduler(
    callback:
      | ((urgency: "immediate" | "deferred") => void)
      | ((error: Error | null, urgency: string) => void),
  ): void;
  connectUpstream(): DirectTransport;
  tick(): void;
  close?(): void;
  free?(): void;
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
  writeState(): unknown;
  nextWriteStateChange(): Promise<void>;
  close?(): boolean;
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

type PendingTx = {
  kind: TransactionKind;
  tx?: DirectTx;
  identity?: Uint8Array;
  writes: Array<{ table: string; rowId: Uint8Array }>;
};

type CompletedTx = {
  kind: TransactionKind;
  state: "committed" | "rolled_back";
};

type SubscriptionState = {
  sources: SubscriptionSourceState[];
  rows: RowState[];
  filters: RowFilter[];
  callback?: Function;
  cancelled: boolean;
};

type SubscriptionSourceState = {
  source: ReadableStreamDefaultReader<unknown> | DirectSubscription;
  reading: boolean;
};

type RowFilter = DirectQueryPredicate;

type RowState = {
  table: string;
  id: string;
  values: Value[];
  valuesByColumn?: Map<string, Value>;
};

type RuntimeQueryJson = {
  table?: unknown;
  relation_ir?: unknown;
  array_subqueries?: unknown;
};

type RuntimeArraySubquery = {
  column_name: string;
  table: string;
  inner_column: string;
  outer_column: string;
  select_columns?: string[] | null;
  nested_arrays?: unknown;
};

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function openPersistentDirectDb(
  Runtime: CoreDbConstructor,
  dataPath: string,
  schema: Uint8Array,
  config: Uint8Array,
): CoreDb {
  if (!Runtime.openPersistent) {
    throw new Error("Direct core runtime does not expose persistent storage");
  }
  return Runtime.openPersistent(dataPath, schema, config);
}

export class CoreRuntime implements Runtime {
  private readonly db: CoreDb;
  private readonly schemaBytes: Uint8Array;
  private readonly configBytes: Uint8Array;
  private readonly peerIdentity: Uint8Array;
  private readonly schemaHash: string;
  private readonly preparedQueries = new Map<string, DirectPreparedQuery>();
  private readonly pendingTxs = new Map<string, PendingTx>();
  private readonly completedTxs = new Map<string, CompletedTx>();
  private readonly writes = new Map<string, DirectWrite>();
  private readonly subscriptions = new Map<number, SubscriptionState>();
  private authFailureCallback: ((reason: string) => void) | null = null;
  private serverTransport: DirectTransport | null = null;
  private serverCarrier: DirectWebSocketCarrier | null = null;
  private serverCarrierPromise: Promise<DirectWebSocketCarrier> | null = null;
  private serverEndpointUrl: string | null = null;
  private readonly queuedServerFrames: Uint8Array[] = [];
  private serverPumpScheduled = false;
  private serverPumpAgain = false;
  private closed = false;
  private nextTransactionId = 1;
  private nextSubscriptionId = 1;

  static fromDb(
    db: CoreDb,
    schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
  ): CoreRuntime {
    return new CoreRuntime(null, schema, node, author, sourceId, historyComplete, { db });
  }

  constructor(
    Runtime: CoreDbConstructor | null,
    private readonly schema: WasmSchema,
    node: Uint8Array,
    author: Uint8Array,
    sourceId: number,
    historyComplete: boolean,
    opts?: { persistentPath?: string; db?: CoreDb },
  ) {
    this.schemaBytes = encodeDirectSchema(schema);
    this.configBytes = openConfig(node, author, sourceId, historyComplete);
    this.peerIdentity = author;
    this.schemaHash = serializeRuntimeSchema(schema);
    if (opts?.db) {
      this.db = opts.db;
    } else if (opts?.persistentPath) {
      if (!Runtime) {
        throw new Error("Direct core runtime constructor required for persistent storage");
      }
      this.db = openPersistentDirectDb(
        Runtime,
        opts.persistentPath,
        this.schemaBytes,
        this.configBytes,
      );
    } else {
      if (!Runtime) {
        throw new Error("Direct core runtime constructor required for memory storage");
      }
      this.db = Runtime.openMemory(this.schemaBytes, this.configBytes);
    }
    if (typeof this.db.setTickScheduler !== "function") {
      throw new Error("Direct core runtime requires db.setTickScheduler");
    }
    this.db.setTickScheduler(((first: Error | string | null, second?: string) => {
      const urgency = typeof first === "string" ? first : second;
      if (urgency === "immediate" || urgency === "deferred") {
        this.scheduleCoreWake(urgency);
      }
    }) as (error: Error | null, urgency: string) => void);
  }

  connectUpstreamPeer(): DirectTransport {
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
  ): DirectInsertResult {
    const rowId = objectId ? parseUuid(objectId) : crypto.getRandomValues(new Uint8Array(16));
    const cells = encodeCellsForRow(this.table(table), values);
    const writeIdentity = identityFromWriteContext(_writeContext);
    const tx = this.currentTx(_writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).insertWithIdEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(_writeContext) ?? "", writeIdentity);
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
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).restoreEncoded(table, rowId, cells);
      tx.writes.push({ table, rowId });
      return this.resultForRow(table, rowId, txIdFromContext(writeContext) ?? "", writeIdentity);
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
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).updateEncoded(table, rowId, patch);
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
    const tx = this.currentTx(writeContext, "Insert");
    if (tx) {
      this.txForWrite(tx, writeIdentity).upsertEncoded(table, rowId, cells);
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
    const tx = this.currentTx(writeContext, "Delete");
    if (tx) {
      this.txForWrite(tx, writeIdentity).delete(table, rowId);
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
  }

  async waitForTransaction(transactionId: string, tier: string): Promise<void> {
    const write = this.writes.get(transactionId);
    if (!write) return;
    for (;;) {
      try {
        this.pumpServerTransport();
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
          write.wait(tier);
          this.pumpSubscriptions();
          return;
        } catch (secondError) {
          const secondRejected = rejectedWaitError(transactionId, secondError);
          if (secondRejected) throw secondRejected;
          if (!isPendingWaitError(secondError)) throw secondError;
        }
        await change;
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
    const relationRows = await this.queryRelationShape(queryJson, sessionJson, tier, optionsJson);
    if (relationRows) return relationRows;
    const query = this.prepareQuery(queryJson);
    const session = readSession(sessionJson);
    const opts = readOptions(tier, queryIncludesDeleted(queryJson));
    await this.propagateQueryIfNeeded(tier, optionsJson, query);
    const rows = session
      ? this.db.allForIdentity(query, parseUuid(session.user_id), opts)
      : this.db.all(query, opts);
    return filterRows(
      rowsFromBatches(readRowBatches(rows), this.schema),
      queryFiltersFromJson(queryJson, this.schema),
      this.schema,
    );
  }

  private async queryRelationShape(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[] | null> {
    const parsed = JSON.parse(queryJson) as RuntimeQueryJson;
    if (typeof parsed.table !== "string") return null;
    const hasArraySubqueries =
      Array.isArray(parsed.array_subqueries) && parsed.array_subqueries.length > 0;
    const relation = parsed.relation_ir;
    const relationKind = relationKindOf(relation);
    if (!hasArraySubqueries && relationKind !== "Project" && relationKind !== "Gather") return null;

    const session = readSession(sessionJson);
    const identity = session ? parseUuid(session.user_id) : undefined;
    let rows =
      relationKind === "Project" || relationKind === "Gather"
        ? await this.evaluateRelation(relation, identity, tier, optionsJson)
        : await this.queryPreparedRows(queryJson, identity, tier, optionsJson);

    if (hasArraySubqueries) {
      rows = await this.attachArraySubqueries(
        rows,
        parsed.array_subqueries,
        identity,
        tier,
        optionsJson,
      );
    }
    return rows;
  }

  private async evaluateRelation(
    relation: unknown,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    if (!relation || typeof relation !== "object") throw unsupportedRelationQueryError();
    const record = relation as Record<string, unknown>;
    if (record.Project && typeof record.Project === "object") {
      return this.evaluateProject(record.Project, identity, tier, optionsJson);
    }
    if (record.Gather && typeof record.Gather === "object") {
      return this.evaluateGather(record.Gather, identity, tier, optionsJson);
    }
    return await this.queryPreparedRows(
      JSON.stringify({ table: tableFromRelation(relation), relation_ir: relation }),
      identity,
      tier,
      optionsJson,
    );
  }

  private async evaluateProject(
    project: unknown,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    const input = (project as { input?: unknown }).input;
    const chain = readJoinChain(input);
    if (!chain || chain.hops.length === 0) throw unsupportedRelationQueryError();
    let rows = await this.evaluateRelation(chain.seed, identity, tier, optionsJson);
    const relations = analyzeRelations(this.schema);
    let currentTable = rows[0]?.table ?? tableFromRelation(chain.seed);
    for (const hop of chain.hops) {
      const relation = relationForTables(relations, currentTable, hop.table, hop.on);
      if (!relation) throw unsupportedRelationQueryError();
      rows = await this.followRelation(rows, relation, identity, tier, optionsJson);
      currentTable = relation.toTable;
    }
    return rows;
  }

  private async evaluateGather(
    gather: unknown,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    const record = gather as { seed?: unknown; step?: unknown; max_depth?: unknown };
    if (typeof record.max_depth !== "number") throw unsupportedRelationQueryError();
    const seedRows = await this.evaluateRelation(record.seed, identity, tier, optionsJson);
    const chain = readJoinChain((record.step as { Project?: { input?: unknown } })?.Project?.input);
    if (!chain || chain.hops.length !== 1) throw unsupportedRelationQueryError();
    const stepTable = tableFromRelation(chain.seed);
    const relation = relationForTables(
      analyzeRelations(this.schema),
      stepTable,
      chain.hops[0]!.table,
      chain.hops[0]!.on,
    );
    if (!relation || relation.type !== "forward") throw unsupportedRelationQueryError();
    const byKey = new Map(seedRows.map((row) => [rowKey(row.table, row.id), row]));
    let frontier = seedRows;
    for (let depth = 0; depth < record.max_depth && frontier.length > 0; depth += 1) {
      const next = (
        await this.followRelation(frontier, relation, identity, tier, optionsJson)
      ).filter((row) => !byKey.has(rowKey(row.table, row.id)));
      for (const row of next) byKey.set(rowKey(row.table, row.id), row);
      frontier = next;
    }
    return Array.from(byKey.values());
  }

  private async followRelation(
    rows: RowState[],
    relation: Relation,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    const ids = uniqueStrings(
      rows.flatMap((row) => relationValues(row, relation.fromColumn, this.schema)),
    );
    if (ids.length === 0) return [];
    const query =
      relation.toColumn === "id"
        ? { table: relation.toTable, conditions: [{ column: "id", op: "in", value: ids }] }
        : {
            table: relation.toTable,
            conditions: [{ column: relation.toColumn, op: "in", value: ids }],
          };
    return this.queryPreparedRows(JSON.stringify(query), identity, tier, optionsJson);
  }

  private async attachArraySubqueries(
    rows: RowState[],
    subqueries: unknown,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    if (!Array.isArray(subqueries)) return rows;
    let result = rows;
    for (const raw of subqueries) {
      const subquery = readRuntimeArraySubquery(raw);
      const outerColumn = subquery.outer_column.split(".").at(-1)!;
      const next: RowState[] = [];
      for (const row of result) {
        const values = relationValues(row, outerColumn, this.schema);
        const select = directSelectForArraySubquery(subquery);
        let included =
          values.length === 0
            ? []
            : await this.queryPreparedRows(
                JSON.stringify({
                  table: subquery.table,
                  conditions: [{ column: subquery.inner_column, op: "in", value: values }],
                  select: select?.columns,
                }),
                identity,
                tier,
                optionsJson,
              );
        if (select) {
          included = projectRowsForDirectSelect(included, select.columns, select.publicColumns);
        }
        included = await this.attachArraySubqueries(
          included,
          subquery.nested_arrays,
          identity,
          tier,
          optionsJson,
        );
        next.push({
          ...row,
          values: row.values.concat({
            type: "Array",
            value: included.map((child) => ({
              type: "Row",
              value: { id: child.id, values: child.values },
            })),
          } as Value),
        });
      }
      result = next;
    }
    return result;
  }

  private async queryPreparedRows(
    queryJson: string,
    identity: Uint8Array | undefined,
    tier?: string | null,
    optionsJson?: string | null,
  ): Promise<RowState[]> {
    const query = this.prepareQuery(queryJson);
    await this.propagateQueryIfNeeded(tier, optionsJson, query);
    const rows = identity
      ? this.db.allForIdentity(query, identity, readOptions(tier, queryIncludesDeleted(queryJson)))
      : this.db.all(query, readOptions(tier, queryIncludesDeleted(queryJson)));
    return filterRows(
      rowsFromBatches(readRowBatches(rows), this.schema),
      queryFiltersFromJson(queryJson, this.schema),
      this.schema,
    );
  }

  createSubscription(
    queryJson: string,
    sessionJson?: string | null,
    tier?: string | null,
    optionsJson?: string | null,
  ): number {
    assertSupportedReadOptions(tier, optionsJson);
    const session = readSession(sessionJson);
    if (!this.db.subscribe) {
      throw new Error("Direct core runtime does not support subscriptions");
    }
    if (session && !this.db.subscribeForIdentity) {
      throw new Error("Direct core runtime does not support session-scoped subscriptions");
    }
    const handle = this.nextSubscriptionId++;
    const opts = readOptions(tier);
    const identity = session ? parseUuid(session.user_id) : undefined;
    const query = this.prepareQuery(queryJson);
    let nativeSubscription: ReadableStream<unknown> | DirectSubscription;
    try {
      nativeSubscription = identity
        ? this.db.subscribeForIdentity!(query, identity, opts)
        : this.db.subscribe!(query, opts);
    } catch (error) {
      throw new Error(`Direct core subscribe failed for ${queryJson}: ${errorMessage(error)}`);
    }
    try {
      this.propagateSubscriptionQueryIfNeeded(tier, optionsJson, query);
    } catch (error) {
      throw new Error(
        `Direct core subscription propagation failed for ${queryJson}: ${errorMessage(error)}`,
      );
    }
    this.subscriptions.set(handle, {
      sources: [{ source: subscriptionSource(nativeSubscription), reading: false }],
      rows: [],
      filters: queryFiltersFromJson(queryJson, this.schema),
      cancelled: false,
    });
    return handle;
  }

  executeSubscription(handle: number, onUpdate: Function): void {
    const subscription = this.subscriptions.get(handle);
    if (!subscription) return;
    subscription.callback = onUpdate;
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

  private finishInsert(
    table: string,
    rowId: Uint8Array,
    write: DirectWrite,
    identity?: Uint8Array,
  ): DirectInsertResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
    return this.resultForRow(table, rowId, transactionId, identity);
  }

  private finishMutation(write: DirectWrite): DirectMutationResult {
    const transactionId = writeId(write, this.writes);
    this.pumpSubscriptions();
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
    const queryBytes = encodeQueryJson(queryJson, this.schema);
    const key = bytesKey(queryBytes);
    let query = this.preparedQueries.get(key);
    if (!query) {
      try {
        query = this.db.prepareQuery(queryBytes);
      } catch (error) {
        throw new Error(`Direct core prepareQuery failed for ${queryJson}: ${errorMessage(error)}`);
      }
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

  private txForWrite(pending: PendingTx, identity: Uint8Array | undefined): DirectTx {
    if (pending.kind === "exclusive") {
      if (identity) {
        throw new Error(
          "Direct core runtime cannot perform session-scoped exclusive transaction writes: " +
            "the core runtime exclusive transaction API has no identity-aware staging methods.",
        );
      }
      if (!pending.tx) {
        pending.tx = this.exclusiveTx();
      }
      return pending.tx;
    }
    if (pending.identity && (!identity || !sameBytes(pending.identity, identity))) {
      throw new Error("Direct core runtime mergeable transaction cannot mix write identities");
    }
    if (identity && pending.tx && !pending.identity) {
      throw new Error("Direct core runtime mergeable transaction cannot mix write identities");
    }
    if (!pending.tx) {
      pending.identity = identity;
      pending.tx = identity ? this.mergeableTxForIdentity(identity) : this.db.mergeableTx();
    }
    return pending.tx;
  }

  private txForKind(kind: TransactionKind): DirectTx {
    return kind === "exclusive" ? this.exclusiveTx() : this.db.mergeableTx();
  }

  private exclusiveTx(): DirectTx {
    if (!this.db.exclusiveTx) {
      throw new Error(
        "Direct core runtime cannot perform exclusive transaction writes: " +
          "the core runtime exclusive transaction API is unavailable.",
      );
    }
    return this.db.exclusiveTx();
  }

  private mergeableTxForIdentity(identity: Uint8Array): DirectTx {
    if (!this.db.mergeableTxForIdentity) {
      throw new Error(
        "Direct core runtime cannot perform session-scoped transaction writes: " +
          "the core runtime mergeable transaction API has no identity-aware staging methods.",
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
      this.pumpSubscriptions();
      this.scheduleServerPump();
      return;
    }
    queueMicrotask(() => {
      this.pumpSubscriptions();
      this.scheduleServerPump();
    });
  }

  private startSubscriptionReader(handle: number, subscription: SubscriptionState): void {
    if (subscription.cancelled || !subscription.callback) return;
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
          console.error("Direct core subscription failed", error);
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
        console.error("Direct core subscription failed", error);
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
      subscription.rows = filterRows(
        rowsFromBatches(chunk.rows, this.schema),
        subscription.filters,
        this.schema,
      );
    } else {
      subscription.rows = applySubscriptionDelta(subscription.rows, chunk.delta, this.schema);
      subscription.rows = filterRows(subscription.rows, subscription.filters, this.schema);
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

function isPendingWaitError(error: unknown): boolean {
  const message = errorMessage(error);
  return (
    message.includes("NotObserved") ||
    message.includes("has not been accepted at requested tier") ||
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
    conditions?: unknown;
    offset?: unknown;
    orderBy?: unknown;
    select?: unknown;
  };
  if (typeof parsed.table !== "string") {
    throw new Error("Direct core runtime only supports table queries in this slice");
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
          select: readSelectColumns(parsed.select),
        },
  );
}

function unsupportedRelationQueryError(): Error {
  return new Error(
    "Direct core runtime does not support this relation query shape yet; refusing to run an overbroad table query.",
  );
}

function relationKindOf(relation: unknown): string | null {
  if (!relation || typeof relation !== "object") return null;
  const record = relation as Record<string, unknown>;
  return Object.keys(record)[0] ?? null;
}

function readRuntimeArraySubquery(raw: unknown): RuntimeArraySubquery {
  if (!raw || typeof raw !== "object") throw unsupportedRelationQueryError();
  const subquery = raw as {
    column_name?: unknown;
    table?: unknown;
    inner_column?: unknown;
    outer_column?: unknown;
    select_columns?: unknown;
    nested_arrays?: unknown;
  };
  if (
    typeof subquery.column_name !== "string" ||
    typeof subquery.table !== "string" ||
    typeof subquery.inner_column !== "string" ||
    typeof subquery.outer_column !== "string"
  ) {
    throw unsupportedRelationQueryError();
  }
  return {
    column_name: subquery.column_name,
    table: subquery.table,
    inner_column: subquery.inner_column,
    outer_column: subquery.outer_column,
    select_columns: readRuntimeArraySubquerySelect(subquery.select_columns),
    nested_arrays: subquery.nested_arrays,
  };
}

function readRuntimeArraySubquerySelect(selectColumns: unknown): string[] | null | undefined {
  if (selectColumns == null) return selectColumns;
  if (!Array.isArray(selectColumns)) throw unsupportedRelationQueryError();
  if (!selectColumns.every((column): column is string => typeof column === "string")) {
    throw unsupportedRelationQueryError();
  }
  return selectColumns;
}

function directSelectForArraySubquery(
  subquery: RuntimeArraySubquery,
): { columns: string[]; publicColumns: string[] } | undefined {
  if (subquery.select_columns == null) return undefined;
  const nestedArrays = Array.isArray(subquery.nested_arrays) ? subquery.nested_arrays : [];
  const columns: string[] = [];
  const publicColumns: string[] = [];

  if (subquery.inner_column !== "id") {
    columns.push(subquery.inner_column);
  }

  for (const column of subquery.select_columns) {
    if (!isHiddenIncludeColumn(column)) {
      if (!columns.includes(column)) columns.push(column);
      publicColumns.push(column);
      continue;
    }
    const nested = nestedArrays
      .map((raw) => {
        try {
          return readRuntimeArraySubquery(raw);
        } catch {
          return null;
        }
      })
      .find((candidate) => candidate?.column_name === column);
    const carrierColumn = nested?.outer_column.split(".").at(-1);
    if (!carrierColumn || carrierColumn === "id" || columns.includes(carrierColumn)) {
      continue;
    }
    columns.push(carrierColumn);
  }

  return { columns, publicColumns };
}

function tableFromRelation(relation: unknown): string {
  if (!relation || typeof relation !== "object") throw unsupportedRelationQueryError();
  const record = relation as Record<string, unknown>;
  const scan = record.TableScan;
  if (scan && typeof scan === "object" && typeof (scan as { table?: unknown }).table === "string") {
    return (scan as { table: string }).table;
  }
  const filter = record.Filter;
  if (filter && typeof filter === "object")
    return tableFromRelation((filter as { input?: unknown }).input);
  const limit = record.Limit;
  if (limit && typeof limit === "object")
    return tableFromRelation((limit as { input?: unknown }).input);
  const offset = record.Offset;
  if (offset && typeof offset === "object")
    return tableFromRelation((offset as { input?: unknown }).input);
  const orderBy = record.OrderBy;
  if (orderBy && typeof orderBy === "object")
    return tableFromRelation((orderBy as { input?: unknown }).input);
  throw unsupportedRelationQueryError();
}

function readJoinChain(relation: unknown): {
  seed: unknown;
  hops: Array<{ table: string; on: Array<{ left: string; right: string }> }>;
} | null {
  const hops: Array<{ table: string; on: Array<{ left: string; right: string }> }> = [];
  let current = relation;
  while (current && typeof current === "object" && "Join" in current) {
    const join = (current as { Join?: unknown }).Join as
      | { left?: unknown; right?: unknown; on?: unknown }
      | undefined;
    if (!join || !Array.isArray(join.on)) return null;
    const table = tableFromRelation(join.right);
    const on = join.on.map((entry) => {
      const record = entry as { left?: unknown; right?: unknown };
      const left = readColumnRef(record.left);
      const right = readColumnRef(record.right);
      return left && right ? { left, right } : null;
    });
    if (!on.every((entry): entry is { left: string; right: string } => entry != null)) return null;
    hops.unshift({ table, on });
    current = join.left;
  }
  return current ? { seed: current, hops } : null;
}

function relationForTables(
  relations: Map<string, Relation[]>,
  fromTable: string,
  toTable: string,
  on: Array<{ left: string; right: string }>,
): Relation | undefined {
  return (relations.get(fromTable) ?? []).find((relation) => {
    if (relation.toTable !== toTable || on.length !== 1) return false;
    const join = on[0]!;
    return relation.type === "forward"
      ? join.left === relation.fromColumn && join.right === "id"
      : join.left === "id" && join.right === relation.toColumn;
  });
}

function relationValues(row: RowState, column: string, schema: WasmSchema): string[] {
  if (column === "id") return [row.id];
  const value = row.valuesByColumn?.get(column) ?? rowValue(row, column, schema);
  if (!value || value.type === "Null") return [];
  if (value.type === "Uuid" || value.type === "Text") return [value.value];
  if (value.type === "Array") {
    return value.value.flatMap((entry) =>
      entry.type === "Uuid" || entry.type === "Text" ? [entry.value] : [],
    );
  }
  return [];
}

function projectRowsForDirectSelect(
  rows: RowState[],
  columns: readonly string[],
  publicColumns: readonly string[],
): RowState[] {
  const publicSet = new Set(publicColumns);
  return rows.map((row) => {
    const valuesByColumn =
      row.valuesByColumn ?? new Map(columns.map((column, index) => [column, row.values[index]!]));
    return withValuesByColumn(
      {
        ...row,
        values: columns.flatMap((column) => {
          if (!publicSet.has(column)) return [];
          const value = valuesByColumn.get(column);
          return value === undefined ? [] : [value];
        }),
      },
      valuesByColumn,
    );
  });
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values));
}

function encodeSimpleRelationQuery(
  table: string,
  query: {
    relation_ir?: unknown;
    conditions?: unknown;
    limit?: unknown;
    offset?: unknown;
    orderBy?: unknown;
  },
  schema: WasmSchema,
): {
  predicates: DirectQueryPredicate[];
  hasPostFilter: boolean;
  limit?: number;
  offset: number;
  orderBy: DirectQueryOrder[];
} {
  const unwrapped = unwrapSimpleQuery(table, query);
  if (!unwrapped) throw unsupportedRelationQueryError();
  const hasPostFilter = unwrapped.predicates.some((filter) => filter.column === "id");
  return {
    hasPostFilter,
    limit: unwrapped.limit,
    offset: unwrapped.offset,
    orderBy: unwrapped.orderBy,
    predicates: unwrapped.predicates
      .filter((filter) => filter.column !== "id")
      .map((filter) => coerceQueryPredicate(table, filter, schema)),
  };
}

function coerceQueryPredicate(
  table: string,
  filter: DirectQueryPredicate,
  schema: WasmSchema,
): DirectQueryPredicate {
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

function unwrapSimpleRelationOrThrow(
  table: string,
  query: {
    relation_ir?: unknown;
    conditions?: unknown;
    limit?: unknown;
    offset?: unknown;
    orderBy?: unknown;
  },
): { predicates: DirectQueryPredicate[]; offset: number; orderBy: DirectQueryOrder[] } {
  const unwrapped = unwrapSimpleQuery(table, query);
  if (!unwrapped) throw unsupportedRelationQueryError();
  return unwrapped;
}

function unwrapSimpleQuery(
  table: string,
  query: {
    relation_ir?: unknown;
    conditions?: unknown;
    limit?: unknown;
    offset?: unknown;
    orderBy?: unknown;
  },
): {
  predicates: DirectQueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: DirectQueryOrder[];
} | null {
  if (query.relation_ir != null) return unwrapSimpleRelation(table, query.relation_ir);
  const predicates = readLegacyConditions(query.conditions);
  const orderBy = readLegacyOrderBy(query.orderBy);
  if (!predicates || !orderBy) return null;
  return {
    predicates,
    limit: query.limit == null ? undefined : readLimit(query.limit),
    offset: query.offset == null ? 0 : readOffset(query.offset),
    orderBy,
  };
}

function unwrapSimpleRelation(
  table: string,
  relationIr: unknown,
): {
  predicates: DirectQueryPredicate[];
  limit?: number;
  offset: number;
  orderBy: DirectQueryOrder[];
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
  const filter = relation.Filter;
  if (!filter || typeof filter !== "object") return null;
  const filterRecord = filter as { input?: unknown; predicate?: unknown };
  const input = unwrapSimpleRelation(table, filterRecord.input);
  if (!input) return null;
  const predicates = predicateToFilters(filterRecord.predicate);
  return predicates ? { ...input, predicates: input.predicates.concat(predicates) } : null;
}

function readLegacyConditions(value: unknown): DirectQueryPredicate[] | null {
  if (value == null) return [];
  if (!Array.isArray(value)) return null;
  const predicates: DirectQueryPredicate[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== "object") return null;
    const condition = entry as { column?: unknown; op?: unknown; value?: unknown };
    if (typeof condition.column !== "string" || typeof condition.op !== "string") return null;
    switch (condition.op) {
      case "eq":
      case "ne":
      case "gt":
      case "gte":
      case "lt":
      case "lte": {
        const op = readLegacyPredicateOp(condition.op);
        const literal = literalFromPlainValue(condition.value);
        if (!op || !literal) return null;
        predicates.push({ column: condition.column, op, value: literal });
        break;
      }
      case "in": {
        if (!Array.isArray(condition.value)) return null;
        const values = condition.value.map(literalFromPlainValue);
        if (!values.every((literal): literal is DirectQueryLiteral => literal != null)) return null;
        predicates.push({ column: condition.column, op: "In", values });
        break;
      }
      case "contains": {
        const literal = literalFromPlainValue(condition.value);
        if (!literal) return null;
        predicates.push({ column: condition.column, op: "Contains", value: literal });
        break;
      }
      case "isNull":
        predicates.push({ column: condition.column, op: "IsNull" });
        break;
      case "isNotNull":
        predicates.push({ column: condition.column, op: "IsNotNull" });
        break;
      default:
        return null;
    }
  }
  return predicates;
}

function readLegacyOrderBy(value: unknown): DirectQueryOrder[] | null {
  if (value == null) return [];
  if (!Array.isArray(value)) return null;
  const terms: DirectQueryOrder[] = [];
  for (const entry of value) {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") return null;
    if (entry[1] !== "asc" && entry[1] !== "desc") return null;
    terms.push({ column: entry[0], direction: entry[1] === "asc" ? "Asc" : "Desc" });
  }
  return terms;
}

function readSelectColumns(value: unknown): string[] | undefined {
  if (value == null) return undefined;
  if (!Array.isArray(value)) throw unsupportedRelationQueryError();
  if (!value.every((column): column is string => typeof column === "string")) {
    throw unsupportedRelationQueryError();
  }
  return value;
}

function readLegacyPredicateOp(value: string): DirectQueryPredicateOp | null {
  switch (value) {
    case "eq":
      return "Eq";
    case "ne":
      return "Ne";
    case "gt":
      return "Gt";
    case "gte":
      return "Gte";
    case "lt":
      return "Lt";
    case "lte":
      return "Lte";
    default:
      return null;
  }
}

function literalFromPlainValue(value: unknown): DirectQueryLiteral | null {
  if (value == null) return { type: "Nullable", value: null };
  if (typeof value === "boolean") return { type: "Boolean", value };
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return { type: "Integer", value };
  }
  if (typeof value === "string") return { type: "Text", value };
  if (value instanceof Uint8Array) return { type: "Bytea", value };
  if (Array.isArray(value)) {
    const values = value.map(literalFromPlainValue);
    return values.every((literal): literal is DirectQueryLiteral => literal != null)
      ? { type: "Array", value: values }
      : null;
  }
  return null;
}

function readOrderByTerms(value: unknown): DirectQueryOrder[] | null {
  if (!Array.isArray(value)) return null;
  const terms: DirectQueryOrder[] = [];
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
  value: DirectQueryLiteral,
  schema: WasmSchema,
): DirectQueryLiteral {
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
  value: DirectQueryLiteral,
  columnType: ColumnType | undefined,
  allowNullable: boolean,
): DirectQueryLiteral {
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

function readByteLiteral(value: DirectQueryLiteral): number {
  if (value.type !== "Integer" || value.value < 0 || value.value > 255) {
    throw new Error("Bytea values must contain integers in range 0..255");
  }
  return value.value;
}

function predicateToFilters(predicate: unknown): DirectQueryPredicate[] | null {
  if (predicate === "True") return [];
  if (predicate === "False") return [{ column: "id", op: "In", values: [] }];
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
    return values.every((value): value is DirectQueryLiteral => value != null)
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
  return column && value ? [{ column, op: op as DirectQueryPredicateOp, value }] : null;
}

function readPredicateOp(value: unknown): DirectQueryPredicateOp | null {
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
    if (values.every((entry): entry is DirectQueryLiteral => entry != null)) {
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

function queryFiltersFromJson(queryJson: string, schema: WasmSchema): RowFilter[] {
  const parsed = JSON.parse(queryJson) as {
    table?: unknown;
    relation_ir?: unknown;
    conditions?: unknown;
    limit?: unknown;
    offset?: unknown;
    orderBy?: unknown;
  };
  if (typeof parsed.table !== "string") return [];
  return unwrapSimpleRelationOrThrow(parsed.table, parsed).predicates.map((filter) =>
    coerceQueryPredicate(parsed.table as string, filter, schema),
  );
}

function filterRows(rows: RowState[], filters: RowFilter[], schema: WasmSchema): RowState[] {
  if (filters.length === 0) return rows;
  return rows.filter((row) => filters.every((filter) => rowMatchesFilter(row, filter, schema)));
}

function rowMatchesFilter(row: RowState, filter: RowFilter, schema: WasmSchema): boolean {
  const value =
    filter.column === "id"
      ? { type: "Uuid" as const, value: row.id }
      : rowValue(row, filter.column, schema);
  switch (filter.op) {
    case "Eq":
      return compareValueToLiteral(value, filter.value) === 0;
    case "Ne":
      return compareValueToLiteral(value, filter.value) !== 0;
    case "Gt":
      return compareValueToLiteral(value, filter.value) > 0;
    case "Gte":
      return compareValueToLiteral(value, filter.value) >= 0;
    case "Lt":
      return compareValueToLiteral(value, filter.value) < 0;
    case "Lte":
      return compareValueToLiteral(value, filter.value) <= 0;
    case "In":
      return filter.values.some((literal) =>
        value?.type === "Array" && literal.type !== "Array"
          ? valueContainsLiteral(value, literal)
          : compareValueToLiteral(value, literal) === 0,
      );
    case "Contains":
      return valueContainsLiteral(value, filter.value);
    case "IsNull":
      return !value || value.type === "Null";
    case "IsNotNull":
      return !!value && value.type !== "Null";
  }
}

function rowValue(row: RowState, column: string, schema: WasmSchema): Value | undefined {
  const index = schema[row.table]?.columns.findIndex((entry) => entry.name === column) ?? -1;
  return index < 0 ? undefined : row.values[index];
}

function compareValueToLiteral(value: Value | undefined, literal: DirectQueryLiteral): number {
  if (literal.type === "Nullable") {
    if (literal.value == null) return !value || value.type === "Null" ? 0 : 1;
    return compareValueToLiteral(value, literal.value);
  }
  if (!value || value.type === "Null") return -1;
  if (literal.type === "Boolean" && value.type === "Boolean") {
    return value.value === literal.value ? 0 : value.value ? 1 : -1;
  }
  if (literal.type === "Integer" && isNumericValue(value)) {
    return value.value === literal.value ? 0 : value.value > literal.value ? 1 : -1;
  }
  if (literal.type === "Bytea" && value.type === "Bytea") {
    return bytesEqual(value.value, literal.value) ? 0 : -1;
  }
  if (literal.type === "Array" && value.type === "Array") {
    return value.value.length === literal.value.length &&
      value.value.every((entry, index) => compareValueToLiteral(entry, literal.value[index]!) === 0)
      ? 0
      : -1;
  }
  const actual = value.type === "Text" || value.type === "Uuid" ? value.value : undefined;
  const expected = literalString(literal);
  if (actual == null || expected == null) return -1;
  return actual === expected ? 0 : actual > expected ? 1 : -1;
}

function isNumericValue(value: Value): value is Extract<Value, { value: number }> {
  return (
    value.type === "Integer" ||
    value.type === "BigInt" ||
    value.type === "Double" ||
    value.type === "Timestamp"
  );
}

function valueContainsLiteral(value: Value | undefined, literal: DirectQueryLiteral): boolean {
  if (!value || value.type === "Null") return false;
  if (value.type === "Text") {
    const expected = literalString(literal);
    return expected != null && value.value.includes(expected);
  }
  if (value.type === "Array") {
    return value.value.some((entry) => compareValueToLiteral(entry, literal) === 0);
  }
  return false;
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
      view.setUint32(0, expectU32(value, "Integer"), true);
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

function expectU32(value: Value, type: string): number {
  const number = expectNumber(value, type);
  if (!Number.isSafeInteger(number) || number < 0 || number > 0x7fffffff) {
    throw new Error(`${type} value must be a non-negative signed 32-bit integer`);
  }
  return number;
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
  | { type: "snapshot"; rows: AbiRowBatch[]; settled?: boolean }
  | {
      type: "delta";
      delta: { added: AbiRowBatch[]; updated: AbiRowBatch[]; removed: AbiRemovedRow[] };
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
      delta: readAbiSubscriptionDelta(
        new PostcardReader(assertBytes(record.delta, "subscription delta")),
      ),
      settled: typeof record.settled === "boolean" ? record.settled : undefined,
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
