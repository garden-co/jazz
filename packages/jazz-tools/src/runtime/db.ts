/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design:
 * - createDb() is async (pre-loads the runtime module)
 * - insert/update/delete are sync (local-first immediate writes, no durability wait)
 * - all/one are async (need storage I/O for queries)
 */

import type {
  ColumnDescriptor,
  ColumnType,
  WasmSchema,
  WasmRow,
  StorageDriver,
} from "../drivers/types.js";
import { getRuntimeSchemaCacheKey, normalizeRuntimeSchema } from "../drivers/schema-wire.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  DirectBatch as RuntimeDirectBatch,
  WriteResult,
  JazzClient,
  type MutationErrorEvent,
  Transaction as RuntimeTransaction,
  WriteHandle,
  type CreateOptions,
  type UpdateOptions,
  type UpsertOptions,
  type DurabilityTier,
  type QueryExecutionOptions,
  type QueryPropagation,
  type QueryVisibility,
  resolveEffectiveQueryExecutionOptions,
  runInBatch,
  Scoped,
} from "./client.js";
import { type DbRuntimeModule, type RuntimeTokenOptions } from "./db-runtime-module.js";
import { WasmRuntimeModule } from "./wasm-runtime-module.js";
import {
  WorkerBridge,
  type WorkerBridgeEndpoint,
  type WorkerBridgeOptions,
} from "./worker-bridge.js";
import type { AuthFailureReason } from "./sync-transport.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toInsertRecord, toUpdateRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import { resolveClientSessionSync } from "./client-session.js";
import {
  createConventionalFileStorage,
  type ConventionalFileApp,
  type FileReadOptions,
  type FileWriteOptions,
} from "./file-storage.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { isPermissionIntrospectionColumn, magicColumnType } from "../magic-columns.js";
import type { WorkerLifecycleEvent } from "./worker-bridge.js";
import {
  normalizeBuiltQuery,
  type BuiltRelation,
  type NormalizedIncludeSpec,
  type NormalizedBuiltQuery,
} from "./query-builder-shape.js";
import { resolveSelectedColumns } from "./select-projection.js";
import {
  appendSharedWorkerRuntimeUrls,
  resolveRuntimeConfigSyncInitInput,
  resolveWorkerBootstrapWasmUrl,
  resolveRuntimeConfigWorkerUrl,
  resolveRuntimeConfigSharedWorkerUrl,
} from "./runtime-config.js";
import { resolveTelemetryCollectorUrlFromEnv } from "./sync-telemetry.js";
import {
  createBrowserLocksBackend,
  createTabSupervisor,
  type TabSupervisor,
  type TabSupervisorState,
} from "./shared-worker-supervisor.js";

type WasmLogLevel = "error" | "warn" | "info" | "debug" | "trace";
type AnyDbRuntimeModule = DbRuntimeModule<any>;

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver mode (defaults to persistent). */
  driver?: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Optional runtime source overrides for WASM and worker loading. */
  runtimeSources?: RuntimeSourcesConfig;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
  /** JWT token for server authentication */
  jwtToken?: string;
  /** Mirrored session for local permission evaluation when sync auth uses cookies. */
  cookieSession?: Session;
  /** Admin secret for catalogue sync */
  adminSecret?: string;
  /** Database name for OPFS persistence (browser only, default: appId) */
  dbName?: string;
  /** Optional WASM tracing level for benchmark/debug scenarios (default: "warn"). */
  logLevel?: WasmLogLevel;
  /** Optional OTLP/HTTP collector URL for WASM trace telemetry. */
  telemetryCollectorUrl?: string;
  /** Enable runtime tracing for DevTools-only diagnostics. */
  devMode?: boolean;
  /** Local-first auth via a local seed. Mutually exclusive with jwtToken. */
  secret?: string;
}

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

function shouldBypassLocalPolicies(config: DbConfig): boolean {
  return !!config.adminSecret;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function stripSchemaPolicies(schema: WasmSchema): WasmSchema {
  return Object.fromEntries(
    Object.entries(schema).map(([tableName, tableSchema]) => [
      tableName,
      {
        ...tableSchema,
        policies: undefined,
      },
    ]),
  ) as WasmSchema;
}

const policyStrippedSchemaCache = new WeakMap<WasmSchema, WasmSchema>();

function getPolicyStrippedSchema(schema: WasmSchema): WasmSchema {
  const cached = policyStrippedSchemaCache.get(schema);
  if (cached) {
    return cached;
  }

  const strippedSchema = stripSchemaPolicies(schema);
  policyStrippedSchemaCache.set(schema, strippedSchema);
  return strippedSchema;
}

function trimOptionalString(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** @internal Derive the default browser persistence namespace for this Db config. */
export function resolveDefaultPersistentDbName(config: DbConfig): string {
  const driver = resolveStorageDriver(config.driver);
  const explicitDbName = trimOptionalString(
    (driver.type === "persistent" ? driver.dbName : undefined) ?? config.dbName,
  );
  if (explicitDbName) {
    return explicitDbName;
  }

  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
  });

  if (!session?.user_id || session.authMode === "anonymous") {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(session.user_id)}`;
}

/**
 * Interface that QueryBuilder classes implement.
 * Generated builders expose these internal properties for Db to use.
 */
export interface QueryBuilder<T> {
  /** Table name for this query */
  readonly _table: string;
  /** Schema reference for translation and transformation */
  readonly _schema: WasmSchema;
  /** Optional TypeScript-only per-column transforms carried by typed query handles. */
  readonly _columnTransforms?: ColumnTransformMap;
  /** Build and return the query as JSON */
  _build(): string;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
}

export type QueryOptions = QueryExecutionOptions;

type DbRuntimeOperationContext = {
  session?: Session;
  attribution?: string;
};

function ordinaryDbQueryOptions(options?: QueryOptions): QueryOptions {
  return { localUpdates: "deferred", ...options };
}

function queryUsesRelationTraversal(builtQuery: NormalizedBuiltQuery): boolean {
  return builtQuery.hops.length > 0 || builtQuery.gather !== undefined;
}

export interface ActiveQuerySubscriptionTrace {
  id: string;
  query: string;
  table: string;
  branches: string[];
  tier: DurabilityTier;
  propagation: QueryPropagation;
  createdAt: string;
  stack?: string;
}

export interface LogoutOptions {
  wipeData?: boolean;
}

type ActiveQuerySubscriptionTraceListener = (
  traces: readonly ActiveQuerySubscriptionTrace[],
) => void;

type StoredActiveQuerySubscriptionTrace = ActiveQuerySubscriptionTrace & {
  visibility: QueryVisibility;
};

type RuntimeQueryTracePayload = {
  table: string;
  branches: string[];
};

function trimSubscriptionTraceStack(stack: string | undefined): string | undefined {
  if (!stack) {
    return stack;
  }

  const lines = stack.split("\n");
  if (lines.length <= 1) {
    return stack;
  }

  const isInternalFrame = (line: string): boolean => {
    return (
      line.includes("Db.registerActiveQuerySubscriptionTrace") ||
      line.includes("Db.subscribeAll") ||
      line.includes("SubscriptionsOrchestrator.ensureEntryForKey") ||
      line.includes("SubscriptionsOrchestrator.getCacheEntry") ||
      line.includes("/node_modules/") ||
      line.includes("react-dom") ||
      line.includes("react_stack_bottom_frame")
    );
  };

  const firstOriginIndex = lines.findIndex((line, index) => index > 0 && !isInternalFrame(line));
  if (firstOriginIndex <= 0) {
    return stack;
  }

  return [lines[0], ...lines.slice(firstOriginIndex)].join("\n");
}

function cloneActiveQuerySubscriptionTrace(
  trace: ActiveQuerySubscriptionTrace,
): ActiveQuerySubscriptionTrace {
  return {
    ...trace,
    branches: [...trace.branches],
  };
}

function resolveHopOutputTable(
  schema: WasmSchema,
  startTable: string,
  hops: readonly string[],
): string {
  if (hops.length === 0) {
    return startTable;
  }
  const relations = analyzeRelations(schema);
  let currentTable = startTable;
  for (const hopName of hops) {
    const candidates = relations.get(currentTable) ?? [];
    const relation = candidates.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
}

function resolveBuiltRelationOutputTable(schema: WasmSchema, relation: BuiltRelation): string {
  if (relation.union) {
    const first = relation.union.inputs[0];
    if (!first) {
      throw new Error("union(...) requires at least one relation.");
    }
    const firstTable = resolveBuiltRelationOutputTable(schema, first);
    for (const input of relation.union.inputs.slice(1)) {
      const inputTable = resolveBuiltRelationOutputTable(schema, input);
      if (inputTable !== firstTable) {
        throw new Error("union(...) requires all relations to output the same table.");
      }
    }
    return firstTable;
  }

  const seedTable = relation.gather?.seed
    ? resolveBuiltRelationOutputTable(schema, relation.gather.seed)
    : relation.table;
  if (!seedTable) {
    throw new Error("gather(...) seed relation is missing table metadata.");
  }
  const hops = relation.hops ?? [];
  return hops.length > 0 ? resolveHopOutputTable(schema, seedTable, hops) : seedTable;
}

function resolveBuiltQueryOutputTable(
  schema: WasmSchema,
  builtQuery: ReturnType<typeof normalizeBuiltQuery>,
): string {
  if (builtQuery.gather?.seed) {
    const gatherTable = resolveBuiltRelationOutputTable(schema, builtQuery.gather.seed);
    return builtQuery.hops.length > 0
      ? resolveHopOutputTable(schema, gatherTable, builtQuery.hops)
      : gatherTable;
  }

  return builtQuery.hops.length > 0
    ? resolveHopOutputTable(schema, builtQuery.table, builtQuery.hops)
    : builtQuery.table;
}

function resolveSchemaWithTable(
  preferredSchema: WasmSchema,
  fallbackSchema: WasmSchema | (() => WasmSchema),
  tableName: string,
): WasmSchema {
  if (preferredSchema[tableName]) {
    return preferredSchema;
  }

  return typeof fallbackSchema === "function" ? fallbackSchema() : fallbackSchema;
}

function resolveOutputColumnDescriptor(
  tableName: string,
  schema: WasmSchema,
  columnName: string,
): ColumnDescriptor | undefined {
  const magicType = magicColumnType(columnName);
  if (magicType) {
    return {
      name: columnName,
      column_type: magicType,
      nullable: isPermissionIntrospectionColumn(columnName),
    };
  }

  return schema[tableName]?.columns.find((column) => column.name === columnName);
}

function resolveNativeSubscriptionColumns(
  tableName: string,
  schema: WasmSchema,
  includes: NormalizedIncludeSpec,
  projection?: readonly string[],
): ColumnDescriptor[] {
  const columns = resolveSelectedColumns(tableName, schema, projection)
    .map((columnName) => resolveOutputColumnDescriptor(tableName, schema, columnName))
    .filter((column): column is ColumnDescriptor => column !== undefined);

  if (Object.keys(includes).length === 0) {
    return columns;
  }

  const relationsByTable = analyzeRelations(schema);
  const relations = relationsByTable.get(tableName) ?? [];

  for (const [relationName, include] of Object.entries(includes)) {
    const relation = relations.find((candidate) => candidate.name === relationName);
    if (!relation) {
      throw new Error(`Unknown relation "${relationName}" on table "${tableName}"`);
    }

    const nestedColumns = resolveNativeSubscriptionColumns(
      relation.toTable,
      schema,
      include.includes,
      include.select.length > 0 ? include.select : undefined,
    );
    const columnType: ColumnType = {
      type: "Array",
      element: { type: "Row", columns: nestedColumns },
    };

    columns.push({
      name: relationName,
      column_type: columnType,
      nullable: false,
    });
  }

  return columns;
}

function createRuntimeSchemaResolver(getRuntimeSchema: () => WasmSchema): {
  get: () => WasmSchema;
  peek: () => WasmSchema | undefined;
} {
  let cachedRuntimeSchema: WasmSchema | undefined;

  return {
    get: () => {
      if (!cachedRuntimeSchema) {
        cachedRuntimeSchema = getRuntimeSchema();
      }
      return cachedRuntimeSchema;
    },
    peek: () => cachedRuntimeSchema,
  };
}

function assertTableBelongsToClient<T, Init>(
  table: TableProxy<T, Init>,
  expectedClient: JazzClient,
  resolveClient: (schema: WasmSchema) => JazzClient,
  operation: string,
): void {
  if (resolveClient(table._schema) === expectedClient) {
    return;
  }
  throw new Error(
    `${operation} is bound to the client chosen by the first table used and cannot be used with table "${table._table}" from a different schema/client.`,
  );
}

/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<T, Init> {
  /** Table name */
  readonly _table: string;
  /** Schema reference */
  readonly _schema: WasmSchema;
  /** Optional TypeScript-only per-column transforms carried by typed table handles. */
  readonly _columnTransforms?: ColumnTransformMap;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
  /** @internal Phantom brand — enables TypeScript to infer Init from usage */
  readonly _initType: Init;
}

export interface ColumnTransform {
  from(value: unknown): unknown;
  to(value: unknown): unknown;
}

export type ColumnTransformMap = Record<string, ColumnTransform>;

type DbBatchHandleBinding = {
  client: JazzClient;
  runtimeHandle: RuntimeTransaction;
};
type AnyDbBatchHandle = DbBatchHandleBase<RuntimeTransaction | RuntimeDirectBatch>;

const dbBatchHandleBindings = new WeakMap<AnyDbBatchHandle, DbBatchHandleBinding>();

function getDbBatchHandleBinding(
  handle: AnyDbBatchHandle,
  operation: string,
  bindingName: "DbTransaction" | "DbDirectBatch",
): DbBatchHandleBinding {
  const binding = dbBatchHandleBindings.get(handle);
  if (!binding) {
    throw new Error(`${bindingName}.${operation}() requires at least one table operation first`);
  }
  return binding;
}

function transformOutputRow<T>(
  source: { readonly _columnTransforms?: ColumnTransformMap },
  row: unknown,
): T {
  return transformOutputColumns(source, row) as T;
}

function transformOutputColumns(
  source: { readonly _columnTransforms?: ColumnTransformMap },
  row: unknown,
): unknown {
  if (!source._columnTransforms || typeof row !== "object" || row === null) {
    return row;
  }

  const transformed = { ...(row as Record<string, unknown>) };
  for (const [column, transform] of Object.entries(source._columnTransforms)) {
    if (column in transformed) {
      transformed[column] = transform.from(transformed[column]);
    }
  }
  return transformed;
}

function transformInputColumns(
  table: TableProxy<unknown, unknown>,
  data: unknown,
): Record<string, unknown> {
  const record = data as Record<string, unknown>;
  if (!table._columnTransforms) {
    return record;
  }

  const transformed = { ...record };
  for (const [column, transform] of Object.entries(table._columnTransforms)) {
    if (column in transformed) {
      transformed[column] = transform.to(transformed[column]);
    }
  }
  return transformed;
}

/**
 * Shared implementation for batches and transactions.
 */
abstract class DbBatchHandleBase<TRuntimeHandle extends RuntimeTransaction | RuntimeDirectBatch> {
  constructor(
    private readonly bindingName: "DbTransaction" | "DbDirectBatch",
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly beginRuntimeHandle: (client: JazzClient) => TRuntimeHandle,
  ) {}

  private bindTable<T, Init>(table: TableProxy<T, Init>, operation: string): DbBatchHandleBinding {
    const existingBinding = dbBatchHandleBindings.get(this);
    if (existingBinding) {
      assertTableBelongsToClient(table, existingBinding.client, this.resolveClient, operation);
      return existingBinding;
    }

    const client = this.resolveClient(table._schema);
    const runtimeHandle = this.beginRuntimeHandle(client);
    const binding = { client, runtimeHandle };
    dbBatchHandleBindings.set(this, binding);
    return binding;
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbBatchHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>, this.bindingName);
  }

  private requireRuntimeHandle(operation: string): TRuntimeHandle {
    return getDbBatchHandleBinding(this, operation, this.bindingName)
      .runtimeHandle as TRuntimeHandle;
  }

  batchId(): string {
    return this.requireRuntimeHandle("batchId").batchId();
  }

  /**
   * Commit this batch.
   */
  commit(): WriteHandle {
    return this.requireRuntimeHandle("commit").commit();
  }

  /**
   * Roll back this batch locally.
   *
   * Pending rows remain pending, but this batch can no longer be committed.
   *
   * Only available on batches/transactions created with {@link Db.beginBatch}/{@link Db.beginTransaction}.
   * When using {@link Db.batch}/{@link Db.transaction}, throw an error inside the callback to roll back.
   */
  rollback(): void {
    this.requireRuntimeHandle("rollback").rollback();
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: CreateOptions): T {
    this.bindTable(table, this.bindingName);
    const transformedData = transformInputColumns(table, data);
    const values = toInsertRecord(transformedData, table._schema, table._table);
    const runtimeHandle = this.requireRuntimeHandle("insert");
    const row = runtimeHandle.create(table._table, values, options);
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Create or update a row with a caller-supplied id.
   *
   * The upsert is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  upsert<T, Init>(table: TableProxy<T, Init>, data: Partial<Init>, options: UpsertOptions): void {
    this.bindTable(table, this.bindingName);
    const transformedData = transformInputColumns(table, data);
    const values = toUpdateRecord(transformedData, table._schema, table._table);
    this.requireRuntimeHandle("upsert").upsert(table._table, values, options);
  }

  /**
   * Update an existing row in a table.
   *
   * The update is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    this.bindTable(table, this.bindingName);
    const transformedData = transformInputColumns(table, data);
    const updates = toUpdateRecord(transformedData, table._schema, table._table);
    this.requireRuntimeHandle("update").update(id, updates);
  }

  /**
   * Delete an existing row from a table.
   *
   * The delete is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const { runtimeHandle } = this.bindTable(table, this.bindingName);
    runtimeHandle.delete(id);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this batch.
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const { client, runtimeHandle } = this.bindQuery(query);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const rows = await runtimeHandle.query(translateQuery(builderJson, planningSchema), options);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const transformedRows = transformRows(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      builtQuery.select,
    );
    return transformedRows.map((row) =>
      transformOutputRow(outputTable === builtQuery.table ? query : {}, row),
    );
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * Read data is scoped to this batch.
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }
}

/**
 * Transactions group a set of writes that should settle together after an authority validates them.
 *
 * Data read and written through this transaction is scoped to it, and will only be
 * globally visible once it's committed using {@link DbTransaction.commit} and
 * accepted by the authority.
 */
export class DbTransaction extends DbBatchHandleBase<RuntimeTransaction> {
  constructor(
    resolveClient: (schema: WasmSchema) => JazzClient,
    beginRuntimeTransaction: (client: JazzClient) => RuntimeTransaction,
  ) {
    super("DbTransaction", resolveClient, beginRuntimeTransaction);
  }
}

/**
 * Transaction object available inside {@link Db.transaction}'s callback.
 */
export type DbTransactionScope = Scoped<DbTransaction>;

/**
 * Direct batches group a set of writes that should become visible together on batch commit,
 * without waiting for an authority approval.
 */
export class DbDirectBatch extends DbBatchHandleBase<RuntimeDirectBatch> {
  constructor(
    resolveClient: (schema: WasmSchema) => JazzClient,
    beginRuntimeBatch: (client: JazzClient) => RuntimeDirectBatch,
  ) {
    super("DbDirectBatch", resolveClient, beginRuntimeBatch);
  }
}

/**
 * Batch object available inside {@link Db.batch}'s callback.
 */
export type DbBatchScope = Scoped<DbDirectBatch>;

/**
 * High-level database interface for typed queries and mutations.
 *
 * Usage:
 * ```typescript
 * const db = await createDb({ appId: "my-app", driver });
 *
 * // Mutations
 * const { value: inserted } = db.insert(app.todos, { title: "Buy milk", done: false });
 * db.update(app.todos, inserted.id, { done: true });
 * db.delete(app.todos, inserted.id);
 *
 * // Async queries (need storage I/O)
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id: inserted.id }));
 *
 * // Subscriptions
 * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
 *   console.log("All todos:", delta.all);
 *   console.log("Changes:", delta.delta);
 * });
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;
  private readonly runtimeModule: AnyDbRuntimeModule | null;
  private readonly authStateStore;
  private workerBridge: WorkerBridge | null = null;
  private sharedWorker: SharedWorker | null = null;
  private supervisor: TabSupervisor | null = null;
  private supervisorUnsubscribe: (() => void) | null = null;
  private workerEndpoint: WorkerBridgeEndpoint | null = null;
  private resetChannel: BroadcastChannel | null = null;
  private resetChannelListener: ((event: MessageEvent) => void) | null = null;
  private disposeWasmTelemetry: (() => void) | null = null;
  private bridgeReady: Promise<void> | null = null;
  private primaryDbName: string | null = null;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;
  private lifecycleHooksAttached = false;
  private readonly activeQuerySubscriptionTraces = new Map<
    string,
    StoredActiveQuerySubscriptionTrace
  >();
  private readonly activeQuerySubscriptionTraceListeners =
    new Set<ActiveQuerySubscriptionTraceListener>();
  /**
   * Listeners attached with {@link Db.onMutationError} that are notified when a write operation
   * (insert, update, delete) is rejected. Errors from all {@link Db.clients} (including those
   * added after the listeners are attached) are forwarded to all Db listeners.
   */
  private readonly mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  /**
   * Persists mutation errors thrown before an {@link onMutationError} listener was attached.
   * Those mutation errors are replayed when `onMutationError` is called.
   */
  private readonly pendingMutationErrorEvents: MutationErrorEvent[] = [];
  private nextActiveQuerySubscriptionTraceId = 1;
  private readonly onVisibilityChange = (): void => {
    if (typeof document === "undefined") return;
    const hidden = document.visibilityState === "hidden";
    this.sendLifecycleHint(hidden ? "visibility-hidden" : "visibility-visible");
  };
  private readonly onPageHide = (): void => {
    this.sendLifecycleHint("pagehide");
  };
  private readonly onPageFreeze = (): void => {
    this.sendLifecycleHint("freeze");
  };
  private readonly onPageResume = (): void => {
    this.sendLifecycleHint("resume");
  };

  /**
   * Protected constructor - use {@link createDb} in regular app code.
   */
  protected constructor(
    config: DbConfig,
    runtimeModule: AnyDbRuntimeModule | null,
    authStateOptions?: AuthStateStoreOptions,
  ) {
    this.config = config;
    this.runtimeModule = runtimeModule;
    this.authStateStore = createAuthStateStore(config, authStateOptions);
  }

  /** @internal Store the seed used for local-first auth and schedule token refresh. */
  initLocalFirstAuth(seed: string, ttlSeconds: number): void {
    this._localFirstSecret = seed;
    this.scheduleLocalFirstRefresh(ttlSeconds);
  }

  private scheduleLocalFirstRefresh(ttlSeconds: number): void {
    if (this.localFirstRefreshTimer) {
      clearTimeout(this.localFirstRefreshTimer);
    }
    // Refresh at 80% of TTL
    const refreshMs = ttlSeconds * 800; // 80% of TTL in ms
    this.localFirstRefreshTimer = setTimeout(() => {
      this.refreshLocalFirstToken();
    }, refreshMs);
  }

  private refreshLocalFirstToken(): void {
    if (!this._localFirstSecret || this.isShuttingDown) return;

    try {
      const ttlSeconds = 3600;
      const newToken = this.mintLocalFirstToken(
        this._localFirstSecret,
        this.config.appId,
        ttlSeconds,
      );
      this.updateAuthToken(newToken);
      this.scheduleLocalFirstRefresh(ttlSeconds);
    } catch (e) {
      console.error("Failed to refresh local-first token:", e);
    }
  }

  private mintLocalFirstToken(secret: string, audience: string, ttlSeconds: number): string {
    if (!this.runtimeModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }

    return this.runtimeModule.mintLocalFirstToken({
      secret,
      audience,
      ttlSeconds,
      nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
    });
  }

  protected markUnauthenticated(reason: AuthFailureReason): void {
    this.authStateStore.markUnauthenticated(reason);
  }

  protected applyAuthUpdate(token: string | null): boolean {
    const jwtToken = token ?? undefined;
    const previousToken = this.config.jwtToken;
    const previousState = this.authStateStore.getState();
    const nextState = this.authStateStore.applyJwtToken(jwtToken);
    const tokenChanged = previousToken !== jwtToken;

    if (!tokenChanged && nextState === previousState) {
      return false;
    }

    this.config.jwtToken = jwtToken;

    for (const client of this.clients.values()) {
      client.updateAuthToken(jwtToken);
    }

    this.workerBridge?.updateAuth({
      jwtToken,
    });

    return true;
  }

  protected applyCookieSessionUpdate(session: Session | null): boolean {
    const cookieSession = session ?? undefined;
    const previousSession = this.config.cookieSession;
    const previousState = this.authStateStore.getState();
    const nextState = this.authStateStore.applyCookieSession(cookieSession);
    const sessionChanged = JSON.stringify(previousSession) !== JSON.stringify(cookieSession);

    if (!sessionChanged && nextState === previousState) {
      return false;
    }

    this.config.cookieSession = cookieSession;

    for (const client of this.clients.values()) {
      client.updateCookieSession(cookieSession);
    }

    this.workerBridge?.updateAuth({
      jwtToken: this.config.jwtToken,
    });

    return true;
  }

  /**
   * Create a Db instance with a loaded runtime module.
   * @internal Use createDb() instead.
   */
  static create(config: DbConfig, runtimeModule: AnyDbRuntimeModule): Db {
    return new Db(config, runtimeModule);
  }

  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use {@link createDb} instead — it auto-detects browser.
   */
  /**
   * Create a Db instance backed by the leader-tab runtime topology.
   *
   * The page owns an in-memory WASM runtime for synchronous reads and writes.
   * A `SharedWorker` acts as a `MessagePort` broker between tabs and the
   * current leader. A per-tab supervisor races for the `navigator.locks`
   * leader lease; the winner spawns its own dedicated `Worker` that hosts the
   * durable runtime (OPFS, upstream socket) and routes follower-tab requests
   * through it. See `specs/todo/b_launch/leader_tab_runtime.md`.
   *
   * @internal Use {@link createDb} instead — it auto-detects browser support.
   */
  static async createWithSharedWorker(
    config: DbConfig,
    runtimeModule: AnyDbRuntimeModule,
  ): Promise<Db> {
    const db = new Db(config, runtimeModule);
    const persistentDriver = resolveStorageDriver(config.driver);
    if (persistentDriver.type !== "persistent") {
      throw new Error("SharedWorker-backed Db requires driver.type='persistent'");
    }

    db.primaryDbName = resolveDefaultPersistentDbName(config);

    const locks = createBrowserLocksBackend();
    if (!locks) {
      throw new Error("Persistent driver requires navigator.locks, which this environment lacks.");
    }

    try {
      db.sharedWorker = Db.spawnSharedWorkerBroker(config, db.primaryDbName);
      const brokerPort = db.sharedWorker.port;
      brokerPort.start();
      const { workerUrl, workerOptions } = Db.resolveRuntimeWorkerSpec(config);
      db.supervisor = createTabSupervisor({
        brokerPort: brokerPort as unknown as Parameters<
          typeof createTabSupervisor
        >[0]["brokerPort"],
        lockName: `jazz:leader:${config.appId}:${db.primaryDbName}:v1`,
        locks,
        WorkerCtor: Worker as unknown as Parameters<typeof createTabSupervisor>[0]["WorkerCtor"],
        workerUrl,
        workerOptions,
      });
      db.supervisorUnsubscribe = db.supervisor.subscribe((state) => {
        db.onSupervisorStateChange(state);
      });
      db.attachLifecycleHooks();
      db.subscribeToResetChannel();

      await db.waitForInitialEndpoint();

      return db;
    } catch (error) {
      await db.supervisor?.shutdown();
      db.supervisor = null;
      db.supervisorUnsubscribe?.();
      db.supervisorUnsubscribe = null;
      db.detachLifecycleHooks();
      db.closeResetChannel();
      db.sharedWorker?.port.close();
      db.sharedWorker = null;
      db.workerEndpoint = null;
      throw error;
    }
  }

  /**
   * Resolve the URL + options for the dedicated runtime `Worker` the leader
   * tab spawns. Mirrors the previous `spawnSharedWorker` URL resolution but
   * for the per-tab `Worker`, not the SharedWorker.
   */
  private static resolveRuntimeWorkerSpec(config: DbConfig): {
    workerUrl: string | URL;
    workerOptions: WorkerOptions;
  } {
    const runtimeSources = config.runtimeSources;
    const locationHref = typeof location !== "undefined" ? location.href : undefined;
    let workerUrl: string | URL;
    if (runtimeSources?.workerUrl || runtimeSources?.baseUrl) {
      workerUrl = resolveRuntimeConfigWorkerUrl(import.meta.url, locationHref, runtimeSources);
    } else {
      workerUrl = new URL("../worker/jazz-worker.js", import.meta.url);
    }
    return {
      workerUrl,
      workerOptions: {
        type: "module",
        name: `jazz-runtime:${config.appId}`,
      },
    };
  }

  /**
   * Reacts to supervisor state transitions. Drops the stale `WorkerBridge`
   * synchronously and points `workerEndpoint` at the new value. The next
   * `getClient()` call (and the {@link ensureWorkerBridge} helper) will
   * attach a fresh bridge against the new endpoint, so existing
   * `JazzClient`s keep working across a leader handoff.
   *
   * In-flight `waitForUpstreamServerConnection` waiters on the *old* bridge
   * reject synchronously with
   * {@link LeaderMigratedError} via {@link WorkerBridge.notifyMigrated} so
   * callers learn the bridge is no longer authoritative without waiting for
   * the Rust-side ack timeout. Transparent retry of those waiters against
   * the freshly-attached bridge is not yet implemented — see
   * `specs/todo/b_launch/leader_tab_runtime.md` §5 — but rejection is
   * deterministic and typed rather than a silent hang.
   */
  private onSupervisorStateChange(state: TabSupervisorState): void {
    const nextEndpoint = state.endpoint;
    if (nextEndpoint === this.workerEndpoint) return;
    if (this.workerBridge) {
      const stale = this.workerBridge;
      this.workerBridge = null;
      this.bridgeReady = null;
      // Reject in-flight waiters synchronously so callers see the typed
      // `LeaderMigratedError` instead of hanging until the Rust ack timeout.
      stale.notifyMigrated();
      void stale.shutdown().catch(() => undefined);
    }
    this.workerEndpoint = nextEndpoint;
    this.ensureWorkerBridge();
  }

  /**
   * Reattach the `WorkerBridge` if we have an active endpoint and at least
   * one existing client but no live bridge. Idempotent. Called both from
   * `getClient()` when a new client is created and from
   * `onSupervisorStateChange()` after the supervisor swaps the endpoint
   * (leader handoff, follower-port refresh).
   */
  private ensureWorkerBridge(): void {
    if (this.isShuttingDown) return;
    if (this.workerBridge) return;
    if (!this.workerEndpoint) return;
    if (this.clients.size === 0) return;
    const entry = this.clients.entries().next();
    if (entry.done) return;
    const [schemaJson, client] = entry.value;
    this.attachWorkerBridge(schemaJson, client);
  }

  /**
   * Wait until the supervisor has produced its first non-null endpoint.
   * Resolves immediately if one is already present, otherwise on the first
   * state change carrying a non-null `endpoint`.
   */
  private async waitForInitialEndpoint(): Promise<void> {
    if (this.supervisor?.state.endpoint) {
      this.workerEndpoint = this.supervisor.state.endpoint;
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error("Timed out waiting for leader-tab runtime endpoint.")),
        15_000,
      );
      const unsubscribe = this.supervisor!.subscribe((state) => {
        if (state.endpoint) {
          clearTimeout(timeout);
          unsubscribe();
          this.workerEndpoint = state.endpoint;
          resolve();
        }
      });
    });
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because the runtime module is loaded before Db is created.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  protected getClient(schema: WasmSchema): JazzClient {
    if (!this.runtimeModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }
    if (this.isShuttingDown && this.primaryDbName !== null) {
      // SharedWorker-backed terminal contract: after `shutdown()` (including
      // `deleteClientStorage()`'s wipe) the supervisor is gone, the
      // dedicated worker has been terminated, and OPFS may have been
      // deleted out from under us. Refuse to lazily mint a direct
      // main-thread client that would silently re-use stale state.
      //
      // Non-SharedWorker Db instances (React Native, in-memory test
      // harnesses) never set `primaryDbName`, and their pre-existing
      // contract is that `shutdown()` clears cached clients but the same
      // Db can lazily recreate them on next `insert` / `all`. Preserving
      // that contract keeps the platform-specific terminal behavior to
      // the path that actually needs it.
      throw new Error("Db has been shut down; create a new instance to keep working.");
    }

    const runtimeSchema =
      this.runtimeModule.supportsPolicyBypass && shouldBypassLocalPolicies(this.config)
        ? getPolicyStrippedSchema(schema)
        : schema;

    // Use the canonical schema JSON as the client cache key, but memoize it by
    // schema identity so write-heavy paths don't stringify the same schema per row.
    const key = getRuntimeSchemaCacheKey(runtimeSchema);

    if (!this.clients.has(key)) {
      this.installMainThreadWasmTelemetry();

      const client = this.runtimeModule.createClient({
        config: { ...this.config },
        schema: runtimeSchema,
        hasWorker: this.workerEndpoint !== null,
        useBinaryEncoding: this.workerEndpoint !== null,
        onAuthFailure: (reason) => {
          this.markUnauthenticated(reason);
        },
      });

      this.attachMutationErrorHandler(client);
      // Direct (non-worker) clients with a serverUrl must open their own
      // Rust transport — the worker bridge is not doing it for them.
      if (!this.workerEndpoint && this.config.serverUrl) {
        client.connectTransport(this.config.serverUrl, {
          jwt_token: this.config.jwtToken,
          admin_secret: this.config.adminSecret,
        });
      }
      this.clients.set(key, client);
    }

    // Always ensure a bridge is attached when an endpoint is available — both
    // on first-client creation (when the bridge has never been built) and on
    // re-creation after a leader handoff (when `onSupervisorStateChange`
    // dropped the stale bridge and we need to reattach against the new
    // endpoint before serving this client's queries).
    this.ensureWorkerBridge();

    return this.clients.get(key)!;
  }

  protected getRuntimeOperationContext(): DbRuntimeOperationContext | null {
    return null;
  }

  /**
   * Attaches a mutation error handler to the given client, ensuring all listeners in
   * {@link Db.mutationErrorListeners} are notified.
   */
  private attachMutationErrorHandler(client: JazzClient): void {
    client.onMutationError((event) => {
      if (this.mutationErrorListeners.size === 0) {
        console.error("Unhandled Jazz mutation error", event);
        this.pendingMutationErrorEvents.push(event);
        return;
      }
      for (const listener of this.mutationErrorListeners) {
        listener(event);
      }
    });
  }
  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  protected async ensureBridgeReady(): Promise<void> {
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
  }

  protected async ensureQueryReady(options?: QueryOptions): Promise<void> {
    await this.ensureBridgeReady();
    if (!this.workerBridge || !this.config.serverUrl) {
      return;
    }
    if (!options?.tier || options.tier === "local") {
      return;
    }
    await this.workerBridge.waitForUpstreamServerConnection();
  }

  private attachWorkerBridge(schemaJson: string, client: JazzClient): void {
    if (!this.workerEndpoint) {
      throw new Error("Cannot attach worker bridge without an active worker");
    }

    const bridge = new WorkerBridge(this.workerEndpoint, client.getRuntime());
    bridge.onAuthFailure((reason) => {
      this.markUnauthenticated(reason);
    });
    this.workerBridge = bridge;
    // `notifyLeaderReady` is now a no-op — the supervisor claims leadership
    // eagerly upon winning the lock. Kept here for forward-compat with any
    // future supervisor that goes back to deferring claim until the worker
    // bootstraps.
    const bridgeReady = bridge.init(this.buildWorkerBridgeOptions(schemaJson)).then(() => {
      this.supervisor?.notifyLeaderReady();
    });
    bridgeReady.catch(() => undefined);
    this.bridgeReady = bridgeReady;
  }

  private installMainThreadWasmTelemetry(): void {
    const collectorUrl = this.resolveTelemetryCollectorUrl();
    if (!collectorUrl || !this.runtimeModule || this.disposeWasmTelemetry) {
      return;
    }

    this.disposeWasmTelemetry =
      this.runtimeModule.installTelemetry?.({
        config: this.config,
        collectorUrl,
        runtimeThread: "main",
      }) ?? null;
  }

  private resolveTelemetryCollectorUrl(): string | undefined {
    return resolveTelemetryCollectorUrlFromEnv() ?? this.config.telemetryCollectorUrl;
  }

  private buildWorkerBridgeOptions(schemaJson: string): WorkerBridgeOptions {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker bridge is only available for driver.type='persistent'");
    }

    const locationHref = typeof location !== "undefined" ? location.href : undefined;

    // Opt-in default: when a bundler plugin (e.g. `withJazz` for Next) copies
    // the wasm into the host app and advertises the URL via
    // NEXT_PUBLIC_JAZZ_WASM_URL, pick it up so the worker receives an
    // absolute URL and skips the (Turbopack-unreliable) bundler default.
    //
    // Precedence follows RuntimeSourcesConfig: any of wasmModule / wasmSource /
    // wasmUrl / baseUrl already supplied by the caller wins — we only fill in
    // when none of those is set, preserving the documented resolution order
    // for Vite/webpack/Svelte/etc. callers.
    const configRuntimeSources = this.config.runtimeSources;
    // Use the literal `process.env.NEXT_PUBLIC_JAZZ_WASM_URL` form: Next's
    // build-time replacement only rewrites that exact property access. Optional
    // chaining on `process.env` can bypass the replacement in Turbopack and
    // leave this as `undefined` in client bundles, defeating the fallback.
    const envWasmUrl =
      typeof process !== "undefined" && process.env
        ? process.env.NEXT_PUBLIC_JAZZ_WASM_URL
        : undefined;
    // Any explicit override means the caller is taking control of wasm/worker
    // resolution — don't second-guess them by injecting a Next-plugin URL.
    // `workerUrl` counts too: the spawn path at `Db.spawnWorker` already
    // resolves a wasm URL colocated with the custom worker script via
    // `appendWorkerRuntimeWasmUrl` + `readWorkerRuntimeWasmUrl`.
    const hasConfiguredSource =
      !!configRuntimeSources?.wasmUrl ||
      !!configRuntimeSources?.baseUrl ||
      !!configRuntimeSources?.workerUrl ||
      !!resolveRuntimeConfigSyncInitInput(configRuntimeSources);
    const runtimeSources =
      hasConfiguredSource || !envWasmUrl || typeof location === "undefined"
        ? configRuntimeSources
        : {
            ...configRuntimeSources,
            wasmUrl: new URL(envWasmUrl, location.href).href,
          };

    // For the static-URL spawn path (no explicit workerUrl/baseUrl), compute a
    // fallback WASM URL for non-bundled contexts where wasmModule.default() may fail.
    let fallbackWasmUrl: string | undefined;
    if (!runtimeSources?.workerUrl && !runtimeSources?.baseUrl && !runtimeSources?.wasmUrl) {
      if (!resolveRuntimeConfigSyncInitInput(runtimeSources)) {
        fallbackWasmUrl =
          resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources) ?? undefined;
      }
    }

    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.primaryDbName ?? driver.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      jwtToken: this.config.jwtToken,
      adminSecret: this.config.adminSecret,
      runtimeSources,
      fallbackWasmUrl,
      logLevel: this.config.logLevel,
      telemetryCollectorUrl: this.resolveTelemetryCollectorUrl(),
    };
  }

  private attachLifecycleHooks(): void {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("pagehide", this.onPageHide);
    // "freeze"/"resume" are non-standard but available in Chromium lifecycle APIs.
    document.addEventListener("freeze", this.onPageFreeze as EventListener);
    document.addEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = true;
  }

  private detachLifecycleHooks(): void {
    if (!this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("pagehide", this.onPageHide);
    document.removeEventListener("freeze", this.onPageFreeze as EventListener);
    document.removeEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = false;
  }

  private sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isShuttingDown || !this.workerEndpoint) return;

    if (this.workerBridge) {
      this.workerBridge.sendLifecycleHint(event);
      return;
    }

    this.workerEndpoint.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }

  private currentWorkerNamespace(): string {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker namespace is only available for driver.type='persistent'");
    }
    return this.primaryDbName ?? driver.dbName ?? this.config.appId;
  }

  private resetChannelName(): string | null {
    if (!this.primaryDbName) return null;
    return `jazz:storage-reset:${this.config.appId}:${this.primaryDbName}`;
  }

  private leaderLockName(): string | null {
    if (!this.primaryDbName) return null;
    return `jazz:leader:${this.config.appId}:${this.primaryDbName}:v1`;
  }

  /**
   * Subscribe to the cross-tab reset channel for this `(appId, dbName)`. When
   * any tab announces `storage-reset-start`, every other tab's `Db` quietly
   * tears itself down so the originating tab can safely delete the OPFS
   * namespace without racing a live leader worker.
   */
  private subscribeToResetChannel(): void {
    if (this.resetChannel) return;
    const name = this.resetChannelName();
    if (!name) return;
    if (typeof BroadcastChannel === "undefined") return;
    const channel = new BroadcastChannel(name);
    const listener = (event: MessageEvent): void => {
      const data = event.data as { type?: unknown } | null;
      if (!data || typeof data !== "object") return;
      if ((data as { type?: unknown }).type !== "storage-reset-start") return;
      // Another tab is about to wipe storage. Drop our local state so the
      // leader's dedicated worker (if it lives in this tab) releases its
      // OPFS handles, and so any further client/bridge creation refuses
      // rather than silently re-attaching to a now-deleted namespace.
      void this.shutdown().catch(() => undefined);
    };
    channel.addEventListener("message", listener);
    this.resetChannel = channel;
    this.resetChannelListener = listener;
  }

  private closeResetChannel(): void {
    if (!this.resetChannel) return;
    if (this.resetChannelListener) {
      this.resetChannel.removeEventListener("message", this.resetChannelListener);
    }
    try {
      this.resetChannel.close();
    } catch {
      // BroadcastChannel may already be closed.
    }
    this.resetChannel = null;
    this.resetChannelListener = null;
  }

  /**
   * Broadcast `storage-reset-start` on this Db's reset channel so every other
   * tab on the same `(appId, dbName)` releases its leader worker / follower
   * port before we try to delete the OPFS namespace.
   *
   * Throws if `BroadcastChannel` is unavailable: cross-tab coordination is a
   * correctness requirement for follower-initiated wipes (the leader tab's
   * dedicated worker would otherwise keep OPFS open), not a nice-to-have.
   * Callers should surface this error to the user rather than silently
   * removing the OPFS file while another tab is still writing to it.
   */
  private broadcastStorageResetStart(): void {
    const name = this.resetChannelName();
    if (!name) {
      throw new Error(
        "deleteClientStorage() cannot resolve a reset-channel name (no primaryDbName).",
      );
    }
    if (typeof BroadcastChannel === "undefined") {
      throw new Error(
        "deleteClientStorage() requires BroadcastChannel for cross-tab reset coordination, " +
          "but it is unavailable in this environment. Close other tabs on this origin and retry, " +
          "or call deleteClientStorage() from a tab that does support BroadcastChannel.",
      );
    }
    let channel: BroadcastChannel | null = null;
    try {
      channel = new BroadcastChannel(name);
      channel.postMessage({ type: "storage-reset-start" });
    } finally {
      try {
        channel?.close();
      } catch {
        // Already closed.
      }
    }
  }

  /**
   * Default deadline for acquiring the leader lock during
   * `deleteClientStorage()`. The supervisor's `shutdown()` is fast (low ms),
   * and reset listeners on other tabs also call `shutdown()` synchronously;
   * 15s is the same envelope used by `waitForInitialEndpoint`. If we hit the
   * timeout it almost always means a peer tab is non-responsive (e.g. JS
   * loop blocked, or it crashed while holding the lock without releasing).
   */
  private static readonly STORAGE_RESET_LOCK_TIMEOUT_MS = 15_000;

  /**
   * Run `fn` while holding the leader-tab lock for this namespace. Acts as a
   * cross-tab mutual exclusion: while we hold the lock, no other tab's
   * supervisor can win it and spawn a dedicated worker against this OPFS
   * namespace — meaning the file is guaranteed not to be reopened mid-delete.
   *
   * Aborts the wait after `STORAGE_RESET_LOCK_TIMEOUT_MS` so the caller
   * surfaces an actionable error instead of hanging when another tab fails
   * to release the lock (e.g. a non-responsive page that ignored the
   * `storage-reset-start` broadcast). Once the lock is granted, `fn` runs
   * without a timeout — only the queued wait is bounded.
   *
   * If `navigator.locks` is unavailable, runs `fn` without the lock — the
   * broadcast + per-tab shutdown is the only safeguard in that environment.
   */
  private async withLeaderLockHeld<T>(fn: () => Promise<T>): Promise<T> {
    const lockName = this.leaderLockName();
    const nav = (globalThis as { navigator?: { locks?: { request: Function } } }).navigator;
    if (!lockName || !nav?.locks || typeof nav.locks.request !== "function") {
      return await fn();
    }
    const ac = typeof AbortController !== "undefined" ? new AbortController() : null;
    const timer = ac ? setTimeout(() => ac.abort(), Db.STORAGE_RESET_LOCK_TIMEOUT_MS) : null;
    let result!: T;
    let acquired = false;
    try {
      await nav.locks.request(
        lockName,
        { mode: "exclusive", ...(ac ? { signal: ac.signal } : {}) },
        async () => {
          acquired = true;
          result = await fn();
        },
      );
    } catch (error) {
      // `navigator.locks.request` rejects with `AbortError` when the signal
      // fires before the lock is granted. Translate to an actionable error.
      const name = (error as { name?: string } | undefined)?.name;
      if (!acquired && (name === "AbortError" || ac?.signal.aborted)) {
        throw new Error(
          `Timed out after ${Db.STORAGE_RESET_LOCK_TIMEOUT_MS}ms waiting for the leader lock on ` +
            `"${lockName}". Another tab is still holding the namespace open — close it and retry.`,
        );
      }
      throw error;
    } finally {
      if (timer !== null) clearTimeout(timer);
    }
    return result;
  }

  /**
   * Remove the OPFS file backing `namespace`, retrying transient
   * OPFS-locked failures.
   *
   * By the time this runs we hold the leader lock, so the leader tab's
   * dedicated worker has finished `runShutdown()` — it was gracefully
   * shut down *and* `terminate()`d. But Chrome releases a terminated
   * worker's `FileSystemSyncAccessHandle` only when it finishes tearing
   * the worker down, which happens asynchronously after `terminate()`
   * returns (typically within a few hundred ms). Until then
   * `removeEntry` rejects with `NoModificationAllowedError` /
   * `InvalidStateError`.
   *
   * Holding the leader lock proves leadership was released; it does not
   * prove the OS-level handle is gone. So we poll the real condition —
   * retry the delete with capped backoff — the same handle conflict the
   * OPFS *open* path already absorbs (`OpfsFile::open` in
   * `crates/opfs-btree/src/file.rs`). If it is still locked after the
   * budget, a live tab is genuinely holding it open: surface the
   * actionable error.
   */
  private async removeBrowserStorageNamespace(namespace: string): Promise<void> {
    const rootDirectory = await navigator.storage.getDirectory();
    const fileName = `${namespace}.opfsbtree`;

    // ~4.25s total budget across 12 attempts: well past the few-hundred-ms
    // GC lag, bounded so a genuinely stuck handle still surfaces an error.
    const maxAttempts = 12;
    const baseDelayMs = 50;
    const maxDelayMs = 500;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        await rootDirectory.removeEntry(fileName, { recursive: false });
        return;
      } catch (error) {
        const name = (error as { name?: string } | undefined)?.name;
        if (name === "NotFoundError") {
          return;
        }
        const opfsLocked = name === "NoModificationAllowedError" || name === "InvalidStateError";
        if (opfsLocked && attempt < maxAttempts - 1) {
          await sleep(Math.min(baseDelayMs * 2 ** attempt, maxDelayMs));
          continue;
        }
        if (opfsLocked) {
          throw new Error(
            `Failed to delete browser storage for "${namespace}" because OPFS is still ` +
              `locked after ${maxAttempts} attempts. Another tab is holding it open — ` +
              `close it and retry.`,
          );
        }
        throw new Error(
          `Failed to delete browser storage for "${namespace}": ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  private static sharedWorkerName(config: DbConfig, dbName: string): string {
    return `jazz:${config.appId}:${dbName}`;
  }

  /**
   * Spawn the SharedWorker port broker (`jazz-shared-worker.js`). The broker
   * does not run any runtime code — it only relays MessagePorts between tabs
   * and the current leader tab (see {@link installSharedWorkerBroker}). No
   * bootstrap handshake is needed.
   */
  private static spawnSharedWorkerBroker(config: DbConfig, dbName: string): SharedWorker {
    if (typeof SharedWorker === "undefined") {
      throw new Error("SharedWorker is not available in this environment");
    }

    const runtimeSources = config.runtimeSources;
    let sharedWorkerUrl: string | URL;
    const hasDynamicRuntimeUrl =
      !!runtimeSources?.sharedWorkerUrl ||
      !!runtimeSources?.baseUrl ||
      !!runtimeSources?.workerUrl ||
      !!runtimeSources?.wasmUrl;

    if (hasDynamicRuntimeUrl) {
      const locationHref = typeof location !== "undefined" ? location.href : undefined;
      const syncInitInput = resolveRuntimeConfigSyncInitInput(runtimeSources);
      const wasmUrl = syncInitInput
        ? null
        : resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources);
      const childWorkerUrl = runtimeSources?.workerUrl
        ? resolveRuntimeConfigWorkerUrl(import.meta.url, locationHref, runtimeSources)
        : null;

      sharedWorkerUrl = appendSharedWorkerRuntimeUrls(
        resolveRuntimeConfigSharedWorkerUrl(import.meta.url, locationHref, runtimeSources),
        {
          wasmUrl,
          workerUrl: childWorkerUrl,
        },
      );
    } else {
      sharedWorkerUrl = new URL("../worker/jazz-shared-worker.js", import.meta.url);
    }

    return new SharedWorker(sharedWorkerUrl, {
      type: "module",
      name: Db.sharedWorkerName(config, dbName),
    });
  }

  updateAuthToken(jwtToken: string | null): void {
    this.applyAuthUpdate(jwtToken);
  }

  updateCookieSession(cookieSession: Session | null): void {
    this.applyCookieSessionUpdate(cookieSession);
  }

  getAuthState(): AuthState {
    return this.authStateStore.getState();
  }

  /**
   * Mint a short-lived local-first JWT proving possession of the current identity.
   * Returns `null` if the current session is not local-first.
   */
  getLocalFirstIdentityProof(options?: { ttlSeconds?: number; audience?: string }): string | null {
    if (!this._localFirstSecret) {
      return null;
    }

    const ttl = options?.ttlSeconds ?? 60;
    const audience = options?.audience ?? this.config.appId;
    return this.mintLocalFirstToken(this._localFirstSecret, audience, ttl);
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.authStateStore.onChange((state) => {
      listener(state);
    });
  }

  /**
   * Attach a fallback listener to be notified when a write operation
   * (insert, update, delete) is rejected.
   * This callback is only called if the write error is not surfaced by
   * {@link WriteHandle.wait}.
   * This callback is called even after app restarts (which does not
   * happen with {@link WriteHandle.wait}).
   * @returns an unsubscribe callback
   */
  onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.mutationErrorListeners.add(listener);
    while (this.pendingMutationErrorEvents.length > 0) {
      const event = this.pendingMutationErrorEvents.shift()!;
      listener(event);
    }
    return () => {
      this.mutationErrorListeners.delete(listener);
    };
  }

  getConfig(): DbConfig {
    // Return a copy of the config to avoid editing the original config.
    return structuredClone(this.config);
  }

  setDevMode(enabled: boolean): void {
    this.config.devMode = enabled;
  }

  /**
   * @internal
   */
  getActiveQuerySubscriptions(): ActiveQuerySubscriptionTrace[] {
    return Array.from(this.activeQuerySubscriptionTraces.values())
      .filter((trace) => trace.visibility === "public")
      .map(({ visibility: _visibility, ...trace }) => cloneActiveQuerySubscriptionTrace(trace));
  }

  /**
   * @internal
   */
  onActiveQuerySubscriptionsChange(listener: ActiveQuerySubscriptionTraceListener): () => void {
    this.activeQuerySubscriptionTraceListeners.add(listener);
    listener(this.getActiveQuerySubscriptions());
    return () => {
      this.activeQuerySubscriptionTraceListeners.delete(listener);
    };
  }

  /**
   * Insert a new row into a table without waiting for durability.
   *
   * Use {@link WriteResult.wait} to wait for durable confirmation.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns Write result containing the inserted row
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: CreateOptions): WriteResult<T> {
    const client = this.getClient(table._schema);
    // Don't wait for bridge to be ready in worker mode. Inserts will be propagated once the bridge is ready.
    // If the bridge fails to initialize, the insert will be lost on restart.
    const transformedData = transformInputColumns(table, data);
    const values = toInsertRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    const inserted = context
      ? client.createHandleInternal(
          table._table,
          values,
          context.session,
          context.attribution,
          options,
        )
      : client.create(table._table, values, options);
    return inserted.mapValue((row) =>
      transformOutputRow(table, transformRow(row, table._schema, table._table)),
    );
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  upsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Partial<Init>,
    options: UpsertOptions,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const values = toUpdateRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    return context
      ? client.upsertHandleInternal(
          table._table,
          values,
          options.id,
          context.session,
          context.attribution,
          options.updatedAt,
        )
      : client.upsert(table._table, values, options);
  }

  /**
   * Update an existing row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const updates = toUpdateRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    return context
      ? client.updateHandleInternal(
          id,
          updates,
          context.session,
          context.attribution,
          undefined,
          options?.updatedAt,
        )
      : client.update(id, updates, options);
  }

  /**
   * Delete a row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): WriteHandle {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return context
      ? client.deleteHandleInternal(id, context.session, context.attribution)
      : client.delete(id);
  }

  /**
   * Begin a new transaction.
   *
   * Use transactions when several writes should settle together after an authority validates them.
   *
   * Use {@link DbTransaction.commit} to commit the transaction.
   *
   * Prefer using {@link Db.transaction} when an explicit commit is not required.
   */
  beginTransaction(): DbTransaction {
    const context = this.getRuntimeOperationContext();
    return new DbTransaction(
      (schema) => this.getClient(schema),
      (client) => client.beginTransactionInternal(context?.session, context?.attribution),
    );
  }

  /**
   * Run {@link callback} inside a transaction and commit it once the callback returns.
   *
   * Use transactions when several writes should settle together after an authority validates them.
   *
   * @returns a write result containing the result of the callback
   */
  transaction<TResult>(
    callback: (tx: DbTransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(callback: (tx: DbTransactionScope) => TResult): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: DbTransactionScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInBatch(
      transaction,
      callback,
      () => getDbBatchHandleBinding(transaction, "result", "DbTransaction").client,
    );
  }

  /**
   * Begin a new batch.
   *
   * Use a batch when several visible writes should settle together.
   * Call {@link DbDirectBatch.commit} to freeze the batch, then wait on the
   * returned handle if you need durable confirmation.
   *
   * Prefer using {@link Db.batch} when an explicit commit is not required.
   */
  beginBatch(): DbDirectBatch {
    const context = this.getRuntimeOperationContext();
    return new DbDirectBatch(
      (schema) => this.getClient(schema),
      (client) => client.beginBatchInternal(context?.session, context?.attribution),
    );
  }

  /**
   * Run {@link callback} inside a batch and commit it once the callback returns.
   *
   * Use a batch when several visible writes should settle together.
   *
   * @returns a write result containing the result of the callback
   */
  batch<TResult>(
    callback: (batch: DbBatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  batch<TResult>(callback: (batch: DbBatchScope) => TResult): WriteResult<TResult>;
  batch<TResult>(
    callback: (batch: DbBatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(
      batch,
      callback,
      () => getDbBatchHandleBinding(batch, "result", "DbDirectBatch").client,
    );
  }

  /**
   * Wipe this Db's browser OPFS namespace. Terminal: the `Db` is fully shut
   * down before the delete (so the leader's dedicated worker releases its
   * OPFS handles) and is not reusable afterwards — callers must recreate
   * the instance to keep working. `logout({ wipeData: true })` already
   * does this; direct callers should follow the same pattern.
   *
   * Cross-tab coordination:
   *
   * When called from any tab (leader *or* follower) on a namespace that is
   * also open in other tabs, this method first broadcasts
   * `storage-reset-start` on `jazz:storage-reset:${appId}:${dbName}`. Every
   * other tab's `Db` reacts by shutting itself down — releasing follower
   * ports and, critically, terminating the leader tab's dedicated worker so
   * OPFS handles are freed. Once all tabs have stepped down we acquire the
   * `navigator.locks` leader lock for this namespace and remove the OPFS
   * file while holding it, so no concurrent supervisor can win the lock and
   * spawn a worker against the namespace mid-delete.
   *
   * Without this coordination, a follower-initiated wipe would only tear
   * down the caller's bridge while the leader's worker kept the OPFS file
   * open — the subsequent `removeEntry` would either fail with
   * `NoModificationAllowedError` or, worse, succeed and leave the leader
   * writing into a deleted/recreated namespace.
   *
   * Behavior:
   * - Browser persistent (SharedWorker-backed) Db only; throws otherwise
   * - Can be initiated from either leader or follower tabs
   * - Does not touch localStorage-based local-first auth state
   *
   * Failure modes (all thrown before any destructive work happens):
   * - `BroadcastChannel` unavailable in this environment → throws; cross-tab
   *   coordination is a correctness requirement, not a nice-to-have.
   * - Another tab holds the leader lock beyond
   *   {@link Db.STORAGE_RESET_LOCK_TIMEOUT_MS} (e.g. it ignored or didn't
   *   process the `storage-reset-start` broadcast) → throws an actionable
   *   error asking the user to close the offending tab and retry. We have
   *   already shut down our own `Db` by this point, so the caller must
   *   recreate the instance regardless.
   */
  async deleteClientStorage(): Promise<void> {
    if (resolveStorageDriver(this.config.driver).type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }

    if (!isBrowser() || !this.supervisor) {
      console.error(
        "deleteClientStorage() is only available on browser SharedWorker-backed Db instances.",
      );
      return;
    }

    // Capture the namespace before `shutdown()` clears the supervisor/config
    // pathway that `currentWorkerNamespace()` resolves through.
    const namespace = this.currentWorkerNamespace();

    // 1. Tell every other tab on this namespace to release its bridge /
    //    leader worker before we try to delete. Their reset-channel
    //    listener calls `this.shutdown()` — terminating the leader tab's
    //    dedicated worker, which is what actually frees OPFS.
    this.broadcastStorageResetStart();

    // 2. Tear ourselves down. If we were the leader this terminates our
    //    own dedicated worker (and releases the lock); if we were a
    //    follower, our port closes here while the leader is being told
    //    to step down by step 1.
    await this.shutdown();

    // 3. Hold the leader-tab lock across the delete so no other tab's
    //    supervisor can promote itself to leader (and reopen the OPFS
    //    file) while `removeEntry` is in flight. `navigator.locks`
    //    serializes requests — if other tabs are queued waiting for the
    //    lock, they'll only get it once we release, after the file is
    //    gone. Their listener-driven shutdown also prevents them from
    //    contending in the first place, but the lock is the belt to the
    //    broadcast's braces.
    await this.withLeaderLockHeld(async () => {
      await this.removeBrowserStorageNamespace(namespace);
    });
  }

  /**
   * Release the current Db instance for logout flows.
   *
   * When `wipeData` is enabled in browser persistent mode, Jazz first coordinates a cross-tab OPFS
   * wipe and then shuts this Db down. Callers should still sign out of their external auth provider
   * separately and recreate `JazzProvider` / `Db` after logout.
   */
  async logout(options: LogoutOptions = {}): Promise<void> {
    if (options.wipeData) {
      await this.deleteClientStorage();
    }

    await this.shutdown();
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const client = this.getClient(query._schema);
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(client.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const queryOptions = ordinaryDbQueryOptions(options);
    await this.ensureQueryReady(queryOptions);
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
    const runtimeQueryOptions = usesRelationTraversal
      ? { ...queryOptions, runtimeSettledTier: null }
      : queryOptions;
    const context = this.getRuntimeOperationContext();
    const rows =
      context || usesRelationTraversal
        ? await client.queryInternal(
            wasmQuery,
            context?.session,
            runtimeQueryOptions,
            runtimeSchema.peek(),
          )
        : await client.query(wasmQuery, queryOptions);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const transformedRows = transformRows(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      builtQuery.select,
    );
    return transformedRows.map((row) =>
      transformOutputRow(outputTable === builtQuery.table ? query : {}, row),
    );
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param options Optional read durability options
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }

  /**
   * Create a conventional `files` row by chunking a browser Blob into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromBlob<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    blob: Blob,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createConventionalFileStorage(this, app).fromBlob(blob, options);
  }

  /**
   * Create a conventional `files` row by chunking a browser ReadableStream into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromStream<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    stream: ReadableStream<unknown>,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createConventionalFileStorage(this, app).fromStream(stream, options);
  }

  /**
   * Load a conventional file as a browser ReadableStream by querying the file row first
   * and then reading each referenced `file_parts` row sequentially.
   */
  async loadFileAsStream<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<ReadableStream<Uint8Array>> {
    return createConventionalFileStorage(this, app).toStream(fileOrId, options);
  }

  /**
   * Load a conventional file as a Blob using the same sequential part-query path as `loadFileAsStream`.
   */
  async loadFileAsBlob<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<Blob> {
    return createConventionalFileStorage(this, app).toBlob(fileOrId, options);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `delta`: Ordered list of row-level changes
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === 0) {
   *       console.log("New row:", change.row);
   *     }
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(client.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const nativeOutputColumns = resolveNativeSubscriptionColumns(
      outputTable,
      outputSchema,
      outputIncludes,
      builtQuery.select,
    );
    const wasmQuery = translateQuery(builderJson, planningSchema);

    const transform = (row: WasmRow): T =>
      transformOutputRow(
        outputTable === builtQuery.table ? query : {},
        transformRow(row, outputSchema, outputTable, outputIncludes, builtQuery.select),
      );
    const nativeTransform =
      Object.keys(outputIncludes).length === 0 && builtQuery.select.length === 0
        ? (row: Record<string, unknown>): T =>
            transformOutputRow(outputTable === builtQuery.table ? query : {}, row)
        : undefined;

    const handleDelta = (delta: Parameters<SubscriptionManager<T>["handleDelta"]>[0]) => {
      const typedDelta = manager.handleDelta(
        delta,
        transform,
        nativeOutputColumns,
        nativeTransform,
      );
      callback(typedDelta);
    };

    const queryOptions = ordinaryDbQueryOptions(options);
    const context = this.getRuntimeOperationContext();
    const subId =
      context || session
        ? client.subscribeInternal(
            wasmQuery,
            handleDelta,
            context?.session ?? session,
            queryOptions,
            runtimeSchema.peek(),
          )
        : client.subscribe(wasmQuery, handleDelta, queryOptions);
    const traceId = this.registerActiveQuerySubscriptionTrace(
      wasmQuery,
      builtQuery.table,
      queryOptions,
    );

    // Return unsubscribe function
    return () => {
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      client.unsubscribe(subId);
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   *
   * Idempotent: concurrent or repeated calls share the same in-flight promise.
   */
  async shutdown(): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shutdownPromise = this.runShutdown();
    return this.shutdownPromise;
  }

  private async runShutdown(): Promise<void> {
    this.isShuttingDown = true;
    if (this.localFirstRefreshTimer) {
      clearTimeout(this.localFirstRefreshTimer);
      this.localFirstRefreshTimer = null;
    }
    this.clearActiveQuerySubscriptionTraces();
    this.detachLifecycleHooks();

    // Wait for bridge init to settle before tearing the bridge down —
    // otherwise the worker may still be opening OPFS handles. A *failed*
    // init must not abort teardown: the supervisor (leader lock), the
    // SharedWorker broker port and the reset channel still need releasing,
    // or a worker-init failure strands the namespace's leader lease and
    // dedicated worker for every other tab. Capture the error, finish the
    // teardown best-effort, and rethrow it once the runtime is fully down.
    let bridgeInitError: unknown;
    let bridgeInitFailed = false;
    try {
      await this.ensureBridgeReady();
    } catch (error) {
      bridgeInitError = error;
      bridgeInitFailed = true;
    }

    // Shutdown worker bridge — waits for OPFS handles to be released.
    if (this.workerBridge && this.workerEndpoint) {
      try {
        await this.workerBridge.shutdown();
      } catch {
        // Best-effort: a bridge teardown failure must not strand the
        // supervisor's leader lock released below.
      }
      this.workerBridge = null;
    }

    this.mutationErrorListeners.clear();
    this.disposeWasmTelemetry?.();
    this.disposeWasmTelemetry = null;
    for (const client of this.clients.values()) {
      try {
        await client.shutdown();
      } catch {
        // Best-effort, as above.
      }
    }
    this.clients.clear();

    if (this.supervisor) {
      try {
        await this.supervisor.shutdown();
      } finally {
        this.supervisor = null;
      }
    }
    this.supervisorUnsubscribe?.();
    this.supervisorUnsubscribe = null;
    this.closeResetChannel();
    this.sharedWorker?.port.close();
    this.sharedWorker = null;
    this.workerEndpoint = null;

    // Teardown ran to completion; surface a failed bridge init to the caller.
    // The Db never became usable, and callers (`deleteClientStorage`) and
    // tests rely on `shutdown()` rejecting in that case.
    if (bridgeInitFailed) {
      throw bridgeInitError;
    }
  }

  private notifyActiveQuerySubscriptionTraceListeners(): void {
    if (this.activeQuerySubscriptionTraceListeners.size === 0) {
      return;
    }

    const snapshot = this.getActiveQuerySubscriptions();
    for (const listener of this.activeQuerySubscriptionTraceListeners) {
      listener(snapshot);
    }
  }

  private registerActiveQuerySubscriptionTrace(
    queryJson: string,
    fallbackTable: string,
    options?: QueryOptions,
  ): string | null {
    if (!this.config.devMode) {
      return null;
    }

    const resolvedOptions = resolveEffectiveQueryExecutionOptions(this.config, options);
    const payload = this.parseRuntimeQueryTracePayload(queryJson, fallbackTable);
    const traceId = `sub-${this.nextActiveQuerySubscriptionTraceId++}`;

    this.activeQuerySubscriptionTraces.set(traceId, {
      id: traceId,
      query: queryJson,
      table: payload.table,
      branches: payload.branches,
      tier: resolvedOptions.tier,
      propagation: resolvedOptions.propagation,
      createdAt: new Date().toISOString(),
      stack: trimSubscriptionTraceStack(new Error().stack),
      visibility: resolvedOptions.visibility ?? "public",
    });
    this.notifyActiveQuerySubscriptionTraceListeners();

    return traceId;
  }

  private unregisterActiveQuerySubscriptionTrace(traceId: string | null): void {
    if (!traceId) {
      return;
    }
    if (!this.activeQuerySubscriptionTraces.delete(traceId)) {
      return;
    }
    this.notifyActiveQuerySubscriptionTraceListeners();
  }

  private clearActiveQuerySubscriptionTraces(): void {
    if (this.activeQuerySubscriptionTraces.size === 0) {
      return;
    }
    this.activeQuerySubscriptionTraces.clear();
    this.notifyActiveQuerySubscriptionTraceListeners();
  }

  private parseRuntimeQueryTracePayload(
    queryJson: string,
    fallbackTable: string,
  ): RuntimeQueryTracePayload {
    try {
      const parsed = JSON.parse(queryJson) as { table?: unknown; branches?: unknown };
      const table = typeof parsed.table === "string" ? parsed.table : fallbackTable;
      const branches = Array.isArray(parsed.branches)
        ? parsed.branches.filter((branch): branch is string => typeof branch === "string")
        : [];

      return {
        table,
        branches: branches.length > 0 ? branches : [this.config.userBranch ?? "main"],
      };
    } catch {
      return {
        table: fallbackTable,
        branches: [this.config.userBranch ?? "main"],
      };
    }
  }
}

/**
 * A Db implementation that delegates all operations to an existing {@link JazzClient}.
 * Used only for tests.
 */
class ClientBackedDb extends Db {
  private readonly hasScopedAuthState: boolean;

  constructor(
    config: DbConfig,
    private readonly runtimeClient: JazzClient,
    private readonly session?: Session,
    private readonly attribution?: string,
    scopedAuthState?: AuthState,
  ) {
    super(
      config,
      null,
      scopedAuthState
        ? {
            initialState: scopedAuthState,
            lockAuthenticatedState: true,
          }
        : undefined,
    );
    this.hasScopedAuthState = scopedAuthState !== undefined;
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.runtimeClient;
  }

  override updateAuthToken(jwtToken: string | null): void {
    if (this.hasScopedAuthState) {
      return;
    }

    if (!this.applyAuthUpdate(jwtToken)) {
      return;
    }

    this.runtimeClient.updateAuthToken(jwtToken ?? undefined);
  }

  override onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.runtimeClient.onMutationError(listener);
    return () => {
      /* Do nothing */
    };
  }

  override updateCookieSession(cookieSession: Session | null): void {
    if (this.hasScopedAuthState) {
      return;
    }

    if (!this.applyCookieSessionUpdate(cookieSession)) {
      return;
    }

    this.runtimeClient.updateCookieSession(cookieSession ?? undefined);
  }

  protected override getRuntimeOperationContext(): DbRuntimeOperationContext {
    return {
      session: this.session,
      attribution: this.attribution,
    };
  }

  override async shutdown(): Promise<void> {
    // The owning JazzContext owns the runtime lifecycle.
  }
}

/**
 * Check if running in a browser environment with Worker support.
 */
function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

function isSharedWorkerAvailable(): boolean {
  return typeof SharedWorker !== "undefined";
}

function hasNavigatorLocks(): boolean {
  const nav = (globalThis as { navigator?: { locks?: unknown } }).navigator;
  return !!nav && typeof nav.locks === "object" && nav.locks !== null;
}

/**
 * Generate a 32-byte ephemeral seed for anonymous auth.
 *
 * Uses `globalThis.crypto.getRandomValues`, which is available in all
 * supported environments (browser, Node ≥15, React Native, edge workers).
 */
function generateEphemeralSeedBase64Url(): string {
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Create a new Db instance with the given configuration.
 *
 * This is an **async** factory function that pre-loads the runtime module.
 * After creation, local-first mutations (`insert`/`update`/`delete`) are synchronous.
 * Use the `wait` method when you need a Promise that resolves at a durability tier.
 *
 * In browser environments, automatically uses a dedicated worker for
 * OPFS persistence. In Node.js, uses in-memory storage.
 *
 * @param config Database configuration
 * @returns Promise resolving to Db instance ready for queries and mutations
 *
 * @example
 * ```typescript
 * const db = await createDb({
 *   appId: "my-app",
 *   schema: mySchema,
 * });
 * ```
 */
function createRuntimeTokenOptions(
  secret: string,
  audience: string,
  ttlSeconds: number,
): RuntimeTokenOptions {
  return {
    secret,
    audience,
    ttlSeconds,
    nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
  };
}

export async function createDbWithRuntimeModule<RuntimeConfig extends DbConfig>(
  config: RuntimeConfig,
  runtimeModule: DbRuntimeModule<RuntimeConfig>,
): Promise<Db> {
  if (config.secret && (config.jwtToken || config.cookieSession)) {
    throw new Error("DbConfig error: secret, jwtToken, and cookieSession are mutually exclusive");
  }
  if (config.jwtToken && config.cookieSession) {
    throw new Error("DbConfig error: jwtToken and cookieSession are mutually exclusive");
  }

  let resolvedConfig = { ...config };
  await runtimeModule.load(config);

  // Local-first auth: resolve seed and mint a JWT
  let localFirstSecret: string | null = null;
  if (config.secret) {
    const secret = config.secret;
    localFirstSecret = secret;

    const jwtToken = runtimeModule.mintLocalFirstToken(
      createRuntimeTokenOptions(secret, config.appId, 3600),
    );
    resolvedConfig = { ...resolvedConfig, jwtToken };
  } else if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    // Anonymous: mint an ephemeral keypair + anonymous JWT.
    // Admin-secret clients intentionally stay sessionless so local policy
    // evaluation does not preempt backend-authorized transport writes.
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const jwtToken = runtimeModule.mintAnonymousToken(
      createRuntimeTokenOptions(ephemeralSeed, config.appId, 3600),
    );
    resolvedConfig = { ...resolvedConfig, jwtToken };
  }

  const driver = resolveStorageDriver(resolvedConfig.driver);

  if (driver.type === "memory" && !resolvedConfig.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }

  let db: Db;
  if (
    runtimeModule.supportsBrowserWorker !== false &&
    isBrowser() &&
    driver.type === "persistent"
  ) {
    if (!isSharedWorkerAvailable()) {
      throw new Error(
        "This browser does not support SharedWorker, which Jazz requires for persistent browser storage. Please update your browser or configure driver.type='memory'.",
      );
    }
    if (!hasNavigatorLocks()) {
      throw new Error(
        "driver.type='persistent' in the browser requires navigator.locks support, which this environment lacks.",
      );
    }
    db = await Db.createWithSharedWorker(resolvedConfig, runtimeModule as AnyDbRuntimeModule);
  } else {
    db = Db.create(resolvedConfig, runtimeModule as AnyDbRuntimeModule);
  }

  if (localFirstSecret) {
    db.initLocalFirstAuth(localFirstSecret, 3600);
  }

  return db;
}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithRuntimeModule(config, new WasmRuntimeModule());
}

export function createDbFromClient(
  config: DbConfig,
  client: JazzClient,
  session?: Session,
  attribution?: string,
  scopedAuthState?: AuthState,
): Db {
  return new ClientBackedDb(
    config,
    client,
    session,
    attribution,
    scopedAuthState ??
      (session || attribution
        ? { authMode: session?.authMode ?? "external", session: session ?? null }
        : undefined),
  );
}
