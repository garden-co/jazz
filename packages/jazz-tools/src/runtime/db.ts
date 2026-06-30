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
import { normalizeRuntimeSchema } from "../drivers/schema-wire.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  WriteResult,
  JazzClient,
  type MutationErrorEvent,
  WriteHandle,
  type BatchMode,
  type CreateOptions,
  type RestoreOptions,
  type UpdateOptions,
  type UpsertOptions,
  type DurabilityTier,
  type QueryExecutionOptions,
  type QueryPropagation,
  type QueryVisibility,
  resolveEffectiveQueryExecutionOptions,
  runInBatch,
  Scoped,
  type DeleteOptions,
} from "./client.js";
import { type DbRuntimeModule, type RuntimeTokenOptions } from "./db-runtime-module.js";
import { WasmRuntimeModule } from "./wasm-runtime-module.js";
import {
  isIncompatibleBrowserBrokerConfigurationError,
  type IncompatibleBrowserBrokerConfigurationHandler,
} from "./browser-broker-errors.js";
import type { AuthFailureReason } from "./sync-transport.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toWriteRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import {
  createConventionalFileStorage,
  type ConventionalFileApp,
  type FileReadOptions,
  type FileWriteOptions,
} from "./file-storage.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { isPermissionIntrospectionColumn, magicColumnType } from "../magic-columns.js";
import {
  normalizeBuiltQuery,
  type BuiltRelation,
  type NormalizedIncludeSpec,
  type NormalizedBuiltQuery,
} from "./query-builder-shape.js";
import { resolveSelectedColumns } from "./select-projection.js";
import {
  BrowserConnectionManager,
  DirectConnectionManager,
  type ConnectionManager,
  type DbForConnection,
} from "./connection-manager/index.js";

export { resolveDefaultPersistentDbName } from "./connection-manager/browser-broker-utils.js";

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
  /**
   * Called when this tab cannot join the persistent browser broker because
   * another tab is already connected with an incompatible app/runtime version.
   * The default browser behavior shows a reload prompt.
   */
  onIncompatibleBrowserBrokerConfiguration?: IncompatibleBrowserBrokerConfigurationHandler;
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

function limitQueryToOne<T>(query: QueryBuilder<T>): QueryBuilder<T> {
  return {
    get _table() {
      return query._table;
    },
    get _schema() {
      return query._schema;
    },
    get _columnTransforms() {
      return query._columnTransforms;
    },
    get _rowType() {
      return query._rowType;
    },
    _build() {
      const builtQuery = JSON.parse(query._build()) as Record<string, unknown>;
      builtQuery.limit = 1;
      return JSON.stringify(builtQuery);
    },
  };
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
} {
  let cachedRuntimeSchema: WasmSchema | undefined;

  return {
    get: () => {
      if (!cachedRuntimeSchema) {
        cachedRuntimeSchema = getRuntimeSchema();
      }
      return cachedRuntimeSchema;
    },
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
    `${operation} is bound to the schema chosen by the first table used and cannot be used with table "${table._table}" from a different schema.`,
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
  batchId: string;
  session?: Session;
  attribution?: string;
};
type AnyDbBatchHandle = DbBatchHandleBase;

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
abstract class DbBatchHandleBase {
  constructor(
    private readonly bindingName: "DbTransaction" | "DbDirectBatch",
    private readonly batchMode: BatchMode,
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  private bindTable<T, Init>(table: TableProxy<T, Init>, operation: string): DbBatchHandleBinding {
    const existingBinding = dbBatchHandleBindings.get(this);
    if (existingBinding) {
      assertTableBelongsToClient(table, existingBinding.client, this.resolveClient, operation);
      return existingBinding;
    }

    const client = this.resolveClient(table._schema);
    const batchId = client.beginBatch(this.batchMode);
    const binding = {
      client,
      batchId,
      session: this.session,
      attribution: this.attribution,
    };
    dbBatchHandleBindings.set(this, binding);
    return binding;
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbBatchHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>, this.bindingName);
  }

  private requireBinding(operation: string): DbBatchHandleBinding {
    return getDbBatchHandleBinding(this, operation, this.bindingName);
  }

  batchId(): string {
    return this.requireBinding("batchId").batchId;
  }

  /**
   * Commit this batch.
   */
  commit(): WriteHandle {
    const { client, batchId } = this.requireBinding("commit");
    return client.commitBatch(batchId);
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
    const { client, batchId } = this.requireBinding("rollback");
    client.rollbackBatch(batchId);
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
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const { client, batchId, session, attribution } = this.requireBinding("insert");
    const row = client.insertInternal(table._table, values, options, session, attribution, batchId);
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Restore a soft-deleted row.
   *
   * The restore is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  restore<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: RestoreOptions,
  ): T {
    this.bindTable(table, this.bindingName);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const { client, batchId, session, attribution } = this.requireBinding("restore");
    const row = client.restoreInternal(
      table._table,
      id,
      values,
      options,
      session,
      attribution,
      batchId,
    );
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
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const { client, batchId, session, attribution } = this.requireBinding("upsert");
    client.upsertInternal(table._table, values, options, session, attribution, batchId);
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
    const updates = toWriteRecord(transformedData, table._schema, table._table);
    const { client, batchId, session, attribution } = this.requireBinding("update");
    client.updateInternal(id, updates, undefined, session, attribution, batchId);
  }

  /**
   * Delete an existing row from a table.
   *
   * The delete is scoped to this batch, and will only be globally visible
   * once it's committed.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const { client, batchId, session, attribution } = this.bindTable(table, this.bindingName);
    client.deleteInternal(id, undefined, session, attribution, batchId);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this batch.
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const { client, batchId, session } = this.bindQuery(query);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const rows = await client.query(
      translateQuery(builderJson, planningSchema),
      {
        ...options,
        localUpdates: "deferred",
        transactionBatchId: batchId,
      },
      session,
    );
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
   * Execute a query with a limit of one and return the first matching row, or null.
   *
   * Read data is scoped to this batch.
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(limitQueryToOne(query), options);
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
export class DbTransaction extends DbBatchHandleBase {
  constructor(
    resolveClient: (schema: WasmSchema) => JazzClient,
    session?: Session,
    attribution?: string,
  ) {
    super("DbTransaction", "transactional", resolveClient, session, attribution);
  }
}

/**
 * Transaction object available inside {@link Db.transaction}'s callback.
 */
export type TransactionScope = Scoped<DbTransaction>;

/**
 * Direct batches group a set of writes that should become visible together on batch commit,
 * without waiting for an authority approval.
 */
export class DbDirectBatch extends DbBatchHandleBase {
  constructor(
    resolveClient: (schema: WasmSchema) => JazzClient,
    session?: Session,
    attribution?: string,
  ) {
    super("DbDirectBatch", "direct", resolveClient, session, attribution);
  }
}

/**
 * Batch object available inside {@link Db.batch}'s callback.
 */
export type BatchScope = Scoped<DbDirectBatch>;

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
  private config: DbConfig;
  private readonly runtimeModule: AnyDbRuntimeModule | null;
  private readonly authStateStore;
  private connection: ConnectionManager;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;
  private readonly activeQuerySubscriptionTraces = new Map<
    string,
    StoredActiveQuerySubscriptionTrace
  >();
  private readonly activeQuerySubscriptionTraceListeners =
    new Set<ActiveQuerySubscriptionTraceListener>();
  /**
   * Listeners attached with {@link Db.onMutationError} that are notified when a write operation
   * (insert, update, delete) is rejected. Errors from the Db's client are forwarded to all Db
   * listeners.
   */
  private readonly mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  /**
   * Persists mutation errors thrown before an {@link onMutationError} listener was attached.
   * Those mutation errors are replayed when `onMutationError` is called.
   */
  private readonly pendingMutationErrorEvents: MutationErrorEvent[] = [];
  private nextActiveQuerySubscriptionTraceId = 1;

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
    this.connection = new DirectConnectionManager(this.dbForConnection());
  }

  private dbForConnection(): DbForConnection {
    // oxlint-disable-next-line typescript/no-this-alias
    const thisDb = this;
    return {
      get config() {
        return thisDb.config;
      },
      get runtimeModule() {
        return thisDb.runtimeModule;
      },
      get isShuttingDown() {
        return thisDb.isShuttingDown;
      },
      markUnauthenticated: (reason) => this.markUnauthenticated(reason),
      onMutationError: (event) => this.handleMutationError(event),
    };
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

    this.connection.updateAuth({ jwtToken });

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

    this.connection.updateAuth({ cookieSession });

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
  static async createWithWorker(config: DbConfig, runtimeModule: AnyDbRuntimeModule): Promise<Db> {
    const db = new Db(config, runtimeModule);
    const connectionManager = new BrowserConnectionManager(db.dbForConnection());
    db.connection = connectionManager;
    await connectionManager.start();
    return db;
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because the runtime module is loaded before Db is created.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  protected getClient(schema: WasmSchema): JazzClient {
    return this.connection.getClient(schema);
  }

  protected getRuntimeOperationContext(): DbRuntimeOperationContext | null {
    return null;
  }

  /**
   * Ensures all listeners in {@link Db.mutationErrorListeners} are notified when
   * the active client reports a mutation error.
   */
  private handleMutationError(event: MutationErrorEvent): void {
    if (this.mutationErrorListeners.size === 0) {
      console.error("Unhandled Jazz mutation error", event);
      this.pendingMutationErrorEvents.push(event);
      return;
    }
    for (const listener of this.mutationErrorListeners) {
      listener(event);
    }
  }

  protected async ensureReady(tier?: DurabilityTier): Promise<void> {
    await this.connection.ensureReady(tier);
  }

  private wrapWriteWait<THandle extends WriteHandle<unknown>>(handle: THandle): THandle {
    const wait = handle.wait.bind(handle);
    handle.wait = (async (options: { tier: DurabilityTier }) => {
      await this.ensureReady(options.tier);
      return wait(options);
    }) as THandle["wait"];
    return handle;
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

  /**
   * The runtime schema of this Db's live client, normalized. Used by the
   * inspector overlay (a same-origin iframe) to render columns and build queries
   * against this connection without bridging or private-field access. Throws if
   * no client exists yet — run a query/subscription (or wait for connection)
   * first.
   */
  getRuntimeSchema(): WasmSchema {
    const schema = this.connection.getRuntimeSchema();
    if (!schema) {
      throw new Error(
        "Db.getRuntimeSchema(): no runtime client yet — run a query or wait for the connection.",
      );
    }
    return normalizeRuntimeSchema(schema);
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
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    const inserted = client.insert(
      table._table,
      values,
      options,
      context?.session,
      context?.attribution,
    );
    return this.wrapWriteWait(
      inserted.mapValue((row) =>
        transformOutputRow(table, transformRow(row, table._schema, table._table)),
      ),
    );
  }

  /**
   * Restore a soft-deleted row without waiting for durability.
   *
   * Use {@link WriteResult.wait} to wait for durable confirmation.
   */
  restore<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: RestoreOptions,
  ): WriteResult<T> {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    const restored = client.restore(
      table._table,
      id,
      values,
      options,
      context?.session,
      context?.attribution,
    );
    return this.wrapWriteWait(
      restored.mapValue((row) =>
        transformOutputRow(table, transformRow(row, table._schema, table._table)),
      ),
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
    const values = toWriteRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    return this.wrapWriteWait(
      client.upsert(table._table, values, options, context?.session, context?.attribution),
    );
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
    const updates = toWriteRecord(transformedData, table._schema, table._table);
    const context = this.getRuntimeOperationContext();
    return this.wrapWriteWait(
      client.update(id, updates, options, context?.session, context?.attribution),
    );
  }

  /**
   * Delete a row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): WriteHandle {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return this.wrapWriteWait(client.delete(id, options, context?.session, context?.attribution));
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
      context?.session,
      context?.attribution,
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
    callback: (tx: TransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(callback: (tx: TransactionScope) => TResult): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: TransactionScope) => TResult | Promise<TResult>,
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
      context?.session,
      context?.attribution,
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
    callback: (batch: BatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  batch<TResult>(callback: (batch: BatchScope) => TResult): WriteResult<TResult>;
  batch<TResult>(
    callback: (batch: BatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(
      batch,
      callback,
      () => getDbBatchHandleBinding(batch, "result", "DbDirectBatch").client,
    );
  }

  /**
   * Delete browser OPFS storage for this Db's active namespace and reopen a clean worker.
   *
   * This clears the brokered primary namespace for the same browser app/database. It does not touch
   * localStorage-based local-first auth state.
   *
   * Behavior:
   * - Browser worker-backed Db only (throws in non-browser/non-worker runtimes)
   * - Can be initiated from either leader or follower tabs
   * - Coordinates worker shutdown through the SharedWorker broker before deleting OPFS files
   * - Serializes with worker reconfigure operations
   * - Tears down worker + client, deletes OPFS files, and reconnects participating tabs
   */
  async deleteClientStorage(): Promise<void> {
    return this.connection.deleteClientStorage();
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
    await this.ensureReady(queryOptions.tier);
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
    const runtimeQueryOptions = usesRelationTraversal
      ? { ...queryOptions, runtimeSettledTier: null }
      : queryOptions;
    const context = this.getRuntimeOperationContext();
    const rows =
      context || usesRelationTraversal
        ? await client.query(wasmQuery, runtimeQueryOptions, context?.session)
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
   * Execute a query with a limit of one and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param options Optional read durability options
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(limitQueryToOne(query), options);
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
   * - `all`: Complete current result set. Freshly allocated on every delta —
   *   the rows are new object references each time, so diffing `all` by identity
   *   sees every row as changed. Reactive-framework consumers should reconcile
   *   with `applyDelta`/`reconcileArray` from `reconcile-array.js` to preserve
   *   identity for unchanged rows.
   * - `delta`: Ordered list of row-level changes (see `RowDelta`)
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * import { RowChangeKind } from "jazz-tools";
   *
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === RowChangeKind.Added) {
   *       console.log("New row:", change.item);
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
    let unsubscribed = false;
    let subId: number | null = null;
    let traceId: string | null = null;

    const startSubscription = () => {
      if (unsubscribed) return;
      subId = client.subscribe(wasmQuery, handleDelta, queryOptions, context?.session ?? session);
      traceId = this.registerActiveQuerySubscriptionTrace(
        wasmQuery,
        builtQuery.table,
        queryOptions,
      );
    };

    if (this.connection.shouldDeferSubscriptionStart()) {
      void this.ensureReady(queryOptions.tier)
        .then(startSubscription)
        .catch((error) => {
          if (unsubscribed) return;
          console.error("Failed to start Jazz subscription", error);
        });
    } else {
      startSubscription();
    }

    // Return unsubscribe function
    return () => {
      unsubscribed = true;
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      if (subId !== null) {
        client.unsubscribe(subId);
      }
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes the JazzClient and the worker.
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

    let shutdownError: unknown = null;

    try {
      await this.connection.shutdown();
    } catch (error) {
      shutdownError = error;
    }

    this.mutationErrorListeners.clear();

    if (shutdownError) {
      throw shutdownError;
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

const DEFAULT_BROWSER_BROKER_COMPATIBILITY_MESSAGE =
  "Another tab is using a different version of this app. Close the other tabs, then reload this page.\n\nReload now?";

function handleIncompatibleBrowserBrokerConfiguration(error: unknown, config: DbConfig): void {
  if (!isIncompatibleBrowserBrokerConfigurationError(error)) {
    return;
  }

  if (config.onIncompatibleBrowserBrokerConfiguration) {
    config.onIncompatibleBrowserBrokerConfiguration(error);
    return;
  }

  showDefaultIncompatibleBrowserBrokerConfigurationPrompt();
}

function showDefaultIncompatibleBrowserBrokerConfigurationPrompt(): void {
  if (typeof window === "undefined" || typeof window.confirm !== "function") {
    return;
  }

  if (window.confirm(DEFAULT_BROWSER_BROKER_COMPATIBILITY_MESSAGE)) {
    window.location.reload();
  }
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

  // Enable subscription tracing in dev so the inspector's Live Query sees the
  // subscriptions an app creates on mount, with no app config. Keyed on
  // "development" specifically (not `!== "production"`) to keep tracing off under
  // `test`/SSR; the runtime is the only client-creation seam shared by every
  // binding (svelte/vue/solid receive a pre-created client), so it lives here
  // rather than in the provider. Overridable explicitly via config.devMode.
  if (resolvedConfig.devMode === undefined && process.env.NODE_ENV === "development") {
    resolvedConfig.devMode = true;
  }
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
    try {
      db = await Db.createWithWorker(resolvedConfig, runtimeModule as AnyDbRuntimeModule);
    } catch (error) {
      handleIncompatibleBrowserBrokerConfiguration(error, resolvedConfig);
      throw error;
    }
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
