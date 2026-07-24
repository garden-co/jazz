/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design:
 * - createDb() is async (pre-loads the runtime source)
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
import { getRuntimeSchemaCacheKey } from "../drivers/schema-wire.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
  WriteResult,
  JazzClient,
  WriteHandle,
  type TransactionKind,
  type CreateOptions,
  type RestoreOptions,
  type UpdateOptions,
  type UpsertOptions,
  type DurabilityTier,
  type QueryExecutionOptions,
  type QueryPropagation,
  type QueryVisibility,
  resolveEffectiveQueryExecutionOptions,
  type DeleteOptions,
  type AuthConfig,
} from "./client.js";
import { type RuntimeSource, type RuntimeTokenOptions } from "./runtime-source.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { AuthFailureReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toWriteRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import type { SubscriptionChannel } from "./subscription-channel.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import { resolveClientSessionSync } from "./client-session.js";
import {
  createBinaryLargeValueFileStorage,
  type BinaryLargeValueFileApp,
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
import { resolveTelemetryCollectorUrlFromEnv } from "./sync-telemetry.js";

type WasmLogLevel = "error" | "warn" | "info" | "debug" | "trace";
type AnyRuntimeSource = RuntimeSource<any>;
type WriteOperationName = "Insert" | "Update" | "Upsert" | "Restore";

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
  /** Optional runtime source overrides for WASM loading. */
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
  /** Backend secret for backend-scoped sync auth with cookieSession. */
  backendSecret?: string;
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
  /**
   * Client-factory option. Defaults to true at `createJazzClient`: subscriptions
   * are served over an API-level channel and no main-thread Db is exposed.
   * `createDb` itself ignores this field.
   */
  asyncSubscriptionsOnly?: boolean;
  /**
   * Client-factory option used when `asyncSubscriptionsOnly` is true, or when a
   * synchronous-mode subscription opts into `subscriptionMode: "async"`.
   * `createDb` itself ignores this field.
   */
  subscriptionChannel?: SubscriptionChannel;
}

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

function shouldBypassLocalPolicies(config: DbConfig): boolean {
  return !!config.adminSecret;
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
  readSession?: Session;
};

function nativeDbQueryOptions(options?: QueryOptions): QueryOptions {
  if (!options?.subscriptionMode) {
    return options ?? {};
  }
  const { subscriptionMode: _subscriptionMode, ...nativeOptions } = options;
  return nativeOptions;
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
  return (
    builtQuery.hops.length > 0 ||
    builtQuery.gather !== undefined ||
    Object.keys(builtQuery.includes).length > 0
  );
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

function requireSchemaWithTable(preferredSchema: WasmSchema, tableName: string): WasmSchema {
  if (preferredSchema[tableName]) {
    return preferredSchema;
  }

  throw new Error(`Query schema is missing table "${tableName}".`);
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

function toWriteRecordForOperation(
  operation: WriteOperationName,
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
) {
  try {
    return toWriteRecord(data, schema, tableName);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${operation} failed: WriteError("${escapeWriteErrorReason(message)}")`);
  }
}

function escapeWriteErrorReason(message: string): string {
  return message.replaceAll('"', '\\"');
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

function assertTableBelongsToClient<T, Init>(
  table: TableProxy<T, Init>,
  expectedClient: JazzClient,
  resolveClient: (schema: WasmSchema) => JazzClient,
): void {
  if (resolveClient(table._schema) === expectedClient) {
    return;
  }
  throw new Error(
    `Transaction is bound to the client chosen by the first table used and cannot be used with table "${table._table}" from a different schema/client.`,
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

type DbTransactionHandleBinding = {
  client: JazzClient;
  transactionId: string;
  session?: Session;
  attribution?: string;
};

const dbTxHandleBindings = new WeakMap<Transaction, DbTransactionHandleBinding>();

function getDbTxHandleBinding(handle: Transaction, operation: string): DbTransactionHandleBinding {
  const binding = dbTxHandleBindings.get(handle);
  if (!binding) {
    throw new Error(`DbTransaction.${operation}() requires at least one table operation first`);
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

export type { TransactionKind } from "./client.js";

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as PromiseLike<T>).then === "function"
  );
}

type TransactionCommitHandle<TKind extends TransactionKind> = TKind extends "exclusive"
  ? ExclusiveWriteHandle
  : WriteHandle;

type TransactionWriteResult<TResult, TKind extends TransactionKind> = TKind extends "exclusive"
  ? ExclusiveWriteResult<TResult>
  : WriteResult<TResult>;

type RunInTransactionResult<TResult, TKind extends TransactionKind> =
  TResult extends PromiseLike<unknown>
    ? Promise<TransactionWriteResult<Awaited<TResult>, TKind>>
    : TransactionWriteResult<TResult, TKind>;

export type Scoped<TTransaction> = Omit<TTransaction, "commit" | "rollback">;

function createTransactionScope<TTransaction extends object>(
  transaction: TTransaction,
): Scoped<TTransaction> {
  return new Proxy(transaction, {
    get(target, property) {
      if (property === "commit" || property === "rollback") {
        return undefined;
      }

      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
    has(target, property) {
      if (property === "commit" || property === "rollback") {
        return false;
      }

      return Reflect.has(target, property);
    },
    set(target, property, value) {
      return Reflect.set(target, property, value, target);
    },
  }) as Scoped<TTransaction>;
}

function createTransactionWriteResult<TResult, TKind extends TransactionKind>(
  transaction: Transaction<TKind>,
  value: TResult,
  transactionId: string,
  client: JazzClient,
): TransactionWriteResult<TResult, TKind> {
  if (transaction.kind === "exclusive") {
    return new ExclusiveWriteResult(value, transactionId, client) as TransactionWriteResult<
      TResult,
      TKind
    >;
  }

  return new WriteResult(value, transactionId, client) as TransactionWriteResult<TResult, TKind>;
}

export function runInTransaction<TResult, TKind extends TransactionKind>(
  transaction: Transaction<TKind>,
  callback: (target: Scoped<Transaction<TKind>>) => TResult,
  client: JazzClient | (() => JazzClient),
): RunInTransactionResult<TResult, TKind> {
  let value: TResult;
  try {
    const scope = createTransactionScope(transaction);
    value = callback(scope);
  } catch (error) {
    try {
      transaction.rollback();
    } catch {
      // Preserve the original callback error.
    }
    throw error;
  }
  const resultClient = typeof client === "function" ? client : () => client;
  if (isPromiseLike(value)) {
    return value.then(
      (resolvedValue) => {
        const committed = transaction.commit();
        return createTransactionWriteResult(
          transaction,
          resolvedValue as Awaited<TResult>,
          committed.transactionId,
          resultClient(),
        );
      },
      (error) => {
        try {
          transaction.rollback();
        } catch {
          // Preserve the original callback error.
        }
        throw error;
      },
    ) as RunInTransactionResult<TResult, TKind>;
  }
  const committed = transaction.commit();
  return createTransactionWriteResult(
    transaction,
    value,
    committed.transactionId,
    resultClient(),
  ) as RunInTransactionResult<TResult, TKind>;
}

/**
 * Groups a set of writes as either a mergeable or exclusive transaction (see {@link TransactionKind}).
 */
export class Transaction<TKind extends TransactionKind = TransactionKind> {
  constructor(
    readonly kind: TKind,
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  private bindTable<T, Init>(table: TableProxy<T, Init>): DbTransactionHandleBinding {
    const existingBinding = dbTxHandleBindings.get(this);
    if (existingBinding) {
      assertTableBelongsToClient(table, existingBinding.client, this.resolveClient);
      return existingBinding;
    }

    const client = this.resolveClient(table._schema);
    const transactionId = client.beginTransaction(this.kind);
    const binding = {
      client,
      transactionId,
      session: this.session,
      attribution: this.attribution,
    };
    dbTxHandleBindings.set(this, binding);
    return binding;
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbTransactionHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>);
  }

  private requireBinding(operation: string): DbTransactionHandleBinding {
    return getDbTxHandleBinding(this, operation);
  }

  transactionId(): string {
    return this.requireBinding("transactionId").transactionId;
  }

  /**
   * Commit this transaction.
   */
  commit(): TransactionCommitHandle<TKind> {
    const { client, transactionId } = this.requireBinding("commit");
    const committed = client.commitTransaction(transactionId);
    if (this.kind === "exclusive") {
      return new ExclusiveWriteHandle(
        committed.transactionId,
        client,
      ) as TransactionCommitHandle<TKind>;
    }
    return committed as TransactionCommitHandle<TKind>;
  }

  /**
   * Roll back this transaction locally.
   *
   * Pending rows remain pending, but this transaction can no longer be committed.
   *
   * Only available on transactions created with {@link Db.beginTransaction}.
   * When using {@link Db.transaction}, throw an error inside the callback to roll back.
   */
  rollback(): void {
    const { client, transactionId } = this.requireBinding("rollback");
    client.rollbackTransaction(transactionId);
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: CreateOptions): T {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const { client, transactionId, session, attribution } = this.requireBinding("insert");
    const row = client.insertInternal(
      table._table,
      values,
      options,
      session,
      attribution,
      transactionId,
    );
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Restore a soft-deleted row.
   *
   * The restore is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  restore<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Init,
    options?: RestoreOptions,
  ): T {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Restore",
      transformedData,
      table._schema,
      table._table,
    );
    const { client, transactionId, session, attribution } = this.requireBinding("restore");
    const row = client.restoreInternal(
      table._table,
      id,
      values,
      options,
      session,
      attribution,
      transactionId,
    );
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Create or update a row with a caller-supplied id.
   *
   * The upsert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  upsert<T, Init>(table: TableProxy<T, Init>, data: Partial<Init>, options: UpsertOptions): void {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Upsert",
      transformedData,
      table._schema,
      table._table,
    );
    const { client, transactionId, session, attribution } = this.requireBinding("upsert");
    client.upsertInternal(table._table, values, options, session, attribution, transactionId);
  }

  /**
   * Update an existing row in a table.
   *
   * The update is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const { client, transactionId, session, attribution } = this.requireBinding("update");
    client.updateInternal(
      table._table,
      id,
      updates,
      undefined,
      session,
      attribution,
      transactionId,
    );
  }

  /**
   * Delete an existing row from a table.
   *
   * The delete is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const { client, transactionId, session, attribution } = this.bindTable(table);
    client.deleteInternal(table._table, id, undefined, session, attribution, transactionId);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this transaction.
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const { client, transactionId, session } = this.bindQuery(query);
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const rows = await client.query(
      translateQuery(builderJson, planningSchema),
      {
        ...options,
        localUpdates: "deferred",
        transactionId,
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
   * Read data is scoped to this transaction.
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(limitQueryToOne(query), options);
    return results[0] ?? null;
  }
}

/**
 * Transaction object available inside {@link Db.transaction}'s callback.
 */
export type TransactionScope<TKind extends TransactionKind = TransactionKind> = Scoped<
  Transaction<TKind>
>;

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
  private clientSchemas = new Map<string, WasmSchema>();
  private config: DbConfig;
  private readonly runtimeSource: AnyRuntimeSource;
  private readonly authStateStore;
  private disposeCoreTelemetry: (() => void) | null = null;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;
  private runtimeOperationContextOverride: DbRuntimeOperationContext | null = null;
  private readonly activeQuerySubscriptionTraces = new Map<
    string,
    StoredActiveQuerySubscriptionTrace
  >();
  private readonly activeQuerySubscriptionTraceListeners =
    new Set<ActiveQuerySubscriptionTraceListener>();
  private nextActiveQuerySubscriptionTraceId = 1;
  private isTransportDisconnected = false;

  /**
   * Protected constructor - use {@link createDb} in regular app code.
   */
  protected constructor(
    config: DbConfig,
    runtimeSource: AnyRuntimeSource,
    authStateOptions?: AuthStateStoreOptions,
  ) {
    this.config = config;
    this.runtimeSource = runtimeSource;
    this.authStateStore = createAuthStateStore(config, authStateOptions);
  }

  /** @internal Store the seed used for local-first auth and optionally schedule token refresh. */
  initLocalFirstAuth(seed: string, ttlSeconds: number, refresh = true): void {
    this._localFirstSecret = seed;
    if (refresh) {
      this.scheduleLocalFirstRefresh(ttlSeconds);
    }
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
    return this.runtimeSource.mintLocalFirstToken({
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

    return true;
  }

  /**
   * Create a Db instance with a loaded runtime source.
   * @internal Use createDb() instead.
   */
  static create(config: DbConfig, runtimeSource: AnyRuntimeSource): Db {
    return new Db(config, runtimeSource);
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because the runtime source is loaded before Db is created.
   *
   */
  protected getClient(schema: WasmSchema): JazzClient {
    const runtimeSchema =
      this.runtimeSource.supportsPolicyBypass && shouldBypassLocalPolicies(this.config)
        ? getPolicyStrippedSchema(schema)
        : schema;

    // Use the canonical schema JSON as the client cache key, but memoize it by
    // schema identity so write-heavy paths don't stringify the same schema per row.
    const key = getRuntimeSchemaCacheKey(runtimeSchema);
    if (!this.clients.has(key)) {
      this.installMainThreadCoreTelemetry();
      const client = this.runtimeSource.createClient({
        config: { ...this.config },
        schema: runtimeSchema,
        onAuthFailure: (reason) => {
          this.markUnauthenticated(reason);
        },
      });

      if (this.config.serverUrl && !this.isTransportDisconnected) {
        client.connectTransport(this.config.serverUrl, this.transportAuthConfig());
      }
      this.clients.set(key, client);
      this.clientSchemas.set(key, runtimeSchema);
    }

    return this.clients.get(key)!;
  }

  private transportAuthConfig(): AuthConfig {
    return {
      jwt_token: this.config.jwtToken,
      admin_secret: this.config.adminSecret,
      backend_secret: this.config.backendSecret,
      backend_session: this.config.cookieSession,
    };
  }

  protected getRuntimeOperationContext(): DbRuntimeOperationContext | null {
    return this.runtimeOperationContextOverride;
  }

  /**
   * @internal Runs one synchronous high-level Db operation with an explicit
   * session context. The browser worker subscription channel uses this when a
   * shared worker Db serves edge-client requests for multiple identities.
   */
  __withRuntimeOperationContext<TResult>(
    context: DbRuntimeOperationContext,
    operation: () => TResult,
  ): TResult {
    const previous = this.runtimeOperationContextOverride;
    this.runtimeOperationContextOverride = context;
    try {
      return operation();
    } finally {
      this.runtimeOperationContextOverride = previous;
    }
  }

  private installMainThreadCoreTelemetry(): void {
    const collectorUrl = this.resolveTelemetryCollectorUrl();
    if (!collectorUrl || this.disposeCoreTelemetry) {
      return;
    }

    this.disposeCoreTelemetry =
      this.runtimeSource.installTelemetry?.({
        config: this.config,
        collectorUrl,
        runtimeThread: "main",
      }) ?? null;
  }

  private resolveTelemetryCollectorUrl(): string | undefined {
    return resolveTelemetryCollectorUrlFromEnv() ?? this.config.telemetryCollectorUrl;
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

  getConfig(): DbConfig {
    // Return a copy of the config to avoid editing the original config.
    return structuredClone(this.config);
  }

  setDevMode(enabled: boolean): void {
    this.config.devMode = enabled;
  }

  /**
   * Temporarily disconnect this Db from its configured Jazz sync server.
   *
   * Local reads and writes can continue while disconnected. Call
   * {@link reconnect} to resume sync using the same Db instance.
   */
  async disconnect(): Promise<void> {
    if (this.isShuttingDown || this.shutdownPromise) {
      throw new Error("Cannot disconnect a Db that is shutting down.");
    }

    this.isTransportDisconnected = true;
    await Promise.all(Array.from(this.clients.values(), (client) => client.disconnectTransport()));
  }

  /**
   * Reconnect this Db to its configured Jazz sync server after
   * {@link disconnect}.
   */
  async reconnect(): Promise<void> {
    if (this.isShuttingDown || this.shutdownPromise) {
      throw new Error("Cannot reconnect a Db that is shutting down.");
    }

    this.isTransportDisconnected = false;
    if (!this.config.serverUrl) {
      return;
    }

    const auth = this.transportAuthConfig();
    for (const client of this.clients.values()) {
      client.connectTransport(this.config.serverUrl, auth);
    }
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
   * The engine-normalized runtime schema of this Db's live client, or null
   * before any client exists. First-client-wins when a Db holds several
   * clients — a dev-introspection accessor (inspector host handle, devtools
   * bridge), not a general schema API.
   */
  getRuntimeSchema(): WasmSchema | null {
    const client = this.clients.values().next().value;
    return client ? client.getSchema() : null;
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
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    const inserted = client.insert(
      table._table,
      values,
      options,
      context?.session,
      context?.attribution,
    );
    return inserted.mapValue((row) =>
      transformOutputRow(table, transformRow(row, table._schema, table._table)),
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
    const values = toWriteRecordForOperation(
      "Restore",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    const restored = client.restore(
      table._table,
      id,
      values,
      options,
      context?.session,
      context?.attribution,
    );
    return restored.mapValue((row) =>
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
    const values = toWriteRecordForOperation(
      "Upsert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.upsert(table._table, values, options, context?.session, context?.attribution);
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
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.update(
      table._table,
      id,
      updates,
      options,
      context?.session,
      context?.attribution,
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
    return client.delete(table._table, id, options, context?.session, context?.attribution);
  }

  canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): boolean {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.canInsert(table._table, values, context?.session);
  }

  canRead<T, Init>(table: TableProxy<T, Init>, id: string): boolean {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return client.canRead(table._table, id, context?.readSession ?? context?.session);
  }

  canUpdate<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): boolean {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.canUpdate(table._table, id, updates, context?.session);
  }

  canDelete<T, Init>(table: TableProxy<T, Init>, id: string): boolean {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return client.canDelete(table._table, id, context?.session);
  }

  private createTransaction<TKind extends TransactionKind>(kind: TKind): Transaction<TKind> {
    const context = this.getRuntimeOperationContext();
    return new Transaction(
      kind,
      (schema) => this.getClient(schema),
      context?.session,
      context?.attribution,
    );
  }

  /**
   * Begin a mergeable transaction.
   *
   * Use {@link Transaction.commit} to commit the transaction.
   *
   * Prefer using {@link Db.transaction} when an explicit commit is not required.
   */
  beginTransaction(): Transaction<"mergeable"> {
    return this.createTransaction("mergeable");
  }

  /**
   * Begin an exclusive transaction for writes that need serializable validation by the authority.
   *
   * Use {@link Transaction.commit} to commit the transaction.
   *
   * Prefer using {@link Db.exclusiveTransaction} when an explicit commit is not required.
   */
  beginExclusiveTransaction(): Transaction<"exclusive"> {
    return this.createTransaction("exclusive");
  }

  /**
   * Run {@link callback} inside a mergeable transaction and commit it once the callback returns.
   *
   * @returns a write result containing the result of the callback
   */
  transaction<TResult>(
    callback: (tx: TransactionScope<"mergeable">) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(
    callback: (tx: TransactionScope<"mergeable">) => TResult,
  ): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").client,
    );
  }

  /**
   * Run {@link callback} inside an exclusive transaction and commit it once the callback returns.
   *
   * @returns a write result containing the result of the callback
   */
  exclusiveTransaction<TResult>(
    callback: (tx: TransactionScope<"exclusive">) => Promise<TResult>,
  ): Promise<ExclusiveWriteResult<Awaited<TResult>>>;
  exclusiveTransaction<TResult>(
    callback: (tx: TransactionScope<"exclusive">) => TResult,
  ): ExclusiveWriteResult<TResult>;
  exclusiveTransaction<TResult>(
    callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>,
  ): ExclusiveWriteResult<TResult> | Promise<ExclusiveWriteResult<Awaited<TResult>>> {
    const transaction = this.beginExclusiveTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").client,
    );
  }

  /**
   * Delete browser OPFS storage for this Db's active namespace.
   */
  async deleteClientStorage(): Promise<void> {
    if (resolveStorageDriver(this.config.driver).type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }

    if (typeof window === "undefined") {
      console.error("deleteClientStorage() is only available in browser runtimes.");
      return;
    }

    const clients = [...this.clients.values()];
    if (clients.length === 0) {
      const client = this.getClient({});
      clients.push(client);
    }

    const [resetClient, ...otherClients] = clients;
    let closeError: unknown = null;
    for (const client of otherClients) {
      try {
        await client.shutdown();
      } catch (error) {
        closeError ??= error;
      }
    }

    try {
      if (closeError) {
        throw closeError;
      }
      await resetClient!.clearClientStorage();
      await resetClient!.shutdown();
    } finally {
      this.clients.clear();
      this.clientSchemas.clear();
    }
  }

  /**
   * Release the current Db instance for logout flows.
   *
   * When `wipeData` is enabled, Jazz clears local client storage before shutting this Db down.
   * Callers should still sign out of their external auth provider separately and recreate
   * `JazzProvider` / `Db` after logout.
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
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const queryOptions = nativeDbQueryOptions(options);
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
    const context = this.getRuntimeOperationContext();
    const rows =
      context || usesRelationTraversal
        ? await client.query(wasmQuery, queryOptions, context?.readSession ?? context?.session)
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
   * Create a `files` row whose `data` column stores the whole Blob as a binary large value.
   */
  async createFileFromBlob<FileRow extends { id: string }, FileInit>(
    app: BinaryLargeValueFileApp<FileRow, FileInit>,
    blob: Blob,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createBinaryLargeValueFileStorage(this, app).fromBlob(blob, options);
  }

  /**
   * Create a `files` row whose `data` column stores the whole stream as a binary large value.
   */
  async createFileFromStream<FileRow extends { id: string }, FileInit>(
    app: BinaryLargeValueFileApp<FileRow, FileInit>,
    stream: ReadableStream<unknown>,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createBinaryLargeValueFileStorage(this, app).fromStream(stream, options);
  }

  /**
   * Load a binary-large-value file row as a browser ReadableStream.
   */
  async loadFileAsStream<FileRow extends { id: string }, FileInit>(
    app: BinaryLargeValueFileApp<FileRow, FileInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<ReadableStream<Uint8Array>> {
    return createBinaryLargeValueFileStorage(this, app).toStream(fileOrId, options);
  }

  /**
   * Load a binary-large-value file row as a Blob.
   */
  async loadFileAsBlob<FileRow extends { id: string }, FileInit>(
    app: BinaryLargeValueFileApp<FileRow, FileInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<Blob> {
    return createBinaryLargeValueFileStorage(this, app).toBlob(fileOrId, options);
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
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
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
    const handleDelta = (delta: Parameters<SubscriptionManager<T>["handleDelta"]>[0]) => {
      const typedDelta = manager.handleDelta(delta, transform, nativeOutputColumns);
      callback(typedDelta);
    };

    const queryOptions = nativeDbQueryOptions(options);
    const context = this.getRuntimeOperationContext();
    let subId: number | null = null;
    let unsubscribed = false;
    const startNativeSubscription = () => {
      if (unsubscribed || subId !== null) return;
      subId = client.subscribe(
        wasmQuery,
        handleDelta,
        queryOptions,
        context?.readSession ?? context?.session ?? session,
      );
      if (unsubscribed) {
        client.unsubscribe(subId);
        subId = null;
      }
    };
    const traceId = this.registerActiveQuerySubscriptionTrace(
      wasmQuery,
      builtQuery.table,
      queryOptions,
    );
    if (queryOptions.tier == null || queryOptions.tier === "local") {
      callback(manager.seed([]));
    }
    startNativeSubscription();
    if (
      this.config.serverUrl &&
      queryOptions.propagation !== "local-only" &&
      queryOptions.tier !== "global" &&
      !queryUsesRelationTraversal(builtQuery)
    ) {
      const seedQuery = () =>
        this.all(query, { ...queryOptions, tier: "local", propagation: "local-only" });
      const seedRows =
        session == null
          ? seedQuery()
          : this.__withRuntimeOperationContext({ session }, () => seedQuery());
      void seedRows
        .then((rows) => {
          if (unsubscribed) return;
          callback(manager.seed(rows));
        })
        .catch((error: unknown) => {
          setTimeout(() => {
            throw error;
          }, 0);
        });
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
   * Closes all memoized JazzClient connections.
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

    this.disposeCoreTelemetry?.();
    this.disposeCoreTelemetry = null;
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
    this.clientSchemas.clear();
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
    _queryTable: string,
    options?: QueryOptions,
  ): string | null {
    if (!this.config.devMode) {
      return null;
    }

    const resolvedOptions = resolveEffectiveQueryExecutionOptions(this.config, options);
    const payload = this.parseRuntimeQueryTracePayload(queryJson);
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

  private parseRuntimeQueryTracePayload(queryJson: string): RuntimeQueryTracePayload {
    try {
      const parsed = JSON.parse(queryJson) as { table?: unknown; branches?: unknown };
      const table = typeof parsed.table === "string" ? parsed.table : "unknown";
      const branches = Array.isArray(parsed.branches)
        ? parsed.branches.filter((branch): branch is string => typeof branch === "string")
        : [];

      return {
        table,
        branches: branches.length > 0 ? branches : [this.config.userBranch ?? "main"],
      };
    } catch {
      return {
        table: "unknown",
        branches: [this.config.userBranch ?? "main"],
      };
    }
  }
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
 * This is an **async** factory function that pre-loads the runtime source.
 * After creation, local-first mutations (`insert`/`update`/`delete`) are synchronous.
 * Use the `wait` method when you need a Promise that resolves at a durability tier.
 *
 * Browser and backend runtimes open the native runtime in-process.
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

export async function createDbWithRuntimeSource<RuntimeConfig extends DbConfig>(
  config: RuntimeConfig,
  runtimeSource: RuntimeSource<RuntimeConfig>,
): Promise<Db> {
  if (config.secret && config.cookieSession) {
    throw new Error("DbConfig error: secret and cookieSession are mutually exclusive");
  }
  if (config.secret && config.jwtToken) {
    throw new Error("DbConfig error: secret and jwtToken are mutually exclusive");
  }
  if (config.jwtToken && config.cookieSession) {
    throw new Error("DbConfig error: jwtToken and cookieSession are mutually exclusive");
  }

  let resolvedConfig = { ...config };
  await runtimeSource.load(config);

  // Local-first auth: resolve seed and mint a JWT
  let localFirstSecret: string | null = null;
  if (config.secret) {
    const secret = config.secret;
    localFirstSecret = secret;

    if (!config.jwtToken) {
      const jwtToken = runtimeSource.mintLocalFirstToken(
        createRuntimeTokenOptions(secret, config.appId, 3600),
      );
      resolvedConfig = { ...resolvedConfig, jwtToken };
    }
  } else if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    // Anonymous: mint an ephemeral keypair + anonymous JWT.
    // Admin-secret clients intentionally stay sessionless so local policy
    // evaluation does not preempt backend-authorized transport writes.
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const jwtToken = runtimeSource.mintAnonymousToken(
      createRuntimeTokenOptions(ephemeralSeed, config.appId, 3600),
    );
    resolvedConfig = { ...resolvedConfig, jwtToken };
  }

  const driver = resolveStorageDriver(resolvedConfig.driver);

  if (driver.type === "memory" && !resolvedConfig.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }

  const db = Db.create(resolvedConfig, runtimeSource as AnyRuntimeSource);

  if (localFirstSecret) {
    db.initLocalFirstAuth(localFirstSecret, 3600, !config.jwtToken);
  }

  return db;
}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithRuntimeSource(config, new DefaultRuntimeSource());
}
