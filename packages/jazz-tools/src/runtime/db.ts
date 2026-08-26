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
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
  WriteResult,
  JazzClient,
  type MutationErrorEvent,
  WriteHandle,
  type TransactionKind,
  type InsertOptions as InternalInsertOptions,
  type RestoreOptions as InternalRestoreOptions,
  type UpdateOptions as InternalUpdateOptions,
  type DurabilityTier,
  type QueryExecutionOptions as InternalQueryExecutionOptions,
  type QueryPropagation,
  type QueryVisibility,
  resolveEffectiveQueryExecutionOptions,
  resolveReadTier,
  ReadTier,
  type BranchSelector,
  type BranchView,
  type OpenBatchId,
  type BatchId,
  type PermissionAdvice,
  type StreamingValueSource,
} from "./client.js";
import { type RuntimeSource, type RuntimeTokenOptions } from "./runtime-source.js";
import { DefaultRuntimeSource } from "./default-runtime-source.js";
import type { AuthFailureReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toValue, toWriteRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import {
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  resolveClientInternalSessionSync,
  type ClientSessionInput,
} from "./client-session.js";
import { canonicalAuthorSubject } from "./author-id.js";
import {
  getTrustedReservedSession,
  setDbInternalSession,
  setTrustedReservedSession,
} from "./db-internal-session.js";
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

type WasmLogLevel = "error" | "warn" | "info" | "debug" | "trace";
type AnyRuntimeSource = RuntimeSource<any>;
type WriteOperationName = "Insert" | "Update" | "Upsert" | "Restore";

/**
 * Configuration for creating a Db instance.
 */
export type DbConfig = {
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
  /** Admin secret for catalogue sync */
  adminSecret?: string;
  /** Backend secret for backend-scoped sync auth with cookieSession. */
  backendSecret?: string;
  /** IndexedDB database name for browser persistence (default: appId). */
  dbName?: string;
  /**
   * Initial-sync durability boundary, in writes (default: 512 for clients).
   * A crash can lose up to M - 1 writes since the previous durable IndexedDB
   * page commit.
   */
  initialSyncFlushEvery?: number;
  /** Optional WASM tracing level for benchmark/debug scenarios (default: "warn"). */
  logLevel?: WasmLogLevel;
  /** Optional OTLP/HTTP collector URL for WASM trace telemetry. */
  telemetryCollectorUrl?: string;
  /** Enable runtime tracing for DevTools-only diagnostics. */
  devMode?: boolean;
} & (
  | {
      /** Local-first auth via a local seed. */
      secret?: string;
      jwtToken?: never;
      cookieSession?: never;
    }
  | {
      secret?: never;
      /** JWT token for server authentication. */
      jwtToken?: string;
      cookieSession?: never;
    }
  | {
      secret?: never;
      jwtToken?: never;
      /** Mirrored session for local permission evaluation when sync auth uses cookies. */
      cookieSession?: Session;
    }
);

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
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

  const session = resolveClientInternalSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
    trustedReservedSession: getTrustedReservedSession(config),
  });

  if (!session?.user_id || session.authMode === "anonymous") {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(canonicalAuthorSubject(session.issuer, session.user_id))}`;
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

export type BranchValue = string | number | bigint;
export type QualifiedBranch = Record<string, BranchValue>;
export type Branch = BranchValue | QualifiedBranch;
export type BranchBase = Branch | readonly [branch: Branch, snapshot: unknown];

export type QueryOptions = Omit<InternalQueryExecutionOptions, "branch"> & {
  /** Current branch coordinate. A scalar selects a table with one `branchBy` column. */
  branch?: Branch;
  /** Optional live base, or `[base, snapshotRef]` for a frozen base. */
  base?: BranchBase;
};

interface TimestampOverrideOptions {
  updatedAt?: number;
}

export interface InsertOptions extends TimestampOverrideOptions {
  id?: string;
  branch?: Branch;
}
export type StreamingInsertOptions = Omit<InsertOptions, "branch">;

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: Branch;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: Branch;
  base?: BranchBase;
}

export interface DeleteOptions extends TimestampOverrideOptions {
  branch?: Branch;
  base?: BranchBase;
}

type DbRuntimeOperationContext = {
  session?: Session;
  attribution?: string;
  readSession?: Session;
};

function branchColumn(schema: WasmSchema, name: string): ColumnDescriptor {
  const matches = Object.values(schema)
    .flatMap((table) => table.columns)
    .filter((column) => column.name === name);
  const column = matches[0];
  if (!column) throw new Error(`Unknown branch column "${name}".`);
  return column;
}

function normalizeBranchSelector(
  schema: WasmSchema,
  tableName: string,
  input: Branch,
  scope: "table" | "schema",
): BranchSelector {
  const table = schema[tableName];
  if (!table) throw new Error(`Unknown table "${tableName}".`);
  const tableColumns = table.branchBy ?? [];
  const expected =
    scope === "table"
      ? new Set(tableColumns)
      : new Set(Object.values(schema).flatMap((candidate) => candidate.branchBy ?? []));
  const qualified: QualifiedBranch =
    typeof input === "object" && input !== null && !Array.isArray(input)
      ? (input as QualifiedBranch)
      : expected.size === 1
        ? { [[...expected][0]!]: input as BranchValue }
        : (() => {
            throw new Error(
              `A scalar branch selector requires exactly one ${scope === "table" ? "table" : "schema"} branch column.`,
            );
          })();
  const actual = Object.keys(qualified);
  if (actual.length !== expected.size || actual.some((name) => !expected.has(name))) {
    throw new Error(
      `Branch selector must provide exactly: ${[...expected].sort().join(", ") || "no columns"}.`,
    );
  }
  return {
    values: Object.fromEntries(
      actual.map((name) => [
        name,
        toValue(qualified[name], branchColumn(schema, name).column_type),
      ]),
    ),
  };
}

function normalizeBranchView(
  schema: WasmSchema,
  tableName: string,
  branch: Branch,
  base?: BranchBase,
): BranchView {
  const head = normalizeBranchSelector(schema, tableName, branch, "schema");
  if (base === undefined) return { head };
  if (Array.isArray(base)) {
    return {
      head,
      base: {
        kind: "snapshot",
        branch: normalizeBranchSelector(schema, tableName, base[0], "schema"),
        snapshot: base[1],
      },
    };
  }
  return {
    head,
    base: {
      kind: "current",
      branch: normalizeBranchSelector(schema, tableName, base as Branch, "schema"),
    },
  };
}

function nativeDbQueryOptions(
  schema: WasmSchema,
  tableName: string,
  options?: QueryOptions,
): InternalQueryExecutionOptions {
  if (!options) return {};
  const { branch, base, ...rest } = options;
  if (branch === undefined) {
    if (base !== undefined) throw new Error("A branch base requires a branch head.");
    return rest;
  }
  return { ...rest, branch: normalizeBranchView(schema, tableName, branch, base) };
}

function normalizeInsertOptions(
  schema: WasmSchema,
  tableName: string,
  options?: InsertOptions,
): InternalInsertOptions | undefined {
  if (!options) return undefined;
  const { branch, ...rest } = options;
  return branch === undefined
    ? rest
    : { ...rest, branch: normalizeBranchSelector(schema, tableName, branch, "table") };
}

function normalizeRestoreOptions(
  schema: WasmSchema,
  tableName: string,
  options?: RestoreOptions,
): InternalRestoreOptions | undefined {
  if (!options) return undefined;
  const { branch, ...rest } = options;
  return branch === undefined
    ? rest
    : { ...rest, branch: normalizeBranchSelector(schema, tableName, branch, "table") };
}

function normalizeUpdateOptions(
  schema: WasmSchema,
  tableName: string,
  options?: UpdateOptions,
): InternalUpdateOptions | undefined {
  if (!options) return undefined;
  const { branch, base, ...rest } = options;
  if (branch === undefined) {
    if (base !== undefined) throw new Error("A branch base requires a branch head.");
    return rest;
  }
  return { ...rest, branch: normalizeBranchView(schema, tableName, branch, base) };
}

export function limitQueryToOne<T>(query: QueryBuilder<T>): QueryBuilder<T> {
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
  rootTerminal = true,
): ColumnDescriptor[] {
  const wildcard = projection === undefined || projection.length === 0;
  const selectedColumns = resolveSelectedColumns(tableName, schema, projection);
  // IDs are implicit in query results, so an explicit `select("id")` resolves
  // to no ordinary public columns. The native query projection represents that
  // state with its empty/default sentinel and therefore retains the full
  // physical record; decode that carrier fully before the row transformer
  // applies the public ID-only projection.
  const usesDefaultNativeProjection = wildcard || selectedColumns.length === 0;
  const nativeColumns = usesDefaultNativeProjection
    ? resolveSelectedColumns(tableName, schema, undefined)
    : selectedColumns;
  const columns = nativeColumns
    .map((columnName) => {
      const column = resolveOutputColumnDescriptor(tableName, schema, columnName);
      return column && usesDefaultNativeProjection && rootTerminal
        ? { ...column, sparse: true }
        : column;
    })
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
      false,
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

/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<T, Init, StreamingInit = unknown, StreamingUpdate = unknown> {
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
  /** @internal Phantom brand — enables exact streaming-insert inference. */
  readonly _streamingInitType?: StreamingInit;
  /** @internal Phantom brand — enables exact streaming update/upsert inference. */
  readonly _streamingUpdateType?: StreamingUpdate;
}

export interface ColumnTransform {
  from(value: unknown): unknown;
  to(value: unknown): unknown;
}

export type ColumnTransformMap = Record<string, ColumnTransform>;

type DbTransactionHandleBinding = {
  ownerClient: JazzClient;
  openBatchId: OpenBatchId;
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
  table: TableProxy<unknown, unknown, unknown>,
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

function splitStreamingMutation(
  table: TableProxy<unknown, unknown, unknown>,
  data: unknown,
): {
  column: string;
  source: StreamingValueSource;
  values: Record<string, unknown>;
} {
  if (typeof data !== "object" || data === null) {
    throw new Error("Streaming insert data must be an object");
  }
  const record = data as Record<string, unknown>;
  const streamableColumns = table._schema[table._table]?.columns.filter((column) =>
    ["Text", "Json", "Bytea"].includes(column.column_type.type),
  );
  const streamed = streamableColumns?.filter(
    (column) => Object.hasOwn(record, column.name) && isStreamingValueSource(record[column.name]),
  );
  if (streamed?.length !== 1) {
    throw new Error("Streaming insert requires exactly one streamed Text, Json, or Bytea column");
  }
  const column = streamed[0]!.name;
  if (table._schema[table._table]?.branchBy?.includes(column)) {
    throw new Error(`Streaming a branchBy column is not supported: ${table._table}.${column}`);
  }
  const source = record[column] as StreamingValueSource;
  const values = { ...record };
  delete values[column];
  return { column, source, values };
}

function isStreamingValueSource(value: unknown): value is StreamingValueSource {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as {
    getReader?: unknown;
    [Symbol.asyncIterator]?: unknown;
  };
  return (
    typeof candidate.getReader === "function" ||
    typeof candidate[Symbol.asyncIterator] === "function"
  );
}

function deriveStreamingInsertBranch(
  table: TableProxy<unknown, unknown, unknown, unknown>,
  values: Record<string, unknown>,
): Branch | undefined {
  const branchColumns = table._schema[table._table]?.branchBy ?? [];
  if (branchColumns.length === 0) return undefined;
  const branch: QualifiedBranch = {};
  for (const column of branchColumns) {
    const value = values[column];
    if (isStreamingValueSource(value)) {
      throw new Error(`Streaming a branchBy column is not supported: ${table._table}.${column}`);
    }
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "bigint") {
      throw new Error(`Streaming insert requires branch column ${table._table}.${column}`);
    }
    branch[column] = value;
  }
  return branch;
}

export type { TransactionKind } from "./client.js";

type TransactionCommitHandle<TKind extends TransactionKind> = TKind extends "exclusive"
  ? ExclusiveWriteHandle
  : WriteHandle;

type TransactionWriteResult<TResult, TKind extends TransactionKind> = TKind extends "exclusive"
  ? ExclusiveWriteResult<TResult>
  : WriteResult<TResult>;

type RunInTransactionResult<TResult, TKind extends TransactionKind> = Promise<
  TransactionWriteResult<Awaited<TResult>, TKind>
>;

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
  batchId: BatchId,
  client: JazzClient,
): TransactionWriteResult<TResult, TKind> {
  if (transaction.kind === "exclusive") {
    return new ExclusiveWriteResult(value, batchId, client) as TransactionWriteResult<
      TResult,
      TKind
    >;
  }

  return new WriteResult(value, batchId, client) as TransactionWriteResult<TResult, TKind>;
}

export async function runInTransaction<TResult, TKind extends TransactionKind>(
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
      await transaction.rollback();
    } catch {
      // Preserve the original callback error.
    }
    throw error;
  }
  const resultClient = typeof client === "function" ? client : () => client;
  let resolvedValue: Awaited<TResult>;
  try {
    resolvedValue = await value;
  } catch (error) {
    try {
      await transaction.rollback();
    } catch {
      // Preserve the original callback error.
    }
    throw error;
  }
  let committed: TransactionCommitHandle<TKind>;
  try {
    committed = await transaction.commit();
  } catch (error) {
    try {
      await transaction.rollback();
    } catch {
      // Preserve the commit error while ensuring an empty mergeable batch is
      // consumed when the callback helper has no handle to return to callers.
    }
    throw error;
  }
  return createTransactionWriteResult(
    transaction,
    resolvedValue,
    await committed.batchId,
    resultClient(),
  );
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
    ownerClient?: JazzClient,
  ) {
    if (ownerClient) this.bindOwnerClient(ownerClient);
  }

  private bindTable<T, Init>(table: TableProxy<T, Init>): DbTransactionHandleBinding {
    const client = this.resolveClient(table._schema);
    if (!dbTxHandleBindings.has(this)) this.bindOwnerClient(client);
    return this.requireBinding("table operation");
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbTransactionHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>);
  }

  private requireBinding(operation: string): DbTransactionHandleBinding {
    return getDbTxHandleBinding(this, operation);
  }

  private bindOwnerClient(ownerClient: JazzClient): void {
    dbTxHandleBindings.set(this, {
      ownerClient,
      openBatchId: ownerClient.beginTransaction(this.kind, this.session, this.attribution),
      session: this.session,
      attribution: this.attribution,
    });
  }

  openBatchId(): OpenBatchId {
    return this.requireBinding("openBatchId").openBatchId;
  }

  /**
   * Commit this transaction.
   */
  commit(): TransactionCommitHandle<TKind> {
    const { ownerClient, openBatchId } = this.requireBinding("commit");
    const committed = ownerClient.commitTransaction(openBatchId);
    if (this.kind === "exclusive") {
      return new ExclusiveWriteHandle(
        committed.batchId,
        ownerClient,
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
  rollback(): Promise<boolean> {
    const { ownerClient, openBatchId } = this.requireBinding("rollback");
    return ownerClient.rollbackTransaction(openBatchId);
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): T {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openBatchId, session, attribution } = this.requireBinding("insert");
    const row = client.insertInternal(
      table._table,
      values,
      normalizeInsertOptions(table._schema, table._table, options),
      session,
      attribution,
      openBatchId,
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
    const client = this.resolveClient(table._schema);
    const { openBatchId, session, attribution } = this.requireBinding("restore");
    const row = client.restoreInternal(
      table._table,
      id,
      values,
      normalizeRestoreOptions(table._schema, table._table, options),
      session,
      attribution,
      openBatchId,
    );
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Create or update a row with a caller-supplied id.
   *
   * The upsert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  upsert<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): void {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Upsert",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openBatchId, session, attribution } = this.requireBinding("upsert");
    client.upsertInternal(
      table._table,
      id,
      values,
      normalizeUpdateOptions(table._schema, table._table, options),
      session,
      attribution,
      openBatchId,
    );
  }

  /**
   * Update an existing row in a table.
   *
   * The update is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): void {
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openBatchId, session, attribution } = this.requireBinding("update");
    const normalizedOptions = normalizeUpdateOptions(table._schema, table._table, options);
    client.updateInternal(
      table._table,
      id,
      updates,
      normalizedOptions?.updatedAt,
      session,
      attribution,
      openBatchId,
      normalizedOptions?.branch,
    );
  }

  /**
   * Delete an existing row from a table.
   *
   * The delete is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): void {
    this.bindTable(table);
    const client = this.resolveClient(table._schema);
    const { openBatchId, session, attribution } = this.requireBinding("delete");
    const normalizedOptions = normalizeUpdateOptions(table._schema, table._table, options);
    client.deleteInternal(
      table._table,
      id,
      normalizedOptions?.updatedAt,
      session,
      attribution,
      openBatchId,
      normalizedOptions?.branch,
    );
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this transaction.
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    this.bindQuery(query);
    const client = this.resolveClient(query._schema);
    const { openBatchId, session } = this.requireBinding("query");
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const queryOptions = nativeDbQueryOptions(query._schema, builtQuery.table, options);
    const rows = await client.query(
      translateQuery(builderJson, planningSchema),
      {
        ...queryOptions,
        localUpdates: "deferred",
        openBatchId,
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
  private config: DbConfig;
  private readonly runtimeSource: AnyRuntimeSource;
  private readonly authStateStore;
  private internalSession: Session | null;
  private connection: ConnectionManager;
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
  private readonly mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  private readonly pendingMutationErrorEvents: MutationErrorEvent[] = [];
  private nextActiveQuerySubscriptionTraceId = 1;

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
    const sessionInput = {
      ...config,
      trustedReservedSession: getTrustedReservedSession(config),
    };
    this.internalSession = resolveClientInternalSessionSync(sessionInput);
    setDbInternalSession(this, this.internalSession);
    this.authStateStore = createAuthStateStore(sessionInput, authStateOptions);
    this.connection = new DirectConnectionManager(this.dbForConnection());
  }

  private dbForConnection(): DbForConnection {
    // oxlint-disable-next-line typescript/no-this-alias
    const thisDb = this;
    return {
      get config() {
        return thisDb.config;
      },
      get runtimeSource() {
        return thisDb.runtimeSource;
      },
      get isShuttingDown() {
        return thisDb.isShuttingDown;
      },
      markUnauthenticated: (reason) => this.markUnauthenticated(reason),
      clearAuthError: () => this.authStateStore.clearError(),
      onMutationError: (event) => this.handleMutationError(event),
    };
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
      const trustedReservedSession = internalSessionFromVerifiedReservedJwtPayload(
        parseJwtPayload(newToken) ?? {},
        "local-first",
      );
      if (!trustedReservedSession) {
        throw new Error("Minted local-first token is missing its reserved session identity");
      }
      this.applyAuthUpdate(newToken, trustedReservedSession);
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

  protected applyAuthUpdate(token: string | null, trustedReservedSession?: Session): boolean {
    const jwtToken = token ?? undefined;
    const previousToken = this.config.jwtToken;
    const previousState = this.authStateStore.getState();
    const previousInternalSession = this.internalSession;
    this.internalSession = resolveClientInternalSessionSync({
      ...this.config,
      jwtToken,
      trustedReservedSession,
    });
    let nextState: AuthState;
    try {
      nextState = this.authStateStore.applyJwtToken(jwtToken, trustedReservedSession);
    } catch (error) {
      this.internalSession = previousInternalSession;
      setDbInternalSession(this, previousInternalSession);
      throw error;
    }
    const tokenChanged = previousToken !== jwtToken;

    if (!tokenChanged && nextState === previousState) {
      return false;
    }

    this.config.jwtToken = jwtToken;
    setTrustedReservedSession(this.config, trustedReservedSession);
    setDbInternalSession(this, this.internalSession);

    this.connection.updateAuth({ jwtToken, trustedReservedSession });

    return true;
  }

  protected applyCookieSessionUpdate(session: Session | null): boolean {
    const cookieSession = session ?? undefined;
    const previousSession = this.config.cookieSession;
    const previousState = this.authStateStore.getState();
    const previousInternalSession = this.internalSession;
    this.internalSession = resolveClientInternalSessionSync({
      ...this.config,
      cookieSession,
    });
    let nextState: AuthState;
    try {
      nextState = this.authStateStore.applyCookieSession(cookieSession);
    } catch (error) {
      this.internalSession = previousInternalSession;
      setDbInternalSession(this, previousInternalSession);
      throw error;
    }
    const sessionChanged = JSON.stringify(previousSession) !== JSON.stringify(cookieSession);

    if (!sessionChanged && nextState === previousState) {
      return false;
    }

    this.config.cookieSession = cookieSession;
    setDbInternalSession(this, this.internalSession);

    this.connection.updateAuth({ cookieSession });

    return true;
  }

  /**
   * Create a Db instance with a loaded runtime source.
   * @internal Use {@link createDb()} instead.
   */
  static create(config: DbConfig, runtimeSource: AnyRuntimeSource): Db {
    return new Db(config, runtimeSource);
  }

  /** @internal Create a Db whose durable peer lives in a dedicated browser worker. */
  static async createWithBrowserWorker(
    config: DbConfig,
    runtimeSource: AnyRuntimeSource,
  ): Promise<Db> {
    const db = new Db(config, runtimeSource);
    const connection = new BrowserConnectionManager(db.dbForConnection());
    db.connection = connection;
    await connection.start();
    return db;
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because the runtime source is loaded before Db is created.
   *
   */
  protected getClient(schema: WasmSchema): JazzClient {
    return this.connection.getClient(schema);
  }

  protected getCurrentClient(): JazzClient | null {
    return this.connection.getCurrentClient();
  }

  protected async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    await this.connection.ensureReady(tier, signal);
  }

  private wrapWriteWait<THandle extends WriteHandle<unknown>>(handle: THandle): THandle {
    const wait = handle.wait.bind(handle);
    handle.wait = (async (options: { tier: DurabilityTier }) => {
      await this.ensureReady(options.tier);
      return wait(options);
    }) as THandle["wait"];
    return handle;
  }

  protected getRuntimeOperationContext(): DbRuntimeOperationContext | null {
    return this.runtimeOperationContextOverride;
  }

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

  private withRuntimeOperationContext<TResult>(
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
   * Attach a fallback listener for write rejections that are not handled by an
   * active {@link WriteHandle.wait} call.
   *
   * @returns an unsubscribe callback
   */
  onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.mutationErrorListeners.add(listener);
    while (this.pendingMutationErrorEvents.length > 0) {
      listener(this.pendingMutationErrorEvents.shift()!);
    }
    return () => {
      this.mutationErrorListeners.delete(listener);
    };
  }

  getConfig(): DbConfig {
    // Return a copy without internal live transport handles. MessagePorts are
    // neither configuration nor cloneable unless transferred.
    const { browserWorkerPort: _browserWorkerPort, ...runtimeSources } =
      this.config.runtimeSources ?? {};
    return structuredClone({
      ...this.config,
      runtimeSources: Object.keys(runtimeSources).length > 0 ? runtimeSources : undefined,
    });
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

    await this.connection.disconnect();
  }

  /**
   * Reconnect this Db to its configured Jazz sync server after
   * {@link disconnect}.
   */
  async reconnect(): Promise<void> {
    if (this.isShuttingDown || this.shutdownPromise) {
      throw new Error("Cannot reconnect a Db that is shutting down.");
    }

    await this.connection.reconnect();
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
   * before the client exists. This is a dev-introspection accessor (inspector
   * host handle, devtools bridge), not a general schema API.
   */
  getRuntimeSchema(): WasmSchema | null {
    return this.connection.getRuntimeSchema();
  }

  /** @internal Open a control channel for the same-origin embedded inspector. */
  openInspectorControlPort(): Promise<MessagePort> {
    return this.connection.openInspectorControlPort();
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
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): WriteResult<T> {
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
      normalizeInsertOptions(table._schema, table._table, options),
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
   * Stream one Text, Json, or Bytea column into a new row. The column's runtime
   * schema determines encoding; callers never pass a large-value kind.
   *
   * Unlike {@link insert}, this is asynchronous because it consumes the source
   * before publishing. Its handle returns only the generated id so the complete
   * streamed value is not copied back into JavaScript memory.
   */
  async insertStreaming<T, Init, StreamingInit>(
    table: TableProxy<T, Init, StreamingInit>,
    data: StreamingInit,
    options?: StreamingInsertOptions,
  ): Promise<WriteHandle<{ id: string }>> {
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    const transformedData = transformInputColumns(table, ordinaryData);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    const branch = deriveStreamingInsertBranch(table, ordinaryData);
    return client.insertStreaming(
      table._table,
      values,
      column,
      source,
      normalizeInsertOptions(
        table._schema,
        table._table,
        branch ? { ...options, branch } : options,
      ),
      context?.session,
      context?.attribution,
    );
  }

  async updateStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<WriteHandle<{ id: string }>> {
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    const transformedData = transformInputColumns(table, ordinaryData);
    const values = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.updateStreaming(
      table._table,
      id,
      values,
      column,
      source,
      normalizeUpdateOptions(table._schema, table._table, options),
      context?.session,
      context?.attribution,
    );
  }

  async upsertStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<WriteHandle<{ id: string }>> {
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    const transformedData = transformInputColumns(table, ordinaryData);
    const values = toWriteRecordForOperation(
      "Upsert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.upsertStreaming(
      table._table,
      id,
      values,
      column,
      source,
      normalizeUpdateOptions(table._schema, table._table, options),
      context?.session,
      context?.attribution,
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
      normalizeRestoreOptions(table._schema, table._table, options),
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
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
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
    return this.wrapWriteWait(
      client.upsert(
        table._table,
        id,
        values,
        normalizeUpdateOptions(table._schema, table._table, options),
        context?.session,
        context?.attribution,
      ),
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
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return this.wrapWriteWait(
      client.update(
        table._table,
        id,
        updates,
        normalizeUpdateOptions(table._schema, table._table, options),
        context?.session,
        context?.attribution,
      ),
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
    return this.wrapWriteWait(
      client.delete(
        table._table,
        id,
        normalizeUpdateOptions(table._schema, table._table, options),
        context?.session,
        context?.attribution,
      ),
    );
  }

  /** Request authoritative permission advice for inserting a row. */
  async canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.requestInsertPermissionAdvice(table._table, values, context?.session);
  }

  /** Request authoritative permission advice for reading a row. */
  async canRead<T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return client.requestReadPermissionAdvice(
      table._table,
      id,
      context?.readSession ?? context?.session,
    );
  }

  /** Request authoritative permission advice for updating a row. */
  async canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const transformedData = transformInputColumns(table, data);
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    return client.requestUpdatePermissionAdvice(table._table, id, updates, context?.session);
  }

  /** Request authoritative permission advice for deleting a row. */
  async canDelete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const context = this.getRuntimeOperationContext();
    return client.requestDeletePermissionAdvice(table._table, id, context?.session);
  }

  private createTransaction<TKind extends TransactionKind>(kind: TKind): Transaction<TKind> {
    const context = this.getRuntimeOperationContext();
    const ownerClient = this.getCurrentClient();
    if (kind === "exclusive" && !ownerClient) {
      throw new Error(
        "Cannot begin an exclusive transaction before the JazzClient has been created. Run a query or mutation first.",
      );
    }
    return new Transaction(
      kind,
      (schema) => this.getClient(schema),
      context?.session,
      context?.attribution,
      ownerClient ?? undefined,
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
    callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").ownerClient,
    );
  }

  /**
   * Run {@link callback} inside an exclusive transaction and commit it once the callback returns.
   *
   * @returns a write result containing the result of the callback
   */
  exclusiveTransaction<TResult>(
    callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>,
  ): Promise<ExclusiveWriteResult<Awaited<TResult>>> {
    const transaction = this.beginExclusiveTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").ownerClient,
    );
  }

  /**
   * Delete browser IndexedDB storage for this Db's active namespace.
   */
  async deleteClientStorage(): Promise<void> {
    await this.connection.deleteClientStorage();
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
    const queryOptions = nativeDbQueryOptions(query._schema, builtQuery.table, options);
    const remoteIfPossibleOffline =
      options?.tier === ReadTier.RemoteIfPossible && this.connection.isExplicitlyOffline();
    if (remoteIfPossibleOffline) queryOptions.tier = "local";
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
    const context = this.getRuntimeOperationContext();
    const effectiveTier = resolveEffectiveQueryExecutionOptions(this.config, queryOptions).tier;
    await this.ensureReady(effectiveTier);
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

    const queryOptions = nativeDbQueryOptions(query._schema, builtQuery.table, options);
    const remoteIfPossibleOffline =
      options?.tier === ReadTier.RemoteIfPossible && this.connection.isExplicitlyOffline();
    const strictRemoteReadTier =
      options?.tier === ReadTier.Remote || options?.tier === ReadTier.RemoteIfPossible;
    if (remoteIfPossibleOffline) queryOptions.tier = "local";
    const context = this.getRuntimeOperationContext();
    type NativeSubscription = { id: number; generation: number };
    let activeSubscription: NativeSubscription | null = null;
    let nextSubscriptionGeneration = 1;
    let unsubscribed = false;
    const readyAbort = new AbortController();
    const startNativeSubscription = (subscriptionOptions = queryOptions, replace = false) => {
      if (unsubscribed || (!replace && activeSubscription !== null)) return null;
      const generation = nextSubscriptionGeneration++;
      const previous = activeSubscription;
      // Select the new stream before entering the native call because a runtime
      // may synchronously publish its opening snapshot from subscribe(). A
      // callback retained by a retired runtime is rejected by this generation.
      activeSubscription = { id: -1, generation };
      const openingDeltas: Parameters<SubscriptionManager<T>["handleDelta"]>[0][] = [];
      let installationComplete = false;
      let id: number;
      try {
        id = client.subscribe(
          wasmQuery,
          (delta) => {
            if (unsubscribed || activeSubscription?.generation !== generation) return;
            if (!installationComplete) {
              openingDeltas.push(delta);
              return;
            }
            handleDelta(delta);
          },
          subscriptionOptions,
          context?.readSession ?? context?.session ?? session,
        );
      } catch (error) {
        activeSubscription = previous;
        throw error;
      }
      const subscription = { id, generation };
      activeSubscription = subscription;
      installationComplete = true;
      if (unsubscribed) {
        client.unsubscribe(id);
        activeSubscription = null;
        return null;
      }
      try {
        for (const delta of openingDeltas) handleDelta(delta);
      } catch (error) {
        client.unsubscribe(id);
        activeSubscription = previous;
        throw error;
      }
      return subscription;
    };
    const traceId = this.registerActiveQuerySubscriptionTrace(
      wasmQuery,
      builtQuery.table,
      queryOptions,
    );
    if (queryOptions.tier == null || queryOptions.tier === "local") {
      callback(manager.seed([]));
    }
    if (
      this.connection.shouldDeferSubscriptionStart(resolveReadTier(queryOptions.tier ?? "local"))
    ) {
      // The worker can only classify the initial authority-tier snapshot as
      // settled after its own server transport is attached. Delay native
      // subscription creation until that topology is ready; the native stream
      // then owns the settled-snapshot gate and remains the sole data source.
      void this.ensureReady(resolveReadTier(queryOptions.tier ?? "local"), readyAbort.signal)
        .then(() => startNativeSubscription())
        .catch((error: unknown) => {
          if (unsubscribed || readyAbort.signal.aborted || this.isShuttingDown) return;
          setTimeout(() => {
            throw error;
          }, 0);
        });
    } else {
      startNativeSubscription();
    }
    // A remote-if-possible subscription opened during an explicit disconnect
    // truthfully starts from local state, then replaces that native stream with
    // the ordinary edge-gated stream after reconnect. Transport errors never
    // take this branch.
    if (remoteIfPossibleOffline) {
      void this.connection
        .waitForReconnect(readyAbort.signal)
        .then(async () => {
          if (unsubscribed || readyAbort.signal.aborted) return;
          await this.ensureReady("edge", readyAbort.signal);
          if (unsubscribed || readyAbort.signal.aborted) return;
          const retired = activeSubscription;
          // Keep the local stream live while the remote authority is not ready.
          // Installing the replacement changes the accepted callback generation
          // atomically; only then is the retired local stream detached.
          const replacement = startNativeSubscription({ ...queryOptions, tier: "edge" }, true);
          if (retired && replacement?.generation !== retired.generation) {
            client.unsubscribe(retired.id);
          }
        })
        .catch((error: unknown) => {
          if (!unsubscribed && !readyAbort.signal.aborted)
            setTimeout(() => {
              throw error;
            }, 0);
        });
    }
    if (
      this.config.serverUrl &&
      !remoteIfPossibleOffline &&
      !strictRemoteReadTier &&
      // `edge` and `global` promise that their first callback is the worker's
      // authority-tier snapshot.  A browser worker cannot establish that
      // snapshot until its server transport is ready, so never race it with a
      // main-thread local-storage seed (including after Db.disconnect()).
      !this.connection.shouldDeferSubscriptionStart(
        resolveReadTier(queryOptions.tier ?? "local"),
      ) &&
      queryOptions.propagation !== "local-only" &&
      resolveReadTier(queryOptions.tier ?? "local") !== "global" &&
      !queryUsesRelationTraversal(builtQuery)
    ) {
      const seedQuery = () =>
        this.all(query, { ...options, tier: "local", propagation: "local-only" });
      const seedRows =
        session == null
          ? seedQuery()
          : this.withRuntimeOperationContext({ session }, () => seedQuery());
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
      readyAbort.abort();
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      if (activeSubscription !== null && activeSubscription.id >= 0) {
        client.unsubscribe(activeSubscription.id);
      }
      activeSubscription = null;
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes the Db's runtime client.
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
    this.mutationErrorListeners.clear();

    await this.connection.shutdown();
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
    options?: InternalQueryExecutionOptions,
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
        branches,
      };
    } catch {
      return {
        table: "unknown",
        branches: [],
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

  let resolvedConfig: DbConfig = { ...config };
  await runtimeSource.load(config);
  const {
    secret: _secret,
    jwtToken: _jwtToken,
    cookieSession: _cookieSession,
    ...configWithoutAuth
  } = config;

  // Local-first auth: resolve seed and mint a JWT
  let localFirstSecret: string | null = null;
  if (config.secret) {
    const secret = config.secret;
    localFirstSecret = secret;

    if (!config.jwtToken) {
      const jwtToken = runtimeSource.mintLocalFirstToken(
        createRuntimeTokenOptions(secret, config.appId, 3600),
      );
      const trustedReservedSession = internalSessionFromVerifiedReservedJwtPayload(
        parseJwtPayload(jwtToken) ?? {},
        "local-first",
      );
      resolvedConfig = { ...configWithoutAuth, jwtToken };
      setTrustedReservedSession(resolvedConfig, trustedReservedSession);
    }
  } else if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    // Anonymous: mint an ephemeral keypair + anonymous JWT.
    // Admin-secret clients intentionally stay sessionless so local policy
    // evaluation does not preempt backend-authorized transport writes.
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const jwtToken = runtimeSource.mintAnonymousToken(
      createRuntimeTokenOptions(ephemeralSeed, config.appId, 3600),
    );
    const trustedReservedSession = internalSessionFromVerifiedReservedJwtPayload(
      parseJwtPayload(jwtToken) ?? {},
      "anonymous",
    );
    resolvedConfig = { ...configWithoutAuth, jwtToken };
    setTrustedReservedSession(resolvedConfig, trustedReservedSession);
  }

  const driver = resolveStorageDriver(resolvedConfig.driver);
  const db =
    runtimeSource.supportsBrowserWorker && isBrowserRuntime() && driver.type === "persistent"
      ? await Db.createWithBrowserWorker(resolvedConfig, runtimeSource as AnyRuntimeSource)
      : Db.create(resolvedConfig, runtimeSource as AnyRuntimeSource);

  if (localFirstSecret) {
    db.initLocalFirstAuth(localFirstSecret, 3600, !config.jwtToken);
  }

  return db;
}

export async function createDb(config: DbConfig): Promise<Db> {
  return await createDbWithRuntimeSource(config, new DefaultRuntimeSource());
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof Worker !== "undefined";
}
