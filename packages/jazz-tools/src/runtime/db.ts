import { Utf8Decoder } from "./utf8.js";
import { runtimeRandomBytes } from "./runtime-entropy.js";
import { initialRecipientIds } from "../e2ee/space-lifecycle.js";
import { acceptInitialHistory, discardInitialHistory } from "../e2ee/accepted-history.js";
import {
  e2eeForDb,
  e2eeSchemaForDb,
  prepareInitialSpaceForTransaction,
  prepareInitialSpaceRows,
  withSpaceKeys,
  type E2ee,
} from "../e2ee/lifecycle.js";
import type { SpaceRoot } from "../e2ee/spaces.js";
import { equalityToken } from "../e2ee/equality-data.js";
import { prepareEqualityQuery } from "../e2ee/equality-query.js";
import { queryKeyDependencies, decryptedQuerySpaces } from "../e2ee/query-dependencies.js";
import { E2eeDataError } from "../e2ee/data-error.js";
import { equalityIndexColumn } from "../e2ee/encrypted-schema.js";
import type { AccountHandle } from "../accounts/state.js";
import { GracefulShutdownSyncError } from "./graceful-shutdown-error.js";
import { accountToken, accountRegistry } from "../accounts/enrollment.js";
import { assertAccountConfig, copyAccountConfigAdmission } from "../accounts/config-capability.js";
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
  WasmSchema,
  WasmRow,
  StorageDriver,
  Value,
} from "../drivers/types.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  ExclusiveWriteHandle,
  ExclusiveWriteResult,
  WriteResult,
  JazzClient,
  type AuthUpdate,
  withTransactionAdmission,
  type MutationErrorEvent,
  WriteHandle,
  setWriteWaitReadiness,
  type TransactionKind,
  type InsertOptions as InternalInsertOptions,
  type RestoreOptions as InternalRestoreOptions,
  type UpdateOptions as InternalUpdateOptions,
  type DurabilityTier,
  type QueryExecutionOptions,
  type InternalQueryExecutionOptions,
  type RowSettlement,
  type QueryPropagation,
  type QueryVisibility,
  isPublicQueryReadTier,
  resolveEffectiveQueryExecutionOptions,
  resolveReadTier,
  ReadTier,
  type BranchSelector,
  type BranchView,
  type OpenTransactionId,
  type TxId,
  type PermissionAdvice,
  type StreamingValueSource,
  type TransactionPreparationIO,
} from "./client.js";
import { type RuntimeSource, type RuntimeTokenOptions } from "./runtime-source.js";
import type { AuthFailureReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { applyColumnTransforms, transformRow, transformRows } from "./row-transformer.js";
import { toValue, toWriteRecord } from "./value-converter.js";
import { encryptedSchemas, encryptedRowSpaces } from "../e2ee/encrypted-schema.js";
import { encryptCell, decryptCellRows, decryptedIndexBytes } from "../e2ee/cell-data.js";
import { TypedTableQueryBuilder, type AnyTableMeta } from "../typed-app.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import {
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  resolveClientInternalSessionSync,
} from "./client-session.js";
import { createBrowserPhysicalDatabaseName } from "./browser-worker-config.js";
import {
  createInspectorLocalQueryOptions,
  isInspectorLocalQueryOptions,
} from "../internal/inspector-query.js";
import { authSecretSeedForMinting } from "./auth-secret-codec.js";
import {
  getDbInternalSession,
  getTrustedReservedSession,
  setDbInternalSession,
  setTrustedReservedSession,
} from "./db-internal-session.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import {
  normalizeBuiltQuery,
  type BuiltRelation,
  type NormalizedBuiltQuery,
  type NormalizedIncludeSpec,
} from "./query-builder-shape.js";
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
  /** @internal Assigned by validated account-handle context creation. */
  accountId?: string;
  /** @internal Handle-derived enrollment authority, independent of active transport. */
  accountRegistryAuthority?: string;
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
  /** @internal Server-only admission credential; client DbConfig must never carry it. */
  backendSecret?: never;
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

/** @internal Resolve the caller-selected logical base for browser persistence. */
export function resolvePersistentDbBaseName(config: DbConfig): string {
  const driver = resolveStorageDriver(config.driver);
  const explicitDbName = trimOptionalString(
    (driver.type === "persistent" ? driver.dbName : undefined) ?? config.dbName,
  );
  return explicitDbName ?? config.appId;
}

/** @internal Derive the physical browser persistence namespace for this Db config. */
export function resolveDefaultPersistentDbName(config: DbConfig): string {
  return createBrowserPhysicalDatabaseName(config, resolvePersistentDbBaseName(config));
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
  /** All app transforms, retained outside the serialised query representation. */
  readonly _columnTransformsByTable?: ColumnTransformRegistry;
  /** Build and return the query as JSON */
  _build(): string;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
}

export type BranchValue = string | number | bigint;
export type QualifiedBranch = Record<string, BranchValue>;
export type Branch = BranchValue | QualifiedBranch;
export type BranchBase = Branch | readonly [branch: Branch, snapshot: unknown];

export type QueryOptions = Omit<QueryExecutionOptions, "branch"> & {
  /** Current branch coordinate. A scalar selects a table with one `branchBy` column. */
  branch?: Branch;
  /** Optional live base, or `[base, snapshotRef]` for a frozen base. */
  base?: BranchBase;
};

type InternalDbQueryOptions = Omit<QueryOptions, "tier"> & {
  tier?: InternalQueryExecutionOptions["tier"];
  localUpdates?: InternalQueryExecutionOptions["localUpdates"];
  propagation?: InternalQueryExecutionOptions["propagation"];
  visibility?: InternalQueryExecutionOptions["visibility"];
};
/**
 * Callbacks for a public live query subscription.
 *
 * The function form of {@link Db.subscribe} remains supported for compatibility,
 * but the object form is preferred because it makes terminal failures explicit.
 * After `onError` runs, that subscription will not publish further values.
 */
export interface DbSubscriptionCallbacks<T extends { id: string }> {
  /** Called with the complete current result whenever the query changes. */
  onUpdate: (rows: T[]) => void;
  /** Called once when the subscription terminates with an error. */
  onError?: (error: Error) => void;
}

/** Package-internal callbacks for incremental subscription consumers. */
export interface DbDeltaSubscriptionCallbacks<T extends { id: string }> {
  onDelta: (delta: SubscriptionDelta<T>) => void;
  onError?: (error: Error) => void;
  /** A newer encrypted result is awaiting logical materialisation. */
  onPending?: () => void;
}

/**
 * Lower product options to the internal native-read controls. This is a
 * runtime boundary, rather than merely a TypeScript one: JavaScript callers
 * must not be able to select local-only propagation or a deferred own-write
 * overlay by adding private fields to an options object.
 */
function lowerPublicDbQueryOptions(options?: QueryOptions): InternalDbQueryOptions | undefined {
  if (!options) return undefined;
  const candidate = options as QueryOptions & {
    tier?: unknown;
    branch?: unknown;
    base?: unknown;
  };
  const lowered: InternalDbQueryOptions = {};
  if (isPublicQueryReadTier(candidate.tier)) lowered.tier = candidate.tier;
  if (candidate.branch !== undefined) lowered.branch = candidate.branch as Branch;
  if (candidate.base !== undefined) lowered.base = candidate.base as BranchBase;
  if (isInspectorLocalQueryOptions(options)) lowered.tier = "local-only";
  return lowered;
}

/** Package-internal subscription surface used by Jazz's UI bindings. */
export interface DbSubscriptionSource {
  /**
   * Prepare public query options before they become part of a subscription
   * cache key. The Inspector attachment uses this private seam to add its
   * local-read capability; applications never receive a constructor for it.
   */
  prepareQueryOptions?(options?: QueryOptions): QueryOptions | undefined;
  all?<T extends { id: string }>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
    session?: Session,
  ): Promise<T[]> | T[];
  subscribeDelta<T extends { id: string }>(
    query: QueryBuilder<T>,
    callbacks: ((delta: SubscriptionDelta<T>) => void) | DbDeltaSubscriptionCallbacks<T>,
    options?: QueryOptions,
    session?: Session,
  ): SubscriptionHandle;
}

/**
 * Cancels a subscription. Browser-worker followers also expose the initial
 * admission boundary so framework bindings can surface an asynchronous open
 * failure through their ordinary subscription error state rather than as an
 * ambient exception.
 *
 * @internal
 */
export type SubscriptionHandle = (() => void) & { readonly ready?: Promise<void> };

const dbSubscriptionSources = new WeakMap<Db, DbSubscriptionSource>();

/** @internal Retrieve the incremental source associated with a public Db. */
export function getDbSubscriptionSource(db: Db): DbSubscriptionSource {
  const source = dbSubscriptionSources.get(db);
  if (!source) throw new Error("Jazz Db is missing its internal subscription source.");
  return source;
}

interface TimestampOverrideOptions {
  updatedAt?: number;
}

export interface InsertOptions extends TimestampOverrideOptions {
  id?: string;
  branch?: Branch;
  /** New encryption scope recipients; omission grants the creator. */
  initialRecipients?: readonly string[];
}
export type StreamingInsertOptions = Omit<InsertOptions, "branch" | "initialRecipients">;

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: Branch;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: Branch;
  base?: BranchBase;
}

export interface UpsertOptions extends UpdateOptions {
  /** Only valid when creating a new encryption scope. Replaces the creator default. */
  initialRecipients?: readonly string[];
}

function scopeRecipients<T, Init>(
  table: TableProxy<T, Init>,
  options?: { initialRecipients?: readonly string[] },
): string[] | undefined {
  if (options?.initialRecipients === undefined) return undefined;
  if (!encryptedSchemas.get(table._schema)?.scopes.has(table._table))
    throw new Error("initialRecipients requires an encryption scope table");
  return initialRecipientIds(options.initialRecipients);
}

type TypedUpdateOptionsWithDiffs<TReplacements extends object, TDiffs> = UpdateOptions & {
  applyDiffs?: TDiffs & { [TColumn in keyof TReplacements]?: never };
};

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
  options?: InternalDbQueryOptions,
): InternalQueryExecutionOptions {
  if (!options) return {};
  const { branch, base, ...rest } = options;
  if (branch === undefined) {
    if (base !== undefined) throw new Error("A branch base requires a branch head.");
    return rest;
  }
  return {
    ...rest,
    branch: normalizeBranchView(schema, tableName, branch, base),
  };
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
    : {
        ...rest,
        branch: normalizeBranchSelector(schema, tableName, branch, "table"),
      };
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
    : {
        ...rest,
        branch: normalizeBranchSelector(schema, tableName, branch, "table"),
      };
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
  return {
    ...rest,
    branch: normalizeBranchView(schema, tableName, branch, base),
  };
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
    get _columnTransformsByTable() {
      return query._columnTransformsByTable;
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
      line.includes("Db.subscribe") ||
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
    const firstTable = resolveBuiltRelationOutputTable(schema, first.input);
    for (const input of relation.union.inputs.slice(1)) {
      const inputTable = resolveBuiltRelationOutputTable(schema, input.input);
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

function toWriteRecordForOperation(
  operation: WriteOperationName,
  data: Record<string, unknown>,
  schema: WasmSchema,
  tableName: string,
) {
  try {
    const encryption = encryptedSchemas.get(schema)?.tables.get(tableName);
    if (encryption?.columns.some((name) => Object.hasOwn(data, name)))
      throw new Error("Encrypted writes require E2EE transaction preparation");
    return toWriteRecord(data, schema, tableName);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${operation} failed: WriteError("${escapeWriteErrorReason(message)}")`);
  }
}

type WireSplicePage = { kind: "bytes" | "text_utf16" | "text_utf8"; from: number; to: number };

type WireLargeValueUpdate =
  | {
      kind: "splice";
      column: string;
      within: WireSplicePage;
      splices: Array<{ at: number; delete: number; insert: number[] }>;
    }
  | {
      kind: "json_set";
      column: string;
      edits: Array<{ at: string; value: unknown }>;
    };

function isPartialLargeValueUpdate(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && ("splices" in value || "edits" in value);
}

function requireRecord(value: unknown, message: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(message);
  }
  return value as Record<string, unknown>;
}

function requireNonNegativeCoordinate(value: unknown, label: string, column: string): number {
  const coordinate = Number(value);
  if (!Number.isSafeInteger(coordinate) || coordinate < 0) {
    throw new Error(`${label} for "${column}" must be a non-negative safe integer.`);
  }
  return coordinate;
}

function requirePageRange(from: number, to: number, column: string): void {
  if (from > to) {
    throw new Error(`Large-value page for "${column}" must have from <= to.`);
  }
}

function splitLargeValueUpdate(
  data: Record<string, unknown>,
  schema: WasmSchema,
  table: string,
): { ordinary: Record<string, unknown>; descriptors: WireLargeValueUpdate[] } {
  const ordinary: Record<string, unknown> = {};
  const descriptors: WireLargeValueUpdate[] = [];
  const columns = schema[table]?.columns;
  if (!columns) throw new Error(`Unknown table "${table}"`);
  for (const [column, value] of Object.entries(data)) {
    if (!isPartialLargeValueUpdate(value)) {
      ordinary[column] = value;
      continue;
    }
    const type = columns.find((candidate) => candidate.name === column)?.column_type;
    if (!type) throw new Error(`Unknown column "${column}" in table "${table}"`);
    if (encryptedSchemas.get(schema)?.tables.get(table)?.columns.includes(column)) {
      throw new Error("Encrypted partial updates are not supported");
    }
    if ("splices" in value && "within" in value) {
      const within = requireRecord(
        value.within,
        `Large-value update "${column}" has an invalid page.`,
      );
      const splices = value.splices;
      if (!Array.isArray(splices))
        throw new Error(`Large-value update "${column}" has invalid splices.`);
      let page: WireSplicePage;
      if ("fromUtf8" in within || "toUtf8" in within) {
        if (type.type !== "Text")
          throw new Error(`UTF-8 splice requires a Text column, got "${column}".`);
        const from = requireNonNegativeCoordinate(within.fromUtf8, "fromUtf8", column);
        const to = requireNonNegativeCoordinate(within.toUtf8, "toUtf8", column);
        requirePageRange(from, to, column);
        page = { kind: "text_utf8", from, to };
      } else if (type.type === "Bytea") {
        const from = requireNonNegativeCoordinate(within.from, "from", column);
        const to = requireNonNegativeCoordinate(within.to, "to", column);
        requirePageRange(from, to, column);
        page = { kind: "bytes", from, to };
      } else if (type.type === "Text") {
        const from = requireNonNegativeCoordinate(within.from, "from", column);
        const to = requireNonNegativeCoordinate(within.to, "to", column);
        requirePageRange(from, to, column);
        page = { kind: "text_utf16", from, to };
      } else {
        throw new Error(`Splice requires a Text or Bytea column, got "${column}".`);
      }
      descriptors.push({
        kind: "splice",
        column,
        within: page,
        splices: splices.map((splice) => {
          const item = requireRecord(splice, `Large-value splice for "${column}" is invalid.`);
          const utf8 = page.kind === "text_utf8";
          const at = requireNonNegativeCoordinate(utf8 ? item.atUtf8 : item.at, "at", column);
          const deleted = requireNonNegativeCoordinate(
            utf8 ? item.deleteUtf8 : item.delete,
            "delete",
            column,
          );
          const insert = item.insert;
          let bytes: number[];
          if (page.kind === "bytes") {
            if (!(insert instanceof Uint8Array)) {
              throw new Error(`Byte splice insert for "${column}" must be a Uint8Array.`);
            }
            bytes = [...insert];
          } else {
            if (typeof insert !== "string") {
              throw new Error(`Text splice insert for "${column}" must be a string.`);
            }
            bytes = [...new TextEncoder().encode(insert)];
          }
          return { at, delete: deleted, insert: bytes };
        }),
      });
      continue;
    }
    if ("edits" in value) {
      if (type.type !== "Json")
        throw new Error(`JSON set requires a JSON column, got "${column}".`);
      if (!Array.isArray(value.edits))
        throw new Error(`JSON update "${column}" has invalid edits.`);
      descriptors.push({
        kind: "json_set",
        column,
        edits: value.edits.map((edit) => {
          const item = requireRecord(edit, `JSON update edit for "${column}" is invalid.`);
          if (item.op !== "set" || typeof item.at !== "string") {
            throw new Error(
              `JSON update "${column}" supports only { op: "set", at, value } edits.`,
            );
          }
          return { at: item.at, value: item.value };
        }),
      });
      continue;
    }
    ordinary[column] = value;
  }
  return { ordinary, descriptors };
}

type PartialValueSelection =
  | { from: number; to: number }
  | { fromUtf8: number; toUtf8: number }
  | { at: string };

function utf16Boundary(text: string, offset: number): boolean {
  if (offset < 0 || offset > text.length) return false;
  if (offset === 0 || offset === text.length) return true;
  const before = text.charCodeAt(offset - 1);
  const after = text.charCodeAt(offset);
  return !(before >= 0xd800 && before <= 0xdbff && after >= 0xdc00 && after <= 0xdfff);
}

function jsonPointerToken(token: string): string {
  let decoded = "";
  for (let index = 0; index < token.length; index += 1) {
    const character = token[index]!;
    if (character !== "~") {
      decoded += character;
      continue;
    }
    const escape = token[++index];
    if (escape === "0") decoded += "~";
    else if (escape === "1") decoded += "/";
    else throw new Error("JSON pointer has an invalid escape.");
  }
  return decoded;
}

function jsonPointerValue(value: unknown, pointer: string): unknown {
  if (pointer === "") return value;
  if (!pointer.startsWith("/")) throw new Error("JSON pointer must be empty or begin with '/'.");
  let current: unknown = value;
  for (const rawToken of pointer.slice(1).split("/")) {
    const token = jsonPointerToken(rawToken);
    if (Array.isArray(current)) {
      if (!/^(0|[1-9]\d*)$/.test(token)) {
        throw new Error("JSON array pointer token is not an index.");
      }
      const index = Number(token);
      if (!Number.isSafeInteger(index) || index >= current.length) {
        throw new Error("JSON pointer path does not exist.");
      }
      current = current[index];
    } else if (typeof current === "object" && current !== null && Object.hasOwn(current, token)) {
      current = (current as Record<string, unknown>)[token];
    } else {
      throw new Error("JSON pointer path does not exist.");
    }
  }
  return current;
}

/**
 * Temporary binding-level materialization until #2090 carries exact terminal
 * demand into Groove. It preserves the public result/coordinate contract and
 * only touches selected columns; it must not be used as a chunk-demand model.
 */
function applyPartialValueSelections<T>(
  row: T,
  selections: Record<string, PartialValueSelection>,
): T {
  if (Object.keys(selections).length === 0 || typeof row !== "object" || row === null) return row;
  const projected = { ...(row as Record<string, unknown>) };
  for (const [column, selection] of Object.entries(selections)) {
    const value = projected[column];
    if ("at" in selection) {
      projected[column] = jsonPointerValue(value, selection.at);
      continue;
    }
    if (value instanceof Uint8Array) {
      if ("fromUtf8" in selection || selection.from > selection.to || selection.to > value.length) {
        throw new Error(`Byte range for "${column}" is out of bounds.`);
      }
      projected[column] = value.slice(selection.from, selection.to);
      continue;
    }
    if (typeof value !== "string") {
      throw new Error(`Large-value selection for "${column}" has an incompatible column type.`);
    }
    if ("fromUtf8" in selection) {
      const bytes = new TextEncoder().encode(value);
      if (
        selection.fromUtf8 > selection.toUtf8 ||
        selection.toUtf8 > bytes.length ||
        (selection.fromUtf8 < bytes.length &&
          (bytes[selection.fromUtf8]! & 0b1100_0000) === 0b1000_0000) ||
        (selection.toUtf8 < bytes.length &&
          (bytes[selection.toUtf8]! & 0b1100_0000) === 0b1000_0000)
      ) {
        throw new Error(`UTF-8 range for "${column}" splits a code point or is out of bounds.`);
      }
      projected[column] = new Utf8Decoder({ fatal: true }).decode(
        bytes.slice(selection.fromUtf8, selection.toUtf8),
      );
      continue;
    }
    if (
      selection.from > selection.to ||
      !utf16Boundary(value, selection.from) ||
      !utf16Boundary(value, selection.to)
    ) {
      throw new Error(`UTF-16 range for "${column}" splits a surrogate pair or is out of bounds.`);
    }
    projected[column] = value.slice(selection.from, selection.to);
  }
  return projected as T;
}

function escapeWriteErrorReason(message: string): string {
  return message.replaceAll('"', '\\"');
}

/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<
  T,
  Init,
  StreamingInit = unknown,
  StreamingUpdate = unknown,
  LargeValueUpdate = unknown,
> {
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
  /** @internal Phantom — preserves typed page-edit descriptors on table handles. */
  readonly _largeValueUpdateType?: LargeValueUpdate;
}

export interface ColumnTransform {
  from(value: unknown): unknown;
  to(value: unknown): unknown;
}

export type ColumnTransformMap = Record<string, ColumnTransform>;
export type ColumnTransformRegistry = Record<string, ColumnTransformMap | undefined>;

function resolveOutputColumnTransforms<T>(
  query: QueryBuilder<T>,
  inputTable: string,
  outputTable: string,
): ColumnTransformMap | undefined {
  return (
    query._columnTransformsByTable?.[outputTable] ??
    (inputTable === outputTable ? query._columnTransforms : undefined)
  );
}
type DbTransactionHandleBinding = {
  ownerClient: JazzClient;
  resolveClient: (schema: WasmSchema) => JazzClient;
  openTransactionId: OpenTransactionId;
  session?: Session;
  attribution?: string;
};

const dbTxHandleBindings = new WeakMap<Transaction, DbTransactionHandleBinding>();
const initialisingTransactions = new WeakSet<Transaction>();

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
  table: TableProxy<any, any, any, any, any>,
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
  table: TableProxy<any, any, any, any, any>,
  data: unknown,
  operation: "insert" | "update" | "upsert",
): {
  column: string;
  source: StreamingValueSource;
  values: Record<string, unknown>;
} {
  if (typeof data !== "object" || data === null) {
    throw new Error("Streaming insert data must be an object");
  }
  const record = data as Record<string, unknown>;
  const encryption = encryptedSchemas.get(table._schema);
  if (operation !== "update" && encryption?.scopes.has(table._table)) {
    throw new Error("Encryption scope streaming creation is not supported");
  }
  const declaration = encryption?.tables.get(table._table);
  if (
    declaration &&
    (operation !== "update" ||
      Object.hasOwn(record, declaration.space) ||
      declaration.columns.some((column) => Object.hasOwn(record, column)))
  ) {
    throw new Error("Encrypted streaming is not supported");
  }
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
  txId: TxId | Promise<TxId>,
  client: JazzClient,
): TransactionWriteResult<TResult, TKind> {
  if (transaction.kind === "exclusive") {
    return new ExclusiveWriteResult(value, txId, client) as TransactionWriteResult<TResult, TKind>;
  }

  return new WriteResult(value, txId, client) as TransactionWriteResult<TResult, TKind>;
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
      // Preserve the commit error while ensuring an empty mergeable transaction is
      // consumed when the callback helper has no handle to return to callers.
    }
    throw error;
  }
  const txId = committed.txId.catch(async (error) => {
    try {
      await transaction.rollback();
    } catch {
      // Preserve the deferred commit error, just as for synchronous commit failure.
    }
    throw error;
  });
  return createTransactionWriteResult(transaction, resolvedValue, txId, resultClient());
}

function copyMutableEncryptedValue(value: Value): Value {
  switch (value.type) {
    case "Bytea":
    case "Array":
    case "Row":
    case "Enum":
      return structuredClone(value);
    default:
      return value;
  }
}

function branchCoordinateKey(branch: BranchView | BranchSelector | undefined): string {
  const selectorKey = (selector: BranchSelector) =>
    Object.entries(selector.values)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([column, value]) => [
        column,
        value.type === "Uuid" ? { ...value, value: value.value.toLowerCase() } : value,
      ]);
  if (!branch) return "default";
  const head = "head" in branch ? branch.head : branch;
  return JSON.stringify(selectorKey(head), (_key, value) =>
    typeof value === "bigint" ? { bigint: value.toString() } : value,
  );
}

function encryptedRowScopeKey(
  table: string,
  id: string,
  branch: BranchView | BranchSelector | undefined,
): string {
  return JSON.stringify([table, id.toLowerCase(), branchCoordinateKey(branch)]);
}

/**
 * Groups a set of writes as either a mergeable or exclusive transaction (see {@link TransactionKind}).
 */
export class Transaction<TKind extends TransactionKind = TransactionKind> {
  private readonly pendingReads = new Set<Promise<unknown>>();
  private committing = false;
  private failedRead = false;
  private failedReadCleanupComplete = false;
  private assertUsable(operation: string): void {
    if (this.failedRead && operation !== "rollback") {
      throw new Error(`DbTransaction.${operation}() cannot run after a pending read failed`);
    }
    if (this.committing) {
      throw new Error(`DbTransaction.${operation}() cannot run after commit has been requested`);
    }
  }
  private cancelled = false;
  private readonly initialSpaceKeys = new Map<string, { secret: Uint8Array; root: SpaceRoot }>();
  private readonly encryptedScopes = new Map<string, string>();

  constructor(
    readonly kind: TKind,
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly session?: Session,
    private readonly attribution?: string,
    ownerClient?: JazzClient,
    private readonly e2ee?: { db: Db },
  ) {
    if (ownerClient) this.bindOwnerClient(ownerClient);
  }

  private bindTable<T, Init, StreamingInit, StreamingUpdate, LargeValueUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate, LargeValueUpdate>,
  ): DbTransactionHandleBinding {
    this.assertUsable("table operation");
    const client = this.resolveClient(table._schema);
    if (!dbTxHandleBindings.has(this)) this.bindOwnerClient(client);
    return this.requireBinding("table operation");
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbTransactionHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>);
  }

  private requireBinding(operation: string): DbTransactionHandleBinding {
    this.assertUsable(operation);
    return getDbTxHandleBinding(this, operation);
  }

  private bindOwnerClient(ownerClient: JazzClient): void {
    dbTxHandleBindings.set(this, {
      ownerClient,
      resolveClient: this.resolveClient,
      openTransactionId: ownerClient.beginTransaction(this.kind, this.session, this.attribution),
      session: this.session,
      attribution: this.attribution,
    });
  }

  openTransactionId(): OpenTransactionId {
    return this.requireBinding("openTransactionId").openTransactionId;
  }

  /**
   * Commit this transaction.
   */
  commit(): TransactionCommitHandle<TKind> {
    const { ownerClient, openTransactionId } = this.requireBinding("commit");
    let txId: Promise<TxId>;
    if (this.pendingReads.size > 0) {
      txId = Promise.all(this.pendingReads).then(
        () => ownerClient.commitTransaction(openTransactionId).txId,
        async (error) => {
          this.failedRead = true;
          try {
            await ownerClient.rollbackTransaction(openTransactionId);
            this.failedReadCleanupComplete = true;
          } catch {
            // Preserve the original pending read error.
          }
          this.clearInitialSpaceKeys();
          throw error;
        },
      );
    } else {
      txId = ownerClient.commitTransaction(openTransactionId).txId;
    }
    this.committing = true;
    const initialRoots: SpaceRoot[] | undefined = initialisingTransactions.has(this)
      ? []
      : undefined;
    const finishCommit = () => {
      this.committing = false;
      if (initialRoots)
        for (const { root } of this.initialSpaceKeys.values()) initialRoots.push(root);
      this.clearInitialSpaceKeys(true);
    };
    // Observe completion without delaying application waits on the transaction ID.
    void txId.then(finishCommit, finishCommit);
    if (initialRoots) {
      // Local durability cannot authorise a provisional epoch. Keeping the
      // acceptance floor on txId also preserves it through callback/map handles.
      txId = txId
        .then(async (id) => {
          await ownerClient.waitForExclusiveTransaction(id, "global");
          // Cache preparation with its accepted identity. The cache cannot reject
          // this receipt and does not perform another authority read.
          for (const root of initialRoots) await acceptInitialHistory(this.e2ee!.db, root, id);
          return id;
        })
        .catch((error) => {
          for (const root of initialRoots) discardInitialHistory(this.e2ee!.db, root);
          throw error;
        });
    }
    if (this.kind === "exclusive") {
      return new ExclusiveWriteHandle(txId, ownerClient) as TransactionCommitHandle<TKind>;
    }
    return new WriteHandle(txId, ownerClient) as TransactionCommitHandle<TKind>;
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
    const { ownerClient, openTransactionId } = this.requireBinding("rollback");
    if (this.failedReadCleanupComplete) return Promise.resolve(false);
    this.cancelled = true;
    this.clearInitialSpaceKeys();
    return ownerClient.rollbackTransaction(openTransactionId);
  }

  private clearInitialSpaceKeys(committing = false): void {
    for (const { secret, root } of this.initialSpaceKeys.values()) {
      secret.fill(0);
      if (!committing && this.e2ee) discardInitialHistory(this.e2ee.db, root);
    }
    this.initialSpaceKeys.clear();
    this.encryptedScopes.clear();
  }
  private encryptedScopeFor(
    table: string,
    id: string,
    branch: BranchView | BranchSelector | undefined,
  ): string | undefined {
    return this.encryptedScopes.get(encryptedRowScopeKey(table, id, branch));
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): T {
    const recipients = scopeRecipients(table, options);
    const initialisesSpace = encryptedSchemas.get(table._schema)?.scopes.has(table._table);
    if (initialisesSpace) this.prepareScopeCreation();
    this.bindTable(table);
    const transformedData = transformInputColumns(table, data);
    const encryption = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (encryption) {
      if (initialisesSpace)
        throw new Error("Encrypted scope rows require dependent scope preparation");
      return this.prepareEncryptedRow(table, transformedData, options);
    }
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("insert");
    const row = client.insertInternal(
      table._table,
      values,
      normalizeInsertOptions(table._schema, table._table, options),
      session,
      attribution,
      openTransactionId,
    );
    if (initialisesSpace) {
      const preparation = prepareInitialSpaceForTransaction(
        this.e2ee!.db,
        this as Transaction<"exclusive">,
        table,
        row.id,
        (secret, root) => this.retainInitialSpaceKey(secret, root),
        recipients,
      );
      // The transaction's pending preparation and wait handle own this failure.
      preparation.catch(() => {});
    }
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  private prepareScopeCreation(): void {
    if (this.kind !== "exclusive" || !this.e2ee)
      throw new Error("E2EE scope creation requires an authenticated exclusive transaction");
  }

  private async retainInitialSpaceKey(secret: Uint8Array, root: SpaceRoot): Promise<void> {
    if (this.cancelled) {
      if (this.e2ee) discardInitialHistory(this.e2ee.db, root);
      throw new Error("Transaction was rolled back during E2EE preparation");
    }
    this.initialSpaceKeys.set(`${root.scopeId}:${root.identifier}`, {
      secret: Uint8Array.from(secret),
      root,
    });
  }

  private prepareEncryptedRow<T, Init>(
    table: TableProxy<T, Init>,
    data: Record<string, unknown>,
    options?: InsertOptions,
    operation: "insert" | "restore" = "insert",
  ): T {
    if (!this.e2ee) throw new Error("Encrypted writes require an authenticated E2EE context");
    const metadata = encryptedSchemas.get(table._schema)!;
    const declaration = metadata.tables.get(table._table)!;
    const logicalTable = metadata.logical[table._table]!;
    const values = structuredClone(toWriteRecord(data, metadata.logical, table._table));
    const physical = { ...values };
    for (const name of declaration.columns) {
      const column = logicalTable.columns.find((column) => column.name === name)!;
      if (!values[name]) {
        if (!column.nullable) throw new Error(`Missing encrypted column "${name}"`);
        values[name] = { type: "Null" };
      }
      physical[name] = { type: "Bytea", value: new Uint8Array() };
      if (declaration.indexes?.[name])
        physical[equalityIndexColumn(name)] = { type: "Bytea", value: new Uint8Array() };
    }
    const identifier = values[declaration.space];
    if (identifier?.type !== "Uuid") throw new Error("Encrypted writes require a space identifier");
    const optionsSnapshot = options ? structuredClone(options) : undefined;
    const binding = this.requireBinding(operation);
    const { ownerClient, openTransactionId, session, attribution } = binding;
    const normalized = normalizeInsertOptions(table._schema, table._table, optionsSnapshot);
    const preview = ownerClient.previewInsertInternal(table._table, physical, normalized?.id);
    const scope = new TypedTableQueryBuilder(declaration.scope, table._schema);
    const db = this.e2ee.db;
    ownerClient.prepareTransaction(openTransactionId, async (io) => {
      if (operation === "restore") {
        const query = new TypedTableQueryBuilder<
          AnyTableMeta & { row: { id: string } & Record<string, unknown> }
        >(table._table, table._schema)
          .includeDeleted()
          .where({ id: preview.id })
          .select(declaration.space);
        const rows = await readTransactionRows<Record<string, unknown>>(
          query,
          {
            tier: "global",
            ...(optionsSnapshot?.branch !== undefined && { branch: optionsSnapshot.branch }),
          },
          false,
          binding,
          io,
          undefined,
          optionsSnapshot?.branch !== undefined,
        );
        const recorded = this.encryptedScopeFor(table._table, preview.id, normalized?.branch);
        if (
          rows.length > 1 ||
          (rows.length === 1 && typeof rows[0]?.[declaration.space] !== "string") ||
          (rows.length === 0 && recorded === undefined)
        )
          throw new Error("Encrypted restore cannot resolve the stored space");
        const storedScope = recorded ?? (rows[0]![declaration.space] as string);
        if (storedScope.toLowerCase() !== identifier.value.toLowerCase())
          throw new Error("The encryption space of a row is immutable");
        this.encryptedScopes.set(
          encryptedRowScopeKey(table._table, preview.id, normalized?.branch),
          storedScope,
        );
      }
      const initial = this.initialSpaceKeys.size
        ? this.initialSpaceKeys.get(`${await db.tableIdentity(scope)}:${identifier.value}`)
        : undefined;
      const stage = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
        for (const name of declaration.columns) {
          if (declaration.indexes?.[name])
            physical[equalityIndexColumn(name)] = {
              type: "Bytea",
              value: await equalityToken(db, table, name, values[name]!, secret, root),
            };
          physical[name] = {
            type: "Bytea",
            value: await encryptCell(db, table, preview.id, name, values[name]!, secret, root),
          };
        }
        if (operation === "restore") {
          io.restoreInternal(
            table._table,
            preview.id,
            physical,
            normalized,
            session,
            attribution,
            openTransactionId,
          );
        } else {
          io.insertInternal(
            table._table,
            physical,
            { ...normalized, id: preview.id },
            session,
            attribution,
          );
        }
        this.encryptedScopes.set(
          encryptedRowScopeKey(table._table, preview.id, normalized?.branch),
          identifier.value,
        );
      };
      if (initial) await stage(initial.secret, initial.root);
      else {
        await withSpaceKeys(db, scope, identifier.value, stage, false, true);
      }
    });
    const logicalRow = {
      ...preview,
      valuesByColumn: undefined,
      values: logicalTable.columns.map((column, index) =>
        declaration.columns.includes(column.name)
          ? copyMutableEncryptedValue(values[column.name]!)
          : preview.values[index]!,
      ),
    };
    return transformOutputRow(table, transformRow(logicalRow, metadata.logical, table._table));
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
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      return this.prepareEncryptedRow(table, transformedData, { ...options, id }, "restore");
    }
    const values = toWriteRecordForOperation(
      "Restore",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("restore");
    const row = client.restoreInternal(
      table._table,
      id,
      values,
      normalizeRestoreOptions(table._schema, table._table, options),
      session,
      attribution,
      openTransactionId,
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
    options?: UpsertOptions,
  ): void {
    const recipients = scopeRecipients(table, options);
    const encryption = encryptedSchemas.get(table._schema);
    if (encryption?.scopes.has(table._table)) {
      if (encryption.tables.has(table._table))
        throw new Error("Encrypted scope rows require dependent scope preparation");
      this.prepareScopeCreation();
      this.bindTable(table);
      const values = structuredClone(
        toWriteRecordForOperation(
          "Upsert",
          transformInputColumns(table, data),
          table._schema,
          table._table,
        ),
      );
      const updateOptions = structuredClone(
        normalizeUpdateOptions(table._schema, table._table, options),
      );
      const insertOptions = structuredClone(
        normalizeInsertOptions(table._schema, table._table, {
          id,
          ...(options?.branch !== undefined && { branch: options.branch }),
        }),
      );
      const binding = this.requireBinding("upsert");
      const { openTransactionId, session, attribution } = binding;
      const preparation = prepareDbTransaction(
        this as Transaction<"exclusive">,
        async (scope, io) => {
          const query = new TypedTableQueryBuilder(table._table, table._schema)
            .includeDeleted()
            .where({ id })
            .select("id");
          const rows = await readTransactionRows<Record<string, unknown>>(
            query,
            { tier: "global", ...(options?.branch !== undefined && { branch: options.branch }) },
            false,
            binding,
            io,
            undefined,
            options?.branch !== undefined,
          );
          const recorded = this.encryptedScopeFor(table._table, id, updateOptions?.branch);
          if (rows.length > 1)
            throw new Error("Encrypted mutation cannot resolve the stored space");
          if (rows.length || recorded !== undefined) {
            if (recipients)
              throw new Error("initialRecipients cannot change an existing encryption scope");
            io.upsertInternal(
              table._table,
              id,
              values,
              updateOptions,
              session,
              attribution,
              openTransactionId,
            );
            this.encryptedScopes.set(
              encryptedRowScopeKey(table._table, id, updateOptions?.branch),
              id,
            );
            return;
          }
          // Insert, rather than upsert, makes hidden/concurrently created rows
          // reject atomically instead of acquiring a new creator grant.
          io.insertInternal(
            table._table,
            values,
            insertOptions,
            session,
            attribution,
            openTransactionId,
          );
          await prepareInitialSpaceRows(
            this.e2ee!.db,
            scope,
            table,
            id,
            (secret, root) => this.retainInitialSpaceKey(secret, root),
            recipients,
          );
          this.encryptedScopes.set(
            encryptedRowScopeKey(table._table, id, updateOptions?.branch),
            id,
          );
        },
      );
      preparation.catch(() => {});
      return;
    }
    this.bindTable(table);
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      return this.updateEncrypted(table, id, transformInputColumns(table, data), options, "upsert");
    }
    // `edits` is valid ordinary JSON data. Only `update`'s `applyDiffs` option interprets the
    // descriptor-shaped DSL, so upsert must preserve that JSON shape exactly.
    const transformedData = transformInputColumns(table, data);
    const values = toWriteRecordForOperation(
      "Upsert",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("upsert");
    client.upsertInternal(
      table._table,
      id,
      values,
      normalizeUpdateOptions(table._schema, table._table, options),
      session,
      attribution,
      openTransactionId,
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
    const encrypted = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (
      encrypted &&
      (Object.hasOwn(transformedData, encrypted.space) ||
        encrypted.columns.some((name) => Object.hasOwn(transformedData, name)))
    ) {
      return this.updateEncrypted(table, id, transformedData, options);
    }
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("update");
    const normalizedOptions = normalizeUpdateOptions(table._schema, table._table, options);
    client.updateInternal(
      table._table,
      id,
      updates,
      normalizedOptions?.updatedAt,
      session,
      attribution,
      openTransactionId,
      normalizedOptions?.branch,
    );
  }

  private updateEncrypted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Record<string, unknown>,
    options?: UpdateOptions,
    operation: "update" | "upsert" = "update",
  ): void {
    const metadata = encryptedSchemas.get(table._schema)!;
    const declaration = metadata.tables.get(table._table)!;
    const changed = declaration.columns.filter((name) => Object.hasOwn(data, name));
    if (changed.length && !this.e2ee)
      throw new Error("Encrypted writes require an authenticated E2EE context");
    const updates = structuredClone(toWriteRecord(data, metadata.logical, table._table));
    const optionsSnapshot = options ? structuredClone(options) : undefined;
    const binding = this.requireBinding("update");
    const { ownerClient, openTransactionId, session, attribution } = binding;
    const normalized = normalizeUpdateOptions(table._schema, table._table, optionsSnapshot);
    const query = new TypedTableQueryBuilder<
      AnyTableMeta & { row: { id: string } & Record<string, unknown> }
    >(table._table, table._schema)
      .includeDeleted()
      .where({ id })
      .select(declaration.space);
    ownerClient.prepareTransaction(openTransactionId, async (io) => {
      const rows = await readTransactionRows<Record<string, unknown>>(
        query,
        {
          ...optionsSnapshot,
          tier: operation === "upsert" ? "global" : "local",
        },
        false,
        binding,
        io,
        undefined,
        optionsSnapshot?.branch !== undefined,
      );
      const requested = updates[declaration.space];
      const recorded = this.encryptedScopeFor(table._table, id, normalized?.branch);
      if (
        rows.length > 1 ||
        (rows.length === 1 && typeof rows[0]?.[declaration.space] !== "string")
      )
        throw new Error("Encrypted mutation cannot resolve the stored space");
      const storedScope = recorded ?? (rows.length ? rows[0]?.[declaration.space] : undefined);
      const creating = operation === "upsert" && rows.length === 0 && typeof recorded !== "string";
      const identifier = creating && requested?.type === "Uuid" ? requested.value : storedScope;
      if (typeof identifier !== "string")
        throw new Error("Encrypted mutation cannot resolve the stored space");
      if (creating) {
        for (const name of declaration.columns) {
          if (Object.hasOwn(updates, name)) continue;
          const column = metadata.logical[table._table]!.columns.find(
            (column) => column.name === name,
          )!;
          if (!column.nullable) throw new Error(`Missing encrypted column "${name}"`);
          updates[name] = { type: "Null" };
          changed.push(name);
        }
      }
      if (
        requested &&
        (requested.type !== "Uuid" || requested.value.toLowerCase() !== identifier.toLowerCase())
      )
        throw new Error("The encryption space of a row is immutable");
      if (typeof storedScope === "string") {
        this.encryptedScopes.set(
          encryptedRowScopeKey(table._table, id, normalized?.branch),
          storedScope,
        );
      }
      const write = () => {
        if (creating) {
          return io.insertInternal(
            table._table,
            updates,
            normalizeInsertOptions(table._schema, table._table, {
              id,
              ...(optionsSnapshot?.updatedAt !== undefined && {
                updatedAt: optionsSnapshot.updatedAt,
              }),
              ...(optionsSnapshot?.branch !== undefined && { branch: optionsSnapshot.branch }),
            }),
            session,
            attribution,
          );
        }
        if (operation === "upsert") {
          return io.upsertInternal(
            table._table,
            id,
            updates,
            normalized,
            session,
            attribution,
            openTransactionId,
          );
        }
        return io.updateInternal(
          table._table,
          id,
          updates,
          normalized?.updatedAt,
          session,
          attribution,
          openTransactionId,
          normalized?.branch,
        );
      };
      if (!changed.length) {
        write();
        if (creating) {
          this.encryptedScopes.set(
            encryptedRowScopeKey(table._table, id, normalized?.branch),
            identifier,
          );
        }
        return;
      }
      if (!this.e2ee) throw new Error("Encrypted writes require an authenticated E2EE context");
      const db = this.e2ee.db;
      const scope = new TypedTableQueryBuilder(declaration.scope, table._schema);
      const initial = this.initialSpaceKeys.size
        ? this.initialSpaceKeys.get(`${await db.tableIdentity(scope)}:${identifier}`)
        : undefined;
      const stage = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
        for (const name of changed) {
          if (declaration.indexes?.[name])
            updates[equalityIndexColumn(name)] = {
              type: "Bytea",
              value: await equalityToken(db, table, name, updates[name]!, secret, root),
            };
          updates[name] = {
            type: "Bytea",
            value: await encryptCell(db, table, id, name, updates[name]!, secret, root),
          };
        }
        write();
        this.encryptedScopes.set(
          encryptedRowScopeKey(table._table, id, normalized?.branch),
          identifier,
        );
      };
      if (initial) await stage(initial.secret, initial.root);
      else {
        await withSpaceKeys(db, scope, identifier, stage, false, true);
      }
    });
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
    const { openTransactionId, session, attribution } = this.requireBinding("delete");
    const normalizedOptions = normalizeUpdateOptions(table._schema, table._table, options);
    client.deleteInternal(
      table._table,
      id,
      normalizedOptions?.updatedAt,
      session,
      attribution,
      openTransactionId,
      normalizedOptions?.branch,
    );
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this transaction.
   */
  all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const reading = this.readAll(query, options);
    this.pendingReads.add(reading);
    const settled = () => this.pendingReads.delete(reading);
    reading.then(settled, settled);
    return reading;
  }

  /**
   * @internal Covered E2EE history and its authority order. Valid only after
   * this exclusive transaction's global wait succeeds. A local read plus a
   * global wait is insufficient: conflict checks do not hydrate missing history.
   */
  allSettledForE2ee<T extends { id: string }>(
    query: QueryBuilder<T>,
  ): Promise<{ rows: T[]; settlements: RowSettlement[] }> {
    if (this.kind !== "exclusive")
      throw new Error("E2EE settlement reads require an exclusive transaction");
    const reading = (async () => {
      const { rows, settlements } = await this.readAll(query, { tier: "global" }, "with-rows");
      checkTransactionSettlements(rows, settlements);
      return { rows, settlements };
    })();
    this.pendingReads.add(reading);
    const settled = () => this.pendingReads.delete(reading);
    reading.then(settled, settled);
    return reading;
  }

  private readAll<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]>;
  private readAll<T>(
    query: QueryBuilder<T>,
    options: QueryOptions | undefined,
    settlementMetadata: "with-rows",
  ): Promise<SettledRows<T>>;
  private async readAll<T>(
    query: QueryBuilder<T>,
    options: QueryOptions | undefined,
    settlementMetadata: false | "with-rows" = false,
  ): Promise<T[] | SettledRows<T>> {
    this.bindQuery(query);
    const client = this.resolveClient(query._schema);
    return readTransactionRows(
      query,
      options,
      settlementMetadata,
      this.requireBinding("query"),
      client,
      this.e2ee
        ? (table, rows) =>
            decryptCellRows(this.e2ee!.db, query._schema, table, rows, this.initialSpaceKeys)
        : undefined,
      false,
      this.e2ee
        ? (json) =>
            prepareEqualityQuery(this.e2ee!.db, query, json, {
              ready: () =>
                client.transactionPreparation(this.requireBinding("query").openTransactionId),
              initialSpaceKeys: this.initialSpaceKeys,
            })
        : undefined,
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

/** @internal Typed operations needed by E2EE metadata preparation, not a public transaction surface. */
export type E2eeTransactionScope = Pick<
  TransactionScope<"exclusive">,
  "kind" | "all" | "one" | "insert" | "upsert" | "allSettledForE2ee"
>;

type SettledRows<T> = { rows: T[]; settlements: RowSettlement[] };

function checkTransactionSettlements(rows: { id: string }[], settlements: RowSettlement[]): void {
  const ids = new Set(rows.map((row) => row.id));
  if (
    ids.size !== rows.length ||
    settlements.length !== ids.size ||
    settlements.some(
      (entry) =>
        !ids.delete(entry.rowId) ||
        typeof entry.transactionId !== "string" ||
        typeof entry.position !== "string" ||
        !/^[0-9]+$/.test(entry.position) ||
        BigInt(entry.position) > 18446744073709551615n,
    )
  )
    throw new Error("Incomplete E2EE authority settlement metadata");
}

type TransactionRowDecoder = (
  table: string,
  rows: Record<string, unknown>[],
) => Promise<Record<string, unknown>[]>;

function hasEncryptedResults(
  schema: WasmSchema,
  table: string,
  includes: NormalizedIncludeSpec,
): boolean {
  const encrypted = encryptedSchemas.get(schema);
  if (!encrypted) return false;
  if (encrypted.tables.has(table)) return true;
  const relations = analyzeRelations(schema).get(table) ?? [];
  return Object.entries(includes).some(([name, spec]) => {
    const relation = relations.find((relation) => relation.name === name);
    return relation !== undefined && hasEncryptedResults(schema, relation.toTable, spec.includes);
  });
}

async function decryptQueryRows(
  schema: WasmSchema,
  table: string,
  rows: Record<string, unknown>[],
  includes: NormalizedIncludeSpec,
  transforms: ColumnTransformRegistry | undefined,
  decrypt: TransactionRowDecoder | undefined,
  nested = false,
): Promise<Record<string, unknown>[]> {
  if (!rows.length || !hasEncryptedResults(schema, table, includes)) return rows;
  const columns = encryptedSchemas.get(schema)?.tables.get(table)?.columns ?? [];
  const needsKeys = rows.some((row) => columns.some((column) => Object.hasOwn(row, column)));
  if (needsKeys && !decrypt) throw new Error("Encrypted queries require E2EE configuration");
  const result = needsKeys ? await decrypt!(table, rows) : rows.map((row) => ({ ...row }));
  const declaration = encryptedSchemas.get(schema)?.tables.get(table);
  for (const [index, row] of result.entries()) {
    const original = rows[index]!;
    const identifier =
      declaration && (encryptedRowSpaces.get(original) ?? original[declaration.space]);
    decryptedQuerySpaces.set(
      row,
      declaration &&
        typeof identifier === "string" &&
        columns.some((column) => Object.hasOwn(original, column))
        ? [{ scope: declaration.scope, identifier }]
        : [],
    );
  }
  await decryptQueryIncludes(schema, table, result, includes, transforms, decrypt);
  if (nested) {
    for (const row of result)
      for (const column of columns) {
        const transform = transforms?.[table]?.[column];
        if (transform && Object.hasOwn(row, column)) row[column] = transform.from(row[column]);
      }
  }
  return result;
}

async function decryptQueryIncludes(
  schema: WasmSchema,
  table: string,
  result: Record<string, unknown>[],
  includes: NormalizedIncludeSpec,
  transforms: ColumnTransformRegistry | undefined,
  decrypt: TransactionRowDecoder | undefined,
) {
  const relations = analyzeRelations(schema).get(table) ?? [];
  for (const [name, spec] of Object.entries(includes)) {
    const relation = relations.find((relation) => relation.name === name);
    if (!relation) throw new Error(`Unknown relation "${name}" on table "${table}"`);
    if (!hasEncryptedResults(schema, relation.toTable, spec.includes)) continue;
    for (const row of result) {
      const value = row[name];
      const children = relation.isArray ? value : value === null ? [] : [value];
      if (!Array.isArray(children)) throw new Error("Invalid encrypted included result");
      const decoded = await decryptQueryRows(
        schema,
        relation.toTable,
        children,
        spec.includes,
        transforms,
        decrypt,
        true,
      );
      row[name] = relation.isArray ? decoded : (decoded[0] ?? null);
      for (const child of decoded)
        decryptedQuerySpaces.get(row)!.push(...(decryptedQuerySpaces.get(child) ?? []));
    }
  }
}

async function decryptEqualityMatches(
  schema: WasmSchema,
  table: string,
  candidates: Record<string, unknown>[],
  includes: NormalizedIncludeSpec,
  transforms: ColumnTransformRegistry | undefined,
  decrypt: TransactionRowDecoder | undefined,
  equality: NonNullable<Awaited<ReturnType<typeof prepareEqualityQuery>>>,
) {
  // Verify and paginate parents before acquiring keys for their children.
  // Keep subscription candidates' child ciphertext untouched for later frontiers.
  const matches = equality.verify(candidates).map((row) => ({ ...row }));
  for (const row of matches) decryptedQuerySpaces.set(row, [equality.space]);
  await decryptQueryIncludes(schema, table, matches, includes, transforms, decrypt);
  return matches;
}

function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: false,
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal">,
  decrypt?: TransactionRowDecoder,
  global?: boolean,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<T[]>;
function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: true,
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal">,
  decrypt?: TransactionRowDecoder,
  global?: boolean,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<RowSettlement[]>;
function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: "with-rows",
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal">,
  decrypt?: TransactionRowDecoder,
  global?: boolean,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<SettledRows<T>>;
function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: false | "with-rows",
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal">,
  decrypt?: TransactionRowDecoder,
  global?: boolean,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<T[] | SettledRows<T>>;
function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: boolean | "with-rows",
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal">,
  decrypt?: TransactionRowDecoder,
  global?: boolean,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<T[] | RowSettlement[] | SettledRows<T>>;
async function readTransactionRows<T>(
  query: QueryBuilder<T>,
  options: QueryOptions | undefined,
  settlementMetadata: boolean | "with-rows",
  binding: DbTransactionHandleBinding,
  client: Pick<JazzClient, "queryInternal"> &
    Partial<Pick<TransactionPreparationIO, "queryGlobal">>,
  decrypt?: TransactionRowDecoder,
  global = false,
  prepare?: (json: string) => ReturnType<typeof prepareEqualityQuery>,
): Promise<T[] | RowSettlement[] | SettledRows<T>> {
  const { openTransactionId, session } = binding;
  const builderJson = query._build();
  const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
  const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
  const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
  const outputSchema = requireSchemaWithTable(query._schema, outputTable);
  // Transactions accept the same public options surface as Db. Lower before
  // reaching native options so JavaScript callers cannot smuggle runtime
  // controls (for example `localUpdates` or `openTransactionId`) through this
  // otherwise separate execution path.
  const queryOptions = nativeDbQueryOptions(
    query._schema,
    builtQuery.table,
    lowerPublicDbQueryOptions(options),
  );
  if (settlementMetadata) queryOptions.settlementMetadata = settlementMetadata;
  const queryClient = global ? client.queryGlobal : client.queryInternal;
  if (!queryClient) throw new Error("Global transaction preflight is unavailable");
  const equality = !settlementMetadata && prepare ? await prepare(builderJson) : undefined;
  try {
    const result = await queryClient.call(
      client,
      translateQuery(equality?.json ?? builderJson, planningSchema),
      {
        ...queryOptions,
        localUpdates: "deferred",
        ...(!global && { openTransactionId }),
      },
      session,
    );
    if (settlementMetadata === true) return result as unknown as RowSettlement[];
    const settled =
      settlementMetadata === "with-rows"
        ? (result as unknown as SettledRows<(typeof result)[number]>)
        : undefined;
    const rows = settled ? settled.rows : result;
    if (equality && !(await equality.isCurrent())) throw new E2eeDataError("key-unavailable");
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const outputTransforms = resolveOutputColumnTransforms(query, builtQuery.table, outputTable);
    const outputRelationNames = Object.keys(outputIncludes);
    let transformedRows = transformRows<Record<string, unknown>>(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      equality ? [] : builtQuery.select,
      query._columnTransformsByTable,
      false,
    );
    transformedRows = await decryptQueryRows(
      outputSchema,
      outputTable,
      transformedRows,
      equality ? {} : outputIncludes,
      query._columnTransformsByTable,
      decrypt,
    );
    if (equality)
      transformedRows = await decryptEqualityMatches(
        outputSchema,
        outputTable,
        transformedRows,
        outputIncludes,
        query._columnTransformsByTable,
        decrypt,
        equality,
      );
    const decoded = transformedRows.map(
      (row) =>
        applyColumnTransforms(
          applyPartialValueSelections(row, builtQuery.partialSelect),
          outputTransforms,
          outputRelationNames,
        ) as T,
    );
    return settled ? { rows: decoded, settlements: settled.settlements } : decoded;
  } finally {
    equality?.dispose();
  }
}

const transactionAdmission = new WeakMap<Db, () => void | Promise<void>>();

/** @internal Lifecycle work must not wait for the enrolment it implements. */
export function exclusiveE2eeTransaction<TResult>(
  db: Db,
  callback: (tx: TransactionScope<"exclusive">) => TResult | Promise<TResult>,
): Promise<ExclusiveWriteResult<Awaited<TResult>>> {
  const transaction = beginDbTransactionAfter(db, async () => {});
  return runInTransaction(
    transaction,
    callback,
    () => getDbTxHandleBinding(transaction, "result").ownerClient,
  );
}
/** @internal The compiler opts in before staging any operations. */
export function beginDbTransactionAfter(
  db: Db,
  prepare: () => Promise<void>,
): Transaction<"exclusive"> {
  transactionAdmission.set(db, prepare);
  try {
    return db.beginExclusiveTransaction();
  } finally {
    transactionAdmission.delete(db);
  }
}

/** @internal Register before returning; the outer commit drains this work. */
export function prepareDbTransaction(
  transaction: Transaction<"exclusive">,
  prepare: (scope: E2eeTransactionScope, io: TransactionPreparationIO) => Promise<void>,
): Promise<void> {
  if (transaction.kind !== "exclusive")
    throw new Error("E2EE initialisation requires an exclusive transaction");
  transaction.openTransactionId(); // Check admission before registering asynchronous work.
  const binding = getDbTxHandleBinding(transaction, "prepare");
  initialisingTransactions.add(transaction);
  const { ownerClient, openTransactionId } = binding;
  return ownerClient.prepareTransaction(openTransactionId, async (io) => {
    await prepare(preparedTransactionScope(binding, io), io);
  });
}

function preparedTransactionScope(
  binding: DbTransactionHandleBinding,
  io: TransactionPreparationIO,
): E2eeTransactionScope {
  const { openTransactionId, session, attribution } = binding;
  const scope: E2eeTransactionScope = {
    kind: "exclusive",
    upsert(table, id, data, options) {
      binding.resolveClient(table._schema);
      const transformed = transformInputColumns(table, data);
      const values = toWriteRecordForOperation("Upsert", transformed, table._schema, table._table);
      io.upsertInternal(
        table._table,
        id,
        values,
        normalizeUpdateOptions(table._schema, table._table, options),
        session,
        attribution,
        openTransactionId,
      );
    },
    all(query, options) {
      binding.resolveClient(query._schema);
      return readTransactionRows(query, options, false, binding, io);
    },
    async one(query, options) {
      const rows = await scope.all(limitQueryToOne(query), options);
      return rows[0] ?? null;
    },
    insert(table, data, options) {
      binding.resolveClient(table._schema);
      const transformed = transformInputColumns(table, data);
      const values = toWriteRecordForOperation("Insert", transformed, table._schema, table._table);
      const row = io.insertInternal(
        table._table,
        values,
        normalizeInsertOptions(table._schema, table._table, options),
        session,
        attribution,
        openTransactionId,
      );
      return transformOutputRow(table, transformRow(row, table._schema, table._table));
    },
    async allSettledForE2ee(query) {
      binding.resolveClient(query._schema);
      const { rows, settlements } = await readTransactionRows(
        query,
        { tier: "global" },
        "with-rows",
        binding,
        io,
      );
      checkTransactionSettlements(rows, settlements);
      return { rows, settlements };
    },
  };
  return scope;
}
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
 * const unsubscribe = db.subscribe(app.todos, (todos) => {
 *   console.log("All todos:", todos);
 * });
 * ```
 */
export interface ShutdownOptions {
  /** Wait for pending writes to reach the core before releasing resources. */
  waitForSync?: boolean;
}

export class Db {
  get e2ee(): E2ee {
    return e2eeForDb(this);
  }
  private config: DbConfig;
  private readonly runtimeSource: AnyRuntimeSource;
  private readonly authStateStore;
  private connection: ConnectionManager;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;
  private readonly shutdownAbort = new AbortController();
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
  #authenticatedInspectorPhysicalDbName: string | null = null;

  /**
   * Protected constructor - use {@link createDb} in regular app code.
   */
  protected constructor(
    config: DbConfig,
    runtimeSource: AnyRuntimeSource,
    authStateOptions?: AuthStateStoreOptions,
  ) {
    assertAccountConfig(config);
    this.config = config;
    this.runtimeSource = runtimeSource;
    const sessionInput = {
      ...config,
      trustedReservedSession: getTrustedReservedSession(config),
    };
    setDbInternalSession(this, resolveClientInternalSessionSync(sessionInput));
    this.authStateStore = createAuthStateStore(sessionInput, authStateOptions);
    this.connection = new DirectConnectionManager(this.dbForConnection());
    // An overlay peer gets its port only through the authenticated Inspector
    // control handoff. Keep the resulting read policy inside this source: the
    // Inspector UI uses ordinary `useAll`, while no application-facing option
    // or package export can manufacture local-only reads.
    const inspectorAttachmentRequested =
      config.runtimeSources?.browserWorkerPort !== undefined &&
      config.runtimeSources.inspectorHostPhysicalDbName !== undefined;
    dbSubscriptionSources.set(this, {
      // Cache identity is reserved before the async worker receipt arrives so
      // an Inspector entry can never share a host application's query cache.
      // This marker is not authority: execution below strips it unless the
      // worker subsequently authenticates this attachment.
      ...(inspectorAttachmentRequested
        ? {
            prepareQueryOptions: (options?: QueryOptions) =>
              createInspectorLocalQueryOptions(options),
          }
        : {}),
      all: (query, options) =>
        inspectorAttachmentRequested
          ? this.allFromInspectorAttachment(query, options)
          : this.allInternal(query, lowerPublicDbQueryOptions(options)),
      subscribeDelta: (query, callback, options, session) =>
        inspectorAttachmentRequested
          ? this.subscribeFromInspectorAttachment(query, callback, options, session)
          : this.subscribeDelta(query, callback, lowerPublicDbQueryOptions(options), session),
    });
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
      enableAuthenticatedInspectorLocalReads: (physicalDbName) =>
        this.#enableAuthenticatedInspectorLocalReads(physicalDbName),
      clearAuthenticatedInspectorLocalReads: () => this.#clearAuthenticatedInspectorLocalReads(),
    };
  }

  #enableAuthenticatedInspectorLocalReads(physicalDbName: string): void {
    // The configured coordinate is only selection metadata. It becomes
    // authority only when the worker returns this exact root in the init
    // receipt for a peer it created through Inspector control.
    if (this.config.runtimeSources?.inspectorHostPhysicalDbName !== physicalDbName) return;
    this.#authenticatedInspectorPhysicalDbName = physicalDbName;
  }

  #clearAuthenticatedInspectorLocalReads(): void {
    this.#authenticatedInspectorPhysicalDbName = null;
  }

  private async inspectorAttachmentOptions<T>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
  ): Promise<InternalDbQueryOptions> {
    // Client construction starts the follower's init handshake. Do not decide
    // the read tier from config/MessagePort shape: wait for the worker receipt.
    this.getClient(query._schema);
    await this.connection.ensureReady("local");
    const selectedOptions = this.#authenticatedInspectorPhysicalDbName
      ? createInspectorLocalQueryOptions(options)
      : // Drop a source's cache-only marker when the worker did not issue an
        // Inspector receipt. Symbols are deliberately non-enumerable.
        options && { ...options };
    return lowerPublicDbQueryOptions(selectedOptions) ?? {};
  }

  private async allFromInspectorAttachment<T>(
    query: QueryBuilder<T>,
    options?: QueryOptions,
  ): Promise<T[]> {
    return this.allInternal(query, await this.inspectorAttachmentOptions(query, options));
  }

  private subscribeFromInspectorAttachment<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: ((delta: SubscriptionDelta<T>) => void) | DbDeltaSubscriptionCallbacks<T>,
    options?: QueryOptions,
    session?: Session,
  ): SubscriptionHandle {
    let inner: SubscriptionHandle | null = null;
    let cancelled = false;
    const ready = this.inspectorAttachmentOptions(query, options).then((prepared) => {
      if (cancelled) return;
      inner = this.subscribeDelta(query, callback, prepared, session);
      return inner.ready;
    });
    const handle = (() => {
      cancelled = true;
      inner?.();
    }) as SubscriptionHandle;
    Object.defineProperty(handle, "ready", { value: ready });
    return handle;
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

  private publishAuthStateWithInternalSession(
    nextSession: Session | null,
    publish: () => void,
  ): void {
    // Commit every snapshot before notifying observers. Observer failures are
    // reported to the caller without rolling back into a split snapshot.
    setDbInternalSession(this, nextSession);
    publish();
  }

  protected applyAuthUpdate(
    token: string | null,
    trustedReservedSession?: Session,
    nativeAccountRefresh = false,
  ): boolean {
    if (!nativeAccountRefresh) this.runtimeSource.assertAuthUpdateAllowed();
    const jwtToken = token ?? undefined;
    const previousToken = this.config.jwtToken;
    const previousCookieSession = this.config.cookieSession;
    const previousTrustedReservedSession = getTrustedReservedSession(this.config);
    const nextConfig = {
      ...this.config,
      jwtToken,
      cookieSession: undefined,
    } as DbConfig;
    setTrustedReservedSession(nextConfig, trustedReservedSession);
    const credentialsChanged =
      previousToken !== nextConfig.jwtToken ||
      previousCookieSession !== undefined ||
      JSON.stringify(previousTrustedReservedSession) !== JSON.stringify(trustedReservedSession);
    if (!credentialsChanged && this.authStateStore.getState().error === undefined) return false;

    if (!nativeAccountRefresh) {
      if (!this.authStateStore.validateJwtToken(jwtToken, trustedReservedSession)) return false;
      this.connection.updateAuth({
        mode: "bearer",
        jwtToken,
        trustedReservedSession,
      });
    }

    const nextInternalSession = resolveClientInternalSessionSync(nextConfig);
    this.config.jwtToken = jwtToken;
    this.config.cookieSession = undefined;
    setTrustedReservedSession(this.config, trustedReservedSession);
    this.publishAuthStateWithInternalSession(nextInternalSession, () => {
      this.authStateStore.applyJwtToken(jwtToken, trustedReservedSession);
    });
    return true;
  }

  protected applyCookieSessionUpdate(session: Session | null): boolean {
    this.runtimeSource.assertAuthUpdateAllowed();
    const cookieSession = session ?? undefined;
    const previousCookieSession = this.config.cookieSession;
    const previousToken = this.config.jwtToken;
    const previousTrustedReservedSession = getTrustedReservedSession(this.config);
    const sessionChanged = JSON.stringify(previousCookieSession) !== JSON.stringify(cookieSession);
    const credentialsChanged =
      previousToken !== undefined || sessionChanged || previousTrustedReservedSession !== undefined;
    if (!credentialsChanged && this.authStateStore.getState().error === undefined) return false;

    if (!this.authStateStore.validateCookieSession(cookieSession)) return false;
    this.connection.updateAuth({ mode: "cookie", cookieSession });

    const nextConfig = {
      ...this.config,
      jwtToken: undefined,
      cookieSession,
    } as DbConfig;
    const nextInternalSession = resolveClientInternalSessionSync(nextConfig);
    this.config.jwtToken = undefined;
    this.config.cookieSession = cookieSession;
    setTrustedReservedSession(this.config, undefined);
    this.publishAuthStateWithInternalSession(nextInternalSession, () => {
      this.authStateStore.applyCookieSession(cookieSession);
    });
    return true;
  }

  /**
   * Create a Db instance with a loaded runtime source.
   * @internal Use {@link createDb()} instead.
   */
  static create(config: DbConfig, runtimeSource: AnyRuntimeSource): Db {
    return new Db(config, runtimeSource);
  }

  /** @internal Create a direct Db after its pre-runtime identity bootstrap. */
  static async createWithDirectConnection(
    config: DbConfig,
    runtimeSource: AnyRuntimeSource,
  ): Promise<Db> {
    const db = new Db(config, runtimeSource);
    await db.connection.start();
    return db;
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
    this.assertOpen();
    return this.connection.getClient(schema);
  }

  protected getCurrentClient(): JazzClient | null {
    return this.connection.getCurrentClient();
  }

  protected async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    await this.connection.ensureReady(tier, signal ?? this.shutdownAbort.signal);
    this.assertOpen();
  }

  private wrapWriteWait<THandle extends WriteHandle<unknown, unknown>>(handle: THandle): THandle {
    return setWriteWaitReadiness(handle, (tier) => this.ensureReady(tier));
  }

  private preparedWrite<T>(
    kind: TransactionKind,
    client: JazzClient,
    write: (tx: Transaction) => T,
  ): WriteResult<T> {
    const tx = this.createTransaction(kind);
    let value: T;
    try {
      value = write(tx);
    } catch (error) {
      try {
        tx.rollback().catch(() => {});
      } catch {
        /* Preserve validation failure. */
      }
      throw error;
    }
    const committed = runInTransaction(tx, () => value, client);
    return this.wrapWriteWait(
      new WriteResult(
        value,
        committed.then((result) => result.txId),
        client,
      ),
    );
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

  /** @internal Refresh only through the immutable handle that owns this context. */
  async refreshAccountAuth(account: AccountHandle): Promise<string> {
    assertAccountConfig(this.config);
    const current = getDbInternalSession(this);
    if (
      account.id !== this.config.accountId ||
      accountRegistry(account) !== this.config.accountRegistryAuthority ||
      account.identity.issuer !== current?.issuer ||
      account.identity.subject !== current?.user_id
    )
      throw new Error("Account context mismatch");
    try {
      const token = await accountToken(account, accountRegistry(account));
      if (this.shutdownAbort.signal.aborted) throw new Error("Account context closed");
      const reserved =
        account.identity.issuer === "urn:jazz:local-first"
          ? internalSessionFromVerifiedReservedJwtPayload(
              parseJwtPayload(token) ?? {},
              "local-first",
            )
          : undefined;
      const nativeRefresh = this.runtimeSource.refreshAccountToken(token);
      this.applyAuthUpdate(token, reserved ?? undefined, nativeRefresh);
      return token;
    } catch (error) {
      this.markUnauthenticated("invalid");
      throw error;
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
    const {
      browserWorkerPort: _browserWorkerPort,
      browserWorkerSession: _browserWorkerSession,
      ...runtimeSources
    } = this.config.runtimeSources ?? {};
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
    // Sync recovery is safe before teardown starts; it must remain available
    // while a graceful transition waits for previously committed writes.
    if ((this.isShuttingDown || this.shutdownPromise) && !this.cancelSyncShutdown) {
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

  /** @internal Observe explicit reconnection until this Db shuts down. */
  onE2eeReconnect(listener: () => void): void {
    let wasOffline = this.connection.isExplicitlyOffline();
    this.connection.onExplicitOfflineChange((offline) => {
      const reconnected = wasOffline && !offline;
      wasOffline = offline;
      if (reconnected) listener();
    }, this.shutdownAbort.signal);
  }

  /** @internal Honour the initial worker connection state before an E2EE offline read. */
  async e2eeIsExplicitlyOffline(): Promise<boolean> {
    const initial = this.connection.initialExplicitOfflineState();
    if (initial) await initial;
    this.assertOpen();
    return this.connection.isExplicitlyOffline();
  }

  /** @internal Observations only: these reads do not prove complete history. */
  async observeE2eeHistory(queries: readonly QueryBuilder<{ id: string }>[]) {
    this.assertOpen();
    if (!queries.length || queries.some((query) => !query._table.startsWith("__e2ee_")))
      throw new Error("E2EE observations require metadata queries");
    this.getClient(queries[0]!._schema);
    // A browser foreground must load the durable owner's local query inputs
    // before freezing its snapshot. These results may include pending writes:
    // discard them; only the accepted-only observation below validates history.
    // Keep ordinary propagation so the foreground can reach its durable worker.
    await Promise.all(queries.map((query) => this.all(query, { tier: "local" })));
    const tx = beginDbTransactionAfter(this, async () => {});
    const binding = getDbTxHandleBinding(tx, "query");
    const local: Pick<JazzClient, "queryInternal"> = {
      queryInternal: (query, options, session) =>
        binding.ownerClient.queryInternal(
          query,
          { ...options, propagation: "local-only" },
          session,
        ),
    };
    try {
      // The E2EE sidecar restricts this fresh snapshot before any row-body read.
      // Ordinary transactions continue to include their local pending prefix.
      const settlements = await Promise.all(
        queries.map((query) =>
          readTransactionRows(query, { tier: "global" }, true, binding, local),
        ),
      );
      return await Promise.all(
        queries.map(async (query, index) => {
          const rows = await readTransactionRows(query, { tier: "global" }, false, binding, local);
          checkTransactionSettlements(rows, settlements[index]!);
          return { rows, settlements: settlements[index]! };
        }),
      );
    } finally {
      await tx.rollback();
    }
  }

  private readonly coveredE2eeTableIdentities = new WeakMap<JazzClient, Map<string, string>>();

  /** @internal Resolve the accepted table lineage for E2EE scope selection. */
  async tableIdentity<T, Init>(
    table: TableProxy<T, Init>,
    localOnly = false,
  ): Promise<string | null> {
    const client = this.getClient(table._schema);
    const runtime = client.getRuntime();
    if (!runtime.tableIdentity)
      throw new Error("Runtime does not expose catalogue table identities");
    const localIdentity = await runtime.tableIdentity(table._table);
    this.assertOpen();
    // A local candidate is not catalogue coverage or proof of key membership.
    if (localOnly) return localIdentity;
    // Fresh runtimes can have local bootstrap identities before receiving the
    // server catalogue. Only reuse an identity already covered for this client.
    if (
      localIdentity &&
      this.coveredE2eeTableIdentities.get(client)?.get(table._table) === localIdentity
    )
      return localIdentity;
    const initialOfflineState = this.connection.initialExplicitOfflineState();
    if (initialOfflineState) await initialOfflineState;
    const offline = this.connection.isExplicitlyOffline();
    await this.ensureReady(offline ? "local" : "edge");
    // Application clients receive the authoritative catalogue with a covered
    // view. Request no rows, and stay outside the transaction preparation queue
    // so encrypted-write preparation cannot wait on itself.
    // Offline preparation uses the engine's existing catalogue; this does not
    // initialise a missing identity or establish accepted encryption membership.
    if (!offline)
      await client.query(JSON.stringify({ table: table._table, limit: 0 }), { tier: "edge" });
    const identity = await runtime.tableIdentity(table._table);
    this.assertOpen();
    if (!offline && identity) {
      let covered = this.coveredE2eeTableIdentities.get(client);
      if (!covered) {
        covered = new Map();
        this.coveredE2eeTableIdentities.set(client, covered);
      }
      covered.set(table._table, identity);
    }
    return identity;
  }

  /** @internal Resolve the accepted column epoch for encrypted-cell context binding. */
  async columnIdentity<T, Init>(
    table: TableProxy<T, Init>,
    column: string,
    localOnly = false,
  ): Promise<string | null> {
    await this.tableIdentity(table, localOnly);
    const runtime = this.getClient(table._schema).getRuntime();
    if (!runtime.columnIdentity)
      throw new Error("Runtime does not expose catalogue column identities");
    const identity = await runtime.columnIdentity(table._table, column);
    this.assertOpen();
    return identity;
  }

  /** @internal Open a control channel for the same-origin embedded inspector. */
  openInspectorControlPort(signal?: AbortSignal): Promise<MessagePort> {
    return this.connection.openInspectorControlPort(signal);
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
    scopeRecipients(table, options);
    const client = this.getClient(table._schema);
    const encryption = encryptedSchemas.get(table._schema);
    if (encryption?.scopes.has(table._table) || encryption?.tables.has(table._table)) {
      return this.preparedWrite(
        encryption.scopes.has(table._table) ? "exclusive" : "mergeable",
        client,
        (tx) => tx.insert(table, data, options),
      );
    }
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
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "insert");
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
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "update");
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
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "upsert");
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
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      return this.preparedWrite("mergeable", client, (tx) => tx.restore(table, id, data, options));
    }
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
    options?: UpsertOptions,
  ): WriteHandle {
    scopeRecipients(table, options);
    const client = this.getClient(table._schema);
    const encryption = encryptedSchemas.get(table._schema);
    if (encryption?.scopes.has(table._table) || encryption?.tables.has(table._table)) {
      return this.preparedWrite(
        encryption.scopes.has(table._table) ? "exclusive" : "mergeable",
        client,
        (tx) => tx.upsert(table, id, data, options),
      );
    }
    // `edits` is valid ordinary JSON data. Only `update`'s `applyDiffs` option interprets the
    // descriptor-shaped DSL, so upsert must preserve that JSON shape exactly.
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
  update<
    T,
    Init,
    StreamingInit,
    StreamingUpdate,
    LargeValueUpdate,
    TReplacements extends Partial<Init>,
  >(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate, LargeValueUpdate> & {
      readonly _largeValueUpdateType: LargeValueUpdate;
    },
    id: string,
    data: TReplacements,
    options?: TypedUpdateOptionsWithDiffs<TReplacements, LargeValueUpdate>,
  ): WriteHandle;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): WriteHandle;
  update(
    table: TableProxy<any, any, any, any, any>,
    id: string,
    data: Record<string, unknown>,
    options?: UpdateOptions & { applyDiffs?: object },
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const diffs = options?.applyDiffs;
    if (
      diffs !== undefined &&
      (typeof diffs !== "object" || diffs === null || Array.isArray(diffs))
    ) {
      throw new Error("update option applyDiffs must be an object keyed by column name.");
    }
    if (diffs && Object.keys(data).some((column) => Object.hasOwn(diffs, column))) {
      throw new Error("update replacements and applyDiffs must not both specify the same column.");
    }
    const encrypted = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (
      encrypted &&
      (Object.hasOwn(data, encrypted.space) ||
        encrypted.columns.some((name) => Object.hasOwn(data, name)))
    ) {
      if (diffs !== undefined) throw new Error("Encrypted updates do not support applyDiffs");
      return this.preparedWrite("mergeable", client, (tx) => tx.update(table, id, data, options));
    }
    const transformedData = transformInputColumns(table, data);
    const updates = toWriteRecordForOperation(
      "Update",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getRuntimeOperationContext();
    if (diffs !== undefined) {
      const { ordinary, descriptors } = splitLargeValueUpdate(
        diffs as Record<string, unknown>,
        table._schema,
        table._table,
      );
      if (Object.keys(ordinary).length > 0) {
        throw new Error(
          "update option applyDiffs accepts only field diff descriptors, not whole-column values.",
        );
      }
      if (descriptors.length > 0) {
        return this.wrapWriteWait(
          client.updateLargeValues(
            table._table,
            id,
            updates,
            descriptors,
            normalizeUpdateOptions(table._schema, table._table, options),
            context?.session,
            context?.attribution,
          ),
        );
      }
    }
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
    this.assertOpen();
    const context = this.getRuntimeOperationContext();
    const configuredSchema = e2eeSchemaForDb(this);
    const ownerClient =
      this.getCurrentClient() ?? (configuredSchema ? this.getClient(configuredSchema) : null);
    if (kind === "exclusive" && !ownerClient) {
      throw new Error(
        "Cannot begin an exclusive transaction before the JazzClient has been created. Run a query or mutation first.",
      );
    }
    const e2ee =
      configuredSchema && encryptedSchemas.has(configuredSchema) ? { db: this } : undefined;
    const prerequisite = transactionAdmission.get(this);
    if (prerequisite && ownerClient)
      return withTransactionAdmission(
        ownerClient,
        prerequisite,
        () =>
          new Transaction(
            kind,
            (schema) => this.getClient(schema),
            context?.session,
            context?.attribution,
            ownerClient,
            e2ee,
          ),
      );
    return new Transaction(
      kind,
      (schema) => this.getClient(schema),
      context?.session,
      context?.attribution,
      ownerClient ?? undefined,
      e2ee,
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
    return this.allInternal(query, lowerPublicDbQueryOptions(options));
  }

  private async allInternal<T>(
    query: QueryBuilder<T>,
    options?: InternalDbQueryOptions,
    equalityRetries = 0,
  ): Promise<T[]> {
    const client = this.getClient(query._schema);
    // A newly attached browser-worker follower has no authoritative
    // namespace-wide explicit-offline state until its init handshake resolves.
    // Established runtimes return null here, preserving their synchronous
    // operation-start tier snapshot even if disconnect happens later.
    const initialOfflineState =
      options?.tier === ReadTier.RemoteIfPossible
        ? this.connection.initialExplicitOfflineState()
        : null;
    if (initialOfflineState) await initialOfflineState;
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const queryOptions = nativeDbQueryOptions(query._schema, builtQuery.table, options);
    const remoteIfPossibleOffline =
      options?.tier === ReadTier.RemoteIfPossible && this.connection.isExplicitlyOffline();
    if (remoteIfPossibleOffline) queryOptions.tier = "local";
    const equality = encryptedSchemas.has(query._schema)
      ? await prepareEqualityQuery(this, query, builderJson)
      : undefined;
    try {
      const wasmQuery = translateQuery(equality?.json ?? builderJson, planningSchema);
      const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
      const context = this.getRuntimeOperationContext();
      const effectiveTier = resolveEffectiveQueryExecutionOptions(
        { ...this.config, defaultDurabilityTier: this.runtimeSource.defaultDurabilityTier },
        queryOptions,
      ).tier;
      await this.ensureReady(effectiveTier);
      const rows =
        context || usesRelationTraversal
          ? await client.queryInternal(
              wasmQuery,
              queryOptions,
              context?.readSession ?? context?.session,
            )
          : await client.queryInternal(wasmQuery, queryOptions);
      if (equality && !(await equality.isCurrent())) {
        // Bound work under continuous rotation; never present incomplete history as exhaustion.
        if (equalityRetries >= 2) throw new E2eeDataError("key-unavailable");
        return this.allInternal(query, options, equalityRetries + 1);
      }
      const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
      const outputTransforms = resolveOutputColumnTransforms(query, builtQuery.table, outputTable);
      const outputRelationNames = Object.keys(outputIncludes);
      let transformedRows = await decryptQueryRows(
        query._schema,
        outputTable,
        transformRows<Record<string, unknown>>(
          rows,
          outputSchema,
          outputTable,
          outputIncludes,
          equality ? [] : builtQuery.select,
          query._columnTransformsByTable,
          false,
        ),
        equality ? {} : outputIncludes,
        query._columnTransformsByTable,
        (table, rows) => decryptCellRows(this, query._schema, table, rows),
      );
      if (equality)
        transformedRows = await decryptEqualityMatches(
          query._schema,
          outputTable,
          transformedRows,
          outputIncludes,
          query._columnTransformsByTable,
          (table, rows) => decryptCellRows(this, query._schema, table, rows),
          equality,
        );
      return transformedRows.map(
        (row) =>
          applyColumnTransforms(
            applyPartialValueSelections(row, builtQuery.partialSelect),
            outputTransforms,
            outputRelationNames,
          ) as T,
      );
    } finally {
      equality?.dispose();
    }
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
   * Subscribe to a query and receive its complete current result whenever it changes.
   * Each update receives a fresh result array. Prefer the object form when the
   * subscription's terminal errors must be handled explicitly. The legacy
   * function form reports unhandled terminal errors to `console.error`.
   */
  subscribe<T extends { id: string }>(
    query: QueryBuilder<T>,
    callbacks: ((rows: T[]) => void) | DbSubscriptionCallbacks<T>,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    const { onUpdate, onError } =
      typeof callbacks === "function" ? { onUpdate: callbacks, onError: undefined } : callbacks;
    const deltaCallbacks: DbDeltaSubscriptionCallbacks<T> = {
      onDelta: (update) => {
        if (update.all === undefined) {
          throw new Error("Jazz subscription update is missing its materialized result.");
        }
        onUpdate(update.all);
      },
      ...(onError === undefined ? {} : { onError }),
    };
    return this.subscribeDelta(query, deltaCallbacks, lowerPublicDbQueryOptions(options), session);
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
   * @param callbacks Called with deltas and, in object form, terminal subscription errors
   * @param options Optional read durability options
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * import { RowChangeKind } from "jazz-tools";
   *
   * const unsubscribe = db.subscribeDelta(app.todos, (delta) => {
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
  private subscribeDelta<T extends { id: string }>(
    query: QueryBuilder<T>,
    callbacks: ((delta: SubscriptionDelta<T>) => void) | DbDeltaSubscriptionCallbacks<T>,
    options?: InternalDbQueryOptions,
    session?: Session,
  ): SubscriptionHandle {
    const { onDelta, onError, onPending } =
      typeof callbacks === "function" ? { onDelta: callbacks, onError: undefined } : callbacks;
    // Constructing a browser follower starts its init handshake. Do that before
    // asking whether this is a newly attaching peer.
    const client = this.getClient(query._schema);
    // A newly attached browser peer does not yet know whether its worker can
    // open durable storage.  Keep the native subscription installation in the
    // old immediate order (several hooks may register together), but hold its
    // public deltas until that one admission succeeds.  This makes a corrupt
    // store fail the subscription that triggered it without publishing a
    // misleading empty opening or perturbing maintained-view registration.
    const initialReadiness = this.connection.initialExplicitOfflineState();
    const manager = new SubscriptionManager<T>();
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const outputTransforms = resolveOutputColumnTransforms(query, builtQuery.table, outputTable);
    const outputRelationNames = Object.keys(outputIncludes);
    const encryptedDeclaration = encryptedSchemas.get(query._schema)?.tables.get(builtQuery.table);
    const encryptedPredicates = builtQuery.conditions.filter((condition) =>
      encryptedDeclaration?.columns.includes(condition.column),
    );
    const encryptedEquality =
      encryptedPredicates.length > 0 &&
      encryptedPredicates.every(
        (condition) =>
          encryptedDeclaration?.indexes?.[condition.column] &&
          (condition.op === "eq" || condition.op === "match"),
      );
    const wasmQuery = encryptedEquality ? builderJson : translateQuery(builderJson, planningSchema);

    const decodeRow = (row: WasmRow) =>
      transformRow<Record<string, unknown>>(
        row,
        outputSchema,
        outputTable,
        outputIncludes,
        encryptedEquality ? [] : builtQuery.select,
        query._columnTransformsByTable,
        false,
      );
    const finishRow = (row: Record<string, unknown>): T =>
      applyColumnTransforms(
        applyPartialValueSelections(row, builtQuery.partialSelect),
        outputTransforms,
        outputRelationNames,
      ) as T;
    const transform = (row: WasmRow): T => finishRow(decodeRow(row));
    const encrypted =
      encryptedEquality || hasEncryptedResults(query._schema, outputTable, outputIncludes);
    let pendingDecryption = Promise.resolve();
    let encryptedFrontier = 0;
    let deliveryReady = initialReadiness === null;
    const bufferedDeltas: SubscriptionDelta<T>[] = [];

    const queryOptions = nativeDbQueryOptions(query._schema, builtQuery.table, options);
    const remoteIfPossibleOffline =
      options?.tier === ReadTier.RemoteIfPossible && this.connection.isExplicitlyOffline();
    if (remoteIfPossibleOffline) queryOptions.tier = "local";
    const context = this.getRuntimeOperationContext();
    type NativeSubscription = {
      id: number | null;
      installing: boolean;
      terminalError: Error | null;
      terminalDelivered: boolean;
      retired: boolean;
      nativeUnsubscribed: boolean;
      predecessor: NativeSubscription | null;
      equality?: Awaited<ReturnType<typeof prepareEqualityQuery>>;
      dependencies?: ReturnType<typeof queryKeyDependencies>;
      hasSnapshot?: boolean;
    };
    let activeSubscription: NativeSubscription | null = null;
    let unsubscribed = false;
    let terminalized = false;
    const readyAbort = new AbortController();
    const retireNativeSubscription = (subscription: NativeSubscription) => {
      subscription.retired = true;
      subscription.dependencies?.stop();
      if (subscription.equality) pendingDecryption.then(() => subscription.equality?.dispose());
      const id = subscription.id;
      if (id === null || subscription.nativeUnsubscribed) return;
      subscription.nativeUnsubscribed = true;
      client.unsubscribe(id);
    };
    const notifyTerminalError = (error: Error) => {
      if (!onError) {
        console.error("Unhandled Jazz subscription error", error);
        return;
      }
      try {
        onError(error);
      } catch (callbackError) {
        console.error("Jazz subscription error callback failed", callbackError);
      }
    };
    const completeTerminalization = (subscription: NativeSubscription) => {
      // A native runtime may fail from inside subscribe(). Record the terminal
      // outcome immediately, but wait for subscribe() to return its handle
      // before detaching and notifying the owner.
      if (subscription.installing) return;
      const terminalError = subscription.terminalError;
      if (terminalError === null || subscription.terminalDelivered || terminalized) return;
      subscription.terminalDelivered = true;
      terminalized = true;
      deliveryReady = false;
      bufferedDeltas.length = 0;
      if (activeSubscription === subscription) {
        activeSubscription = null;
      }
      retireNativeSubscription(subscription);
      if (subscription.predecessor !== null) {
        retireNativeSubscription(subscription.predecessor);
        subscription.predecessor = null;
      }
      readyAbort.abort();
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      manager.clear();
      notifyTerminalError(terminalError);
    };
    const terminalizeSubscription = (subscription: NativeSubscription, error: unknown) => {
      if (
        unsubscribed ||
        terminalized ||
        activeSubscription !== subscription ||
        subscription.terminalDelivered
      ) {
        return;
      }
      subscription.terminalError ??= error instanceof Error ? error : new Error(String(error));
      completeTerminalization(subscription);
    };
    const createSubscriptionGeneration = (
      predecessor: NativeSubscription | null = null,
    ): NativeSubscription => {
      const subscription: NativeSubscription = {
        id: null,
        installing: false,
        terminalError: null,
        terminalDelivered: false,
        retired: false,
        nativeUnsubscribed: false,
        predecessor,
      };
      activeSubscription = subscription;
      return subscription;
    };
    const deliver = (delta: SubscriptionDelta<T>) => {
      if (unsubscribed || terminalized || activeSubscription === null) return;
      if (!deliveryReady) {
        bufferedDeltas.push(delta);
        return;
      }
      try {
        onDelta(delta);
      } catch (error) {
        const subscription = activeSubscription;
        if (subscription !== null) terminalizeSubscription(subscription, error);
      }
    };
    const materializeEncryptedResult = async (subscription: NativeSubscription) => {
      const rows = subscription.equality
        ? await decryptEqualityMatches(
            query._schema,
            outputTable,
            manager.all(),
            outputIncludes,
            query._columnTransformsByTable,
            (table, rows) => decryptCellRows(this, query._schema, table, rows),
            subscription.equality,
          )
        : manager.all();
      await subscription.dependencies?.update([
        ...(subscription.equality ? [subscription.equality.space] : []),
        ...rows.flatMap((row) => decryptedQuerySpaces.get(row as object) ?? []),
      ]);
      await subscription.dependencies?.validate(subscription.equality?.space);
      return subscription.equality ? rows.map(finishRow) : (rows as T[]);
    };
    const handleDelta = (delta: Parameters<SubscriptionManager<T>["handleDelta"]>[0]) => {
      if (unsubscribed || terminalized || activeSubscription === null) return;
      if (!encrypted) {
        deliver(manager.handleDelta(delta, transform));
        return;
      }
      const subscription = activeSubscription;
      const frontier = ++encryptedFrontier;
      try {
        onPending?.();
      } catch (error) {
        terminalizeSubscription(subscription, error);
        return;
      }
      pendingDecryption = pendingDecryption
        .then(async () => {
          if (unsubscribed || terminalized || activeSubscription !== subscription) return;
          const decodedRows: Record<string, unknown>[] = [];
          const typedDelta = manager.handleDelta(delta, (row) => {
            const decoded = decodeRow(row);
            // Keep the reducer's occurrence identity and only decrypt changed rows.
            // No result is delivered until every changed row has been decrypted.
            decodedRows.push(decoded);
            return decoded as T;
          });
          const plaintextRows = await decryptQueryRows(
            query._schema,
            outputTable,
            decodedRows,
            subscription.equality ? {} : outputIncludes,
            query._columnTransformsByTable,
            (table, rows) => decryptCellRows(this, query._schema, table, rows),
          );
          for (const [index, row] of plaintextRows.entries()) {
            Object.assign(decodedRows[index]!, subscription.equality ? row : finishRow(row));
            const spaces = decryptedQuerySpaces.get(row);
            if (spaces) decryptedQuerySpaces.set(decodedRows[index]!, spaces);
            if (subscription.equality) {
              const indexed = decryptedIndexBytes.get(row);
              if (indexed) decryptedIndexBytes.set(decodedRows[index]!, indexed);
            }
          }
          subscription.hasSnapshot = true;
          const materialized = await materializeEncryptedResult(subscription);
          if (subscription.equality && !(await subscription.equality.isCurrent())) {
            restartEncryptedSubscription(subscription);
            return;
          }
          if (
            !unsubscribed &&
            !terminalized &&
            activeSubscription === subscription &&
            frontier === encryptedFrontier
          ) {
            if (subscription.equality) {
              deliver({
                reset: true,
                delta: [],
                all: materialized,
              });
            } else deliver(typedDelta);
          }
        })
        .catch((error) => {
          if (frontier === encryptedFrontier) {
            terminalizeSubscription(subscription, error);
          } else {
            // The reducer may contain an undeciphered occurrence from this
            // failed delta. Reopen from a complete snapshot instead of letting
            // a newer delta reuse it or rejecting a now child-free frontier.
            restartEncryptedSubscription(subscription);
          }
        });
    };
    const installNativeSubscription = (
      subscription: NativeSubscription,
      subscriptionOptions = queryOptions,
    ) => {
      if (
        unsubscribed ||
        activeSubscription !== subscription ||
        subscription.retired ||
        subscription.terminalError !== null
      ) {
        return null;
      }
      const openingDeltas: Parameters<SubscriptionManager<T>["handleDelta"]>[0][] = [];
      subscription.installing = true;
      try {
        subscription.id = client.subscribeInternal(
          subscription.equality
            ? translateQuery(subscription.equality.json, planningSchema)
            : wasmQuery,
          {
            onUpdate: (delta) => {
              if (
                unsubscribed ||
                activeSubscription !== subscription ||
                subscription.terminalError !== null
              ) {
                return;
              }
              if (subscription.installing) {
                openingDeltas.push(delta);
                return;
              }
              try {
                handleDelta(delta);
              } catch (error) {
                terminalizeSubscription(subscription, error);
              }
            },
            onError: (error) => {
              terminalizeSubscription(subscription, error);
            },
          },
          subscriptionOptions,
          context?.readSession ?? context?.session ?? session,
        );
      } catch (error) {
        subscription.installing = false;
        terminalizeSubscription(subscription, error);
        return null;
      }
      subscription.installing = false;
      if (unsubscribed || activeSubscription !== subscription || subscription.retired) {
        retireNativeSubscription(subscription);
        return null;
      }
      if (subscription.terminalError !== null) {
        completeTerminalization(subscription);
        return null;
      }
      try {
        for (const delta of openingDeltas) {
          if (activeSubscription !== subscription || subscription.terminalError !== null) break;
          handleDelta(delta);
        }
      } catch (error) {
        terminalizeSubscription(subscription, error);
        return null;
      }
      return unsubscribed || activeSubscription !== subscription || subscription.retired
        ? null
        : subscription;
    };
    const traceId = this.registerActiveQuerySubscriptionTrace(
      wasmQuery,
      builtQuery.table,
      queryOptions,
    );
    const startNativeSubscription = (
      subscription: NativeSubscription,
      subscriptionOptions = queryOptions,
    ) => {
      if (!encrypted) return installNativeSubscription(subscription, subscriptionOptions);
      const prepare = encryptedEquality
        ? prepareEqualityQuery(this, query, builderJson)
        : Promise.resolve(undefined);
      prepare
        .then(async (equality) => {
          if (unsubscribed || subscription.retired || activeSubscription !== subscription) {
            equality?.dispose();
            return;
          }
          subscription.equality = equality;
          subscription.dependencies = queryKeyDependencies(
            this,
            query._schema,
            () => {
              if (unsubscribed || subscription.retired || activeSubscription !== subscription)
                return;
              const frontier = ++encryptedFrontier;
              try {
                onPending?.();
              } catch (error) {
                terminalizeSubscription(subscription, error);
                return;
              }
              pendingDecryption = pendingDecryption
                .then(async () => {
                  if (
                    unsubscribed ||
                    subscription.retired ||
                    activeSubscription !== subscription ||
                    frontier !== encryptedFrontier
                  )
                    return;
                  const materialized = await materializeEncryptedResult(subscription);
                  if (equality && !(await equality.isCurrent())) {
                    restartEncryptedSubscription(subscription);
                  } else if (
                    !unsubscribed &&
                    !subscription.retired &&
                    activeSubscription === subscription &&
                    frontier === encryptedFrontier &&
                    subscription.hasSnapshot
                  ) {
                    deliver({
                      reset: true,
                      delta: [],
                      all: materialized,
                    });
                  }
                })
                .catch((error) => {
                  // A newer queued frontier revalidates its own dependency set.
                  // An obsolete history failure must not reject a child-free result.
                  if (frontier === encryptedFrontier) terminalizeSubscription(subscription, error);
                });
            },
            (error) => terminalizeSubscription(subscription, error),
            { tier: options?.tier === "local-only" ? "local" : (options?.tier ?? "global") },
          );
          await subscription.dependencies.update(equality ? [equality.space] : []);
          installNativeSubscription(subscription, subscriptionOptions);
        })
        .catch((error) => terminalizeSubscription(subscription, error));
      return null;
    };
    const restartEncryptedSubscription = (subscription: NativeSubscription) => {
      if (unsubscribed || subscription.retired || activeSubscription !== subscription) return;
      const replacement = createSubscriptionGeneration();
      retireNativeSubscription(subscription);
      bufferedDeltas.length = 0;
      manager.clear();
      try {
        onPending?.();
      } catch (error) {
        terminalizeSubscription(replacement, error);
        return;
      }
      startNativeSubscription(replacement, {
        ...queryOptions,
        ...(options?.tier === ReadTier.RemoteIfPossible && this.connection.isExplicitlyOffline()
          ? { tier: "local" as const }
          : {}),
      });
    };
    const unsubscribe = () => {
      if (unsubscribed) return;
      unsubscribed = true;
      deliveryReady = false;
      bufferedDeltas.length = 0;
      readyAbort.abort();
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      if (activeSubscription !== null) {
        retireNativeSubscription(activeSubscription);
        if (activeSubscription.predecessor !== null) {
          retireNativeSubscription(activeSubscription.predecessor);
          activeSubscription.predecessor = null;
        }
      }
      activeSubscription = null;
      manager.clear();
    };
    const ready = initialReadiness
      ?.then(() => {
        if (unsubscribed || terminalized || activeSubscription === null || this.isShuttingDown) {
          return;
        }
        deliveryReady = true;
        for (const delta of bufferedDeltas.splice(0)) {
          if (unsubscribed || terminalized || activeSubscription === null) return;
          deliver(delta);
        }
      })
      .catch((error: unknown) => {
        if (unsubscribed || terminalized || activeSubscription === null || this.isShuttingDown) {
          return;
        }
        // Admission failed after native registration. Terminalize the same
        // generation before rejecting readiness so public and framework
        // consumers observe one error and no buffered opening can escape.
        terminalizeSubscription(activeSubscription, error);
        throw error;
      });
    // The public error callback owns terminal notification. Retain a rejection
    // handler because the readiness property is an internal orchestration seam
    // and public Db.subscribe callers are not required to consume it.
    if (ready) void ready.catch(() => undefined);
    const initialSubscription = createSubscriptionGeneration();
    // The native maintained stream owns both the opening snapshot and later
    // changes. Do not fabricate an empty opening or race it with a one-shot
    // cache read: that snapshot may be older than deltas already delivered.
    if (
      this.connection.shouldDeferSubscriptionStart(resolveReadTier(queryOptions.tier ?? "local"))
    ) {
      // The worker can only classify the initial authority-tier snapshot as
      // settled after its own server transport is attached. Delay native
      // subscription creation until that topology is ready; the native stream
      // then owns the settled-snapshot gate and remains the sole data source.
      void this.ensureReady(resolveReadTier(queryOptions.tier ?? "local"), readyAbort.signal)
        .then(() => startNativeSubscription(initialSubscription))
        .catch((error: unknown) => {
          if (unsubscribed || readyAbort.signal.aborted || this.isShuttingDown) return;
          terminalizeSubscription(initialSubscription, error);
        });
    } else {
      startNativeSubscription(initialSubscription);
    }
    // Connectivity changes select inputs, not a second result merger. Retire
    // the old generation immediately so late local/remote callbacks cannot
    // cross the transition. Reconnecting waits for a fresh remote opening.
    if (options?.tier === ReadTier.RemoteIfPossible) {
      let selectedOffline = remoteIfPossibleOffline;
      this.connection.onExplicitOfflineChange((offline) => {
        if (
          offline === selectedOffline ||
          unsubscribed ||
          terminalized ||
          activeSubscription === null
        )
          return;
        selectedOffline = offline;
        const retired = activeSubscription;
        const replacement = createSubscriptionGeneration();
        retireNativeSubscription(retired);
        bufferedDeltas.length = 0;
        const replacementOptions = {
          ...queryOptions,
          tier: offline ? ("local" as const) : ReadTier.RemoteIfPossible,
        };
        if (offline) {
          startNativeSubscription(replacement, replacementOptions);
        } else {
          void this.ensureReady("edge", readyAbort.signal)
            .then(() => startNativeSubscription(replacement, replacementOptions))
            .catch((error: unknown) => terminalizeSubscription(replacement, error));
        }
      }, readyAbort.signal);
    }

    const handle = unsubscribe as SubscriptionHandle;
    if (ready) Object.defineProperty(handle, "ready", { value: ready });
    return handle;
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes the Db's runtime client.
   *
   * Idempotent: concurrent or repeated calls share the same in-flight promise.
   */
  async shutdown(options: ShutdownOptions = {}): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shutdownPromise = this.runShutdown(options);
    try {
      await this.shutdownPromise;
    } catch (error) {
      if (!this.isShuttingDown) this.shutdownPromise = null;
      throw error;
    }
  }

  private cancelSyncShutdown: (() => void) | undefined;

  /** @internal Credential invalidation must interrupt a graceful sync wait. */
  abortGracefulShutdown(): void {
    this.cancelSyncShutdown?.();
  }

  protected assertOpen(): void {
    if (this.isShuttingDown || this.shutdownPromise) {
      throw new Error("Cannot operate on a Db that is shutting down or closed.");
    }
  }

  private readonly shutdownListeners = new Set<() => void>();

  /** @internal Dispose account refresh and invalidation observers with the context. */
  onShutdown(listener: () => void): () => void {
    if (this.isShuttingDown) listener();
    else this.shutdownListeners.add(listener);
    return () => this.shutdownListeners.delete(listener);
  }

  private async runShutdown(options: ShutdownOptions): Promise<void> {
    this.isShuttingDown = true;
    if (options.waitForSync) {
      const syncAbort = new AbortController();
      const cancelled = new Promise<never>((_resolve, reject) => {
        this.cancelSyncShutdown = () => {
          syncAbort.abort();
          reject(new Error("Graceful shutdown cancelled"));
        };
      });
      try {
        await Promise.race([
          this.runtimeSource.waitForPendingWrites?.(syncAbort.signal) ??
            this.connection.waitForPendingWrites(),
          cancelled,
        ]);
      } catch (error) {
        syncAbort.abort();
        this.isShuttingDown = false;
        throw new GracefulShutdownSyncError(error);
      } finally {
        this.cancelSyncShutdown = undefined;
      }
    }
    for (const listener of this.shutdownListeners) {
      try {
        listener();
      } catch (error) {
        console.error("Context cleanup failed", error);
      }
    }
    this.shutdownListeners.clear();
    this.shutdownAbort.abort();
    if (this.localFirstRefreshTimer) {
      clearTimeout(this.localFirstRefreshTimer);
      this.localFirstRefreshTimer = null;
    }
    this.clearActiveQuerySubscriptionTraces();
    this.mutationErrorListeners.clear();

    try {
      await this.connection.shutdown();
    } finally {
      await this.runtimeSource.shutdown();
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
    _queryTable: string,
    options?: InternalQueryExecutionOptions,
  ): string | null {
    if (!this.config.devMode) {
      return null;
    }

    const resolvedOptions = resolveEffectiveQueryExecutionOptions(
      { ...this.config, defaultDurabilityTier: this.runtimeSource.defaultDurabilityTier },
      options,
    );
    // Inspector-only reads must not recursively appear in the inspector's
    // own subscription list. Public local-first still propagates and is listed.
    if (resolvedOptions.propagation === "local-only") return null;
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
      const parsed = JSON.parse(queryJson) as {
        table?: unknown;
        branches?: unknown;
      };
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
 * Uses Web Crypto or the installed native host's OS entropy.
 */
function generateEphemeralSeedBase64Url(): string {
  const bytes = runtimeRandomBytes(32);
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
  assertAccountConfig(config);
  assertNoClientBackendSecret(config);
  if (config.secret && config.cookieSession) {
    throw new Error("DbConfig error: secret and cookieSession are mutually exclusive");
  }
  if (config.secret && config.jwtToken) {
    throw new Error("DbConfig error: secret and jwtToken are mutually exclusive");
  }
  if (config.jwtToken && config.cookieSession) {
    throw new Error("DbConfig error: jwtToken and cookieSession are mutually exclusive");
  }

  // Validate a durable root before loading a runtime or creating any author-
  // adjacent state. This makes malformed/legacy input fail deterministically
  // even when a platform artifact cannot be loaded.
  const parsedLocalFirstSeed = config.secret ? authSecretSeedForMinting(config.secret) : null;

  let resolvedConfig: DbConfig = { ...config };
  setTrustedReservedSession(resolvedConfig, getTrustedReservedSession(config));
  await runtimeSource.load(config);
  const {
    secret: _secret,
    jwtToken: _jwtToken,
    cookieSession: _cookieSession,
    ...configWithoutAuth
  } = config;

  // Local-first auth: resolve seed and mint a JWT
  const localFirstSecret = parsedLocalFirstSeed;
  if (localFirstSecret) {
    const secret = localFirstSecret;

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

  copyAccountConfigAdmission(config, resolvedConfig);
  runtimeSource.admitConfig(resolvedConfig as RuntimeConfig);

  const driver = resolveStorageDriver(resolvedConfig.driver);
  const db =
    runtimeSource.supportsBrowserWorker && isBrowserRuntime() && driver.type === "persistent"
      ? await Db.createWithBrowserWorker(resolvedConfig, runtimeSource as AnyRuntimeSource)
      : await Db.createWithDirectConnection(resolvedConfig, runtimeSource as AnyRuntimeSource);

  if (localFirstSecret) {
    db.initLocalFirstAuth(localFirstSecret, 3600, !config.jwtToken);
  }

  return db;
}

/** Keep server-only admission credentials out of every client runtime factory. */
export function assertNoClientBackendSecret(config: object): void {
  if (Object.hasOwn(config, "backendSecret")) {
    throw new Error(
      "DbConfig does not accept backendSecret. Use createJazzContext() from jazz-tools/backend on a trusted server instead.",
    );
  }
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof Worker !== "undefined";
}
