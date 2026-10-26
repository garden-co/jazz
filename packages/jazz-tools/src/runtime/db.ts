import { checkTransactionSettlements } from "./transaction-settlements.js";
import { isHistoryTable, resolveQueryTable } from "./history-relation.js";
import type { ConnectionRequirement } from "./connection-manager/types.js";
import type { DbAccessContext } from "./db-access-context.js";
import { Utf8Decoder } from "./utf8.js";
import type { ReservedTxId } from "./provisional-initialization.js";
import { parseSnapshotRef, type SnapshotRef } from "./snapshot-ref.js";
import { runtimeRandomBytes } from "./runtime-entropy.js";
import { streamingBytes } from "./streaming-source.js";
import { parseUuid, formatUuid } from "./uuid.js";
import { assertRequiredRowColumnsPresent } from "./row-validation.js";
import {
  initialRecipientIds,
  SpaceInitialisationRequired,
  ProvisionalSpaceRequiresExclusive,
} from "../e2ee/space-lifecycle.js";
import {
  e2eeForDb,
  e2eeSchemaForDb,
  assertSpaceWriteTransaction,
  prepareInitialSpaceForTransaction,
  prepareInitialSpaceRows,
  prepareMissingSpaceWrite,
  prepareStreamingSpace,
  type StreamingSpacePlan,
  type InitialSpaceSeed,
  withSpaceKeys,
  type E2ee,
} from "../e2ee/lifecycle.js";
import type { SpaceRoot } from "../e2ee/spaces.js";
import { equalityToken } from "../e2ee/equality-data.js";
import { prepareEqualityQuery } from "../e2ee/equality-query.js";
import { queryKeyDependencies, decryptedQuerySpaces } from "../e2ee/query-dependencies.js";
import { E2eeDataError } from "../e2ee/data-error.js";
import { equalityIndexColumn } from "../e2ee/encrypted-schema.js";
import { permissionSpaceColumn } from "../e2ee/current-membership-names.js";
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
  SerializableWriteHandle,
  SerializableWriteResult,
  WriteResult,
  JazzClient,
  withTransactionAdmission,
  type MutationErrorEvent,
  WriteHandle,
  createOpenTransactionId,
  setWriteWaitReadiness,
  type TransactionKind,
  type RowSettlement,
  type ReadTierOptions,
  type InternalQueryExecutionOptions,
  type QueryVisibility,
  isPublicQueryReadTier,
  rejectRemovedReadTier,
  normalizeFirstLoadRemoteWaitMs,
  resolveEffectiveQueryExecutionOptions,
  type InternalReadTier,
  type BranchSelector,
  type BranchView,
  type OpenTransactionId,
  type TxId,
  type PermissionAdvice,
  type StreamingValueSource,
  type TransactionPreparationIO,
  type StagedStreamingMutation,
} from "./client.js";
import {
  type BrowserForegroundNodeLease,
  type RuntimeSource,
  type RuntimeTokenOptions,
} from "./runtime-source.js";
import type { AuthFailureReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { resolveQueryOutputTable } from "./query-output-table.js";
import {
  applyColumnTransforms,
  createRowTransformer,
  transformRow,
  unwrapValue,
} from "./row-transformer.js";
import { getOwnerPreparation, runAfterPreparation, WritePreparation } from "./write-preparation.js";
import { writeRow } from "./write-row.js";
import {
  ComputedReadState,
  MissingComputedDependency,
  columnTransformDependencies,
  computedColumnValues,
  dependencyContext,
  storedDependencyValues,
  type ColumnTransform,
  type ColumnTransformMap,
  type ColumnTransformRegistry,
} from "./column-transforms.js";
export type {
  ColumnTransform,
  ColumnTransformMap,
  ColumnTransformRegistry,
} from "./column-transforms.js";
import { toValue, toWriteRecord } from "./value-converter.js";
import {
  encryptedSchemas,
  encryptedRowSpaces,
  usesEncryptionSpacePolicy,
} from "../e2ee/encrypted-schema.js";
import {
  encryptCell,
  encryptStreamingCell,
  decryptCellRows,
  decryptedIndexBytes,
} from "../e2ee/cell-data.js";
import { TypedTableQueryBuilder, type AnyTableMeta } from "../typed-app.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import { AuthRenewalBackoff } from "./auth-renewal-backoff.js";
import {
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  resolveClientInternalSessionSync,
  isRetainedAccountSessionInput,
} from "./client-session.js";
import {
  createBrowserPhysicalDatabaseName,
  createBrowserStorageOwner,
} from "./browser-worker-config.js";
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
import { notifyLocalWrite } from "./local-write-observer.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import {
  normalizeBuiltQuery,
  type NormalizedBuiltQuery,
  type NormalizedIncludeSpec,
} from "./query-builder-shape.js";
import {
  BrowserConnectionManager,
  DirectConnectionManager,
  type ConnectionManager,
  type DbForConnection,
} from "./connection-manager/index.js";
import { toError } from "./util.js";

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
export type BranchBase = Branch | readonly [branch: Branch, snapshot: SnapshotRef];

export type QueryOptions = ReadTierOptions & {
  /** Current branch coordinate. A scalar selects a table with one `branchBy` column. */
  branch?: Branch;
  /** Optional live base, or `[base, snapshotRef]` for a frozen base. */
  base?: BranchBase;
};

type InternalDbQueryOptions = Omit<QueryOptions, "tier"> & {
  tier?: InternalQueryExecutionOptions["tier"];
  visibility?: InternalQueryExecutionOptions["visibility"];
  observeAcceptedEvidence?: boolean;
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
  rejectRemovedReadTier(candidate.tier);
  const lowered: InternalDbQueryOptions = {};
  if (isPublicQueryReadTier(candidate.tier)) lowered.tier = candidate.tier;
  const firstLoadRemoteWaitMs = normalizeFirstLoadRemoteWaitMs(
    (candidate as { firstLoadRemoteWaitMs?: unknown }).firstLoadRemoteWaitMs,
  );
  if (firstLoadRemoteWaitMs !== undefined) lowered.firstLoadRemoteWaitMs = firstLoadRemoteWaitMs;
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
  ): Promise<T[]> | T[];
  subscribeDelta<T extends { id: string }>(
    query: QueryBuilder<T>,
    callbacks: ((delta: SubscriptionDelta<T>) => void) | DbDeltaSubscriptionCallbacks<T>,
    options?: QueryOptions,
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
const acceptedEvidenceSubscriptions = new WeakMap<
  Db,
  (
    query: QueryBuilder<{ id: string }>,
    callbacks: DbDeltaSubscriptionCallbacks<{ id: string }>,
    options: QueryOptions,
  ) => SubscriptionHandle
>();

/** @internal E2EE dependency invalidation; a notification is not acceptance proof. */
export function observeAcceptedQueryDependencies(
  db: Db,
  query: QueryBuilder<{ id: string }>,
  onChange: () => void,
  onError: (error: Error) => void,
  options: QueryOptions,
): SubscriptionHandle {
  return acceptedEvidenceSubscriptions.get(db)!(query, { onDelta: onChange, onError }, options);
}

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

/** A declaration-only plan. Sources are consumed after the callback, before any transaction opens. */
export interface StreamingWritePlan {
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): { id: string };
  insertStreaming<T, Init, Stream>(
    table: TableProxy<T, Init, Stream>,
    data: Stream,
    options?: StreamingInsertOptions,
  ): { id: string };
  updateStreaming<T, Init, Stream, Patch>(
    table: TableProxy<T, Init, Stream, Patch>,
    id: string,
    data: Patch,
    options?: UpdateOptions,
  ): { id: string };
  upsertStreaming<T, Init, Stream, Patch>(
    table: TableProxy<T, Init, Stream, Patch>,
    id: string,
    data: Patch,
    options?: UpdateOptions,
  ): { id: string };
}

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
  const table = resolveQueryTable(schema, tableName);
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
        snapshot: parseSnapshotRef(base[1]),
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

function normalizeSelectorOptions<Options extends InsertOptions | RestoreOptions>(
  schema: WasmSchema,
  tableName: string,
  options?: Options,
): (Omit<Options, "branch"> & { branch?: BranchSelector }) | undefined {
  if (!options) return undefined;
  const { branch, ...rest } = options;
  return branch === undefined
    ? rest
    : {
        ...rest,
        branch: normalizeBranchSelector(schema, tableName, branch, "table"),
      };
}

function normalizeViewOptions<Options extends UpdateOptions | InternalDbQueryOptions>(
  schema: WasmSchema,
  tableName: string,
  options?: Options,
): (Omit<Options, "branch" | "base"> & { branch?: BranchView }) | undefined {
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
  tier: InternalReadTier;
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

function requireSchemaWithTable(preferredSchema: WasmSchema, tableName: string): WasmSchema {
  if (resolveQueryTable(preferredSchema, tableName)) {
    return preferredSchema;
  }

  throw new Error(`Query schema is missing table "${tableName}".`);
}

function normalizeQueryOptions(
  schema: WasmSchema,
  table: string,
  options?: InternalDbQueryOptions,
) {
  return normalizeViewOptions(schema, table, options) ?? {};
}

function assertWritableTable(table: { readonly _table: string }): void {
  if (isHistoryTable(table._table)) {
    throw new Error(`History table "${table._table}" is read-only`);
  }
}

/** Apply the table's input transforms, then encode `data` as the write record for `operation`. */
function writeRecord(
  operation: WriteOperationName,
  table: TableProxy<any, any, any, any, any>,
  data: unknown,
) {
  return toWriteRecordForOperation(
    operation,
    transformInputColumns(table, data),
    table._schema,
    table._table,
  );
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

/** Validate an `update` call's `applyDiffs` option and lower it to wire descriptors. */
function lowerApplyDiffs(
  table: TableProxy<any, any, any, any, any>,
  data: Record<string, unknown>,
  diffs: unknown,
): WireLargeValueUpdate[] {
  if (diffs === undefined) return [];
  if (typeof diffs !== "object" || diffs === null || Array.isArray(diffs)) {
    throw new Error("update option applyDiffs must be an object keyed by column name.");
  }
  if (Object.keys(data).some((column) => Object.hasOwn(diffs, column))) {
    throw new Error("update replacements and applyDiffs must not both specify the same column.");
  }
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
  return descriptors;
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

function prepareTransformRead<T>(
  query: QueryBuilder<T>,
  built: NormalizedBuiltQuery,
  outputTable: string,
  computedState?: ComputedReadState,
) {
  const outputTransforms = resolveOutputColumnTransforms(query, built.table, outputTable);
  const outputIncludes =
    outputTable === built.table || !Object.keys(built.includes).length ? built.includes : {};
  const hasIncludes = Object.keys(outputIncludes).length > 0;
  if (!outputTransforms && (!query._columnTransformsByTable || !hasIncludes)) {
    const decode = createRowTransformer<T>(
      query._schema,
      outputTable,
      outputIncludes,
      built.select,
    );
    const finish = (row: T) =>
      Object.keys(built.partialSelect).length
        ? applyPartialValueSelections(row, built.partialSelect)
        : row;
    return {
      computedState: undefined,
      version: 0,
      physical: outputIncludes === built.includes ? built : { ...built, includes: outputIncludes },
      decode,
      finish,
      transform: (row: WasmRow) => finish(decode(row)),
    };
  }
  const relations = hasIncludes ? analyzeRelations(query._schema) : undefined;
  const prepare = (
    table: string,
    select: string[],
    includes: NormalizedIncludeSpec,
    partialSelect: NormalizedBuiltQuery["partialSelect"],
  ) => {
    const transforms =
      query._columnTransformsByTable?.[table] ??
      (table === outputTable ? outputTransforms : undefined);
    let physical = select;
    let partial = partialSelect;
    const selectAll = !select.length || select.includes("*");
    let visibleTransforms: ColumnTransformMap | undefined;
    let dependencies: Set<string> | undefined;
    let hidden: string[] | undefined;
    let computed: string[] | undefined;
    const addDependency = (dependency: string) => {
      if (transforms?.[dependency]?.computed) return;
      (dependencies ??= new Set()).add(dependency);
      if (!selectAll && dependency !== "id" && !physical.includes(dependency)) {
        if (physical === select) physical = [...select];
        physical.push(dependency);
        (hidden ??= []).push(dependency);
      }
      if (Object.hasOwn(partial, dependency)) {
        if (partial === partialSelect) partial = { ...partialSelect };
        delete partial[dependency];
      }
    };
    if (transforms && select.some((column) => transforms[column]?.computed))
      physical = select.filter((column) => !transforms[column]?.computed);
    if (transforms)
      for (const column of selectAll ? Object.keys(transforms) : select) {
        if (!Object.hasOwn(transforms, column)) continue;
        (visibleTransforms ??= {})[column] = transforms[column]!;
        if (transforms[column]!.computed) {
          computedState ??= new ComputedReadState();
          (computed ??= []).push(column);
          for (const dependency of computedState.storedColumns(transforms, column))
            addDependency(dependency);
        } else {
          for (const dependency of transforms[column]!.dependsOn ?? []) addDependency(dependency);
        }
      }
    if (!selectAll && !physical.length) physical = ["id"];
    let preparedIncludes = includes;
    const finishIncludes: Array<
      [string, (row: Record<string, unknown>) => Record<string, unknown>]
    > = [];
    for (const [name, include] of Object.entries(includes)) {
      const target = relations?.get(table)?.find((relation) => relation.name === name)?.toTable;
      if (!target) continue;
      const prepared = prepare(target, include.select, include.includes, include.partialSelect);
      if (
        prepared.select !== include.select ||
        prepared.includes !== include.includes ||
        prepared.partialSelect !== include.partialSelect
      ) {
        if (preparedIncludes === includes) preparedIncludes = { ...includes };
        preparedIncludes[name] = {
          ...include,
          select: prepared.select,
          includes: prepared.includes,
          partialSelect: prepared.partialSelect,
        };
      }
      if (prepared.finish) finishIncludes.push([name, prepared.finish]);
    }
    const dependencyColumns = dependencies ? [...dependencies] : noWriteDependencies;
    const hasPartial = Object.keys(partialSelect).length > 0;
    const excluded = Object.keys(includes);
    return {
      select: physical,
      includes: preparedIncludes,
      partialSelect: partial,
      finish:
        visibleTransforms || hasPartial || finishIncludes.length
          ? (row: Record<string, unknown>): Record<string, unknown> => {
              const stored = computed
                ? { ...row }
                : visibleTransforms
                  ? storedDependencyValues(dependencyColumns, row)
                  : undefined;
              const appValues = computed
                ? computedColumnValues(transforms!, computed, stored!, computedState, partial)
                : undefined;
              const result = hasPartial ? applyPartialValueSelections(row, partialSelect) : row;
              if (visibleTransforms)
                applyColumnTransforms(
                  result,
                  visibleTransforms,
                  excluded,
                  stored,
                  appValues,
                  hasPartial ? partialSelect : undefined,
                );
              if (computed) for (const column of computed) result[column] = appValues!.get(column);
              for (const [name, finish] of finishIncludes) {
                const value = result[name];
                if (Array.isArray(value))
                  for (let i = 0; i < value.length; i++) value[i] = finish(value[i]);
                else if (value && typeof value === "object")
                  result[name] = finish(value as Record<string, unknown>);
              }
              if (hidden) for (const column of hidden) delete result[column];
              return result;
            }
          : undefined,
    };
  };
  const prepared = prepare(outputTable, built.select, outputIncludes, built.partialSelect);
  const decode = createRowTransformer<Record<string, unknown>>(
    query._schema,
    outputTable,
    prepared.includes,
    prepared.select,
    undefined,
    false,
    relations,
  );
  return {
    computedState,
    version: computedState?.version ?? 0,
    physical:
      prepared.select === built.select &&
      prepared.partialSelect === built.partialSelect &&
      prepared.includes === built.includes
        ? built
        : {
            ...built,
            select: prepared.select,
            partialSelect: prepared.partialSelect,
            includes: prepared.includes,
          },
    decode,
    finish: prepared.finish ?? ((row: Record<string, unknown>) => row as T),
    transform: prepared.finish
      ? (row: WasmRow) => prepared.finish!(decode(row)) as T
      : (decode as (row: WasmRow) => T),
  };
}

/** Retry only computations that discovered a missing field, preserving the caller's read scope. */
async function finishComputedRead<T>(
  query: QueryBuilder<T>,
  built: NormalizedBuiltQuery,
  outputTable: string,
  prepared: ReturnType<typeof prepareTransformRead<T>>,
  rows: WasmRow[],
  read: (physical: NormalizedBuiltQuery) => Promise<WasmRow[]>,
  decrypt?: TransactionRowDecoder,
): Promise<T[]> {
  for (;;) {
    const results: T[] = [];
    let missing: MissingComputedDependency | undefined;
    // Visit every root before retrying so different rows' branches share a lookup.
    const decoded = await decryptQueryRows(
      query._schema,
      outputTable,
      rows.map((row) => prepared.decode(row) as Record<string, unknown>),
      outputTable !== built.table ? {} : prepared.physical.includes,
      query._columnTransformsByTable,
      decrypt,
    );
    for (const row of decoded) {
      try {
        results.push(prepared.finish(row as T & Record<string, unknown>) as T);
      } catch (error) {
        if (!(error instanceof MissingComputedDependency)) throw error;
        missing = error;
      }
    }
    if (!missing) return results;
    if (!prepared.computedState || prepared.computedState.version === prepared.version)
      throw missing;
    prepared = prepareTransformRead(query, built, outputTable, prepared.computedState);
    rows = await read(prepared.physical);
  }
}
type DbTransactionHandleBinding = {
  ownerClient: JazzClient;
  resolveClient: (schema: WasmSchema) => JazzClient;
  openTransactionId: OpenTransactionId;
  session?: Session;
  readSession?: Session;
  attribution?: string;
  preparation: WritePreparation;
  committing?: boolean;
  begun: boolean;
};

const dbTxHandleBindings = new WeakMap<Transaction, DbTransactionHandleBinding>();
type InitializationJournal = {
  sealed(id: ReservedTxId): Promise<void>;
  local(): Promise<void>;
  completion(): Promise<void>;
};
const initializationJournals = new WeakMap<Transaction, InitializationJournal[]>();
const initializationScopes = new WeakMap<object, Transaction>();
const initializationIO = new WeakMap<object, TransactionPreparationIO>();
/** @internal Preserve INSERT while capturing exact absence on its owning transaction. */
export async function insertInitializationRow<T, Init>(
  scope: E2eeTransactionScope,
  table: TableProxy<T, Init>,
  data: Init,
  options: InsertOptions & { id: string },
): Promise<T> {
  const io = initializationIO.get(scope);
  if (io) await io.recordInitializationInsertAbsence(table._table, options.id);
  else {
    const tx = initializationScopes.get(scope);
    if (!tx) throw new Error("Initialization requires an owned transaction");
    const { ownerClient, openTransactionId } = getDbTxHandleBinding(tx, "initialization insert");
    await ownerClient.recordInitializationInsertAbsence(
      openTransactionId,
      table._table,
      options.id,
    );
  }
  return scope.insert(table, data, options);
}

/** @internal Attach sealed linkage before the enclosing transaction can publish. */
export function journalE2eeInitialization(
  scope: E2eeTransactionScope,
  journal: InitializationJournal,
): void {
  const tx = initializationScopes.get(scope);
  if (!tx) throw new Error("Initialization requires an owned preparation scope");
  const journals = initializationJournals.get(tx) ?? [];
  journals.push(journal);
  initializationJournals.set(tx, journals);
}

/** @internal Exact owner status; no SDK-supplied author and no transaction enumeration. */
export async function initializationStatus(db: Db, ids: readonly ReservedTxId[]) {
  return db.e2eeInitializationTransactionStatus(ids);
}
const streamingInitialSeeds = new WeakMap<
  Transaction,
  Map<WasmSchema, Map<string, InitialSpaceSeed>>
>();
const streamingInitialRetainers = new WeakMap<
  Transaction,
  (secret: Uint8Array, root: SpaceRoot) => Promise<void>
>();

function streamingInitialSeed(
  tx: Transaction,
  table: TableProxy<any, any>,
  id: string,
): InitialSpaceSeed | undefined {
  return streamingInitialSeeds
    .get(tx)
    ?.get(table._schema)
    ?.get(`${table._table}:${id.toLowerCase()}`);
}
const standaloneWriteRetries = new WeakMap<
  Transaction,
  { prepare: (tx: Transaction) => void; branch: boolean; initialiseMissing: boolean } | null
>();

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

  const record = row as Record<string, unknown>;
  const computed = columnTransformDependencies(source._columnTransforms).computedColumns;
  const appValues = computed.length
    ? computedColumnValues(source._columnTransforms, computed, record)
    : undefined;
  applyColumnTransforms(record, source._columnTransforms, [], undefined, appValues);
  for (const column of computed) record[column] = appValues!.get(column);
  return record;
}

function transformInputColumns(
  table: TableProxy<any, any, any, any, any>,
  data: unknown,
  storedValues?: Record<string, unknown>,
  creation = false,
): Record<string, unknown> {
  const record = data as Record<string, unknown>;
  if (!table._columnTransforms) {
    return record;
  }

  const transforms = table._columnTransforms;
  let hasTransform = false;
  let hasDependencies = false;
  for (const column in record) {
    if (Object.hasOwn(record, column) && transforms[column]?.computed)
      throw new TypeError(`Computed field "${column}" is read-only.`);
    if (!Object.hasOwn(record, column) || record[column] === undefined) continue;
    const transform = transforms[column];
    if (transform) {
      hasTransform = true;
      if (transform.dependsOn?.length) hasDependencies = true;
    }
  }
  if (!hasTransform) return record;
  if (!hasDependencies) {
    const result = { ...record };
    for (const column in record) {
      if (!Object.hasOwn(record, column) || record[column] === undefined) continue;
      const transform = transforms[column];
      if (transform)
        result[column] = transform.to(record[column], dependencyContext(transform, record));
    }
    return result;
  }
  const stored = { ...storedValues };
  const app: Record<string, unknown> = {};
  const resolved = new Set<string>();
  const resolve = (column: string) => {
    if (resolved.has(column)) return;
    const transform = table._columnTransforms?.[column];
    for (const dependency of transform?.dependsOn ?? []) resolve(dependency);
    if (Object.hasOwn(record, column) && record[column] !== undefined) {
      app[column] = record[column];
      stored[column] = transform
        ? transform.to(record[column], dependencyContext(transform, app))
        : record[column];
    } else {
      if (creation && !Object.hasOwn(stored, column)) {
        const descriptor = table._schema[table._table]!.columns.find(
          (item) => item.name === column,
        );
        if (descriptor?.default !== undefined)
          stored[column] = unwrapValue(descriptor.default, descriptor.column_type);
        else if (descriptor?.nullable) stored[column] = null;
      }
      if (!Object.hasOwn(stored, column))
        throw new Error(`Transform dependency "${column}" is unavailable.`);
      app[column] = transform
        ? transform.from(stored[column], dependencyContext(transform, stored))
        : stored[column];
    }
    resolved.add(column);
  };
  const result = { ...record };
  for (const column of Object.keys(record)) {
    if (record[column] === undefined) continue;
    resolve(column);
    result[column] = stored[column];
  }
  return result;
}

const noWriteDependencies: readonly string[] = [];

function missingWriteDependencies(
  table: TableProxy<any, any>,
  data: Record<string, unknown>,
  id?: string,
): readonly string[] {
  if (!table._columnTransforms) return noWriteDependencies;
  const { byColumn } = columnTransformDependencies(table._columnTransforms);
  let missing: Set<string> | undefined;
  for (const column of Object.keys(data)) {
    if (table._columnTransforms[column]?.computed)
      throw new TypeError(`Computed field "${column}" is read-only.`);
    if (data[column] === undefined) continue;
    for (const dependency of byColumn.get(column) ?? noWriteDependencies) {
      if (dependency !== "id" || id === undefined) {
        if (!Object.hasOwn(data, dependency) || data[dependency] === undefined)
          (missing ??= new Set()).add(dependency);
      }
    }
  }
  return missing ? [...missing] : noWriteDependencies;
}

function prepareInputColumns(
  table: TableProxy<any, any>,
  data: Record<string, unknown>,
  operation: WriteOperationName,
  id: string | undefined,
  client: JazzClient,
  session?: Session,
  openTransactionId?: OpenTransactionId,
  options?: Pick<NonNullable<ReturnType<typeof normalizeViewOptions>>, "branch">,
  missing?: readonly string[],
): Record<string, unknown> | Promise<Record<string, unknown>> {
  if (!table._columnTransforms) return data;
  missing ??= missingWriteDependencies(table, data, id);
  const stored = id === undefined ? {} : { id };
  if (operation === "Insert" || operation === "Restore" || missing.length === 0)
    return transformInputColumns(
      table,
      data,
      stored,
      operation === "Insert" || operation === "Restore",
    );
  const built = JSON.stringify({
    table: table._table,
    conditions: [{ column: "id", op: "eq", value: id }],
    select: missing.filter((column) => column !== "id"),
    limit: 1,
    includeDeleted: operation === "Upsert",
  });
  return client
    .queryInternal(
      translateQuery(built, table._schema),
      {
        ...(options?.branch ? { branch: options.branch } : {}),
        ...(openTransactionId ? { openTransactionId, localUpdates: "deferred" as const } : {}),
      },
      session,
    )
    .then((rows) => {
      const row = rows[0];
      if (!row && operation !== "Upsert")
        throw new Error("Transform dependency row is unavailable.");
      if (!row && !client.readCanEstablishAbsence(session, openTransactionId))
        throw new Error(
          "Transform dependencies are unavailable: a scoped read cannot establish that the upsert row is absent.",
        );
      const existing = row
        ? transformRow<Record<string, unknown>>(row, table._schema, table._table, {}, missing)
        : stored;
      return transformInputColumns(table, data, existing, !row && operation === "Upsert");
    });
}

function rejectIncompleteDependencies(
  table: TableProxy<any, any>,
  data: Record<string, unknown>,
  incomplete: readonly string[],
) {
  if (!table._columnTransforms || !incomplete.length) return;
  const { byColumn } = columnTransformDependencies(table._columnTransforms);
  for (const column of Object.keys(data)) {
    for (const dependency of byColumn.get(column) ?? noWriteDependencies) {
      if (incomplete.includes(dependency))
        throw new Error(
          `Transform dependency "${dependency}" requires a whole-value replacement in this write.`,
        );
    }
  }
}

function reserveRowId(): string {
  const hex = createOpenTransactionId();
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function hasDependentTransforms(table: TableProxy<any, any>): boolean {
  return (
    !!table._columnTransforms &&
    columnTransformDependencies(table._columnTransforms).hasWriteDependencies
  );
}

function immediateWriteRow<T>(
  table: TableProxy<T, unknown>,
  id: string,
  values: ReturnType<typeof toWriteRecord>,
): T {
  return transformOutputRow(
    table,
    transformRow(
      writeRow(table._table, id, table._schema[table._table]!.columns, values),
      table._schema,
      table._table,
    ),
  );
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
  if (encryption?.scopes.has(table._table)) {
    throw new Error("Encryption scope streaming values are not supported");
  }
  const declaration = encryption?.tables.get(table._table);
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
  if (
    declaration &&
    (operation !== "update" ||
      Object.hasOwn(record, declaration.space) ||
      declaration.columns.some((name) => Object.hasOwn(record, name)))
  ) {
    const logicalColumn = encryption!.logical[table._table]?.columns.find(
      (entry) => entry.name === column,
    );
    if (!declaration.columns.includes(column) || logicalColumn?.column_type.type !== "Bytea")
      throw new Error("Encrypted streaming requires an encrypted Bytea column");
    if (declaration.indexes?.[column])
      throw new Error("Equality-indexed encrypted streaming is not supported");
  }
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

type TransactionCommitHandle<TKind extends TransactionKind> = TKind extends "serializable"
  ? SerializableWriteHandle
  : WriteHandle;

type TransactionWriteResult<TResult, TKind extends TransactionKind> = TKind extends "serializable"
  ? SerializableWriteResult<TResult>
  : WriteResult<TResult>;

type RunInTransactionResult<TResult, TKind extends TransactionKind> = Promise<
  TransactionWriteResult<Awaited<TResult>, TKind>
>;

export type Scoped<TTransaction> = Omit<TTransaction, "commit" | "rollback">;

function createTransactionScope<TTransaction extends object>(
  transaction: TTransaction,
): Scoped<TTransaction> {
  const scope = new Proxy(transaction, {
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
  if (transaction instanceof Transaction) initializationScopes.set(scope, transaction);
  return scope;
}

function createTransactionWriteResult<TResult, TKind extends TransactionKind>(
  transaction: Transaction<TKind>,
  value: TResult,
  txId: TxId | Promise<TxId>,
  client: JazzClient,
): TransactionWriteResult<TResult, TKind> {
  if (transaction.kind === "serializable") {
    return new SerializableWriteResult(value, txId, client) as TransactionWriteResult<
      TResult,
      TKind
    >;
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
  const ownerClient = resultClient();
  ownerClient.bindTransactionWait(
    getDbTxHandleBinding(transaction, "result").openTransactionId,
    txId,
  );
  return createTransactionWriteResult(transaction, resolvedValue, txId, ownerClient);
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

type PreparedEncryptedUpdate = {
  identifier: string;
  initial: boolean;
  stage: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>;
  withKey: (use: PreparedEncryptedUpdate["stage"]) => Promise<void>;
};

type EncryptedUpdateBatch = {
  schema: WasmSchema;
  table: string;
  pending: boolean;
  writes: ((io: TransactionPreparationIO) => Promise<PreparedEncryptedUpdate>)[];
};

type EncryptedInsertBatch = {
  schema: WasmSchema;
  requireAccepted: boolean;
  scope: string;
  identifier: string;
  branch: string;
  pending: boolean;
  writes: ((
    io: TransactionPreparationIO,
    key?: { secret: Uint8Array; root: Readonly<SpaceRoot> },
  ) => Promise<void>)[];
};

/**
 * Groups a set of writes as either a mergeable or serializable transaction (see {@link TransactionKind}).
 */
export class Transaction<TKind extends TransactionKind = TransactionKind> {
  private readonly pendingReads = new Set<Promise<unknown>>();
  private committing = false;
  private failedRead = false;
  private failedReadCleanupComplete = false;
  private cancelled = false;
  private mayInitialiseSpace = false;
  private readonly initialSpaceKeys = new Map<string, { secret: Uint8Array; root: SpaceRoot }>();
  private readonly encryptedScopes = new Map<string, string>();
  private encryptedInsertBatch?: EncryptedInsertBatch;
  private encryptedUpdateBatch?: EncryptedUpdateBatch;

  constructor(
    readonly kind: TKind,
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly context: DbAccessContext | null = null,
    ownerClient?: JazzClient,
    private readonly e2ee?: { db: Db },
  ) {
    if (ownerClient) this.bindOwnerClient(ownerClient);
    streamingInitialRetainers.set(this, (secret, root) => this.retainInitialSpaceKey(secret, root));
  }

  private bindTable<T, Init, StreamingInit, StreamingUpdate, LargeValueUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate, LargeValueUpdate>,
  ): DbTransactionHandleBinding {
    const client = this.resolveClient(table._schema);
    if (!dbTxHandleBindings.has(this)) this.bindOwnerClient(client);
    if (this.requireBinding("table operation").committing)
      throw new Error("Transaction commit has already been requested.");
    return this.requireBinding("table operation");
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbTransactionHandleBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>);
  }

  private requireBinding(operation: string): DbTransactionHandleBinding {
    // Adjacent inserts or updates can share preparation until it starts.
    // Other handle operations close admission to both kinds of batch.
    this.encryptedInsertBatch = undefined;
    this.encryptedUpdateBatch = undefined;
    if (this.failedRead && operation !== "rollback") {
      throw new Error(`DbTransaction.${operation}() cannot run after a pending read failed`);
    }
    if (this.committing) {
      throw new Error(`DbTransaction.${operation}() cannot run after commit has been requested`);
    }
    return getDbTxHandleBinding(this, operation);
  }

  private bindOwnerClient(ownerClient: JazzClient): void {
    const { writeSession: session, readSession, attribution } = this.context ?? {};
    const preparation = new WritePreparation(true);
    const owner = getOwnerPreparation(ownerClient);
    const openTransactionId = owner?.pending
      ? createOpenTransactionId()
      : ownerClient.beginTransaction(this.kind, session, attribution);
    const binding: DbTransactionHandleBinding = {
      ownerClient,
      resolveClient: this.resolveClient,
      openTransactionId,
      session: session,
      readSession,
      attribution: attribution,
      preparation,
      begun: !owner?.pending,
    };
    dbTxHandleBindings.set(this, binding);
    if (!binding.begun)
      preparation.run(() =>
        owner!.run(() => {
          if (preparation.cancelled) return;
          const begin = () =>
            ownerClient.beginTransaction(this.kind, session, attribution, openTransactionId);
          const prerequisite = transactionAdmission.get(this);
          if (prerequisite) withTransactionAdmission(ownerClient, prerequisite, begin);
          else begin();
          binding.begun = true;
        }),
      );
  }

  openTransactionId(): OpenTransactionId {
    return this.requireBinding("openTransactionId").openTransactionId;
  }

  /**
   * Commit this transaction.
   */
  commit(): TransactionCommitHandle<TKind> {
    const binding = this.requireBinding("commit");
    const { ownerClient, openTransactionId, preparation } = binding;
    if (binding.committing) throw new Error("Transaction commit has already been requested.");
    if (preparation.pending || preparation.failed) binding.committing = true;
    const publish = async () => {
      await preparation.drain();
      // Drain existing preparation before deciding whether it registered a journal.
      await ownerClient.prepareTransaction(openTransactionId, async () => {});
      const journals = initializationJournals.get(this);
      if (!journals?.length) return ownerClient.commitTransaction(openTransactionId).txId;
      const seal = await ownerClient.sealInitializationTransaction(openTransactionId);
      try {
        for (const journal of journals) await journal.sealed(seal.reservedTxId);
      } catch (error) {
        await ownerClient.cancelInitializationTransaction(seal);
        throw error;
      }
      const id = await ownerClient.publishInitializationTransaction(seal);
      ownerClient.setInitializationCompletion(
        id,
        Promise.all(journals.map((journal) => journal.completion())).then(() => {}),
      );
      await ownerClient.waitForSerializableTransaction(id, "local");
      for (const journal of journals) await journal.local();
      return id;
    };
    // Ordinary commits must not acquire initialization preparation or extra
    // promise hops: application waits claim their mutation errors immediately.
    const commit = () =>
      this.mayInitialiseSpace || initializationJournals.has(this)
        ? publish()
        : ownerClient.commitTransaction(openTransactionId).txId;
    let txId: Promise<TxId>;
    if (preparation.pending || preparation.failed || this.pendingReads.size > 0) {
      txId = Promise.all([preparation.drain(), ...this.pendingReads]).then(
        commit,
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
      txId = commit();
    }
    ownerClient.bindTransactionWait(openTransactionId, txId);
    this.committing = true;
    const finishCommit = () => {
      this.committing = false;
      this.clearInitialSpaceKeys();
    };
    // Observe completion without delaying application waits on the transaction ID.
    void txId.then(finishCommit, finishCommit);
    if (this.kind === "serializable") {
      return new SerializableWriteHandle(txId, ownerClient) as TransactionCommitHandle<TKind>;
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
    const { ownerClient, openTransactionId, preparation, begun } = this.requireBinding("rollback");
    if (this.failedReadCleanupComplete) return Promise.resolve(false);
    this.cancelled = true;
    this.clearInitialSpaceKeys();
    if (!begun) {
      preparation.cancelled = true;
      return Promise.resolve(true);
    }
    const result = ownerClient.rollbackTransaction(openTransactionId);
    preparation.cancelled = true;
    return result;
  }

  private clearInitialSpaceKeys(): void {
    for (const { secret } of this.initialSpaceKeys.values()) secret.fill(0);
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
    assertWritableTable(table);
    const previousBatch = this.encryptedInsertBatch;
    this.encryptedInsertBatch = undefined;
    const recipients = scopeRecipients(table, options);
    const initialisesSpace = encryptedSchemas.get(table._schema)?.scopes.has(table._table);
    if (initialisesSpace) this.prepareScopeCreation();
    this.bindTable(table);
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution, preparation } = this.requireBinding("insert");
    let id =
      options?.id ??
      (initialisesSpace ||
      hasDependentTransforms(table) ||
      preparation.pending ||
      getOwnerPreparation(client)?.pending
        ? reserveRowId()
        : undefined);
    const encryption = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (encryption) {
      if (initialisesSpace)
        throw new Error("Encrypted scope rows require dependent scope preparation");
      const transformedData = transformInputColumns(
        table,
        data,
        id === undefined ? undefined : { id },
        true,
      );
      return this.prepareEncryptedRow(
        table,
        transformedData,
        id ? { ...options, id } : options,
        "insert",
        undefined,
        previousBatch,
      );
    }
    const { values, value } = this.prepareCreation(table, data, id, "Insert");
    const normalized = normalizeSelectorOptions(
      table._schema,
      table._table,
      id ? { ...options, id } : options,
    );
    const captureAbsence =
      this.kind === "serializable" && this.e2ee && encryptedSchemas.has(table._schema);
    let row: ReturnType<typeof client.previewInsertInternal> | undefined;
    const stage = () => {
      if (captureAbsence) {
        row = client.previewInsertInternal(table._table, values, normalized?.id);
        const snapshot = structuredClone(values);
        client
          .prepareTransaction(openTransactionId, async (io) => {
            await io.recordInitializationInsertAbsence(table._table, row!.id);
            io.insertInternal(
              table._table,
              snapshot,
              { ...normalized, id: row!.id },
              session,
              attribution,
              openTransactionId,
            );
          })
          .catch(() => {});
      } else {
        row = client.insertInternal(
          table._table,
          values,
          normalized,
          session,
          attribution,
          openTransactionId,
        );
      }
      if (initialisesSpace) {
        const initPreparation = prepareInitialSpaceForTransaction(
          this.e2ee!.db,
          this as Transaction<"serializable">,
          table,
          id!,
          (secret, root) => this.retainInitialSpaceKey(secret, root),
          recipients,
          streamingInitialSeed(this, table, id!),
        );
        initPreparation.catch(() => {});
      }
    };
    if (preparation.cancelled) stage();
    else
      preparation.run(() =>
        runAfterPreparation(client, () => {
          if (!preparation.cancelled) stage();
        }),
      );
    try {
      return (
        value ??
        (row
          ? transformOutputRow(table, transformRow(row, table._schema, table._table))
          : immediateWriteRow(table, id!, values))
      );
    } catch (error) {
      preparation.failure = error;
      throw error;
    }
  }

  private prepareScopeCreation(): void {
    if (this.kind !== "serializable" || !this.e2ee)
      throw new Error("E2EE scope creation requires an authenticated exclusive transaction");
    this.mayInitialiseSpace = true;
  }

  private async retainInitialSpaceKey(secret: Uint8Array, root: SpaceRoot): Promise<void> {
    if (this.cancelled) {
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
    preparedValues?: ReturnType<typeof toWriteRecord>,
    previousBatch?: EncryptedInsertBatch,
  ): T {
    if (!this.e2ee) throw new Error("Encrypted writes require an authenticated E2EE context");
    const metadata = encryptedSchemas.get(table._schema)!;
    const declaration = metadata.tables.get(table._table)!;
    const logicalTable = metadata.logical[table._table]!;
    const requireAccepted = usesEncryptionSpacePolicy(table._schema, table._table);
    const values =
      preparedValues ?? structuredClone(toWriteRecord(data, metadata.logical, table._table));
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
    if (
      table._schema[table._table]!.columns.some((column) => column.name === permissionSpaceColumn)
    )
      physical[permissionSpaceColumn] = identifier;
    const optionsSnapshot = options ? structuredClone(options) : undefined;
    const binding = this.requireBinding(operation);
    const { ownerClient, openTransactionId, session, attribution } = binding;
    const normalized = normalizeSelectorOptions(table._schema, table._table, optionsSnapshot);
    const preview = ownerClient.previewInsertInternal(table._table, physical, normalized?.id);
    if (operation === "insert" && standaloneWriteRetries.has(this)) {
      const retryOptions = { ...optionsSnapshot, id: preview.id };
      standaloneWriteRetries.set(this, {
        branch: optionsSnapshot?.branch !== undefined,
        initialiseMissing: true,
        prepare: (tx) => {
          tx.bindTable(table);
          tx.prepareEncryptedRow(table, data, retryOptions, "insert", values);
        },
      });
    }
    const scope = new TypedTableQueryBuilder(declaration.scope, table._schema);
    const db = this.e2ee.db;
    this.mayInitialiseSpace = true;
    const prepare: EncryptedInsertBatch["writes"][number] = async (io, borrowed) => {
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
            tier: "remote",
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
      const initial =
        borrowed ??
        (this.initialSpaceKeys.size
          ? this.initialSpaceKeys.get(
              `${(await db.authenticatedTableIdentity(scope)) ?? (await db.tableIdentity(scope))}:${identifier.value}`,
            )
          : undefined);
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
          if (this.kind === "serializable")
            await io.recordInitializationInsertAbsence(table._table, preview.id);
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
      if (initial) {
        if (
          requireAccepted &&
          this.initialSpaceKeys.has(`${initial.root.scopeId}:${initial.root.identifier}`)
        )
          throw new E2eeDataError("space-not-initialised");
        await stage(initial.secret, initial.root);
      } else {
        await this.withWriteSpaceKeys(
          scope,
          identifier.value,
          stage,
          binding,
          io,
          operation === "insert",
          requireAccepted,
        );
      }
    };
    if (operation === "restore") {
      ownerClient.prepareTransaction(openTransactionId, prepare);
    } else {
      const branch = branchCoordinateKey(normalized?.branch);
      if (
        previousBatch?.pending &&
        previousBatch.schema === table._schema &&
        previousBatch.scope === declaration.scope &&
        previousBatch.requireAccepted === requireAccepted &&
        previousBatch.identifier === identifier.value &&
        previousBatch.branch === branch
      ) {
        previousBatch.writes.push(prepare);
        this.encryptedInsertBatch = previousBatch;
      } else {
        const batch: EncryptedInsertBatch = {
          schema: table._schema,
          requireAccepted,
          scope: declaration.scope,
          identifier: identifier.value,
          branch,
          pending: true,
          writes: [prepare],
        };
        this.encryptedInsertBatch = batch;
        ownerClient.prepareTransaction(openTransactionId, async (io) => {
          batch.pending = false;
          if (this.encryptedInsertBatch === batch) this.encryptedInsertBatch = undefined;
          if (batch.writes.length === 1) return batch.writes[0]!(io);
          const initial = this.initialSpaceKeys.size
            ? this.initialSpaceKeys.get(
                `${(await db.authenticatedTableIdentity(scope)) ?? (await db.tableIdentity(scope))}:${identifier.value}`,
              )
            : undefined;
          let epoch: string | undefined;
          let initialisedHere = false;
          const stage = async (secret: Uint8Array, root: Readonly<SpaceRoot>) => {
            epoch = root.epochId;
            initialisedHere = this.initialSpaceKeys.has(`${root.scopeId}:${root.identifier}`);
            for (const write of batch.writes) await write(io, { secret, root });
          };
          if (initial) {
            await stage(initial.secret, initial.root);
          } else {
            // Borrow a key only while this bounded batch runs. Reobserve accepted
            // authority after encryption, before preparation can permit commit.
            await this.withWriteSpaceKeys(
              scope,
              identifier.value,
              stage,
              binding,
              io,
              true,
              requireAccepted,
            );
            // A newly staged root remains owned by this transaction's initial
            // acceptance boundary; it is not yet eligible for a global recheck.
            if (initialisedHere) return;
            await this.withWriteSpaceKeys(
              scope,
              identifier.value,
              async (_secret, root) => {
                if (root.epochId !== epoch)
                  throw new Error("Encryption space changed during transaction preparation");
              },
              binding,
              io,
              false,
              requireAccepted,
            );
          }
        });
      }
    }
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

  private async withWriteSpaceKeys<T, Init>(
    scope: TableProxy<T, Init>,
    identifier: string,
    stage: (secret: Uint8Array, root: Readonly<SpaceRoot>) => Promise<void>,
    binding: DbTransactionHandleBinding,
    io: TransactionPreparationIO,
    mayCreate: boolean,
    requireAccepted: boolean,
  ): Promise<void> {
    const db = this.e2ee!.db;
    await assertSpaceWriteTransaction(db, scope, identifier, this.kind, requireAccepted);
    let entered = false;
    try {
      await withSpaceKeys(
        db,
        scope,
        identifier,
        async (secret, root, source) => {
          if (source === "provisional" || this.kind === "serializable") {
            const rootQuery = new TypedTableQueryBuilder("__e2ee_spaces", scope._schema)
              .where({ id: root.id })
              .select("id");
            if (
              !(await readTransactionRows(rootQuery, { tier: "local-first" }, false, binding, io))
                .length
            )
              throw new E2eeDataError("key-unavailable");
          }
          entered = true;
          await stage(secret, root);
        },
        false,
        true,
      );
    } catch (error) {
      if (
        entered ||
        requireAccepted ||
        !mayCreate ||
        !(error instanceof E2eeDataError) ||
        error.code !== "key-unavailable"
      )
        throw error;
      const prepared =
        this.kind === "serializable" ? preparedTransactionScope(binding, io) : undefined;
      if (prepared) initializationScopes.set(prepared, this);
      if (
        !(await prepareMissingSpaceWrite(
          db,
          prepared,
          scope,
          identifier,
          async (secret, root) => {
            await this.retainInitialSpaceKey(secret, root);
            await stage(secret, root);
          },
          streamingInitialSeed(this, scope, identifier),
        ))
      )
        throw error;
    }
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
    assertWritableTable(table);
    this.bindTable(table);
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      const transformedData = transformInputColumns(table, data, { id }, true);
      return this.prepareEncryptedRow(table, transformedData, { ...options, id }, "restore");
    }
    const { values, value } = this.prepareCreation(table, data, id, "Restore");
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution, preparation } = this.requireBinding("restore");
    const stage = () => {
      client.restoreInternal(
        table._table,
        id,
        values,
        normalizeSelectorOptions(table._schema, table._table, options),
        session,
        attribution,
        openTransactionId,
      );
    };
    if (preparation.cancelled) stage();
    else
      preparation.run(() =>
        runAfterPreparation(client, () => {
          if (!preparation.cancelled) stage();
        }),
      );
    return value!;
  }

  private prepareCreation<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    id: string | undefined,
    operation: "Insert" | "Restore",
  ) {
    let record: Record<string, unknown>;
    try {
      record = table._columnTransforms
        ? transformInputColumns(table, data, id === undefined ? undefined : { id }, true)
        : (data as Record<string, unknown>);
    } catch (error) {
      this.requireBinding(operation).preparation.failure = error;
      throw error;
    }
    const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
    try {
      return { values, value: id === undefined ? undefined : immediateWriteRow(table, id, values) };
    } catch (error) {
      this.requireBinding(operation).preparation.failure = error;
      throw error;
    }
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
    assertWritableTable(table);
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
        normalizeViewOptions(table._schema, table._table, options),
      );
      const insertOptions = structuredClone(
        normalizeSelectorOptions(table._schema, table._table, {
          id,
          ...(options?.branch !== undefined && { branch: options.branch }),
        }),
      );
      const binding = this.requireBinding("upsert");
      const { openTransactionId, session, attribution } = binding;
      const preparation = prepareDbTransaction(
        this as Transaction<"serializable">,
        async (scope, io) => {
          const query = new TypedTableQueryBuilder(table._table, table._schema)
            .includeDeleted()
            .where({ id })
            .select("id");
          const rows = await readTransactionRows<Record<string, unknown>>(
            query,
            {
              tier: "local-first",
              ...(options?.branch !== undefined && { branch: options.branch }),
            },
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
          await io.recordInitializationInsertAbsence(table._table, id);
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
            streamingInitialSeed(this, table, id),
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
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("upsert");
    this.prepareMutation(
      table,
      id,
      data as Record<string, unknown>,
      "Upsert",
      options,
      (values) => {
        client.upsertInternal(
          table._table,
          id,
          values,
          normalizeViewOptions(table._schema, table._table, options),
          session,
          attribution,
          openTransactionId,
        );
      },
    );
  }

  /**
   * Update an existing row in a table.
   *
   * The update is scoped to this transaction, and will only be globally visible
   * once it's committed.
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
  ): void;
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): void;
  update(
    table: TableProxy<any, any, any, any, any>,
    id: string,
    data: Record<string, unknown>,
    options?: UpdateOptions & { applyDiffs?: object },
  ): void {
    assertWritableTable(table);
    const previousBatch = this.encryptedUpdateBatch;
    this.encryptedUpdateBatch = undefined;
    this.bindTable(table);
    const descriptors = lowerApplyDiffs(table, data, options?.applyDiffs);
    const encrypted = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (
      encrypted &&
      (Object.hasOwn(data, encrypted.space) ||
        encrypted.columns.some((name) => Object.hasOwn(data, name)))
    ) {
      return this.updateEncrypted(
        table,
        id,
        transformInputColumns(table, data),
        options,
        "update",
        undefined,
        previousBatch,
      );
    }
    rejectIncompleteDependencies(table, data, Object.keys(options?.applyDiffs ?? {}));
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("update");
    const normalizedOptions = normalizeViewOptions(table._schema, table._table, options);
    this.prepareMutation(
      table,
      id,
      data as Record<string, unknown>,
      "Update",
      options,
      (updates) => {
        if (descriptors.length > 0) {
          client.updateLargeValuesInternal(
            table._table,
            id,
            updates,
            descriptors,
            normalizedOptions?.updatedAt,
            session,
            attribution,
            openTransactionId,
            normalizedOptions?.branch,
          );
          return;
        }
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
      },
    );
  }

  private prepareMutation(
    table: TableProxy<any, any>,
    id: string,
    data: Record<string, unknown>,
    operation: "Update" | "Upsert",
    options: UpdateOptions | undefined,
    stage: (values: ReturnType<typeof toWriteRecord>) => void,
  ): void {
    const { ownerClient, openTransactionId, preparation } = this.requireBinding(operation);
    if (
      preparation.pending ||
      getOwnerPreparation(ownerClient)?.pending ||
      missingWriteDependencies(table, data, id).length
    )
      data = { ...data };
    if (preparation.cancelled) {
      stage({});
      return;
    }
    preparation.run(() =>
      runAfterPreparation(ownerClient, () => {
        if (preparation.cancelled) return;
        let prepared: ReturnType<typeof prepareInputColumns>;
        try {
          prepared = prepareInputColumns(
            table,
            data,
            operation,
            id,
            ownerClient,
            this.context?.readSession,
            openTransactionId,
            normalizeViewOptions(table._schema, table._table, options),
          );
        } catch (error) {
          preparation.failure = error;
          throw error;
        }
        const finish = (record: Record<string, unknown>) => {
          if (preparation.cancelled) return;
          if (preparation.failed) throw preparation.failure;
          const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
          stage(values);
        };
        return prepared instanceof Promise ? prepared.then(finish) : finish(prepared);
      }),
    );
  }

  private updateEncrypted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Record<string, unknown>,
    options?: UpdateOptions,
    operation: "update" | "upsert" = "update",
    preparedUpdates?: ReturnType<typeof toWriteRecord>,
    previousBatch?: EncryptedUpdateBatch,
  ): void {
    const metadata = encryptedSchemas.get(table._schema)!;
    const declaration = metadata.tables.get(table._table)!;
    const changed = declaration.columns.filter((name) =>
      Object.hasOwn(preparedUpdates ?? data, name),
    );
    if (changed.length && !this.e2ee)
      throw new Error("Encrypted writes require an authenticated E2EE context");
    const updates = structuredClone(
      preparedUpdates ?? toWriteRecord(data, metadata.logical, table._table),
    );
    const optionsSnapshot = options ? structuredClone(options) : undefined;
    const binding = this.requireBinding("update");
    const { ownerClient, openTransactionId, session, attribution } = binding;
    const normalized = normalizeViewOptions(table._schema, table._table, optionsSnapshot);
    if (standaloneWriteRetries.has(this)) {
      const retryValues = structuredClone(updates);
      const retryOptions = optionsSnapshot;
      standaloneWriteRetries.set(this, {
        branch: optionsSnapshot?.branch !== undefined,
        initialiseMissing: operation === "upsert",
        prepare: (tx) => {
          tx.bindTable(table);
          tx.updateEncrypted(table, id, data, retryOptions, operation, retryValues);
        },
      });
    }
    const query = new TypedTableQueryBuilder<
      AnyTableMeta & { row: { id: string } & Record<string, unknown> }
    >(table._table, table._schema)
      .includeDeleted()
      .where({ id })
      .select(declaration.space);
    if (operation === "upsert") this.mayInitialiseSpace = true;
    const prepare = async (
      io: TransactionPreparationIO,
    ): Promise<PreparedEncryptedUpdate | undefined> => {
      const rows = await readTransactionRows<Record<string, unknown>>(
        query,
        {
          ...optionsSnapshot,
          tier: "local-first",
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
      if (
        table._schema[table._table]!.columns.some((column) => column.name === permissionSpaceColumn)
      )
        updates[permissionSpaceColumn] = { type: "Uuid", value: identifier };
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
            normalizeSelectorOptions(table._schema, table._table, {
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
        ? this.initialSpaceKeys.get(
            `${(await db.authenticatedTableIdentity(scope)) ?? (await db.tableIdentity(scope))}:${identifier}`,
          )
        : undefined;
      if (initial && usesEncryptionSpacePolicy(table._schema, table._table))
        throw new E2eeDataError("space-not-initialised");
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
        if (creating && this.kind === "serializable")
          await io.recordInitializationInsertAbsence(table._table, id);
        write();
        this.encryptedScopes.set(
          encryptedRowScopeKey(table._table, id, normalized?.branch),
          identifier,
        );
      };
      return {
        identifier,
        initial: !!initial,
        stage,
        withKey: (use) =>
          initial
            ? use(initial.secret, initial.root)
            : this.withWriteSpaceKeys(
                scope,
                identifier,
                use,
                binding,
                io,
                creating,
                usesEncryptionSpacePolicy(table._schema, table._table),
              ),
      };
    };
    // Explicit branch writes retain their independent preparation path.
    if (operation !== "update" || !changed.length || normalized?.branch !== undefined) {
      ownerClient.prepareTransaction(openTransactionId, async (io) => {
        const prepared = await prepare(io);
        await prepared?.withKey(prepared.stage);
      });
      return;
    }
    // Scope queries stay between their original writes. Resolving all scopes
    // up front could miss visibility changes caused by an earlier update.
    const resolve = async (io: TransactionPreparationIO) => (await prepare(io))!;
    if (
      previousBatch?.pending &&
      previousBatch.schema === table._schema &&
      previousBatch.table === table._table
    ) {
      previousBatch.writes.push(resolve);
      this.encryptedUpdateBatch = previousBatch;
      return;
    }
    const batch: EncryptedUpdateBatch = {
      schema: table._schema,
      table: table._table,
      pending: true,
      writes: [resolve],
    };
    this.encryptedUpdateBatch = batch;
    ownerClient.prepareTransaction(openTransactionId, async (io) => {
      batch.pending = false;
      if (this.encryptedUpdateBatch === batch) this.encryptedUpdateBatch = undefined;
      let index = 0;
      let next: PreparedEncryptedUpdate | undefined = await batch.writes[index]!(io);
      while (next) {
        const first = next;
        let count = 0;
        let epoch: string | undefined;
        await first.withKey(async (secret, root) => {
          epoch = root.epochId;
          do {
            await next!.stage(secret, root);
            count++;
            next = ++index < batch.writes.length ? await batch.writes[index]!(io) : undefined;
          } while (next?.identifier === first.identifier);
        });
        if (count > 1 && !first.initial) {
          await first.withKey(async (_secret, root) => {
            if (root.epochId !== epoch)
              throw new Error("Encryption space changed during transaction preparation");
          });
        }
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
    assertWritableTable(table);
    this.bindTable(table);
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution, preparation } = this.requireBinding("delete");
    const normalizedOptions = normalizeViewOptions(table._schema, table._table, options);
    const stage = () => {
      client.deleteInternal(
        table._table,
        id,
        normalizedOptions?.updatedAt,
        session,
        attribution,
        openTransactionId,
        normalizedOptions?.branch,
      );
    };
    if (preparation.cancelled) stage();
    else
      preparation.run(() =>
        runAfterPreparation(client, () => {
          if (!preparation.cancelled) stage();
        }),
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
   * @internal Accepted E2EE rows and their authority order. Ordinary Global reads
   * qualify this transaction's visible witnesses at read time and require covered
   * history plus this transaction's Global acceptance. snapshotAtBegin additionally
   * requires those witnesses accepted when this transaction began; initial recipient
   * eligibility selects it explicitly. Local-only reads default to that stronger cut
   * and validate retained history without claiming live authority freshness.
   */
  allSettledForE2ee<T extends { id: string }>(
    query: QueryBuilder<T>,
    propagation?: "local-only",
    snapshotAtBegin = propagation === "local-only",
  ): Promise<{ rows: T[]; settlements: RowSettlement[] }> {
    if (this.kind !== "serializable")
      throw new Error("E2EE settlement reads require a serializable transaction");
    const reading = (async () => {
      const { rows, settlements } = await this.readAll(
        query,
        { tier: "remote" },
        "with-rows",
        propagation,
        snapshotAtBegin,
      );
      checkTransactionSettlements(
        rows,
        settlements,
        normalizeBuiltQuery(JSON.parse(query._build())).union !== undefined,
      );
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
    propagation?: "local-only",
    snapshotAtBegin?: boolean,
  ): Promise<SettledRows<T>>;
  private async readAll<T>(
    query: QueryBuilder<T>,
    options: QueryOptions | undefined,
    settlementMetadata: false | "with-rows" = false,
    propagation?: "local-only",
    snapshotAtBegin = false,
  ): Promise<T[] | SettledRows<T>> {
    this.bindQuery(query);
    const { preparation } = this.requireBinding("query");
    if (preparation.pending || preparation.failed) await preparation.drain();
    const client = this.resolveClient(query._schema);
    return readTransactionRows(
      query,
      options,
      settlementMetadata,
      this.requireBinding("query"),
      transactionHistoryQueries(client, propagation, snapshotAtBegin),
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
  TransactionScope<"serializable">,
  "kind" | "all" | "one" | "insert" | "upsert" | "allSettledForE2ee"
>;

type SettledRows<T> = { rows: T[]; settlements: RowSettlement[] };

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
        if (transform && Object.hasOwn(row, column)) {
          const tableTransforms = transforms?.[table];
          const stored = tableTransforms
            ? storedDependencyValues(columnTransformDependencies(tableTransforms).columns, row)
            : undefined;
          row[column] = transform.from(row[column], dependencyContext(transform, stored ?? {}));
        }
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

function transactionHistoryQueries(
  client: Pick<JazzClient, "queryInternal">,
  propagation?: "local-only",
  e2eeSnapshotAtBegin = false,
): Pick<JazzClient, "queryInternal"> {
  if (!propagation && !e2eeSnapshotAtBegin) return client;
  return {
    queryInternal: (query, options, session) =>
      client.queryInternal(
        query,
        {
          ...options,
          ...(propagation ? { tier: "local-only" } : {}),
          ...(e2eeSnapshotAtBegin ? { e2eeSnapshotAtBegin: true } : {}),
        },
        session,
      ),
  };
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
  const { openTransactionId, readSession } = binding;
  const builderJson = query._build();
  const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
  const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
  const outputTable = resolveQueryOutputTable(planningSchema, builtQuery);
  const outputSchema = requireSchemaWithTable(query._schema, outputTable);
  const prepared = prepareTransformRead(query, builtQuery, outputTable);
  // Transactions accept the same public options surface as Db. Lower before
  // reaching native options so JavaScript callers cannot smuggle runtime
  // controls (for example `localUpdates` or `openTransactionId`) through this
  // otherwise separate execution path.
  const queryOptions: InternalQueryExecutionOptions =
    normalizeViewOptions(query._schema, builtQuery.table, lowerPublicDbQueryOptions(options)) ?? {};
  if (settlementMetadata) queryOptions.settlementMetadata = settlementMetadata;
  const queryClient = global ? client.queryGlobal : client.queryInternal;
  if (!queryClient) throw new Error("Global transaction preflight is unavailable");
  const equality = !settlementMetadata && prepare ? await prepare(builderJson) : undefined;
  try {
    if (!equality) {
      const result = await queryClient.call(
        client,
        translateQuery(builderJson, planningSchema, prepared.physical),
        {
          ...queryOptions,
          ...(!global && { openTransactionId }),
        },
        readSession,
      );
      if (settlementMetadata === true) return result as unknown as RowSettlement[];
      let settled =
        settlementMetadata === "with-rows"
          ? (result as unknown as SettledRows<(typeof result)[number]>)
          : undefined;
      const rows = settled ? settled.rows : result;
      const decoded = await finishComputedRead(
        query,
        builtQuery,
        outputTable,
        prepared,
        rows,
        async (physical) => {
          const result = await queryClient.call(
            client,
            translateQuery(builderJson, planningSchema, physical),
            { ...queryOptions, ...(!global && { openTransactionId }) },
            readSession,
          );
          if (settlementMetadata === "with-rows") {
            settled = result as unknown as SettledRows<(typeof result)[number]>;
            return settled.rows;
          }
          return result;
        },
        decrypt,
      );
      return settled ? { rows: decoded, settlements: settled.settlements } : decoded;
    }
    if (prepared.computedState)
      throw new Error(
        "Computed projections over encrypted equality queries are not supported by the draft SQLite restack yet",
      );
    const queryJson = equality?.json ?? builderJson;
    const translated = translateQuery(
      queryJson,
      planningSchema,
      equality ? undefined : prepared.physical,
    );
    const result = await queryClient.call(
      client,
      translated,
      {
        ...queryOptions,
        ...(!global && { openTransactionId }),
      },
      readSession,
    );
    if (settlementMetadata === true) return result as unknown as RowSettlement[];
    const settled =
      settlementMetadata === "with-rows"
        ? (result as unknown as SettledRows<(typeof result)[number]>)
        : undefined;
    const rows = settled ? settled.rows : result;
    if (equality && !(await equality.isCurrent())) throw new E2eeDataError("key-unavailable");
    const outputIncludes =
      outputTable !== builtQuery.table
        ? {}
        : equality
          ? builtQuery.includes
          : prepared.physical.includes;
    const decode = equality
      ? createRowTransformer<Record<string, unknown>>(
          outputSchema,
          outputTable,
          outputIncludes,
          [],
          query._columnTransformsByTable,
          false,
        )
      : prepared.decode;
    const rawRows = rows.map((row) => decode(row) as Record<string, unknown>);
    const decryptedRows = await decryptQueryRows(
      outputSchema,
      outputTable,
      rawRows,
      equality ? {} : outputIncludes,
      query._columnTransformsByTable,
      decrypt,
    );
    const resolvedRows = equality
      ? await decryptEqualityMatches(
          outputSchema,
          outputTable,
          decryptedRows,
          outputIncludes,
          query._columnTransformsByTable,
          decrypt,
          equality,
        )
      : decryptedRows;
    const decoded = resolvedRows.map(
      (row) => prepared.finish(row as T & Record<string, unknown>) as T,
    );
    return settled ? { rows: decoded, settlements: settled.settlements } : decoded;
  } finally {
    equality?.dispose();
  }
}

const transactionAdmission = new WeakMap<Db | Transaction, () => Promise<void>>();

/** @internal Lifecycle work must not wait for the enrolment it implements. */
export function serializableE2eeTransaction<TResult>(
  db: Db,
  callback: (tx: TransactionScope<"serializable">) => TResult | Promise<TResult>,
): Promise<SerializableWriteResult<Awaited<TResult>>> {
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
): Transaction<"serializable"> {
  transactionAdmission.set(db, prepare);
  try {
    return db.beginSerializableTransaction();
  } finally {
    transactionAdmission.delete(db);
  }
}

/** @internal Register before returning; the outer commit drains this work. */
export function prepareDbTransaction(
  transaction: Transaction<"serializable">,
  prepare: (scope: E2eeTransactionScope, io: TransactionPreparationIO) => Promise<void>,
): Promise<void> {
  if (transaction.kind !== "serializable")
    throw new Error("E2EE initialisation requires a serializable transaction");
  transaction.openTransactionId(); // Check admission before registering asynchronous work.
  const binding = getDbTxHandleBinding(transaction, "prepare");
  const { ownerClient, openTransactionId } = binding;
  return ownerClient.prepareTransaction(openTransactionId, async (io) => {
    const scope = preparedTransactionScope(binding, io);
    initializationScopes.set(scope, transaction);
    await prepare(scope, io);
  });
}

function preparedTransactionScope(
  binding: DbTransactionHandleBinding,
  io: TransactionPreparationIO,
): E2eeTransactionScope {
  const { openTransactionId, session, attribution } = binding;
  const scope: E2eeTransactionScope = {
    kind: "serializable",
    upsert(table, id, data, options) {
      binding.resolveClient(table._schema);
      const values = writeRecord("Upsert", table, data);
      io.upsertInternal(
        table._table,
        id,
        values,
        normalizeViewOptions(table._schema, table._table, options),
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
      const values = writeRecord("Insert", table, data);
      const row = io.insertInternal(
        table._table,
        values,
        normalizeSelectorOptions(table._schema, table._table, options),
        session,
        attribution,
        openTransactionId,
      );
      return transformOutputRow(table, transformRow(row, table._schema, table._table));
    },
    async allSettledForE2ee(query, propagation, snapshotAtBegin = propagation === "local-only") {
      binding.resolveClient(query._schema);
      const { rows, settlements } = await readTransactionRows(
        query,
        { tier: "remote" },
        "with-rows",
        binding,
        transactionHistoryQueries(io, propagation, snapshotAtBegin),
      );
      checkTransactionSettlements(
        rows,
        settlements,
        normalizeBuiltQuery(JSON.parse(query._build())).union !== undefined,
      );
      return { rows, settlements };
    },
  };
  initializationIO.set(scope, io);
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
  private localFirstRefreshOwned = false;
  private readonly localFirstRenewals = new AuthRenewalBackoff();
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private shutdownPromise: Promise<void> | null = null;
  private readonly shutdownAbort = new AbortController();
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
    acceptedEvidenceSubscriptions.set(this, (query, callbacks, options) =>
      this.subscribeDelta(query, callbacks, {
        ...lowerPublicDbQueryOptions(options),
        observeAcceptedEvidence: true,
      }),
    );
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
      subscribeDelta: (query, callback, options) =>
        inspectorAttachmentRequested
          ? this.subscribeFromInspectorAttachment(query, callback, options)
          : this.subscribeDelta(query, callback, lowerPublicDbQueryOptions(options)),
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
        return thisDb.shutdownPromise !== null;
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
    await this.connection.ensureReady("runtime");
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
  ): SubscriptionHandle {
    let inner: SubscriptionHandle | null = null;
    let cancelled = false;
    const ready = this.inspectorAttachmentOptions(query, options).then((prepared) => {
      if (cancelled) return;
      inner = this.subscribeDelta(query, callback, prepared);
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
    this.localFirstRefreshOwned = refresh;
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
      // The token lived to its scheduled refresh, so earlier renewals took.
      this.localFirstRenewals.reset();
      this.refreshLocalFirstToken();
    }, refreshMs);
  }

  private refreshLocalFirstToken(): void {
    if (!this._localFirstSecret || this.shutdownPromise) return;

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
    // A self-minted local-first token can always be reminted, so expiry is
    // recoverable: renew and let the auth update reconnect.
    if (reason === "expired" && this.localFirstRefreshOwned) this.renewExpiredLocalFirstToken();
  }

  private renewExpiredLocalFirstToken(): void {
    const delay = this.localFirstRenewals.next();
    if (delay === 0) {
      this.refreshLocalFirstToken();
      return;
    }
    if (this.localFirstRefreshTimer) clearTimeout(this.localFirstRefreshTimer);
    this.localFirstRefreshTimer = setTimeout(() => this.refreshLocalFirstToken(), delay);
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

  protected applyAuthUpdate(token: string | null, trustedReservedSession?: Session): boolean {
    this.runtimeSource.assertAuthUpdateAllowed();
    const jwtToken = token ?? undefined;
    const previousToken = this.config.jwtToken;
    const previousCookieSession = this.config.cookieSession;
    const previousTrustedReservedSession = getTrustedReservedSession(this.config);
    const credentialsChanged =
      previousToken !== jwtToken ||
      previousCookieSession !== undefined ||
      JSON.stringify(previousTrustedReservedSession) !== JSON.stringify(trustedReservedSession);
    if (!credentialsChanged && this.authStateStore.getState().error === undefined) return false;

    if (!this.authStateStore.validateJwtToken(jwtToken, trustedReservedSession)) return false;
    this.connection.updateAuth({ mode: "bearer", jwtToken, trustedReservedSession });
    this.publishAuthUpdate(jwtToken, trustedReservedSession);
    return true;
  }

  private publishAuthUpdate(jwtToken?: string, trustedReservedSession?: Session): void {
    const nextConfig = { ...this.config, jwtToken, cookieSession: undefined } as DbConfig;
    setTrustedReservedSession(nextConfig, trustedReservedSession);
    const nextInternalSession = resolveClientInternalSessionSync(nextConfig);
    this.config.jwtToken = jwtToken;
    this.config.cookieSession = undefined;
    setTrustedReservedSession(this.config, trustedReservedSession);
    this.publishAuthStateWithInternalSession(nextInternalSession, () => {
      this.authStateStore.applyJwtToken(jwtToken, trustedReservedSession);
    });
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
    earlyLease?: Promise<BrowserForegroundNodeLease>,
  ): Promise<Db> {
    const db = new Db(config, runtimeSource);
    const connection = new BrowserConnectionManager(db.dbForConnection(), earlyLease);
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

  protected async ensureReady(
    requirement: ConnectionRequirement,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.connection.ensureReady(requirement, signal ?? this.shutdownAbort.signal);
    this.assertOpen();
  }

  private wrapWriteWait<THandle extends WriteHandle<unknown, unknown>>(handle: THandle): THandle {
    notifyLocalWrite(this);
    return setWriteWaitReadiness(handle, (tier) =>
      this.ensureReady(tier === "global" ? "server" : "runtime"),
    );
  }

  protected getAccessContext(): DbAccessContext | null {
    // Ordinary Dbs use the client's session. BackendDb overrides this to supply
    // per-Db session and attribution without changing the shared client's identity.
    return null;
  }

  private preparedWrite<T>(
    kind: TransactionKind,
    client: JazzClient,
    write: (tx: Transaction) => T,
    initialiseMissingSpace = false,
  ): WriteResult<T> {
    const context = this.getAccessContext();
    const tx = this.createTransaction(kind, context);
    if (initialiseMissingSpace && kind === "mergeable") standaloneWriteRetries.set(tx, null);
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
    const result: WriteResult<T> = new WriteResult(
      value,
      committed
        .then((result) => result.txId)
        .catch(async (error) => {
          const retry = standaloneWriteRetries.get(tx);
          if (
            !retry ||
            !(error instanceof SpaceInitialisationRequired) ||
            (!retry.initialiseMissing && !(error instanceof ProvisionalSpaceRequiresExclusive))
          )
            throw error;
          if (retry.branch && !(error instanceof ProvisionalSpaceRequiresExclusive))
            throw new Error(
              "Encrypted branch first-use is unsupported; initialise the space with a root-target write first",
            );
          const exclusive = this.createTransaction("serializable", context);
          client.bindTransactionWait(
            getDbTxHandleBinding(exclusive, "retry").openTransactionId,
            result.txId,
          );
          return (await runInTransaction(exclusive, () => retry.prepare(exclusive), client)).txId;
        }),
      client,
    );
    client.bindTransactionWait(getDbTxHandleBinding(tx, "result").openTransactionId, result.txId);
    return this.wrapWriteWait(result);
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
      if (!this.authStateStore.validateJwtToken(token, reserved ?? undefined)) return token;
      this.connection.refreshAccountAuth(token, reserved ?? undefined);
      this.publishAuthUpdate(token, reserved ?? undefined);
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
    if (this.shutdownPromise) {
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
    if (this.shutdownPromise && !this.cancelSyncShutdown) {
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

  /** @internal Reconcile known spaces when the configured server becomes reachable. */
  onE2eeReconnect(listener: () => void): void {
    let connected = this.connection.remoteLinkState() === "connected";
    let interrupted = this.connection.remoteLinkState() === "unavailable";
    this.connection.onRemoteLinkStateChange((state) => {
      if (state === "connected") {
        const recovered = interrupted;
        connected = true;
        interrupted = false;
        if (recovered) listener();
      } else if (connected || state === "unavailable") {
        interrupted = true;
      }
    }, this.shutdownAbort.signal);
  }

  /** @internal Honour the initial worker connection state before an E2EE offline read. */
  async e2eeIsExplicitlyOffline(): Promise<boolean> {
    const initial = this.connection.initialExplicitOfflineState();
    if (initial) await initial;
    this.assertOpen();
    return this.connection.isExplicitlyOffline();
  }

  /** @internal Query exact durable-owner status after local host admission. */
  async e2eeInitializationTransactionStatus(ids: readonly ReservedTxId[]) {
    this.assertOpen();
    const schema = e2eeSchemaForDb(this);
    if (!schema) throw new Error("Initialization status requires an E2EE schema");
    const client = this.getClient(schema);
    await this.ensureReady("runtime");
    return client.initializationTransactionStatus(ids);
  }

  /** @internal Observations only: these reads do not prove complete history. */
  async observeE2eeHistory(queries: readonly QueryBuilder<{ id: string }>[]) {
    this.assertOpen();
    if (!queries.length || queries.some((query) => !query._table.startsWith("__e2ee_")))
      throw new Error("E2EE observations require metadata queries");
    for (const query of queries) this.getClient(query._schema);
    // A browser foreground must load the durable owner's local query inputs
    // before freezing its snapshot. These results may include pending writes:
    // discard them; only the accepted-only observation below validates history.
    // Keep ordinary propagation so the foreground can reach its durable worker.
    if (this.connection.requiresLocalObservationHydration)
      await Promise.all(queries.map((query) => this.all(query, { tier: "local-first" })));
    else await this.ensureReady("runtime");
    const tx = beginDbTransactionAfter(this, async () => {});
    const binding = getDbTxHandleBinding(tx, "query");
    const local: Pick<JazzClient, "queryInternal"> = {
      queryInternal: (query, options, session) =>
        binding.ownerClient.queryInternal(query, { ...options, tier: "local-only" }, session),
    };
    try {
      // Each paired read restricts the shared snapshot to accepted versions
      // before returning rows and their settlement evidence from one evaluation.
      // Ordinary transactions continue to include their local pending prefix.
      return await Promise.all(
        queries.map(async (query) => {
          const snapshot = await readTransactionRows(
            query,
            { tier: "remote" },
            "with-rows",
            binding,
            local,
          );
          checkTransactionSettlements(
            snapshot.rows,
            snapshot.settlements,
            normalizeBuiltQuery(JSON.parse(query._build())).union !== undefined,
          );
          return snapshot;
        }),
      );
    } finally {
      await tx.rollback();
    }
  }

  /** @internal Resolve the accepted table lineage for E2EE scope selection. */
  async tableIdentity<T, Init>(
    table: TableProxy<T, Init>,
    localOnly = false,
  ): Promise<string | null> {
    const client = this.getClient(table._schema);
    await this.ensureReady("runtime");
    const runtime = client.getRuntime();
    if (!runtime.tableIdentity)
      throw new Error("Runtime does not expose catalogue table identities");
    if (!localOnly) {
      const initialOfflineState = this.connection.initialExplicitOfflineState();
      if (initialOfflineState) await initialOfflineState;
      const offline = this.connection.isExplicitlyOffline();
      await this.ensureReady(offline ? "runtime" : "server");
      // Every online lookup needs fresh coverage, without returning row bodies
      // or waiting on preparation. Serving-side query and policy work still applies.
      // Offline identities remain local observations, not accepted membership.
      if (!offline) await client.coverCatalogue(table._table, this.getAccessContext()?.readSession);
    }
    const identity = await runtime.tableIdentity(table._table);
    this.assertOpen();
    return identity;
  }

  /** @internal Stable cached identity for sealed offline initialization, never fresh membership. */
  async authenticatedTableIdentity<T, Init>(table: TableProxy<T, Init>): Promise<string | null> {
    const client = this.getClient(table._schema);
    await this.ensureReady("runtime");
    if (!(await client.hasAuthenticatedCatalogue())) return null;
    // Owner publication does not admit a schema view. A zero-row local read
    // checks this requested schema without fetching metadata or refreshing authority.
    await client.queryInternal(
      translateQuery(JSON.stringify({ table: table._table, limit: 0 }), table._schema),
      { tier: "local-only" },
      this.getAccessContext()?.readSession,
    );
    return this.tableIdentity(table, true);
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
    assertWritableTable(table);
    scopeRecipients(table, options);
    const client = this.getClient(table._schema);
    const encryption = encryptedSchemas.get(table._schema);
    if (encryption?.scopes.has(table._table) || encryption?.tables.has(table._table)) {
      return this.preparedWrite(
        encryption.scopes.has(table._table) ? "serializable" : "mergeable",
        client,
        (tx) => tx.insert(table, data, options),
        true,
      );
    }
    let id = options?.id ?? (hasDependentTransforms(table) ? reserveRowId() : undefined);
    const transformedData = table._columnTransforms
      ? transformInputColumns(table, data, id ? { id } : undefined, true)
      : (data as Record<string, unknown>);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getAccessContext();
    const deferred = !!getOwnerPreparation(client)?.pending;
    if (deferred && !id) id = reserveRowId();
    const value = deferred ? immediateWriteRow(table, id!, values) : undefined;
    const inserted = runAfterPreparation(client, () =>
      client.insert(
        table._table,
        values,
        normalizeSelectorOptions(table._schema, table._table, id ? { ...options, id } : options),
        context?.writeSession,
        context?.attribution,
      ),
    );
    return this.wrapWriteWait(
      inserted instanceof Promise
        ? new WriteResult(
            value as T,
            inserted.then((result) => result.txId),
            client,
          )
        : inserted.mapValue((row) =>
            value !== undefined
              ? (value as T)
              : transformOutputRow<T>(table, transformRow(row, table._schema, table._table)),
          ),
    );
  }

  /**
   * Declare a root-view write plan, stage each source once, then publish it atomically.
   * No database transaction is open during declaration or source consumption.
   * Successful waits require authoritative acceptance, including key-history preconditions.
   */
  async streamingTransaction<TResult>(
    callback: (plan: StreamingWritePlan) => TResult | Promise<TResult>,
  ): Promise<SerializableWriteResult<Awaited<TResult>>> {
    const context = this.getAccessContext();
    type Declaration = {
      table: TableProxy<any, any>;
      id: string;
      operation: "insert" | "update" | "upsert";
      data: Record<string, unknown>;
      values: Record<string, Value>;
      options: InsertOptions & UpdateOptions;
      stream?: { column: string; source: StreamingValueSource };
      identifier?: string;
      existing?: boolean;
      tableId?: string;
      columnId?: string;
      staged?: StagedStreamingMutation;
    };
    const declarations: Declaration[] = [];
    let declaring = true;
    const declare = (
      operation: Declaration["operation"],
      table: TableProxy<any, any>,
      data: unknown,
      options: InsertOptions & UpdateOptions = {},
      streaming = false,
      id?: string,
    ): { id: string } => {
      if (!declaring) throw new Error("Streaming write plan declaration is closed");
      if (
        options.branch !== undefined ||
        options.base !== undefined ||
        table._schema[table._table]?.branchBy?.length
      )
        throw new Error("Streaming transactions require root-view tables and targets");
      if (
        options.updatedAt !== undefined &&
        (!Number.isSafeInteger(options.updatedAt) || options.updatedAt < 0)
      )
        throw new Error("updatedAt must be a non-negative safe integer");
      const snapshot = structuredClone(options);
      scopeRecipients(table, snapshot);
      const split = streaming ? splitStreamingMutation(table, data, operation) : undefined;
      const copied = structuredClone(
        split?.values ?? requireRecord(data, "Insert data must be an object"),
      );
      const metadata = encryptedSchemas.get(table._schema);
      const encryption = metadata?.tables.get(table._table);
      if (split && encryption) e2eeForDb(this);
      const values = structuredClone(
        toWriteRecord(
          transformInputColumns(table, copied),
          metadata?.logical ?? table._schema,
          table._table,
        ),
      );
      if (operation === "insert")
        assertRequiredRowColumnsPresent(
          (metadata?.logical ?? table._schema)[table._table]!.columns.filter(
            (column) => column.name !== split?.column,
          ),
          values,
          table._table,
        );
      const physical = { ...values };
      if (
        encryption &&
        table._schema[table._table]!.columns.some(
          (column) => column.name === permissionSpaceColumn,
        ) &&
        values[encryption.space]
      )
        physical[permissionSpaceColumn] = values[encryption.space]!;
      if (split) {
        const kind = table._schema[table._table]!.columns.find(
          (column) => column.name === split.column,
        )!.column_type.type;
        physical[split.column] =
          kind === "Text"
            ? { type: "Text", value: "" }
            : kind === "Json"
              ? { type: "Text", value: "{}" }
              : { type: "Bytea", value: new Uint8Array() };
      }
      for (const name of encryption?.columns ?? []) {
        if (operation === "insert" && name !== split?.column && !values[name]) {
          if (
            !metadata!.logical[table._table]!.columns.find((column) => column.name === name)!
              .nullable
          )
            throw new Error(`Missing encrypted column "${name}"`);
          values[name] = { type: "Null" };
        }
        if (values[name] || name === split?.column) {
          physical[name] = { type: "Bytea", value: new Uint8Array() };
          if (encryption!.indexes?.[name])
            physical[equalityIndexColumn(name)] = { type: "Bytea", value: new Uint8Array() };
        }
      }
      const client = this.getClient(table._schema);
      const rowId = formatUuid(
        parseUuid(id ?? snapshot.id ?? client.previewInsertInternal(table._table, physical).id),
      );
      if (operation === "insert") client.previewInsertInternal(table._table, physical, rowId);
      else normalizeViewOptions(table._schema, table._table, snapshot);
      declarations.push({
        table,
        id: rowId,
        operation,
        data: copied,
        values,
        options: { ...snapshot, id: rowId },
        ...(split && { stream: { column: split.column, source: split.source } }),
      });
      return { id: rowId };
    };
    let value: Awaited<TResult>;
    try {
      value = await callback({
        insert: (table, data, options) => declare("insert", table, data, options),
        insertStreaming: (table, data, options) => declare("insert", table, data, options, true),
        updateStreaming: (table, id, data, options) =>
          declare("update", table, data, options, true, id),
        upsertStreaming: (table, id, data, options) =>
          declare("upsert", table, data, options, true, id),
      });
    } finally {
      declaring = false;
    }
    const streams = declarations.filter((entry) => entry.stream);
    if (!streams.length) throw new Error("Streaming transactions require at least one stream");
    type Group = {
      scope: TableProxy<any, any>;
      identifier: string;
      initial?: Declaration;
      entries: Declaration[];
      plan?: StreamingSpacePlan;
    };
    const groups: Group[] = [];
    const priorDeclarations = new Map<Declaration, Declaration>();
    const latestRows = new Map<WasmSchema, Map<string, Declaration>>();
    const initialScopes = new Map<WasmSchema, Map<string, Declaration>>();
    const groupsBySchema = new Map<WasmSchema, Map<string, Group>>();
    for (const entry of declarations) {
      const schema = entry.table._schema;
      const key = `${entry.table._table}\0${entry.id}`;
      let rows = latestRows.get(schema);
      if (!rows) latestRows.set(schema, (rows = new Map()));
      const prior = rows.get(key);
      if (prior) priorDeclarations.set(entry, prior);
      rows.set(key, entry);
      if (!entry.stream) {
        let scopes = initialScopes.get(schema);
        if (!scopes) initialScopes.set(schema, (scopes = new Map()));
        if (!scopes.has(key)) scopes.set(key, entry);
      }
    }
    const staged: StagedStreamingMutation[] = [];
    try {
      for (const entry of streams) {
        const { table, stream } = entry;
        const encryption = encryptedSchemas.get(table._schema)?.tables.get(table._table);
        const retainedIdentity = encryption ? await this.authenticatedTableIdentity(table) : null;
        entry.tableId = retainedIdentity ?? (await this.tableIdentity(table)) ?? undefined;
        entry.columnId =
          (await this.columnIdentity(table, stream!.column, retainedIdentity !== null)) ??
          undefined;
        if (!entry.tableId || !entry.columnId)
          throw new Error("Streaming writes require accepted table and column identities");
        const prior = priorDeclarations.get(entry);
        if (entry.operation !== "insert") {
          const row = (await this.one(
            new TypedTableQueryBuilder<
              AnyTableMeta & { row: { id: string } & Record<string, unknown> }
            >(table._table, table._schema)
              .includeDeleted()
              .where({ id: entry.id })
              .select(encryption?.space ?? "id"),
            { tier: encryption ? "local-first" : "remote" },
          )) as Record<string, unknown> | null;
          entry.existing = row !== null;
          if (row && encryption) {
            if (typeof row[encryption.space] !== "string")
              throw new Error("Encrypted mutation cannot resolve the stored space");
            entry.identifier = row[encryption.space] as string;
          } else if (!row && !prior && entry.operation === "update") {
            throw new Error("Streaming update requires an existing row");
          }
        }
        if (entry.operation === "upsert" && !entry.existing && !prior)
          assertRequiredRowColumnsPresent(
            (encryptedSchemas.get(table._schema)?.logical ?? table._schema)[
              table._table
            ]!.columns.filter((column) => column.name !== stream!.column),
            entry.values,
            table._table,
          );
        if (!encryption) continue;
        const requested = entry.values[encryption.space];
        if (prior) {
          const previousScope = prior.values[encryption.space];
          const identifier =
            prior.identifier ?? (previousScope?.type === "Uuid" ? previousScope.value : undefined);
          if (
            entry.identifier &&
            identifier &&
            entry.identifier.toLowerCase() !== identifier.toLowerCase()
          )
            throw new Error("The encryption space of a row is immutable");
          entry.identifier ??= identifier;
        }
        if (!entry.identifier && requested?.type === "Uuid") entry.identifier = requested.value;
        if (!entry.identifier) throw new Error("Encrypted writes require a space identifier");
        if (
          requested &&
          (requested.type !== "Uuid" ||
            requested.value.toLowerCase() !== entry.identifier.toLowerCase())
        )
          throw new Error("The encryption space of a row is immutable");
        entry.identifier = entry.identifier.toLowerCase();
        if (entry.operation === "upsert" && !entry.existing && !prior) {
          const metadata = encryptedSchemas.get(table._schema)!;
          for (const name of encryption.columns) {
            if (name === stream!.column || entry.values[name]) continue;
            if (
              !metadata.logical[table._table]!.columns.find((column) => column.name === name)!
                .nullable
            )
              throw new Error(`Missing encrypted column "${name}"`);
            entry.values[name] = { type: "Null" };
          }
        }
        let scopeGroups = groupsBySchema.get(table._schema);
        if (!scopeGroups) groupsBySchema.set(table._schema, (scopeGroups = new Map()));
        const groupKey = `${encryption.scope}\0${entry.identifier}`;
        let group = scopeGroups.get(groupKey);
        if (!group) {
          group = {
            scope: new TypedTableQueryBuilder(encryption.scope, table._schema),
            identifier: entry.identifier,
            initial: initialScopes.get(table._schema)?.get(groupKey),
            entries: [],
          };
          groups.push(group);
          scopeGroups.set(groupKey, group);
        }
        group.entries.push(entry);
      }
      const stage = async (
        entry: Declaration,
        source: StreamingValueSource,
        physical: Record<string, Value>,
      ) => {
        entry.staged = await this.getClient(entry.table._schema).stageStreamingMutation(
          entry.operation,
          entry.table._table,
          physical,
          entry.stream!.column,
          source,
          entry.operation === "insert"
            ? normalizeSelectorOptions(entry.table._schema, entry.table._table, entry.options)
            : normalizeViewOptions(entry.table._schema, entry.table._table, entry.options),
          context?.writeSession,
          context?.attribution,
          entry.id,
        );
        staged.push(entry.staged);
      };
      const preparedSources: Array<() => Promise<void>> = [];
      // Nest borrowed-key callbacks so every space is eligible before the first source is pulled.
      const prepareGroup = async (index: number): Promise<void> => {
        const group = groups[index];
        if (!group) {
          for (const consume of preparedSources) await consume();
          return;
        }
        const prepared = await prepareStreamingSpace(
          this,
          group.scope,
          group.identifier,
          {
            newScope: !!group.initial,
            initialRecipients: group.initial?.options.initialRecipients,
            requireAccepted: group.entries.some((entry) =>
              usesEncryptionSpacePolicy(entry.table._schema, entry.table._table),
            ),
          },
          async (secret, root) => {
            for (const entry of group.entries) {
              const encryption = encryptedSchemas
                .get(entry.table._schema)!
                .tables.get(entry.table._table)!;
              const physical = { ...entry.values };
              if (
                entry.table._schema[entry.table._table]!.columns.some(
                  (column) => column.name === permissionSpaceColumn,
                )
              )
                physical[permissionSpaceColumn] = { type: "Uuid", value: group.identifier };
              for (const name of encryption.columns) {
                if (name === entry.stream!.column || !entry.values[name]) continue;
                if (encryption.indexes?.[name])
                  physical[equalityIndexColumn(name)] = {
                    type: "Bytea",
                    value: await equalityToken(
                      this,
                      entry.table,
                      name,
                      entry.values[name]!,
                      secret,
                      root,
                    ),
                  };
                physical[name] = {
                  type: "Bytea",
                  value: await encryptCell(
                    this,
                    entry.table,
                    entry.id,
                    name,
                    entry.values[name]!,
                    secret,
                    root,
                  ),
                };
              }
              const source = await encryptStreamingCell(
                this,
                entry.table,
                entry.id,
                entry.stream!.column,
                secret,
                root,
                streamingBytes(entry.stream!.source),
              );
              preparedSources.push(() => stage(entry, source, physical));
            }
            await prepareGroup(index + 1);
          },
        );
        group.plan = prepared.plan;
      };
      await prepareGroup(0);
      for (const entry of streams)
        if (!entry.staged) await stage(entry, entry.stream!.source, entry.values);
      const transaction = this.createTransaction("serializable", context);
      const seeds = new Map<WasmSchema, Map<string, InitialSpaceSeed>>();
      for (const group of groups) {
        if (!group.plan!.initialSeed) continue;
        let schemaSeeds = seeds.get(group.scope._schema);
        if (!schemaSeeds) seeds.set(group.scope._schema, (schemaSeeds = new Map()));
        schemaSeeds.set(`${group.scope._table}:${group.identifier}`, group.plan!.initialSeed);
      }
      streamingInitialSeeds.set(transaction, seeds);
      return await runInTransaction(
        transaction,
        async () => {
          await prepareDbTransaction(transaction, async (tx) => {
            for (const group of groups) await group.plan!.validate(tx);
            for (const entry of streams) {
              const encryption = encryptedSchemas
                .get(entry.table._schema)
                ?.tables.get(entry.table._table);
              const retainedIdentity = encryption
                ? await this.authenticatedTableIdentity(entry.table)
                : null;
              if (
                (retainedIdentity ?? (await this.tableIdentity(entry.table))) !== entry.tableId ||
                (await this.columnIdentity(
                  entry.table,
                  entry.stream!.column,
                  retainedIdentity !== null,
                )) !== entry.columnId
              )
                throw new Error("Streaming upload schema identity changed");
              if (!encryption || entry.operation === "insert") continue;
              const row = (await tx.one(
                new TypedTableQueryBuilder<
                  AnyTableMeta & { row: { id: string } & Record<string, unknown> }
                >(entry.table._table, entry.table._schema)
                  .includeDeleted()
                  .where({ id: entry.id })
                  .select(encryption.space),
                { tier: "local-first" },
              )) as Record<string, unknown> | null;
              if (
                !!row !== !!entry.existing ||
                (row &&
                  (typeof row[encryption.space] !== "string" ||
                    (row[encryption.space] as string).toLowerCase() !== entry.identifier))
              )
                throw new Error("Streaming upload target changed");
            }
          });
          for (const group of groups) {
            if (group.initial || !group.plan!.initialSeed) continue;
            await prepareInitialSpaceForTransaction(
              this,
              transaction,
              group.scope,
              group.identifier,
              streamingInitialRetainers.get(transaction)!,
              undefined,
              group.plan!.initialSeed,
            );
          }
          for (const entry of declarations) {
            if (!entry.stream) transaction.insert(entry.table, entry.data, entry.options);
            else
              await prepareDbTransaction(transaction, async (_tx, io) => {
                if (entry.operation === "insert")
                  await io.recordInitializationInsertAbsence(entry.table._table, entry.id);
                await io.attachStreamingMutation(entry.staged!);
              });
          }
          return value;
        },
        () => getDbTxHandleBinding(transaction, "streamingTransaction").ownerClient,
      );
    } finally {
      // Consumed capabilities cannot evict committed/rejected-version payloads.
      await Promise.all(
        staged.map(async (entry) => {
          try {
            await entry.abort();
          } catch {
            /* Preserve the publication/source failure. */
          }
        }),
      );
      for (const group of groups) group.plan?.dispose();
    }
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
    assertWritableTable(table);
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      const result = await this.streamingTransaction((plan) =>
        plan.insertStreaming(table, data, options),
      );
      return new WriteHandle(result.txId, this.getClient(table._schema), result.value);
    }
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "insert");
    rejectIncompleteDependencies(table, ordinaryData, [column]);
    notifyLocalWrite(this);
    const id =
      options?.id ??
      (missingWriteDependencies(table, ordinaryData).includes("id") ? reserveRowId() : undefined);
    return runAfterPreparation(client, async () => {
      const transformedData = transformInputColumns(table, ordinaryData, id ? { id } : {}, true);
      const values = toWriteRecordForOperation(
        "Insert",
        transformedData,
        table._schema,
        table._table,
      );
      const context = this.getAccessContext();
      const branch = deriveStreamingInsertBranch(table, ordinaryData);
      return client.insertStreaming(
        table._table,
        values,
        column,
        source,
        normalizeSelectorOptions(
          table._schema,
          table._table,
          branch
            ? { ...options, ...(id ? { id } : {}), branch }
            : id
              ? { ...options, id }
              : options,
        ),
        context?.writeSession,
        context?.attribution,
      );
    });
  }

  async updateStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<WriteHandle<{ id: string }>> {
    assertWritableTable(table);
    const encryption = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (
      encryption &&
      typeof data === "object" &&
      data !== null &&
      (Object.hasOwn(data, encryption.space) ||
        encryption.columns.some((name) => Object.hasOwn(data, name)))
    ) {
      const result = await this.streamingTransaction((plan) =>
        plan.updateStreaming(table, id, data, options),
      );
      return new WriteHandle(result.txId, this.getClient(table._schema), result.value);
    }
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "update");
    rejectIncompleteDependencies(table, ordinaryData, [column]);
    notifyLocalWrite(this);
    const context = this.getAccessContext();
    return runAfterPreparation(client, async () => {
      const transformedData = await prepareInputColumns(
        table,
        ordinaryData,
        "Update",
        id,
        client,
        context?.readSession,
        undefined,
        normalizeViewOptions(table._schema, table._table, options),
      );
      const values = toWriteRecordForOperation(
        "Update",
        transformedData,
        table._schema,
        table._table,
      );
      return client.updateStreaming(
        table._table,
        id,
        values,
        column,
        source,
        normalizeViewOptions(table._schema, table._table, options),
        context?.writeSession,
        context?.attribution,
      );
    });
  }

  async upsertStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<WriteHandle<{ id: string }>> {
    assertWritableTable(table);
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      const result = await this.streamingTransaction((plan) =>
        plan.upsertStreaming(table, id, data, options),
      );
      return new WriteHandle(result.txId, this.getClient(table._schema), result.value);
    }
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data, "upsert");
    rejectIncompleteDependencies(table, ordinaryData, [column]);
    notifyLocalWrite(this);
    const context = this.getAccessContext();
    return runAfterPreparation(client, async () => {
      const transformedData = await prepareInputColumns(
        table,
        ordinaryData,
        "Upsert",
        id,
        client,
        context?.readSession,
        undefined,
        normalizeViewOptions(table._schema, table._table, options),
      );
      const values = toWriteRecordForOperation(
        "Upsert",
        transformedData,
        table._schema,
        table._table,
      );
      return client.upsertStreaming(
        table._table,
        id,
        values,
        column,
        source,
        normalizeViewOptions(table._schema, table._table, options),
        context?.writeSession,
        context?.attribution,
      );
    });
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
    assertWritableTable(table);
    const client = this.getClient(table._schema);
    if (encryptedSchemas.get(table._schema)?.tables.has(table._table)) {
      return this.preparedWrite("mergeable", client, (tx) => tx.restore(table, id, data, options));
    }
    const transformedData = table._columnTransforms
      ? transformInputColumns(table, data, { id }, true)
      : (data as Record<string, unknown>);
    const values = toWriteRecordForOperation(
      "Restore",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getAccessContext();
    const deferred = !!getOwnerPreparation(client)?.pending;
    const value = deferred ? immediateWriteRow(table, id, values) : undefined;
    const restored = runAfterPreparation(client, () =>
      client.restore(
        table._table,
        id,
        values,
        normalizeSelectorOptions(table._schema, table._table, options),
        context?.writeSession,
        context?.attribution,
      ),
    );
    return this.wrapWriteWait(
      restored instanceof Promise
        ? new WriteResult(
            value as T,
            restored.then((result) => result.txId),
            client,
          )
        : restored.mapValue((row) =>
            transformOutputRow<T>(table, transformRow(row, table._schema, table._table)),
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
    assertWritableTable(table);
    scopeRecipients(table, options);
    const client = this.getClient(table._schema);
    const encryption = encryptedSchemas.get(table._schema);
    if (encryption?.scopes.has(table._table) || encryption?.tables.has(table._table)) {
      return this.preparedWrite(
        encryption.scopes.has(table._table) ? "serializable" : "mergeable",
        client,
        (tx) => tx.upsert(table, id, data, options),
        true,
      );
    }
    return this.prepareMutation(table, id, data as Record<string, unknown>, "Upsert", options);
  }

  private prepareMutation(
    table: TableProxy<any, any>,
    id: string,
    data: Record<string, unknown>,
    operation: "Update" | "Upsert",
    options?: UpdateOptions,
    stage?: (values: ReturnType<typeof toWriteRecord>) => WriteHandle,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const missing = missingWriteDependencies(table, data, id);
    if (getOwnerPreparation(client)?.pending || missing.length) data = { ...data };
    const context = this.getAccessContext();
    const normalized = normalizeViewOptions(table._schema, table._table, options);
    const result = runAfterPreparation(client, () => {
      const submit = (record: Record<string, unknown>, transaction?: OpenTransactionId) => {
        const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
        if (stage) return stage(values);
        if (transaction) {
          if (operation === "Upsert")
            client.upsertInternal(
              table._table,
              id,
              values,
              normalized,
              context?.writeSession,
              context?.attribution,
              transaction,
            );
          else
            client.updateInternal(
              table._table,
              id,
              values,
              normalized?.updatedAt,
              context?.writeSession,
              context?.attribution,
              transaction,
              normalized?.branch,
            );
          return client.commitTransaction(transaction);
        }
        return operation === "Upsert"
          ? client.upsert(
              table._table,
              id,
              values,
              normalized,
              context?.writeSession,
              context?.attribution,
            )
          : client.update(
              table._table,
              id,
              values,
              normalized,
              context?.writeSession,
              context?.attribution,
            );
      };
      if (missing.length === 0)
        return submit(table._columnTransforms ? transformInputColumns(table, data, { id }) : data);
      const transaction = client.beginTransaction(
        "mergeable",
        context?.writeSession,
        context?.attribution,
      );
      // An empty mergeable transaction has no staged overlay or read set. Native branch reads
      // currently require the ordinary scoped path; explicit transactions still fail closed.
      return Promise.resolve(
        prepareInputColumns(
          table,
          data,
          operation,
          id,
          client,
          context?.readSession,
          normalized?.branch ? undefined : transaction,
          normalized,
          missing,
        ),
      )
        .then(async (record) => {
          // Partial edits already own their native transaction; mergeable reads add no read precondition.
          if (stage) {
            await client.rollbackTransaction(transaction);
            return submit(record);
          }
          return submit(record, transaction);
        })
        .catch(async (error) => {
          await client.rollbackTransaction(transaction).catch(() => {});
          throw error;
        });
    });
    return this.wrapWriteWait(
      result instanceof Promise
        ? new WriteHandle(
            result.then((handle) => handle.txId),
            client,
          )
        : result,
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
    assertWritableTable(table);
    const client = this.getClient(table._schema);
    const diffs = options?.applyDiffs;
    const encrypted = encryptedSchemas.get(table._schema)?.tables.get(table._table);
    if (
      encrypted &&
      (Object.hasOwn(data, encrypted.space) ||
        encrypted.columns.some((name) => Object.hasOwn(data, name)))
    ) {
      if (diffs !== undefined) throw new Error("Encrypted updates do not support applyDiffs");
      return this.preparedWrite(
        "mergeable",
        client,
        (tx) => tx.update(table, id, data, options),
        true,
      );
    }
    const descriptors = lowerApplyDiffs(table, data, options?.applyDiffs);
    rejectIncompleteDependencies(table, data, Object.keys(options?.applyDiffs ?? {}));
    const context = this.getAccessContext();
    if (descriptors.length > 0) {
      return this.prepareMutation(table, id, data, "Update", options, (updates) =>
        client.updateLargeValues(
          table._table,
          id,
          updates,
          descriptors,
          normalizeViewOptions(table._schema, table._table, options),
          context?.writeSession,
          context?.attribution,
        ),
      );
    }
    return this.prepareMutation(table, id, data, "Update", options);
  }

  /**
   * Delete a row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string, options?: DeleteOptions): WriteHandle {
    assertWritableTable(table);
    const client = this.getClient(table._schema);
    const context = this.getAccessContext();
    const result = runAfterPreparation(client, () =>
      client.delete(
        table._table,
        id,
        normalizeViewOptions(table._schema, table._table, options),
        context?.writeSession,
        context?.attribution,
      ),
    );
    return this.wrapWriteWait(
      result instanceof Promise
        ? new WriteHandle(
            result.then((handle) => handle.txId),
            client,
          )
        : result,
    );
  }

  /** Request authoritative permission advice for inserting a row. */
  async canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const values = writeRecord("Insert", table, data);
    const context = this.getAccessContext();
    return client.requestInsertPermissionAdvice(table._table, values, context?.writeSession);
  }

  /** Request authoritative permission advice for reading a row. */
  async canRead<T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const context = this.getAccessContext();
    return client.requestReadPermissionAdvice(table._table, id, context?.readSession);
  }

  /** Request authoritative permission advice for updating a row. */
  async canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const updates = writeRecord("Update", table, data);
    const context = this.getAccessContext();
    return client.requestUpdatePermissionAdvice(table._table, id, updates, context?.writeSession);
  }

  /** Request authoritative permission advice for deleting a row. */
  async canDelete<T, Init>(table: TableProxy<T, Init>, id: string): Promise<PermissionAdvice> {
    const client = this.getClient(table._schema);
    const context = this.getAccessContext();
    return client.requestDeletePermissionAdvice(table._table, id, context?.writeSession);
  }

  private createTransaction<TKind extends TransactionKind>(
    kind: TKind,
    context = this.getAccessContext(),
  ): Transaction<TKind> {
    this.assertOpen();
    notifyLocalWrite(this);
    const configuredSchema = e2eeSchemaForDb(this);
    const ownerClient =
      this.getCurrentClient() ?? (configuredSchema ? this.getClient(configuredSchema) : null);
    if (kind === "serializable" && !ownerClient) {
      throw new Error(
        "Cannot begin a serializable transaction before the JazzClient has been created. Run a query or mutation first.",
      );
    }
    const e2ee =
      configuredSchema && encryptedSchemas.has(configuredSchema) ? { db: this } : undefined;
    const prerequisite = transactionAdmission.get(this);
    if (prerequisite && ownerClient) {
      const transaction = withTransactionAdmission(
        ownerClient,
        prerequisite,
        () => new Transaction(kind, (schema) => this.getClient(schema), context, ownerClient, e2ee),
      );
      // Owner preparation can postpone opening beyond the synchronous admission scope.
      transactionAdmission.set(transaction, prerequisite);
      return transaction;
    }
    return new Transaction(
      kind,
      (schema) => this.getClient(schema),
      context,
      ownerClient ?? undefined,
      e2ee,
    );
  }

  /**
   * Capture this database's local read frontier for a frozen branch base.
   *
   * Includes earlier locally committed writes, including pending server
   * acceptance, and excludes uncommitted transaction changes and later writes.
   * Works offline after the first query or mutation has attached a schema.
   * The reference does not retain history or grant permission. Persist it with
   * `serializeSnapshotRef`; restoring it requires the same dataset and enough
   * retained history to answer the requested read.
   */
  async captureSnapshot(): Promise<SnapshotRef> {
    this.assertOpen();
    const client = this.getCurrentClient();
    if (!client) {
      throw new Error(
        "Cannot capture a snapshot before a schema is attached. Run a query or mutation first.",
      );
    }
    // Admit capture now, before later writes, including writes whose async
    // transforms share this owner's preparation queue.
    return runAfterPreparation(client, () => client.captureSnapshot());
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
   * Begin a serializable transaction for writes that need serializable validation by the authority.
   *
   * Use {@link Transaction.commit} to commit the transaction.
   *
   * Prefer using {@link Db.serializableTransaction} when an explicit commit is not required.
   */
  beginSerializableTransaction(): Transaction<"serializable"> {
    return this.createTransaction("serializable");
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
   * Run {@link callback} inside a serializable transaction and commit it once the callback returns.
   *
   * @returns a write result containing the result of the callback
   */
  serializableTransaction<TResult>(
    callback: (tx: TransactionScope<"serializable">) => TResult | Promise<TResult>,
  ): Promise<SerializableWriteResult<Awaited<TResult>>> {
    const transaction = this.beginSerializableTransaction();
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
    const preparation = getOwnerPreparation(client);
    if (preparation?.pending) await preparation.drain();
    // A newly attached browser-worker follower learns the namespace-wide
    // connection state during its init handshake. The core read gate needs
    // that state before it decides whether the first load may wait.
    const mayWaitForServer = (options?.firstLoadRemoteWaitMs ?? 0) > 0;
    const initialOfflineState = mayWaitForServer
      ? this.connection.initialExplicitOfflineState()
      : null;
    if (initialOfflineState) await initialOfflineState;
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    const prepared = prepareTransformRead(query, builtQuery, outputTable);
    const queryOptions = normalizeQueryOptions(query._schema, builtQuery.table, options);
    const equality = encryptedSchemas.has(query._schema)
      ? await prepareEqualityQuery(this, query, builderJson)
      : undefined;
    try {
      if (!equality) {
        const wasmQuery = translateQuery(builderJson, planningSchema, prepared.physical);
        const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
        const context = this.getAccessContext();
        const effectiveTier = resolveEffectiveQueryExecutionOptions(this.config, queryOptions).tier;
        await this.ensureReady(effectiveTier === "remote" ? "server" : "runtime");
        const rows =
          context || usesRelationTraversal
            ? await client.queryInternal(wasmQuery, queryOptions, context?.readSession)
            : await client.queryInternal(wasmQuery, queryOptions);
        return finishComputedRead(
          query,
          builtQuery,
          outputTable,
          prepared,
          rows,
          (physical) =>
            client.queryInternal(
              translateQuery(builderJson, planningSchema, physical),
              queryOptions,
              context?.readSession,
            ),
          (table, encryptedRows) => decryptCellRows(this, query._schema, table, encryptedRows),
        );
      }
      if (prepared.computedState)
        throw new Error(
          "Computed projections over encrypted equality queries are not supported by the draft SQLite restack yet",
        );
      const wasmQuery = translateQuery(
        equality?.json ?? builderJson,
        planningSchema,
        equality ? undefined : prepared.physical,
      );
      const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
      const context = this.getAccessContext();
      const effectiveTier = resolveEffectiveQueryExecutionOptions(this.config, queryOptions).tier;
      await this.ensureReady(effectiveTier === "remote" ? "server" : "runtime");
      const rows =
        context || usesRelationTraversal
          ? await client.queryInternal(wasmQuery, queryOptions, context?.readSession)
          : await client.queryInternal(wasmQuery, queryOptions);
      const outputIncludes =
        outputTable !== builtQuery.table
          ? {}
          : equality
            ? builtQuery.includes
            : prepared.physical.includes;
      const decode = equality
        ? createRowTransformer<Record<string, unknown>>(
            outputSchema,
            outputTable,
            outputIncludes,
            [],
            query._columnTransformsByTable,
            false,
          )
        : prepared.decode;
      let candidateEpochs: ReadonlySet<string> | undefined;
      const rawRows = rows.map((row) => decode(row) as Record<string, unknown>);
      const decryptedRows = await decryptQueryRows(
        query._schema,
        outputTable,
        rawRows,
        equality ? {} : outputIncludes,
        query._columnTransformsByTable,
        (table, encryptedRows) =>
          decryptCellRows(
            this,
            query._schema,
            table,
            encryptedRows,
            undefined,
            equality
              ? (identifier, epochs) => {
                  if (table === builtQuery.table && identifier === equality.space.identifier)
                    candidateEpochs = epochs;
                }
              : undefined,
          ),
      );
      if (
        equality &&
        !(candidateEpochs ? equality.matchesEpochs(candidateEpochs) : await equality.isCurrent())
      ) {
        if (equalityRetries >= 2) throw new E2eeDataError("key-unavailable");
        return this.allInternal(query, options, equalityRetries + 1);
      }
      const resolvedRows = equality
        ? await decryptEqualityMatches(
            query._schema,
            outputTable,
            decryptedRows,
            outputIncludes,
            query._columnTransformsByTable,
            (table, encryptedRows) => decryptCellRows(this, query._schema, table, encryptedRows),
            equality,
          )
        : decryptedRows;
      return resolvedRows.map((row) => prepared.finish(row as T & Record<string, unknown>) as T);
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
    const outputTable = resolveQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = requireSchemaWithTable(query._schema, outputTable);
    let prepared = prepareTransformRead(query, builtQuery, outputTable);
    const outputIncludes = outputTable !== builtQuery.table ? {} : prepared.physical.includes;
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
    let wasmQuery = encryptedEquality
      ? builderJson
      : translateQuery(builderJson, planningSchema, prepared.physical);
    const decodeRow = encryptedEquality
      ? createRowTransformer<Record<string, unknown>>(
          outputSchema,
          outputTable,
          builtQuery.includes,
          [],
          query._columnTransformsByTable,
          false,
        )
      : prepared.decode;
    const finishRow = (row: Record<string, unknown>): T =>
      prepared.finish(row as T & Record<string, unknown>) as T;
    const encrypted =
      encryptedEquality || hasEncryptedResults(query._schema, outputTable, outputIncludes);
    if (encrypted && prepared.computedState)
      throw new Error(
        "Computed projections over encrypted subscriptions are not supported by the draft SQLite restack yet",
      );
    let pendingDecryption = Promise.resolve();
    let encryptedFrontier = 0;
    let deliveryReady = initialReadiness === null;
    const bufferedDeltas: SubscriptionDelta<T>[] = [];

    const queryOptions = normalizeQueryOptions(query._schema, builtQuery.table, options);
    const readSession = this.getAccessContext()?.readSession ?? session;
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
      subscription.terminalError ??= toError(error);
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
        try {
          deliver(manager.handleDelta(delta, prepared.transform));
        } catch (error) {
          if (
            !(error instanceof MissingComputedDependency) ||
            !prepared.computedState ||
            prepared.computedState.version === prepared.version
          )
            throw error;
          prepared = prepareTransformRead(query, builtQuery, outputTable, prepared.computedState);
          wasmQuery = translateQuery(builderJson, planningSchema, prepared.physical);
          retireNativeSubscription(activeSubscription);
          const replacement = createSubscriptionGeneration();
          manager.clear();
          bufferedDeltas.length = 0;
          this.unregisterActiveQuerySubscriptionTrace(traceId);
          traceId = this.registerActiveQuerySubscriptionTrace(
            wasmQuery,
            builtQuery.table,
            queryOptions,
          );
          // Native initial snapshots may arrive synchronously. Defer the next
          // generation to avoid recursive installation as new inputs are found.
          queueMicrotask(() => {
            if (!this.shutdownPromise) startNativeSubscription(replacement);
          });
        }
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
          readSession,
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
    let traceId = this.registerActiveQuerySubscriptionTrace(
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
                  // The initial native snapshot validates its own current authority.
                  // Before it arrives there is no result for a dependency to refresh.
                  if (!subscription.hasSnapshot) return;
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
            { tier: options?.tier === "local-only" ? "local-first" : (options?.tier ?? "remote") },
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
      startNativeSubscription(replacement, queryOptions);
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
        if (unsubscribed || terminalized || activeSubscription === null || this.shutdownPromise) {
          return;
        }
        deliveryReady = true;
        for (const delta of bufferedDeltas.splice(0)) {
          if (unsubscribed || terminalized || activeSubscription === null) return;
          deliver(delta);
        }
      })
      .catch((error: unknown) => {
        if (unsubscribed || terminalized || activeSubscription === null || this.shutdownPromise) {
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
    const requirement = queryOptions.tier === "remote" ? "server" : "runtime";
    if (this.connection.shouldDeferSubscriptionStart(requirement)) {
      // The worker can only classify the initial authority-tier snapshot as
      // settled after its own server transport is attached. Delay native
      // subscription creation until that topology is ready; the native stream
      // then owns the settled-snapshot gate and remains the sole data source.
      void this.ensureReady(requirement, readyAbort.signal)
        .then(() => startNativeSubscription(initialSubscription))
        .catch((error: unknown) => {
          if (unsubscribed || readyAbort.signal.aborted || this.shutdownPromise) return;
          terminalizeSubscription(initialSubscription, error);
        });
    } else {
      startNativeSubscription(initialSubscription);
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
  shutdown(options: ShutdownOptions = {}): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;
    // Publish the promise before teardown can synchronously call back into Db.
    let resolveShutdown!: (result: Promise<void>) => void;
    const shutdown = new Promise<void>((resolve) => {
      resolveShutdown = resolve;
    });
    this.shutdownPromise = shutdown;
    resolveShutdown(this.runShutdown(options));
    return shutdown;
  }

  private cancelSyncShutdown: (() => void) | undefined;

  /** @internal Credential invalidation must interrupt a graceful sync wait. */
  abortGracefulShutdown(): void {
    this.cancelSyncShutdown?.();
  }

  protected assertOpen(): void {
    if (this.shutdownPromise) {
      throw new Error("Cannot operate on a Db that is shutting down or closed.");
    }
  }

  private readonly shutdownListeners = new Set<() => void>();

  /** @internal Dispose account refresh and invalidation observers with the context. */
  onShutdown(listener: () => void): () => void {
    if (this.shutdownPromise) listener();
    else this.shutdownListeners.add(listener);
    return () => this.shutdownListeners.delete(listener);
  }

  private async runShutdown(options: ShutdownOptions): Promise<void> {
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
        this.shutdownPromise = null;
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

    const resolvedOptions = resolveEffectiveQueryExecutionOptions(this.config, options);
    // Inspector-only reads must not recursively appear in the inspector's
    // own subscription list. Public local-first still propagates and is listed.
    if (resolvedOptions.tier === "local-only") return null;
    const payload = this.parseRuntimeQueryTracePayload(queryJson);
    const traceId = `sub-${this.nextActiveQuerySubscriptionTraceId++}`;

    this.activeQuerySubscriptionTraces.set(traceId, {
      id: traceId,
      query: queryJson,
      table: payload.table,
      branches: payload.branches,
      tier: resolvedOptions.tier,
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

/**
 * @internal Start the persistent browser worker for an account context before
 * its runtime and credential are ready. An account's browser root and worker
 * are named by its account id and registry alone, so the SharedWorker can boot
 * and compile its WASM while the page loads its own runtime and resolves auth.
 * Returns undefined when the root depends on a credential or no worker is used.
 */
export function startBrowserWorkerLease(
  config: DbConfig,
  runtimeSource: RuntimeSource<any>,
): Promise<BrowserForegroundNodeLease> | undefined {
  if (
    !runtimeSource.supportsBrowserWorker ||
    !isBrowserRuntime() ||
    resolveStorageDriver(config.driver).type !== "persistent" ||
    !config.accountId ||
    !config.accountRegistryAuthority ||
    config.adminSecret ||
    config.runtimeSources?.browserWorkerPort
  )
    return undefined;
  const lease = runtimeSource.acquireBrowserForegroundNodeLease(config);
  // Observed by the Db that adopts it, or returned by releaseUnusedLease.
  void lease.catch(() => undefined);
  leaseRoots.set(lease, browserLeaseRoot(config));
  return lease;
}

/** The worker realm, physical root and owner a lease was acquired for. */
const leaseRoots = new WeakMap<Promise<BrowserForegroundNodeLease>, string | undefined>();

function browserLeaseRoot(config: DbConfig): string | undefined {
  try {
    const sources = config.runtimeSources;
    return JSON.stringify([
      resolveDefaultPersistentDbName(config),
      createBrowserStorageOwner(config),
      sources?.wasmUrl ?? null,
      sources?.baseUrl ?? null,
      sources?.brokerWorkerUrl ?? null,
      sources?.wasmVersion ?? null,
      Boolean(sources?.wasmModule || sources?.wasmSource),
    ]);
  } catch {
    return undefined;
  }
}

/**
 * An early lease is only adopted for the exact root it was acquired for; a
 * config whose principal or assets changed while it resolved gets its own.
 */
function leaseMatches(lease: Promise<BrowserForegroundNodeLease>, config: DbConfig): boolean {
  const root = leaseRoots.get(lease);
  return root !== undefined && root === browserLeaseRoot(config);
}

/**
 * @internal Return a lease that no Db adopted, keeping its node reusable.
 * Resolves once the worker has the lease back (or it never issued one), so a
 * replacement acquired afterwards never queues behind it.
 */
export async function releaseUnusedLease(
  lease: Promise<BrowserForegroundNodeLease> | undefined,
): Promise<void> {
  if (!lease || leaseClaimed.has(lease)) return;
  leaseClaimed.add(lease);
  try {
    const acquired = await lease;
    await acquired.returnWithHighWater(acquired.confirmedTxTime);
  } catch {
    // A lease that failed to arrive, or could not be returned, holds nothing.
  }
}

export async function createDbWithRuntimeSource<RuntimeConfig extends DbConfig>(
  config: RuntimeConfig,
  runtimeSource: RuntimeSource<RuntimeConfig>,
  earlyLease?: Promise<BrowserForegroundNodeLease>,
): Promise<Db> {
  let lease = earlyLease;
  try {
    return await openDbWithRuntimeSource(config, runtimeSource, () => {
      lease ??= startBrowserWorkerLease(config, runtimeSource);
      return lease;
    });
  } finally {
    void releaseUnusedLease(lease);
  }
}

/** Leases already adopted by a Db or released; neither may happen twice. */
const leaseClaimed = new WeakSet<Promise<BrowserForegroundNodeLease>>();

async function openDbWithRuntimeSource<RuntimeConfig extends DbConfig>(
  config: RuntimeConfig,
  runtimeSource: RuntimeSource<RuntimeConfig>,
  startLease: () => Promise<BrowserForegroundNodeLease> | undefined,
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
  // Boot the worker while this page loads its own runtime.
  const earlyLease = startLease();
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
  } else if (
    !config.jwtToken &&
    !config.cookieSession &&
    !config.adminSecret &&
    !isRetainedAccountSessionInput({
      ...config,
      trustedReservedSession: getTrustedReservedSession(config),
    })
  ) {
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
  const browserWorker =
    runtimeSource.supportsBrowserWorker && isBrowserRuntime() && driver.type === "persistent";
  const adoptedLease =
    earlyLease && browserWorker && leaseMatches(earlyLease, resolvedConfig)
      ? earlyLease
      : undefined;
  if (adoptedLease) {
    leaseClaimed.add(adoptedLease);
  } else if (earlyLease) {
    // Hand a lease for some other root back before this Db acquires its own,
    // so the two are never held, or queued behind each other, at once.
    await releaseUnusedLease(earlyLease);
  }
  const db = browserWorker
    ? await Db.createWithBrowserWorker(
        resolvedConfig,
        runtimeSource as AnyRuntimeSource,
        adoptedLease,
      )
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
