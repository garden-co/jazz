import { isHistoryTable, resolveQueryTable } from "./history-relation.js";
import type { ConnectionRequirement } from "./connection-manager/types.js";
import type { DbAccessContext } from "./db-access-context.js";
import { Utf8Decoder } from "./utf8.js";
import { parseSnapshotRef, type SnapshotRef } from "./snapshot-ref.js";
import { runtimeRandomBytes } from "./runtime-entropy.js";
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

import type { ColumnDescriptor, WasmSchema, WasmRow, StorageDriver } from "../drivers/types.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  SerializableWriteHandle,
  SerializableWriteResult,
  WriteResult,
  JazzClient,
  beginClientTransactionAfter,
  type TransactionAdmission,
  type MutationErrorEvent,
  WriteHandle,
  createOpenTransactionId,
  setWriteWaitReadiness,
  type TransactionKind,
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
}
export type StreamingInsertOptions = Omit<InsertOptions, "branch">;

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: Branch;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: Branch;
  base?: BranchBase;
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
 * demand into the engine. It preserves the public result/coordinate contract and
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
    return {
      computedState: undefined,
      version: 0,
      physical: outputIncludes === built.includes ? built : { ...built, includes: outputIncludes },
      transform: Object.keys(built.partialSelect).length
        ? (row: WasmRow) => applyPartialValueSelections(decode(row), built.partialSelect)
        : decode,
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
): Promise<T[]> {
  for (;;) {
    const results: T[] = [];
    let missing: MissingComputedDependency | undefined;
    // Visit every root before retrying so different rows' branches share a lookup.
    for (const row of rows) {
      try {
        results.push(prepared.transform(row));
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
  openTransactionId: OpenTransactionId;
  session?: Session;
  attribution?: string;
  preparation: WritePreparation;
  committing?: boolean;
  begun: boolean;
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
  return createTransactionWriteResult(transaction, resolvedValue, txId, resultClient());
}

// Installed once by Transaction so internal construction can bind its owner only
// after recording admission, without adding a public constructor argument.
let createTransactionWithAdmission: <TKind extends TransactionKind>(
  kind: TKind,
  resolveClient: (schema: WasmSchema) => JazzClient,
  context: DbAccessContext | null,
  ownerClient: JazzClient | undefined,
  prerequisite: TransactionAdmission | undefined,
) => Transaction<TKind>;

/**
 * Groups a set of writes as either a mergeable or serializable transaction (see {@link TransactionKind}).
 */
export class Transaction<TKind extends TransactionKind = TransactionKind> {
  static {
    createTransactionWithAdmission = (kind, resolveClient, context, ownerClient, prerequisite) => {
      const transaction = new Transaction(kind, resolveClient, context);
      transaction.openingPrerequisite = prerequisite;
      if (ownerClient) transaction.bindOwnerClient(ownerClient);
      return transaction;
    };
  }

  private openingPrerequisite?: TransactionAdmission;

  private readonly pendingReads = new Set<Promise<unknown>>();
  private committing = false;

  constructor(
    readonly kind: TKind,
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly context: DbAccessContext | null = null,
    ownerClient?: JazzClient,
  ) {
    if (ownerClient) this.bindOwnerClient(ownerClient);
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
    if (this.committing) {
      throw new Error(`DbTransaction.${operation}() cannot run after commit has been requested`);
    }
    return getDbTxHandleBinding(this, operation);
  }

  private bindOwnerClient(ownerClient: JazzClient): void {
    const { writeSession: session, attribution } = this.context ?? {};
    const preparation = new WritePreparation(true);
    const owner = getOwnerPreparation(ownerClient);
    const openTransactionId = owner?.pending
      ? createOpenTransactionId()
      : this.openingPrerequisite
        ? beginClientTransactionAfter(
            ownerClient,
            this.kind,
            session,
            attribution,
            undefined,
            this.openingPrerequisite,
          )
        : ownerClient.beginTransaction(this.kind, session, attribution);
    const binding: DbTransactionHandleBinding = {
      ownerClient,
      openTransactionId,
      session: session,
      attribution: attribution,
      preparation,
      begun: !owner?.pending,
    };
    dbTxHandleBindings.set(this, binding);
    if (!binding.begun)
      preparation.run(() =>
        owner!.run(() => {
          if (preparation.cancelled) return;
          if (this.openingPrerequisite)
            beginClientTransactionAfter(
              ownerClient,
              this.kind,
              session,
              attribution,
              openTransactionId,
              this.openingPrerequisite,
            );
          else ownerClient.beginTransaction(this.kind, session, attribution, openTransactionId);
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
    const txId =
      preparation.pending || preparation.failed || this.pendingReads.size > 0
        ? Promise.all([preparation.drain(), ...this.pendingReads]).then(
            () => ownerClient.commitTransaction(openTransactionId).txId,
          )
        : ownerClient.commitTransaction(openTransactionId).txId;
    this.committing = true;
    const finishCommit = () => {
      this.committing = false;
    };
    txId.then(finishCommit, finishCommit);
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
    if (!begun) {
      preparation.cancelled = true;
      return Promise.resolve(true);
    }
    const result = ownerClient.rollbackTransaction(openTransactionId);
    preparation.cancelled = true;
    return result;
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this transaction, and will only be globally visible
   * once it's committed.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: InsertOptions): T {
    assertWritableTable(table);
    this.bindTable(table);
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution, preparation } = this.requireBinding("insert");
    let id = options?.id ?? (hasDependentTransforms(table) ? reserveRowId() : undefined);
    const { values, value } = this.prepareCreation(table, data, id, "Insert");
    if (!id && (preparation.pending || getOwnerPreparation(client)?.pending)) id = reserveRowId();
    let row: ReturnType<typeof client.insertInternal> | undefined;
    const stage = () => {
      row = client.insertInternal(
        table._table,
        values,
        normalizeSelectorOptions(table._schema, table._table, id ? { ...options, id } : options),
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
    options?: UpdateOptions,
  ): void {
    assertWritableTable(table);
    this.bindTable(table);
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
    this.bindTable(table);
    const descriptors = lowerApplyDiffs(table, data, options?.applyDiffs);
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

  private async readAll<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    this.bindQuery(query);
    const { preparation, openTransactionId } = this.requireBinding("query");
    if (preparation.pending || preparation.failed) await preparation.drain();
    const client = this.resolveClient(query._schema);
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson));
    const planningSchema = requireSchemaWithTable(query._schema, builtQuery.table);
    const outputTable = resolveQueryOutputTable(planningSchema, builtQuery);
    const prepared = prepareTransformRead(query, builtQuery, outputTable);
    // Transactions accept the same public options surface as Db. Lower before
    // reaching native options so JavaScript callers cannot smuggle runtime
    // controls (for example `localUpdates` or `openTransactionId`) through this
    // otherwise separate execution path.
    const queryOptions =
      normalizeViewOptions(query._schema, builtQuery.table, lowerPublicDbQueryOptions(options)) ??
      {};
    const rows = await client.queryInternal(
      translateQuery(builderJson, planningSchema, prepared.physical),
      {
        ...queryOptions,
        openTransactionId,
      },
      this.context?.readSession,
    );
    if (!prepared.computedState) return rows.map(prepared.transform);
    return finishComputedRead(query, builtQuery, outputTable, prepared, rows, (physical) =>
      client.queryInternal(
        translateQuery(builderJson, planningSchema, physical),
        { ...queryOptions, openTransactionId },
        this.context?.readSession,
      ),
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

/** @internal The compiler opts in before staging any operations. */
export let beginDbTransactionAfter: (
  db: Db,
  prepare: TransactionAdmission,
) => Transaction<"serializable">;

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
  static {
    beginDbTransactionAfter = (db, prerequisite) =>
      db.createTransaction("serializable", prerequisite);
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
    try {
      this.authStateStore.markUnauthenticated(reason);
    } finally {
      // Observer failures must not skip renewal of an owned local-first token.
      if (reason === "expired" && this.localFirstRefreshOwned) this.renewExpiredLocalFirstToken();
    }
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
    const client = this.getClient(table._schema);
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
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
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
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
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
    const client = this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
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
    options?: UpdateOptions,
  ): WriteHandle {
    assertWritableTable(table);
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
    prerequisite?: TransactionAdmission,
  ): Transaction<TKind> {
    this.assertOpen();
    notifyLocalWrite(this);
    const context = this.getAccessContext();
    const ownerClient = this.getCurrentClient();
    if (kind === "serializable" && !ownerClient) {
      throw new Error(
        "Cannot begin a serializable transaction before the JazzClient has been created. Run a query or mutation first.",
      );
    }
    return createTransactionWithAdmission(
      kind,
      (schema) => this.getClient(schema),
      context,
      ownerClient ?? undefined,
      prerequisite,
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
    const prepared = prepareTransformRead(query, builtQuery, outputTable);
    const queryOptions = normalizeQueryOptions(query._schema, builtQuery.table, options);
    const wasmQuery = translateQuery(builderJson, planningSchema, prepared.physical);
    const usesRelationTraversal = queryUsesRelationTraversal(builtQuery);
    const context = this.getAccessContext();
    const effectiveTier = resolveEffectiveQueryExecutionOptions(this.config, queryOptions).tier;
    await this.ensureReady(effectiveTier === "remote" ? "server" : "runtime");
    const rows =
      context || usesRelationTraversal
        ? await client.queryInternal(wasmQuery, queryOptions, context?.readSession)
        : await client.queryInternal(wasmQuery, queryOptions);
    if (!prepared.computedState) return rows.map(prepared.transform);
    return finishComputedRead(query, builtQuery, outputTable, prepared, rows, (physical) =>
      client.queryInternal(
        translateQuery(builderJson, planningSchema, physical),
        queryOptions,
        context?.readSession,
      ),
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
    const { onDelta, onError } =
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
    let prepared = prepareTransformRead(query, builtQuery, outputTable);
    let wasmQuery = translateQuery(builderJson, planningSchema, prepared.physical);
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
    };
    let activeSubscription: NativeSubscription | null = null;
    let unsubscribed = false;
    let terminalized = false;
    const readyAbort = new AbortController();
    const retireNativeSubscription = (subscription: NativeSubscription) => {
      subscription.retired = true;
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
    const handleDelta = (delta: Parameters<SubscriptionManager<T>["handleDelta"]>[0]) => {
      if (unsubscribed || terminalized || activeSubscription === null) return;
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
    };
    const startNativeSubscription = (
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
          wasmQuery,
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
