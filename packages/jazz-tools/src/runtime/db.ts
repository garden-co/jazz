import { clientScope } from "./client-scope.js";
import { registerPageDb } from "./page-handle.js";
import { isHistoryTable, resolveQueryTable } from "./history-relation.js";
import type { ConnectionRequirement } from "./connection-manager/types.js";
import type { DbAccessContext } from "./db-access-context.js";
import { Utf8Decoder } from "./utf8.js";
import { parseSnapshotRef, type SnapshotRef } from "./snapshot-ref.js";
import { runtimeRandomBytes } from "./runtime-entropy.js";
import type { AccountHandle } from "../accounts/state.js";
import { declaredEntries, declaresEncryption, plainSchema } from "../encryption/plain-schema.js";
import { DbClosedError, GracefulShutdownSyncError } from "./graceful-shutdown-error.js";
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
  EmptyTransactionWriteResult,
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
  type Runtime,
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
  type ExplainedRows,
  type PermissionAdvice,
  type StreamingMutationKind,
  type StreamingValueSource,
  validateWriteWaitTimeout,
  waitAsWritten,
  WriteWaitTimeoutError,
} from "./client.js";
import { assertBacklogRows } from "./backlog-rows.js";
import { samePendingUploadRows, sameSyncStatus, type SyncStatus } from "./sync-status.js";
import type { TextAnchors } from "./text-anchors.js";
import {
  type BrowserForegroundNodeLease,
  type RuntimeSource,
  type RuntimeTokenOptions,
} from "./runtime-source.js";
import type { AuthFailureReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { resolveQueryOutputTable } from "./query-output-table.js";
import { applyColumnTransforms, createRowTransformer, transformRow } from "./row-transformer.js";
import {
  getOwnerPreparation,
  ownerPreparation,
  runAfterPreparation,
  WritePreparation,
} from "./write-preparation.js";
import { nativeCoreErrorCode, unreadableTargetError } from "./native-runtime/native-error-code.js";
import { writeRow } from "./write-row.js";
import {
  ComputedReadState,
  MissingComputedDependency,
  computedColumnValues,
  computedColumns,
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
  RowChangeKind,
  SubscriptionManager,
  type SubscriptionDelta,
  type ValueSplices,
} from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import { AuthRenewalBackoff } from "./auth-renewal-backoff.js";
import { accountAppId } from "../accounts/local-first.js";
import {
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
  internalSessionFromVerifiedReservedJwtPayload,
  resolveClientInternalSessionSync,
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
import {
  hasLocalFirstUpgrader,
  isThinDb,
  localFirstLoaderOf,
  upgradeDbToLocalFirst,
  type LocalFirstLoader,
} from "./local-first-upgrade.js";
import { EncryptedGuard } from "../thin/encrypted-guard.js";
import { EncryptionNeedsLocalFirstError } from "./encryption-needs-local-first.js";
import { DbReplacedError, type DbReplacedReason } from "./db-replaced-error.js";
import { RuntimeClosedError } from "./runtime-closed-error.js";
import {
  encryptionForDb,
  memberInsert,
  notDeclaredEncryption,
  type AddMemberArgs,
  type Encryption,
  type MemberAddress,
  type MemberRow,
} from "../encryption/index.js";
import { pendingEncryption } from "../encryption/pending-handle.js";
import { resolveSchemaSource } from "../schema-source.js";
import type { BrowserEncryptionConfig } from "../encryption/browser-host.js";
import { unreadableRows, withUnreadable } from "../encryption/unreadable.js";

/** `getDeviceAccountProof` hands out a session's sync proof only with this long left. */
const SESSION_PROOF_MIN_REMAINING_SECONDS = 30;
/**
 * How long an operation on an encrypted column waits for the local-first
 * client before it fails; a live query warns then and keeps waiting.
 */
const LOCAL_FIRST_WAIT_MS = 30_000;
/** How often a write tries a local-first client that has not synced yet again. */
const NOT_OBSERVED_RETRY_MS = 50;

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
  /**
   * Application identifier. Backend and test code only: a browser client
   * leaves it out, and its local data, secret and identity are scoped by the
   * endpoint it syncs with (see `clientScope`).
   */
  appId?: string;
  /**
   * For apps that already have local-first users: the app id their secrets
   * and accounts were created under, so they keep them. New apps leave it out.
   */
  scope?: string;
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
   * For an app that declares end-to-end encryption: its app and, optionally,
   * where this device keeps its keys (see {@link BrowserEncryptionConfig}).
   * A plain app leaves it out. The browser's thin client holds no keys: an
   * operation on an encrypted column waits for the local-first client (see
   * `client` of `createJazzClient`), or fails with
   * `EncryptionNeedsLocalFirstError` when none can load.
   */
  encryption?: BrowserEncryptionConfig & {
    /** @internal A device account's secret, read from its handle for each use. */
    accountSecret?: () => string | undefined;
  };
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
  /**
   * Slow-query log: queries whose runs take at least this many milliseconds
   * are reported with `console.warn`, one line per query shape with its plan
   * and the index that would likely make it fast (default 50; 0 turns the
   * log off).
   */
  slowQueryThresholdMs?: number;
  /**
   * How many changed rows a subscription's undelivered deltas may hold before
   * they are coalesced into one update of its current result, marked
   * `reason: "backlog"` (default 10000). It bounds the memory a subscriber
   * that falls behind a fast writer holds, and cannot be turned off. A query
   * can set its own with `backlogRows`.
   */
  subscriptionBacklogRows?: number;
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
  return explicitDbName ?? clientScope(config);
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
  /**
   * TEXT or BYTEA columns whose changes a subscription also delivers as splices
   * (see {@link DbSubscriptionUpdate}). One-shot reads ignore it.
   */
  splices?: readonly string[];
  /**
   * How many changed rows a subscription's undelivered deltas may hold before
   * they are replaced by one update of the current result (marked
   * `reason: "backlog"`). Overrides {@link DbConfig.subscriptionBacklogRows}
   * (default 10000). A whole number, at least 1. One-shot reads ignore it.
   */
  backlogRows?: number;
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
  onUpdate: (rows: T[], update: DbSubscriptionUpdate) => void;
  /** Called once when the subscription terminates with an error. */
  onError?: (error: Error) => void;
}

/** What changed in one subscription update, beyond the complete result. */
export interface DbSubscriptionUpdate {
  /**
   * Per updated row id, the splices of each column named in the `splices`
   * option. They turn the value the previous update showed into the new one,
   * and apply in order: each splice's offsets are in the value the earlier ones
   * produced. TEXT splices count UTF-16 code units and insert a string; BYTEA
   * splices count bytes and insert a `Uint8Array`. On a range select they are
   * relative to the selected window. A column the update left unchanged has
   * an empty list. A row or column without an entry has splices that could
   * not be computed on this device (its previous version is no longer held,
   * say): read its new value from the rows instead.
   */
  splices: ReadonlyMap<string, ValueSplices>;
  /**
   * `"backlog"` when this update replaced the result wholesale because the
   * subscriber fell too far behind to be told every change (more than
   * `backlogRows` changed rows waited undelivered): the rows are current,
   * the intermediate states were skipped, and `splices` is empty.
   */
  reason?: "backlog";
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
  const splices = (candidate as { splices?: unknown }).splices;
  if (splices !== undefined) {
    if (!Array.isArray(splices) || !splices.every((column) => typeof column === "string")) {
      throw new Error("splices must be an array of column names.");
    }
    if (splices.length > 0) lowered.splices = [...splices];
  }
  const backlogRows = (candidate as { backlogRows?: unknown }).backlogRows;
  if (backlogRows !== undefined) {
    assertBacklogRows(backlogRows, "backlogRows");
    lowered.backlogRows = backlogRows;
  }
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
export type SubscriptionHandle = (() => void) & {
  readonly ready?: Promise<void>;
};

const dbSubscriptionSources = new WeakMap<Db, DbSubscriptionSource>();

/**
 * What a Db hands to its successor after the local-first upgrade: everything
 * an app or framework calls. Its shutdown, and the internals its own runtime
 * needs to finish uploading, stay with it.
 */
const FORWARDED_TO_SUCCESSOR = [
  "allFromInspectorAttachment",
  "subscribeFromInspectorAttachment",
  "currentRuntime",
  "prefetchRuntimeQueries",
  "refreshAccountAuth",
  "updateAuthToken",
  "updateCookieSession",
  "getAuthState",
  "getDeviceAccountProof",
  "onAuthChanged",
  "onMutationError",
  "textAnchors",
  "getSyncStatus",
  "getPendingUploadRows",
  "waitForSync",
  "getConfig",
  "setDevMode",
  "disconnect",
  "reconnect",
  "getActiveQuerySubscriptions",
  "onActiveQuerySubscriptionsChange",
  "getRuntimeSchema",
  "openInspectorControlPort",
  "insert",
  "insertStreaming",
  "updateStreaming",
  "upsertStreaming",
  "restore",
  "upsert",
  "update",
  "delete",
  "canInsert",
  "canRead",
  "canUpdate",
  "canDelete",
  "createTransaction",
  "captureSnapshot",
  "beginTransaction",
  "beginSerializableTransaction",
  "transaction",
  "serializableTransaction",
  "upgradeToLocalFirst",
  "deleteClientStorage",
  "storePendingUploads",
  "logout",
  "all",
  "allInternal",
  "one",
  "subscribe",
  "subscribeDelta",
] as const;

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
  /** Set to false to create an encryption space without adding its creator. */
  addCreatorAsMember?: boolean;
}
export type StreamingInsertOptions = Omit<InsertOptions, "branch" | "addCreatorAsMember">;

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: Branch;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: Branch;
  base?: BranchBase;
}

/** `update` options, with `editedFrom` for the columns `TReplacements` writes. */
export type UpdateEditOptions<TReplacements extends object> = UpdateOptions & {
  /**
   * For text-merge columns written whole by an editor: the value each was
   * edited from, which the editor last showed or wrote. When the column
   * holds a newer value (a merge that reached this store before the editor
   * showed it), the write is merged over it instead of undoing it. A column
   * left `undefined` here is written as usual.
   */
  editedFrom?: { readonly [TColumn in keyof TReplacements]?: string };
};

type TypedUpdateOptionsWithDiffs<
  TReplacements extends object,
  TDiffs,
> = UpdateEditOptions<TReplacements> & {
  applyDiffs?: TDiffs & { [TColumn in keyof TReplacements]?: never };
};

/** The options an untyped `update` call is lowered from. */
type LoweredUpdateOptions = UpdateOptions & {
  applyDiffs?: object;
  editedFrom?: { readonly [column: string]: string | undefined };
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

/** Splices are only defined for plain TEXT and BYTEA columns of the result table. */
function assertSpliceableColumns(
  schema: WasmSchema,
  table: string,
  query: { readonly _columnTransforms?: ColumnTransformMap },
  columns: readonly string[],
): void {
  const descriptors = resolveQueryTable(schema, table)?.columns ?? [];
  for (const column of columns) {
    const descriptor = descriptors.find((candidate) => candidate.name === column);
    if (!descriptor) throw new Error(`splices: "${column}" is not a column of "${table}".`);
    const type = descriptor.column_type.type;
    if (type !== "Text" && type !== "Bytea") {
      throw new Error(
        `splices: column "${column}" is ${type}; splices are not supported yet for columns other than TEXT and BYTEA.`,
      );
    }
    if (query._columnTransforms?.[column]) {
      throw new Error(
        `splices: column "${column}" has a column transform; splices are not supported yet for transformed columns.`,
      );
    }
  }
}

/**
 * A range-selected splice column must be read in part: a computed or
 * transformed column that depends on it makes the engine read it whole and
 * TypeScript cut the window, so the engine's splices would be in whole-value
 * coordinates while the rows show the window.
 */
function assertSplicesReadInPart(
  built: NormalizedBuiltQuery,
  physical: NormalizedBuiltQuery,
  columns: readonly string[],
): void {
  for (const column of columns) {
    if (
      Object.hasOwn(built.partialSelect, column) &&
      !Object.hasOwn(physical.partialSelect, column)
    )
      throw new Error(
        `splices: column "${column}" is selected in part, but a computed or transformed column depends on it, so it is read whole; splices are not supported yet for such columns.`,
      );
  }
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
    return toWriteRecord(data, plainSchema(schema), tableName);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`${operation} failed: WriteError("${escapeWriteErrorReason(message)}")`);
  }
}

type WireSplicePage = {
  kind: "bytes" | "text_utf16" | "text_utf8";
  from: number;
  to: number;
};

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
    }
  | {
      kind: "text_edit";
      column: string;
      text: string;
      edited_from: string;
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

/**
 * An `update` call's replacements and wire descriptors: its `applyDiffs`,
 * and each `editedFrom` column's replacement as a text edit of that value.
 */
function lowerUpdate(
  table: TableProxy<any, any, any, any, any>,
  data: Record<string, unknown>,
  options: LoweredUpdateOptions | undefined,
): { replacements: Record<string, unknown>; descriptors: WireLargeValueUpdate[] } {
  const descriptors = lowerApplyDiffs(table, data, options?.applyDiffs);
  const editedFrom = options?.editedFrom;
  if (editedFrom === undefined) {
    return { replacements: data, descriptors };
  }
  if (typeof editedFrom !== "object" || editedFrom === null || Array.isArray(editedFrom)) {
    throw new Error("update option editedFrom must be an object keyed by column name.");
  }
  const replacements = { ...data };
  const edited: string[] = [];
  for (const [column, from] of Object.entries(editedFrom)) {
    if (from === undefined) continue;
    const transform = table._columnTransforms?.[column];
    if (transform && !transform.computed) {
      throw new Error(
        `update option editedFrom is not supported yet on "${column}", which has a transform.`,
      );
    }
    const text = replacements[column];
    if (typeof text !== "string" || typeof from !== "string") {
      throw new Error(
        `update option editedFrom.${column} needs a string, and a string replacement for "${column}".`,
      );
    }
    delete replacements[column];
    edited.push(column);
    descriptors.push({ kind: "text_edit", column, text, edited_from: from });
  }
  return { replacements, descriptors };
}

/** `options` without `editedFrom`, which {@link lowerUpdate} turned into descriptors. */
function withoutEditedFrom(options: LoweredUpdateOptions | undefined) {
  if (options?.editedFrom === undefined) return options;
  const { editedFrom: _, ...rest } = options;
  return rest;
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
 * The engine reads each partial selection of the physical query only in part
 * (SPEC 19.12). A column a transform also depends on is read whole instead,
 * and its visible selection is taken here from the whole value.
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
    // Like the engine (#5502): a range past the end of the value clamps to
    // it, empty when it starts past it; an inverted range fails.
    if (value instanceof Uint8Array) {
      if ("fromUtf8" in selection) {
        throw new Error(`Byte range for "${column}" takes from and to, not fromUtf8 and toUtf8.`);
      }
      if (selection.from > selection.to) {
        throw new Error(`Byte range for "${column}" ends before it starts.`);
      }
      projected[column] = value.slice(selection.from, selection.to);
      continue;
    }
    if (typeof value !== "string") {
      throw new Error(`Large-value selection for "${column}" has an incompatible column type.`);
    }
    if ("fromUtf8" in selection) {
      const bytes = new TextEncoder().encode(value);
      const to = Math.min(selection.toUtf8, bytes.length);
      const from = Math.min(selection.fromUtf8, to);
      if (selection.fromUtf8 > selection.toUtf8) {
        throw new Error(`UTF-8 range for "${column}" ends before it starts.`);
      }
      if (
        (from < bytes.length && (bytes[from]! & 0b1100_0000) === 0b1000_0000) ||
        (to < bytes.length && (bytes[to]! & 0b1100_0000) === 0b1000_0000)
      ) {
        throw new Error(`UTF-8 range for "${column}" splits a code point.`);
      }
      projected[column] = new Utf8Decoder({ fatal: true }).decode(bytes.slice(from, to));
      continue;
    }
    const to = Math.min(selection.to, value.length);
    const from = Math.min(selection.from, to);
    if (selection.from > selection.to) {
      throw new Error(`UTF-16 range for "${column}" ends before it starts.`);
    }
    if (!utf16Boundary(value, from) || !utf16Boundary(value, to)) {
      throw new Error(`UTF-16 range for "${column}" splits a surrogate pair.`);
    }
    projected[column] = value.slice(from, to);
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
      // The engine applies every partial selection of this physical query.
      transform: decode,
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
    let hidden: string[] | undefined;
    let computed: string[] | undefined;
    const addDependency = (dependency: string) => {
      if (transforms?.[dependency]?.computed) return;
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
    const hasPartial = Object.keys(partialSelect).length > 0;
    // Selections the engine did not apply: those whose column it read whole.
    const wholeRead = Object.fromEntries(
      Object.entries(partialSelect).filter(([column]) => !Object.hasOwn(partial, column)),
    );
    const sliceWhole = Object.keys(wholeRead).length > 0;
    const excluded = Object.keys(includes);
    return {
      select: physical,
      includes: preparedIncludes,
      partialSelect: partial,
      finish:
        visibleTransforms || hasPartial || finishIncludes.length
          ? (row: Record<string, unknown>): Record<string, unknown> => {
              const appValues = computed
                ? computedColumnValues(transforms!, computed, { ...row }, computedState, partial)
                : undefined;
              const result = sliceWhole ? applyPartialValueSelections(row, wholeRead) : row;
              if (visibleTransforms)
                applyColumnTransforms(
                  result,
                  visibleTransforms,
                  excluded,
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

/** Keep omissions with the typed rows and retry missing computed dependencies in the same read scope. */
async function finishRead<T>(
  query: QueryBuilder<T>,
  built: NormalizedBuiltQuery,
  outputTable: string,
  prepared: ReturnType<typeof prepareTransformRead<T>>,
  rows: WasmRow[],
  read: (physical: NormalizedBuiltQuery) => Promise<WasmRow[]>,
): Promise<T[]> {
  const unreadable = unreadableRows(rows);
  if (!prepared.computedState) return withUnreadable(rows.map(prepared.transform), unreadable);
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
    if (!missing) return withUnreadable(results, unreadable);
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
  const computed = computedColumns(source._columnTransforms);
  const appValues = computed.length
    ? computedColumnValues(source._columnTransforms, computed, record)
    : undefined;
  applyColumnTransforms(record, source._columnTransforms, [], appValues);
  for (const column of computed) record[column] = appValues!.get(column);
  return record;
}

/** Refuse writes to computed fields before a write is staged or deferred. */
function rejectComputedWrites(table: TableProxy<any, any>, data: Record<string, unknown>): void {
  const transforms = table._columnTransforms;
  if (!transforms) return;
  for (const column of Object.keys(data))
    if (transforms[column]?.computed)
      throw new TypeError(`Computed field "${column}" is read-only.`);
}

function transformInputColumns(
  table: TableProxy<any, any, any, any, any>,
  data: unknown,
): Record<string, unknown> {
  const record = data as Record<string, unknown>;
  const transforms = table._columnTransforms;
  if (!transforms) return record;
  let result: Record<string, unknown> | undefined;
  for (const column in record) {
    if (!Object.hasOwn(record, column)) continue;
    const transform = transforms[column];
    if (transform?.computed) throw new TypeError(`Computed field "${column}" is read-only.`);
    if (!transform || record[column] === undefined) continue;
    (result ??= { ...record })[column] = transform.to(record[column]);
  }
  return result ?? record;
}

/**
 * How long an update of a row this replica hasn't loaded waits for the
 * server to send it, before it is refused as a row the session can't find.
 */
const LOAD_ROW_WAIT_MS = 10_000;

/**
 * Read one row by id from the server through the caller's session, so this
 * replica holds it before a write stages against it. The read waits for the
 * server at most {@link LOAD_ROW_WAIT_MS}: offline, it finds no row and the
 * update fails instead of hanging, with the writes queued behind it.
 */
function loadRow(
  client: JazzClient,
  table: TableProxy<any, any>,
  id: string,
  session: Session | undefined,
  branch: NonNullable<ReturnType<typeof normalizeViewOptions>>["branch"],
): Promise<unknown[]> {
  const built = JSON.stringify({
    table: table._table,
    conditions: [{ column: "id", op: "eq", value: id }],
    limit: 1,
  });
  const read = client.queryInternal(
    translateQuery(built, table._schema),
    branch ? { tier: "remote", branch } : { tier: "remote" },
    session,
  );
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<unknown[]>((resolve) => {
    timer = setTimeout(() => resolve([]), LOAD_ROW_WAIT_MS);
  });
  return Promise.race([read, timeout]).finally(() => clearTimeout(timer));
}

/**
 * @internal The row an insert or restore of `data` as row `id` writes, as
 * the table's schema alone decides it: for writes made before a client
 * exists (`createJazz`'s `db` while its store opens).
 */
export function previewWrittenRow<T, Init>(
  table: TableProxy<T, Init>,
  operation: "Insert" | "Restore",
  id: string,
  data: Init,
): T {
  assertWritableTable(table);
  const transformedData = table._columnTransforms
    ? transformInputColumns(table, data)
    : (data as Record<string, unknown>);
  return immediateWriteRow(
    table,
    id,
    toWriteRecordForOperation(operation, transformedData, table._schema, table._table),
  );
}

/** @internal A fresh row id, as an insert without one would get. */
export function reserveRowId(): string {
  const hex = createOpenTransactionId();
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
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
  settledRefused?: () => void,
): RunInTransactionResult<TResult, TKind> {
  // The callback settled (returned, threw, or its promise settled): its
  // commit point, where a refused transaction tells `settledRefused`.
  let settled = false;
  const settle = () => {
    if (settled) return;
    settled = true;
    if (settledRefused && transaction.refusal()) settledRefused();
  };
  let value: TResult;
  try {
    const scope = createTransactionScope(transaction);
    value = callback(scope);
  } catch (error) {
    settle();
    try {
      await transaction.rollback();
    } catch {
      // Preserve the original callback error.
    }
    throw error;
  }
  const resultClient = typeof client === "function" ? client : () => client;
  if (typeof (value as { then?: unknown } | null)?.then !== "function") settle();
  let resolvedValue: Awaited<TResult>;
  try {
    resolvedValue = await value;
    settle();
    // An operation in it was refused and the callback caught that: it can
    // never commit, and fails as if the callback had not caught it.
    const refusal = transaction.refusal();
    if (refusal) throw refusal;
  } catch (error) {
    settle();
    try {
      await transaction.rollback();
    } catch {
      // Preserve the original callback error.
    }
    throw error;
  }
  let committed: TransactionCommitHandle<TKind>;
  try {
    // A callback that wrote nothing has nothing to commit: its result
    // resolves at once (an explicit `commit()` of an empty mergeable
    // transaction still refuses).
    const outcome = await transaction.commitUnlessEmpty();
    if (outcome === "empty") {
      return new EmptyTransactionWriteResult(
        resolvedValue,
        resultClient(),
      ) as unknown as TransactionWriteResult<Awaited<TResult>, TKind>;
    }
    committed = outcome;
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

/**
 * Groups a set of writes as either a mergeable or serializable transaction (see {@link TransactionKind}).
 */
export class Transaction<TKind extends TransactionKind = TransactionKind> {
  /** @internal Install the prerequisite before binding the owner. */
  static createWithAdmission<TKind extends TransactionKind>(
    kind: TKind,
    resolveClient: (schema: WasmSchema) => JazzClient,
    context: DbAccessContext | null,
    ownerClient: JazzClient | undefined,
    prerequisite: TransactionAdmission | undefined,
    schema?: WasmSchema | null,
  ): Transaction<TKind> {
    const transaction = new Transaction(kind, resolveClient, context);
    transaction.openingPrerequisite = prerequisite;
    transaction.encryptionSchema = schema ?? null;
    if (ownerClient) transaction.bindOwnerClient(ownerClient);
    return transaction;
  }

  private openingPrerequisite?: TransactionAdmission;
  private encryptionSchema: WasmSchema | null = null;
  private memberAddRefusal?: Error;

  readonly encryption = {
    members: {
      add: (...args: AddMemberArgs): MemberRow => {
        const [table, row] = memberInsert(
          this.encryptionSchema,
          (schema, spaceTable) => {
            const client = this.resolveClient(schema);
            const runtime = client.getRuntime();
            if (runtime.memberAddRefusal) {
              if (!dbTxHandleBindings.has(this)) this.bindOwnerClient(client);
              this.requireBinding("encryption.members.add");
              throw (this.memberAddRefusal ??=
                this.refusal() ?? runtime.memberAddRefusal(spaceTable));
            }
            return runtime;
          },
          args,
        );
        return this.insert<MemberRow, MemberAddress>(table, row);
      },
    },
  };

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
   * @internal Why this transaction can only roll back: an operation in it
   * was refused, even if the callback caught that.
   */
  refusal(): Error | undefined {
    const binding = dbTxHandleBindings.get(this);
    return (
      this.memberAddRefusal ?? binding?.ownerClient.transactionRefusal(binding.openTransactionId)
    );
  }

  /**
   * Commit this transaction.
   */
  commit(): TransactionCommitHandle<TKind> {
    const binding = this.requireBinding("commit");
    const { ownerClient, openTransactionId, preparation } = binding;
    if (binding.committing) throw new Error("Transaction commit has already been requested.");
    if (preparation.pending || preparation.failed) binding.committing = true;
    let txId: Promise<TxId>;
    if (this.memberAddRefusal) {
      void this.rollback().catch(() => {});
      txId = Promise.reject(this.memberAddRefusal);
    } else {
      txId =
        preparation.pending || preparation.failed || this.pendingReads.size > 0
          ? Promise.all([preparation.drain(), ...this.pendingReads]).then(
              () => ownerClient.commitTransaction(openTransactionId).txId,
            )
          : ownerClient.commitTransaction(openTransactionId).txId;
    }
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
   * @internal The callback form's commit: a mergeable transaction that staged
   * no write is rolled back instead, as there is nothing to commit, and
   * `"empty"` is returned. Anything else commits as {@link commit} does.
   */
  async commitUnlessEmpty(): Promise<TransactionCommitHandle<TKind> | "empty"> {
    if (this.kind !== "mergeable") return this.commit();
    const { ownerClient, openTransactionId, preparation } = this.requireBinding("commit");
    // Staged, or this runtime can't tell: commit at once, as `commit` does
    // (reads still in flight settle through the commit's `txId`).
    if (ownerClient.hasStagedTransactionWrites(openTransactionId) !== false) return this.commit();
    // Nothing staged yet, but a write still being prepared may stage. Reads
    // stage nothing, so only preparations are waited for, and the
    // transaction takes no more operations meanwhile.
    if (preparation.pending || preparation.failed) {
      this.committing = true;
      let drained = true;
      try {
        await preparation.drain();
      } catch {
        drained = false;
      } finally {
        this.committing = false;
      }
      // A preparation that failed fails the commit, as for `commit`.
      if (!drained || ownerClient.hasStagedTransactionWrites(openTransactionId) !== false) {
        return this.commit();
      }
    }
    await this.rollback();
    return "empty";
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
   * Stream one Text, Json, or Bytea column into a new row, staged in this
   * transaction. Resolves with the row id once the stream is consumed; the
   * commit waits for it and fails if the stream did.
   *
   * The streamed value is held for the transaction for at most the store's
   * staging max age (10 minutes by default) after the stream finishes. A
   * transaction left open longer than that is refused at commit, with
   * `streaming upload expired`, so its storage is never held indefinitely;
   * start it again. The same applies to `updateStreaming` and
   * `upsertStreaming`.
   */
  insertStreaming<T, Init, StreamingInit>(
    table: TableProxy<T, Init, StreamingInit>,
    data: StreamingInit,
    options?: StreamingInsertOptions,
  ): Promise<{ id: string }> {
    return this.streamingMutation("insert", table, options?.id, data, options);
  }

  /** Stream one large column of an existing row, staged in this transaction. */
  updateStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<{ id: string }> {
    return this.streamingMutation("update", table, id, data, options);
  }

  /** Stream one large column of a row, creating it if absent, staged in this transaction. */
  upsertStreaming<T, Init, StreamingInit, StreamingUpdate>(
    table: TableProxy<T, Init, StreamingInit, StreamingUpdate>,
    id: string,
    data: StreamingUpdate,
    options?: UpdateOptions,
  ): Promise<{ id: string }> {
    return this.streamingMutation("upsert", table, id, data, options);
  }

  private streamingMutation(
    mutation: StreamingMutationKind,
    table: TableProxy<any, any, any, any>,
    id: string | undefined,
    data: unknown,
    options?: StreamingInsertOptions | UpdateOptions,
  ): Promise<{ id: string }> {
    assertWritableTable(table);
    this.bindTable(table);
    const client = this.resolveClient(table._schema);
    const operation =
      mutation === "insert" ? "Insert" : mutation === "update" ? "Update" : "Upsert";
    const { openTransactionId, session, attribution, preparation } = this.requireBinding(
      `${mutation}Streaming`,
    );
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    rejectComputedWrites(table, ordinaryData);
    const stage = async () => {
      const record = transformInputColumns(table, ordinaryData);
      const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
      const branch =
        mutation === "insert" ? deriveStreamingInsertBranch(table, ordinaryData) : undefined;
      const normalized =
        mutation === "insert"
          ? normalizeSelectorOptions(table._schema, table._table, {
              ...options,
              ...(id ? { id } : {}),
              ...(branch ? { branch } : {}),
            })
          : normalizeViewOptions(table._schema, table._table, options as UpdateOptions);
      return client.streamingMutationInTransaction(
        mutation,
        table._table,
        values,
        column,
        source,
        openTransactionId,
        normalized,
        session,
        attribution,
        id,
      );
    };
    return Promise.resolve(
      preparation.run(() =>
        runAfterPreparation(client, () => (preparation.cancelled ? undefined : stage())),
      ),
    ).then((staged) => {
      if (!staged) throw new Error("Transaction was rolled back before the stream was staged");
      return staged;
    });
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
    let id = options?.id;
    const { values, value } = this.prepareCreation(table, data, id, "Insert");
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
    // Not staged yet (waiting for preparation, or for its space's key): the
    // later staging reads `id`, so it commits the row returned here.
    if (!row && !id) id = reserveRowId();
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
        ? transformInputColumns(table, data)
        : (data as Record<string, unknown>);
    } catch (error) {
      this.requireBinding(operation).preparation.failure = error;
      throw error;
    }
    const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
    try {
      return {
        values,
        value: id === undefined ? undefined : immediateWriteRow(table, id, values),
      };
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
    this.prepareMutation(table, data as Record<string, unknown>, "Upsert", (values) => {
      client.upsertInternal(
        table._table,
        id,
        values,
        normalizeViewOptions(table._schema, table._table, options),
        session,
        attribution,
        openTransactionId,
      );
    });
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
    options?: UpdateEditOptions<Partial<Init>>,
  ): void;
  update(
    table: TableProxy<any, any, any, any, any>,
    id: string,
    data: Record<string, unknown>,
    options?: LoweredUpdateOptions,
  ): void {
    assertWritableTable(table);
    this.bindTable(table);
    const { replacements, descriptors } = lowerUpdate(table, data, options);
    options = withoutEditedFrom(options);
    const client = this.resolveClient(table._schema);
    const { openTransactionId, session, attribution } = this.requireBinding("update");
    const normalizedOptions = normalizeViewOptions(table._schema, table._table, options);
    this.prepareMutation(table, replacements, "Update", (updates) => {
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
    });
  }

  private prepareMutation(
    table: TableProxy<any, any>,
    data: Record<string, unknown>,
    operation: "Update" | "Upsert",
    stage: (values: ReturnType<typeof toWriteRecord>) => void,
  ): void {
    const { ownerClient, preparation } = this.requireBinding(operation);
    rejectComputedWrites(table, data);
    if (preparation.pending || getOwnerPreparation(ownerClient)?.pending) data = { ...data };
    if (preparation.cancelled) {
      stage({});
      return;
    }
    preparation.run(() =>
      runAfterPreparation(ownerClient, () => {
        if (preparation.cancelled) return;
        let record: Record<string, unknown>;
        try {
          record = transformInputColumns(table, data);
        } catch (error) {
          preparation.failure = error;
          throw error;
        }
        if (preparation.failed) throw preparation.failure;
        stage(toWriteRecordForOperation(operation, record, table._schema, table._table));
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
    return finishRead(query, builtQuery, outputTable, prepared, rows, (physical) =>
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
export function beginDbTransactionAfter(
  db: Db,
  prepare: TransactionAdmission,
): Transaction<"serializable"> {
  return db.createTransaction("serializable", prepare);
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

/**
 * Where a {@link Db} is in its life. It only moves forward:
 *
 * - `open`: it reads and writes. A graceful shutdown (`waitForSync`) drains
 *   here first: while it waits for pending writes to upload, the Db takes no
 *   new work, but its connection stays up and keeps recovering, since the
 *   upload needs it. A drain that fails or is cancelled rejects with a
 *   {@link GracefulShutdownSyncError} and leaves the Db open: the shutdown
 *   never started, so nothing moved back, and the app may retry it (an
 *   offline sign-out does).
 * - `closing`: teardown started. Final: nothing reopens the Db, and its
 *   connection manager stops recovering its connection.
 * - `closed`: teardown ended. If it failed, the shutdown promise says so;
 *   the Db stays closed either way.
 */
export type DbPhase = "open" | "closing" | "closed";

export class Db {
  private config: DbConfig;
  private readonly runtimeSource: AnyRuntimeSource;
  private readonly authStateStore;
  private connection: ConnectionManager;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshOwned = false;
  private readonly localFirstRenewals = new AuthRenewalBackoff();
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  /** See {@link DbPhase}. Only {@link beginTeardown} and its end move it. */
  private phase: DbPhase = "open";
  /** A graceful shutdown waiting for pending writes, while still `open`. */
  private gracefulDrain: {
    done: Promise<void>;
    cancel(): void;
    /** A plain shutdown arrived meanwhile: a failed drain still tears down. */
    teardownAnyway: boolean;
  } | null = null;
  /** Teardown's outcome, from the moment the Db enters `closing`. */
  private teardown: Promise<void> | null = null;
  /** Set once the session that owned this Db moved on to another one. */
  private replaced: DbReplacedError | null = null;
  /** Ends each live subscription with a terminal error. */
  private readonly liveSubscriptionEnders = new Set<(error: Error) => void>();
  /** The Db this one hands its work to after the local-first upgrade. */
  private successor: Db | null = null;
  /** Queries and listeners that move to the successor with the upgrade. */
  private readonly movable = new Set<{ move(next: Db): void }>();
  /** The schema this Db's client was opened with. */
  private clientSchema: WasmSchema | null = null;
  /** How many writes this Db took, numbering each (see {@link failedWait}). */
  private writesMade = 0;
  /** Writes waiting for the local-first client now. */
  private writesWaiting = 0;
  /**
   * Writes made after a refused callback transaction settled wait on this
   * until its rerun starts (see {@link rerunOnLocalFirst}).
   */
  private heldWrites: Promise<void> | null = null;
  /**
   * The last wait for the local-first client that failed, and how many
   * writes had been made by then: an encrypted write queued behind that one
   * fails with the same error rather than waiting again.
   */
  private failedWait: { error: unknown; writesMade: number } | null = null;
  /** End waits for the local-first client: a sign-out or an account switch. */
  private readonly localFirstWaitEnders = new Set<(error: Error) => void>();
  private readonly shutdownAbort = new AbortController();
  private syncStatusSnapshot: SyncStatus | null = null;
  private pendingUploadRowsSnapshots = new Map<string, readonly string[] | null>();
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
   * @internal Opens the runtime client for `schema` now rather than at the
   * first query: an encrypted app starts verifying its account at open.
   */
  openClientFor(schema: WasmSchema): void {
    this.getClient(schema);
  }

  /**
   * End-to-end encryption: device approval, recovery and the reports of rows
   * that could not be read. Present on every `Db`; for an app that declares
   * no encryption, using it throws `EncryptionNotDeclaredError`. Reading and
   * writing encrypted columns needs nothing from it.
   */
  get encryption(): Encryption {
    // A thin Db that upgraded is a handle on its local-first successor, which holds the runtime.
    const live = this.current();
    if (live !== this) return live.encryption;
    const real = this.runtimeSource?.encryptionHandle();
    if (real && this.clientSchema)
      return (this.boundEncryption ??= encryptionForDb(real, this, this.clientSchema));
    if (real) return real;
    return this.thinEncryption() ?? notDeclaredEncryption();
  }

  private boundEncryption: Encryption | undefined;
  private pendingEncryption: Encryption | undefined;
  private encryptedQueries?: EncryptedGuard;
  private encryptedReadsReady?: Promise<void>;
  private encryptionReadable = false;
  private readonly successorWaiters = new Set<(next: Db) => void>();

  /**
   * @internal Runs `then` with the Db that does this one's work once it is
   * local-first, however that comes about; never starts the upgrade. At once
   * if it already is. Returns how to cancel.
   */
  whenLocalFirst(then: (next: Db) => void): () => void {
    const live = this.current();
    if (live !== this) {
      then(live);
      return () => {};
    }
    this.successorWaiters.add(then);
    // A Db that closes or is replaced without upgrading never calls it.
    const stopOnShutdown = this.onShutdown(() => void this.successorWaiters.delete(then));
    const cancel = () => {
      stopOnShutdown();
      this.successorWaiters.delete(then);
    };
    return cancel;
  }

  /** Whether this Db's app declares an end-to-end encrypted column. */
  private declaresEncryption(): boolean {
    const app = this.config?.encryption?.app;
    if (!app) return false;
    const declared = declaredEntries(resolveSchemaSource(app)) ?? {};
    return Object.values(declared).some((entry) => Object.keys(entry.columns).length > 0);
  }

  /**
   * A thin `Db` of an app that declares encryption has no handle of its own:
   * this one waits for the local-first client like any operation, then calls
   * the real one. Taken once, so a stored handle is the one in use later.
   */
  private thinEncryption(): Encryption | undefined {
    if (this.pendingEncryption) return this.pendingEncryption;
    const app = this.config?.encryption?.app;
    if (!app || !isThinDb(this) || !declaresEncryption(app)) return undefined;
    const entries = Object.entries(declaredEntries(resolveSchemaSource(app)) ?? {});
    const table =
      entries.find(([, entry]) => Object.keys(entry.columns).length > 0)?.[0] ??
      entries[0]?.[0] ??
      "__e2ee_members";
    const refusal = (operation: string, reason: "no-loader" | "direct-thin") =>
      new EncryptionNeedsLocalFirstError({ reason, operation, table });
    const upgradable = () => hasLocalFirstUpgrader(this);
    return (this.pendingEncryption = pendingEncryption({
      resolve: async (timeoutMs) => {
        const live = this.current();
        if (live !== this) return live.encryption;
        const refused = refusal("encryption", upgradable() ? "no-loader" : "direct-thin");
        // Without a loader this throws the refusal; without an upgrader it is the direct-thin advice.
        const waiting = this.awaitLocalFirst(refused);
        if (timeoutMs === undefined || timeoutMs >= LOCAL_FIRST_WAIT_MS)
          return (await waiting).encryption;
        // The caller's shorter timeout: the wait itself still ends at its own cap.
        waiting.catch(() => undefined);
        let timer: ReturnType<typeof setTimeout> | undefined;
        const early = new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(
                new EncryptionNeedsLocalFirstError({
                  reason: "timeout",
                  operation: "encryption",
                  table,
                }),
              ),
            Math.max(0, timeoutMs),
          );
        });
        try {
          return (await Promise.race([waiting, early])).encryption;
        } finally {
          clearTimeout(timer);
        }
      },
      live: () => {
        const live = this.current();
        if (live !== this) return live.encryption;
        if (this.replaced) throw this.replaced;
        if (this.phase !== "open") throw new DbClosedError(this.phase);
        return undefined;
      },
      refuseSync: (operation) => {
        const refused = refusal(operation, upgradable() ? "no-loader" : "direct-thin");
        const loader = localFirstLoaderOf(this);
        if (!loader) return refused;
        // Handed over: the real handle answers, so there is nothing to load.
        if (this.successor)
          return new EncryptionNeedsLocalFirstError({ reason: "not-loaded", operation, table });
        // Start the load so that a second try, or `await db.encryption.ready()`, finds it done.
        void upgradeDbToLocalFirst(this, loader).catch(() => {});
        return new EncryptionNeedsLocalFirstError({ reason: "not-loaded", operation, table });
      },
      whenLocalFirst: (attach) => this.whenLocalFirst((next) => attach(next.encryption)),
    }));
  }

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
    // The Inspector's own Db reads the page's data; it is not one of the page's Dbs.
    if (!inspectorAttachmentRequested) {
      const unregister = registerPageDb(this, config.appId ?? null);
      this.shutdownAbort.signal.addEventListener("abort", unregister, { once: true });
    }
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
      get phase() {
        return thisDb.phase;
      },
      get draining() {
        return thisDb.gracefulDrain !== null;
      },
      markUnauthenticated: (reason) => this.markUnauthenticated(reason),
      renewsCredential: (reason) =>
        (credentialRenewers.get(this) ?? 0) > 0 ||
        (reason === "expired" && this.localFirstRefreshOwned),
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
    if (!this._localFirstSecret || this.stopping) return;

    try {
      const ttlSeconds = 3600;
      const newToken = this.mintLocalFirstToken(
        this._localFirstSecret,
        clientScope(this.config),
        ttlSeconds,
      );
      const trustedReservedSession = internalSessionFromVerifiedReservedJwtPayload(
        parseJwtPayload(newToken) ?? {},
        "device-account",
      );
      if (!trustedReservedSession) {
        throw new Error("Minted device account token is missing its reserved session identity");
      }
      this.applyAuthUpdate(newToken, trustedReservedSession);
      this.scheduleLocalFirstRefresh(ttlSeconds);
    } catch (e) {
      console.error("Failed to refresh device account token:", e);
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
    this.connection.updateAuth({
      mode: "bearer",
      jwtToken,
      trustedReservedSession,
    });
    this.publishAuthUpdate(jwtToken, trustedReservedSession);
    return true;
  }

  private publishAuthUpdate(jwtToken?: string, trustedReservedSession?: Session): void {
    const nextConfig = {
      ...this.config,
      jwtToken,
      cookieSession: undefined,
    } as DbConfig;
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
    const client = this.connection.getClient(schema);
    this.clientSchema ??= schema;
    return client;
  }

  protected getCurrentClient(): JazzClient | null {
    return this.connection.getCurrentClient();
  }

  /** @internal The runtime behind this Db's client, once it exists. */
  currentRuntime(): Runtime | null {
    return this.getCurrentClient()?.getRuntime() ?? null;
  }

  /** @internal The runtime attached to the membership table's schema. */
  runtimeForEncryption(schema: WasmSchema): Runtime {
    return this.getClient(schema).getRuntime();
  }

  /**
   * @internal Read each runtime query at the remote tier, so that this Db's
   * store holds what the server holds for it before anything subscribes.
   */
  async prefetchRuntimeQueries(schema: WasmSchema, queryJsons: readonly string[]): Promise<void> {
    const client = this.getClient(schema);
    await this.ensureReady("server");
    await Promise.all(queryJsons.map((queryJson) => client.query(queryJson, { tier: "remote" })));
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
    return this.readied(handle);
  }

  /** `result`, a write's handle, waits for this Db to be ready (see `setWriteWaitReadiness`). */
  private readied<T>(result: T): T {
    if (!(result instanceof WriteHandle)) return result;
    return setWriteWaitReadiness(result, async (tier) => {
      // A write made before this Db handed over to a successor waits on its
      // own runtime's last uploads, which the drain keeps going.
      if (this.drain) return;
      await this.ensureReady(tier === "global" ? "upload" : "runtime");
    });
  }

  /**
   * `handle` is for a write held back behind earlier ones: it waits as the
   * handle the write returns once it ran, on whichever client ran it. A
   * failure before it ran that no wait takes is reported as a mutation
   * error, as an unsent thin write's is.
   */
  private heldBack<THandle extends WriteHandle<unknown, unknown>>(
    handle: THandle,
    written: Promise<WriteHandle<unknown, unknown>>,
  ): THandle {
    return waitAsWritten(handle, written, (error) =>
      this.handleMutationError(unmadeWriteEvent(error, ++this.writesMade)),
    );
  }

  /**
   * Run `write` after the writes this Db took before it
   * (`runAfterPreparation`), on the client that does this Db's work by the
   * time it runs: one held back across the local-first upgrade runs on the
   * successor, ahead of the writes made there since. One the thin client
   * refuses because it touches an end-to-end encrypted column waits for the
   * local-first client (starting it) and runs there; without a loader, the
   * refusal is thrown.
   */
  private runWrite<T>(
    schema: WasmSchema,
    write: (client: JazzClient, db: Db) => T | Promise<T>,
  ): T | Promise<T> {
    if (this.heldWrites)
      // By the time it runs, this Db may have handed over to its successor.
      return this.afterHeldWrites(() =>
        this.successor
          ? this.runOnSuccessor(() => this.runOnCurrent(schema, write))
          : this.runWriteNow(schema, write),
      );
    return this.runWriteNow(schema, write);
  }

  private runWriteNow<T>(
    schema: WasmSchema,
    write: (client: JazzClient, db: Db) => T | Promise<T>,
  ): T | Promise<T> {
    const client = this.getClient(schema);
    const made = ++this.writesMade;
    // Made before the hand-over: if it runs only after it, that is on a
    // local-first client that may not have synced yet.
    const madeBefore = this.successor === null;
    return runAfterPreparation(client, () => {
      if (this.successor)
        return madeBefore
          ? this.runOnSuccessor(() => this.runOnCurrent(schema, write))
          : this.runOnCurrent(schema, write);
      const retry = (error: unknown) => this.retryOnLocalFirst(error, made, schema, write);
      let result: T | Promise<T>;
      try {
        result = write(client, this);
      } catch (error) {
        return retry(error);
      }
      return result instanceof Promise ? result.catch(retry) : result;
    });
  }

  /** Whether a write made now runs later: behind the client's queue, or held. */
  private writeDeferred(client: JazzClient): boolean {
    return this.heldWrites !== null || getOwnerPreparation(client)?.pending !== undefined;
  }

  /** Run `write` on the Db that does this one's work now, not behind its queue. */
  private runOnCurrent<T>(
    schema: WasmSchema,
    write: (client: JazzClient, db: Db) => T | Promise<T>,
  ): T | Promise<T> {
    const db = this.current();
    const client = db.getClient(schema);
    return client.retryAfterKey(() => write(client, db)) as T | Promise<T>;
  }

  /**
   * Run `run`, work made while this Db was thin (a write, a callback
   * transaction's rerun, or a read), on the local-first client it handed
   * over to. That client refuses encrypted tables (`not_observed`) until it
   * has synced their catalogue identities once, which can land after the
   * hand-over, and after the account's verification that a wait for it
   * includes. A refused run committed nothing, so it is tried again until
   * `deadline`: the 30 seconds the wait for the local-first client promises.
   */
  private runOnSuccessor<T>(
    run: () => T | Promise<T>,
    deadline = Date.now() + LOCAL_FIRST_WAIT_MS,
  ): T | Promise<T> {
    const retry = (error: unknown): Promise<T> => {
      if (nativeCoreErrorCode(error) !== "not_observed" || Date.now() >= deadline) throw error;
      return new Promise<void>((resolve) => setTimeout(resolve, NOT_OBSERVED_RETRY_MS)).then(() =>
        this.runOnSuccessor(run, deadline),
      );
    };
    let result: T | Promise<T>;
    try {
      result = run();
    } catch (error) {
      return retry(error);
    }
    return result instanceof Promise ? result.catch(retry) : result;
  }

  private retryOnLocalFirst<T>(
    error: unknown,
    made: number,
    schema: WasmSchema,
    write: (client: JazzClient, db: Db) => T | Promise<T>,
  ): Promise<T> {
    if (!(error instanceof EncryptionNeedsLocalFirstError)) throw error;
    const failed = this.failedWait;
    // Queued behind a write whose wait failed: it fails alike.
    if (failed && made <= failed.writesMade) throw failed.error;
    const deadline = Date.now() + LOCAL_FIRST_WAIT_MS;
    const waited = this.awaitLocalFirst(error);
    this.writesWaiting++;
    return waited
      .then(
        () => this.runOnSuccessor(() => this.runOnCurrent(schema, write), deadline),
        (failure: unknown) => {
          this.failedWait = { error: failure, writesMade: this.writesMade };
          throw failure;
        },
      )
      .finally(() => {
        this.writesWaiting--;
      });
  }

  /**
   * Wait for the local-first client after the thin one refused `refused`,
   * which touches an end-to-end encrypted column, and start loading it if
   * it isn't: resolves with the Db that does this one's work then, once it
   * has verified the account. Throws `refused` at once when no loader is
   * known. A failed load, the deadline, a sign-out, an account switch or a
   * close ends the wait. A live query passes `deadline: false`: it waits
   * for the hand-over only, with no deadline.
   */
  private awaitLocalFirst(refused: EncryptionNeedsLocalFirstError, deadline = true): Promise<Db> {
    const loader = localFirstLoaderOf(this);
    if (!loader) throw refused;
    return this.untilLocalFirst(refused, loader, deadline);
  }

  private untilLocalFirst(
    refused: EncryptionNeedsLocalFirstError,
    loader: LocalFirstLoader,
    deadline: boolean,
  ): Promise<Db> {
    const { operation, table, column, creatorMembership } = refused;
    const failed = (reason: "load-failed" | "timeout", cause?: unknown) =>
      new EncryptionNeedsLocalFirstError(
        { reason, operation, table, column, creatorMembership },
        cause === undefined ? undefined : { cause },
      );
    let end!: (error: Error) => void;
    const ended = new Promise<never>((_, reject) => {
      // Handed over: this Db closes, and the wait ends with the upgrade.
      end = (error) => {
        if (!this.successor) reject(error);
      };
    });
    this.localFirstWaitEnders.add(end);
    const stopOnShutdown = this.onShutdown(() =>
      end(this.replaced ?? new DbClosedError("closing")),
    );
    const started = Date.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<never>((_, reject) => {
      if (deadline) timer = setTimeout(() => reject(failed("timeout")), LOCAL_FIRST_WAIT_MS);
    });
    const upgraded = upgradeDbToLocalFirst(this, loader).catch((cause: unknown) => {
      throw this.replaced ?? failed("load-failed", cause);
    });
    for (const outcome of [ended, timedOut, upgraded]) outcome.catch(() => undefined);
    return Promise.race([upgraded, ended, timedOut])
      .then(() => {
        const next = this.current();
        if (next === this) {
          // The session moved on without upgrading this Db.
          this.assertOpen();
          throw failed("load-failed", new Error("the session did not upgrade this Db"));
        }
        if (!deadline) return next;
        // A local-first client that has just opened reads and writes
        // encrypted columns once it has verified the account, which syncs
        // what names them: that is part of the wait. If it fails, or is
        // still pending at the deadline, the operation goes ahead and gets
        // the local-first client's own answer. The deadline is this wait's
        // own: a verification already in flight may run longer.
        const left = Math.max(1, LOCAL_FIRST_WAIT_MS - (Date.now() - started));
        let cap: ReturnType<typeof setTimeout> | undefined;
        const deadlinePassed = new Promise<void>((resolve) => {
          cap = setTimeout(resolve, left);
        });
        const verified = next.encryption.ready({ timeoutMs: left }).catch(() => undefined);
        return Promise.race([verified, deadlinePassed])
          .then(() => next)
          .finally(() => clearTimeout(cap));
      })
      .finally(() => {
        clearTimeout(timer);
        this.localFirstWaitEnders.delete(end);
        stopOnShutdown();
      });
  }

  /**
   * Hold a live query the thin client refused (it reads an encrypted
   * column) until the local-first client takes over this Db's live queries,
   * which opens it there, and start loading that client. It emits nothing
   * meanwhile. At the deadline it warns once and keeps waiting: only a
   * failed load, a sign-out, an account switch or a close end it, through
   * `fail`. Returns how to stop holding it, or nothing without a loader.
   */
  private parkUntilLocalFirst(
    refused: EncryptionNeedsLocalFirstError,
    fail: (error: unknown) => void,
  ): (() => void) | undefined {
    if (!localFirstLoaderOf(this)) return undefined;
    let stopped = false;
    const warning = setTimeout(
      () =>
        console.warn(
          `A live query on \`${refused.table}\` reads end-to-end encrypted columns and is waiting for the local-first client, which has not loaded within ${LOCAL_FIRST_WAIT_MS / 1000} seconds. It stays loading until that client loads.`,
        ),
      LOCAL_FIRST_WAIT_MS,
    );
    void this.awaitLocalFirst(refused, false)
      .then(
        () => undefined,
        (error: unknown) => {
          if (!stopped) fail(error);
        },
      )
      .finally(() => clearTimeout(warning));
    return () => {
      stopped = true;
      clearTimeout(warning);
    };
  }

  /**
   * A callback transaction runs once more, on the local-first client, when
   * the thin client refused an operation that needs local-first, once that
   * client has loaded; or when this Db handed over to
   * that client while the transaction waited, so the closed thin runtime
   * ended it before anything of it was sent. At most once: `rerun` itself
   * never runs it again, except while the local-first client refuses it as
   * not yet synced (`runOnSuccessor`), which commits nothing of it.
   *
   * One made behind earlier writes on a thin client (one waiting for the
   * local-first client, or one still in the queue that may yet wait) begins
   * only after them, so what ends it shows in its commit: it resolves once
   * that commit is made, or with its rerun.
   *
   * A refused one holds this Db's writes while it waits for the
   * local-first client: writes and callback transactions made after its
   * callback settles run after the load, in their own order. The hold opens
   * when its rerun starts, or when the wait ends any other way (the load
   * failed, the deadline, sign-out, close), so nothing waits on the rerun:
   * during it, writes behave as beside any pending transaction on the
   * local-first client, where a callback transaction holds nothing. Writes
   * made while the refused callback still runs are concurrent with it and
   * aren't held; nor are reads. One refused only at its deferred commit
   * holds from when its commit fails: writes made before that, already in
   * the client's write queue, aren't held. They are held here, not in that
   * queue: the hand-over moves it ahead of the rerun.
   */
  private rerunOnLocalFirst<R extends WriteHandle<unknown, unknown>>(
    attempt: (settledRefused: () => void) => Promise<R>,
    rerun: (db: Db) => Promise<R>,
  ): Promise<R> {
    if (this.heldWrites)
      // Handed over meanwhile: its one run is on the successor.
      return this.afterHeldWrites(() =>
        this.successor
          ? this.runOnSuccessor(() => rerun(this.current()))
          : this.rerunOnLocalFirstNow(attempt, rerun),
      );
    return this.rerunOnLocalFirstNow(attempt, rerun);
  }

  private rerunOnLocalFirstNow<R extends WriteHandle<unknown, unknown>>(
    attempt: (settledRefused: () => void) => Promise<R>,
    rerun: (db: Db) => Promise<R>,
  ): Promise<R> {
    let release: (() => void) | undefined;
    // The wait is over: writes held behind it run, beside the rerun.
    const end = () => release?.();
    // On the successor, which may not have synced yet.
    const start = (deadline?: number) => {
      end();
      return Promise.resolve(this.runOnSuccessor(() => rerun(this.current()), deadline));
    };
    const again = (error: unknown): Promise<R> | undefined => {
      if (error instanceof EncryptionNeedsLocalFirstError) {
        const deadline = Date.now() + LOCAL_FIRST_WAIT_MS;
        return this.awaitLocalFirst(error).then(() => start(deadline));
      }
      if (this.successor && closedBeforeSending(error)) return start();
      return undefined;
    };
    // Decided before it is queued itself. Only an app that declares
    // encryption has writes that may yet wait: a plain app's transaction
    // resolves as it always has.
    const client = this.getCurrentClient();
    const behindWait =
      this.writesWaiting > 0 ||
      (isThinDb(this) &&
        client !== null &&
        getOwnerPreparation(client)?.pending !== undefined &&
        this.declaresEncryption());
    const holdLaterWrites = () => {
      // Without a loader it fails at once: nothing to hold writes for.
      if (!release && localFirstLoaderOf(this)) release = this.holdWrites();
    };
    const settled = attempt(holdLaterWrites).then(
      (result) =>
        behindWait
          ? result.txId.then(
              () => result,
              (error: unknown) => {
                // Refused only at its deferred commit: it holds the writes
                // made from now on while it waits, as one refused earlier.
                if (error instanceof EncryptionNeedsLocalFirstError) holdLaterWrites();
                // Otherwise its wait reports the failure, as for any commit.
                return again(error) ?? result;
              },
            )
          : result,
      (error: unknown) => {
        const rerun = again(error);
        if (!rerun) throw error;
        return rerun;
      },
    );
    // Not rerun, or the wait failed: open all the same.
    settled.then(end, end);
    return settled;
  }

  /** Hold the writes made from now on until the returned function is called. */
  private holdWrites(): () => void {
    // Handed over: the writes made from now on are the successor's.
    if (this.successor) return this.current().holdWrites();
    let open!: () => void;
    const opened = new Promise<void>((resolve) => (open = resolve));
    this.chainHeldWrites((this.heldWrites ?? Promise.resolve()).then(() => opened));
    return open;
  }

  /** Run `operation` after the writes held so far, and hold later ones behind it. */
  private afterHeldWrites<T>(operation: () => T | Promise<T>): Promise<T> {
    const ran = this.heldWrites!.then(operation);
    this.chainHeldWrites(ran);
    return ran;
  }

  private chainHeldWrites(next: Promise<unknown>): void {
    const tail = next.then(
      () => undefined,
      () => undefined,
    );
    this.heldWrites = tail;
    void tail.then(() => {
      if (this.heldWrites === tail) this.heldWrites = null;
    });
  }

  protected getAccessContext(): DbAccessContext | null {
    // Ordinary Dbs use the client's session. BackendDb overrides this to supply
    // per-Db session and attribution without changing the shared client's identity.
    return null;
  }

  private handleMutationError(event: MutationErrorEvent, logged = false): void {
    // A write this Db took before the upgrade, rejected while it uploads:
    // the app's listeners are on the successor now.
    if (this.successor) return this.successor.handleMutationError(event, logged);
    if (this.mutationErrorListeners.size === 0) {
      if (!logged) console.error("Unhandled Jazz mutation error", event);
      this.pendingMutationErrorEvents.push(event);
      return;
    }
    for (const listener of this.mutationErrorListeners) {
      listener(event);
    }
  }

  /** @internal Refresh only through the immutable handle that owns this context. */
  async refreshAccountAuth(account: AccountHandle): Promise<string> {
    return this.refreshOwnAccountAuth(account);
  }

  /**
   * @internal Renew this Db's own runtime's credential. Its account context
   * calls this rather than {@link refreshAccountAuth}, which after the
   * local-first upgrade renews the successor: the thin runtime still needs
   * its own credential to finish uploading.
   */
  async refreshOwnAccountAuth(account: AccountHandle): Promise<string> {
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
      // A renewal: never the token the server may just have refused.
      const token = await accountToken(account, accountRegistry(account), { fresh: true });
      if (this.shutdownAbort.signal.aborted) throw new Error("Account context closed");
      const reserved =
        account.identity.issuer === "urn:jazz:local-first"
          ? internalSessionFromVerifiedReservedJwtPayload(
              parseJwtPayload(token) ?? {},
              "device-account",
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
   * A short-lived JWT proving possession of the current device account: what
   * a device account sends its own backend's routes as
   * `Authorization: Bearer` (`jazz.forReqUser(request)` there). Returns
   * `null` if the current session is not a device account.
   *
   * A Db opened by a session (`createJazzClient`, a provider) doesn't hold the
   * device key: it returns the session's current sync proof, which lasts up
   * to an hour and is for this app, so `ttlSeconds` doesn't apply and any
   * other `audience` returns `null`, as does one with under 30 seconds left
   * (the session renews it). Ask for one per request, and don't log or
   * forward it: until it expires it also opens this account's sync.
   */
  getDeviceAccountProof(options?: { ttlSeconds?: number; audience?: string }): string | null {
    if (!this._localFirstSecret) {
      const token = this.config.jwtToken;
      const payload = token ? parseJwtPayload(token) : null;
      if (
        !token ||
        payload?.iss !== LOCAL_FIRST_JWT_ISSUER ||
        typeof payload.exp !== "number" ||
        // One about to expire could lapse on its way to the backend.
        payload.exp <= Date.now() / 1000 + SESSION_PROOF_MIN_REMAINING_SECONDS ||
        (options?.audience !== undefined && payload.aud !== accountAppId(options.audience))
      )
        return null;
      return token;
    }

    const ttl = options?.ttlSeconds ?? 60;
    const audience = options?.audience ?? clientScope(this.config);
    return this.mintLocalFirstToken(this._localFirstSecret, audience, ttl);
  }

  /** @internal This Db's own runtime's auth state changes, which never move to a successor. */
  onOwnAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.authStateStore.onChange((state) => listener(state));
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.registerMovable((db) =>
      db.authStateStore.onChange((state) => {
        listener(state);
      }),
    );
  }

  /**
   * Register something on this Db that moves to its successor when the
   * session upgrades it to the local-first client, so code holding the
   * returned handle keeps hearing from whichever Db does the work.
   */
  private registerMovable(
    register: (db: Db) => () => void,
    // Register again on this Db rather than on its successor: for what this
    // Db answers itself after the upgrade (its sync status covers the writes
    // its own runtime is still uploading).
    reregisterHere = false,
  ): () => void {
    let detach = register(this);
    let closed = false;
    const entry = {
      move: (next: Db) => {
        if (closed) return;
        detach();
        detach = reregisterHere ? register(this) : next.registerMovable(register);
      },
    };
    this.movable.add(entry);
    return () => {
      if (closed) return;
      closed = true;
      this.movable.delete(entry);
      detach();
    };
  }

  /**
   * Attach a fallback listener for write rejections that are not handled by an
   * active {@link WriteHandle.wait} call.
   *
   * @returns an unsubscribe callback
   */
  onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    if (this.replaced) throw this.replaced;
    return this.registerMovable((db) => {
      db.mutationErrorListeners.add(listener);
      while (db.pendingMutationErrorEvents.length > 0) {
        listener(db.pendingMutationErrorEvents.shift()!);
      }
      return () => {
        db.mutationErrorListeners.delete(listener);
      };
    });
  }

  /**
   * Stateless anchors and ranges into text values, such as cursors,
   * selections and comments on a text column: captured from one value and
   * found again in later ones by the text around them.
   */
  textAnchors(): TextAnchors {
    return this.runtimeSource.textAnchors();
  }

  /**
   * Whether this Db's writes have reached the server: the upstream
   * connection, and how many committed transactions the server has not
   * accepted or rejected yet. The same object is returned until the status
   * changes, so it can back `useSyncExternalStore` directly. In a browser,
   * agents and tests read the same through `window.__jazz.status()`.
   */
  getSyncStatus(): SyncStatus {
    const next = withAuthReason(this.connection.syncStatus(), this.authStateStore.getState().error);
    if (this.syncStatusSnapshot && sameSyncStatus(next, this.syncStatusSnapshot)) {
      return this.syncStatusSnapshot;
    }
    this.syncStatusSnapshot = next;
    return next;
  }

  /**
   * Call `listener` whenever {@link getSyncStatus} changes.
   *
   * @returns an unsubscribe callback
   */
  onSyncStatusChange(listener: (status: SyncStatus) => void): () => void {
    let last = this.getSyncStatus();
    const deliver = (status: SyncStatus) => {
      if (status === last || sameSyncStatus(status, last)) return;
      last = status;
      listener(status);
    };
    return this.registerMovable((db) => {
      const stop = db.syncStatusChangeRegistration(deliver);
      // Moved at the upgrade: the status now covers both runtimes.
      if (db.successor) deliver(db.getSyncStatus());
      return stop;
    }, true);
  }

  /** How this Db reports sync status changes: its own, or combined with its successor's. */
  private syncStatusChangeRegistration(listener: (status: SyncStatus) => void): () => void {
    return this.onSyncStatusChangeHere(listener);
  }

  private onSyncStatusChangeHere(listener: (status: SyncStatus) => void): () => void {
    const subscription = new AbortController();
    const end = () => subscription.abort();
    this.shutdownAbort.signal.addEventListener("abort", end, { once: true });
    subscription.signal.addEventListener(
      "abort",
      () => this.shutdownAbort.signal.removeEventListener("abort", end),
      { once: true },
    );
    let published = this.getSyncStatus();
    const publish = () => {
      const status = this.getSyncStatus();
      if (status === published) return;
      published = status;
      listener(status);
    };
    this.connection.onSyncStatusChange(publish, subscription.signal);
    // Why the server refused the credential may be known after the link
    // went offline: the status says it then (`authReason`).
    const stopAuth = this.authStateStore.onChange(publish);
    subscription.signal.addEventListener("abort", stopAuth, { once: true });
    return end;
  }

  /**
   * Ids of `table`'s rows with a write the server has neither accepted nor
   * refused yet, sorted: the rows to show as "not sent yet". In Node and
   * React Native this includes writes recovered from local storage after a
   * restart; a browser tab names its own writes, as
   * {@link SyncStatus.pendingUploads} counts them.
   *
   * `null` when this Db cannot tell (its client is not created yet, or its
   * runtime does not report rows), which is not "none pending". The same
   * array is returned until the rows change, so it can back
   * `useSyncExternalStore` directly.
   */
  getPendingUploadRows<T, Init>(table: TableProxy<T, Init>): readonly string[] | null {
    return this.pendingUploadRowsSnapshot(
      table._table,
      this.connection.pendingUploadRows(table._table),
    );
  }

  private pendingUploadRowsSnapshot(
    table: string,
    next: readonly string[] | null,
  ): readonly string[] | null {
    const last = this.pendingUploadRowsSnapshots.get(table);
    if (last !== undefined && samePendingUploadRows(last, next)) return last;
    this.pendingUploadRowsSnapshots.set(table, next);
    return next;
  }

  /**
   * Call `listener` whenever {@link getPendingUploadRows} of `table` changes.
   *
   * @returns an unsubscribe callback
   */
  onPendingUploadRowsChange<T, Init>(
    table: TableProxy<T, Init>,
    listener: (ids: readonly string[] | null) => void,
  ): () => void {
    let last = this.getPendingUploadRows(table);
    const deliver = (ids: readonly string[] | null) => {
      if (samePendingUploadRows(ids, last)) return;
      last = ids;
      listener(ids);
    };
    return this.registerMovable((db) => {
      const stop = db.pendingUploadRowsChangeRegistration(table, deliver);
      // Moved at the upgrade: the rows now cover both runtimes.
      if (db.successor) deliver(db.getPendingUploadRows(table));
      return stop;
    }, true);
  }

  /** How this Db reports pending row changes: its own, or combined with its successor's. */
  private pendingUploadRowsChangeRegistration<T, Init>(
    table: TableProxy<T, Init>,
    listener: (ids: readonly string[] | null) => void,
  ): () => void {
    return this.onPendingUploadRowsChangeHere(table, listener);
  }

  private onPendingUploadRowsChangeHere<T, Init>(
    table: TableProxy<T, Init>,
    listener: (ids: readonly string[] | null) => void,
  ): () => void {
    const subscription = new AbortController();
    const end = () => subscription.abort();
    this.shutdownAbort.signal.addEventListener("abort", end, { once: true });
    subscription.signal.addEventListener(
      "abort",
      () => this.shutdownAbort.signal.removeEventListener("abort", end),
      { once: true },
    );
    let published = this.getPendingUploadRows(table);
    // The raw upload-status signal, not `getSyncStatus`: a write that joins
    // as another settles keeps the public status equal, and on React Native
    // this subscription is what keeps the relay's store polled.
    this.connection.onSyncStatusChange(() => {
      const ids = this.getPendingUploadRows(table);
      if (ids === published) return;
      published = ids;
      listener(ids);
    }, subscription.signal);
    return end;
  }

  /**
   * Resolve once every write committed on this Db before the call has been
   * accepted by the server.
   *
   * Like `write.wait({ tier: "global" })`, the wait lasts through network
   * outages, reconnects and credential renewal. It rejects with the
   * server's error for the first of those writes it rejected, when the link
   * stops for good, with a {@link WriteWaitTimeoutError} once `timeoutMs`
   * passes, or with `signal.reason` when `signal` aborts. Ending the wait
   * never cancels the writes: they stay queued. In a browser tab it covers
   * that tab's own writes, not ones the shared worker recovered from storage.
   */
  async waitForSync(options: { timeoutMs?: number; signal?: AbortSignal } = {}): Promise<void> {
    const { timeoutMs, signal } = options;
    validateWriteWaitTimeout(timeoutMs);
    this.assertOpen();
    signal?.throwIfAborted();
    const control = new AbortController();
    const forward = () => control.abort(signal!.reason);
    signal?.addEventListener("abort", forward, { once: true });
    // Shutdown ends the wait instead of leaving it parked on a closed runtime.
    const shutdown = () => control.abort(new Error("Db was shut down"));
    this.shutdownAbort.signal.addEventListener("abort", shutdown, {
      once: true,
    });
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(
            () => control.abort(new WriteWaitTimeoutError("global", timeoutMs)),
            timeoutMs,
          );
    try {
      await this.waitForUpstreamSync(control.signal);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      signal?.removeEventListener("abort", forward);
      this.shutdownAbort.signal.removeEventListener("abort", shutdown);
    }
  }

  /** @internal How {@link waitForSync} waits; a backend `Db` waits on its client. */
  protected waitForUpstreamSync(signal: AbortSignal): Promise<void> {
    return this.connection.waitForSync(signal);
  }

  getConfig(): DbConfig {
    // Return a copy without internal live transport handles. MessagePorts are
    // neither configuration nor cloneable unless transferred.
    const {
      browserWorkerPort: _browserWorkerPort,
      browserWorkerSession: _browserWorkerSession,
      ...runtimeSources
    } = this.config.runtimeSources ?? {};
    // The encryption option holds the app's own store (functions): a live
    // handle, like the ports above, not configuration.
    const { encryption: _encryption, ...config } = this.config;
    return structuredClone({
      ...config,
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
    if (this.stopping) {
      throw new Error("Cannot disconnect a Db that is shutting down.");
    }

    this.offlineRequested = true;
    await this.connection.disconnect();
  }

  /**
   * Reconnect this Db to its configured Jazz sync server after
   * {@link disconnect}.
   */
  async reconnect(): Promise<void> {
    // Sync recovery is safe before teardown starts; it must remain available
    // while a graceful transition waits for previously committed writes.
    if (this.phase !== "open") {
      throw new Error("Cannot reconnect a Db that is shutting down.");
    }

    this.offlineRequested = false;
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
    return this.registerMovable((db) => {
      db.activeQuerySubscriptionTraceListeners.add(listener);
      listener(db.getActiveQuerySubscriptions());
      return () => {
        db.activeQuerySubscriptionTraceListeners.delete(listener);
      };
    });
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
    let id = options?.id;
    const transformedData = table._columnTransforms
      ? transformInputColumns(table, data)
      : (data as Record<string, unknown>);
    const values = toWriteRecordForOperation(
      "Insert",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getAccessContext();
    const deferred = this.writeDeferred(client);
    const encrypted = declaredEntries(table._schema);
    // The row ID can also name its encryption space. Keep it across the first
    // attempt and any retries, including one that starts key preparation.
    if (
      !id &&
      (deferred ||
        (encrypted &&
          (Object.hasOwn(encrypted, table._table) ||
            Object.values(encrypted).some((entry) => entry.scope === table._table))))
    ) {
      id = reserveRowId();
    }
    const value = deferred ? immediateWriteRow(table, id!, values) : undefined;
    const output = (row: WasmRow) =>
      transformOutputRow<T>(table, transformRow(row, table._schema, table._table));
    const inserted = this.runWrite(table._schema, (client, db) =>
      db.readied(
        client.insert(
          table._table,
          values,
          normalizeSelectorOptions(table._schema, table._table, id ? { ...options, id } : options),
          context?.writeSession,
          context?.attribution,
        ),
      ),
    );
    return this.wrapWriteWait(
      inserted instanceof Promise
        ? this.heldBack(
            new WriteResult(
              // Waiting for the local-first client: a thin one can't
              // preview, and the row's id was reserved.
              value ??
                (isThinDb(this)
                  ? immediateWriteRow(table, id!, values)
                  : output(client.previewInsertInternal(table._table, values, id))),
              inserted.then((result) => result.txId),
              client,
            ),
            inserted,
          )
        : inserted.mapValue(output),
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
    this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    rejectComputedWrites(table, ordinaryData);
    notifyLocalWrite(this);
    const id = options?.id;
    return this.runWrite(table._schema, async (client) => {
      const transformedData = transformInputColumns(table, ordinaryData);
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
    this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    rejectComputedWrites(table, ordinaryData);
    notifyLocalWrite(this);
    const context = this.getAccessContext();
    return this.runWrite(table._schema, async (client) => {
      const transformedData = transformInputColumns(table, ordinaryData);
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
    this.getClient(table._schema);
    const { column, source, values: ordinaryData } = splitStreamingMutation(table, data);
    rejectComputedWrites(table, ordinaryData);
    notifyLocalWrite(this);
    const context = this.getAccessContext();
    return this.runWrite(table._schema, async (client) => {
      const transformedData = transformInputColumns(table, ordinaryData);
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
      ? transformInputColumns(table, data)
      : (data as Record<string, unknown>);
    const values = toWriteRecordForOperation(
      "Restore",
      transformedData,
      table._schema,
      table._table,
    );
    const context = this.getAccessContext();
    const deferred = this.writeDeferred(client);
    const value = deferred ? immediateWriteRow(table, id, values) : undefined;
    const output = (row: WasmRow) =>
      transformOutputRow<T>(table, transformRow(row, table._schema, table._table));
    const restored = this.runWrite(table._schema, (client, db) =>
      db.readied(
        client.restore(
          table._table,
          id,
          values,
          normalizeSelectorOptions(table._schema, table._table, options),
          context?.writeSession,
          context?.attribution,
        ),
      ),
    );
    return this.wrapWriteWait(
      restored instanceof Promise
        ? this.heldBack(
            new WriteResult(
              value ??
                (isThinDb(this)
                  ? immediateWriteRow(table, id, values)
                  : output(client.previewInsertInternal(table._table, values, id))),
              restored.then((result) => result.txId),
              client,
            ),
            restored,
          )
        : restored.mapValue(output),
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
    stage?: (values: ReturnType<typeof toWriteRecord>, client: JazzClient) => WriteHandle,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    rejectComputedWrites(table, data);
    if (this.writeDeferred(client)) data = { ...data };
    const context = this.getAccessContext();
    const normalized = normalizeViewOptions(table._schema, table._table, options);
    const result = this.runWrite(table._schema, (client, db) => db.readied(mutate(client)));
    function mutate(client: JazzClient): WriteHandle | Promise<WriteHandle> {
      const submit = (record: Record<string, unknown>) => {
        const values = toWriteRecordForOperation(operation, record, table._schema, table._table);
        if (stage) return stage(values, client);
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
      const record = transformInputColumns(table, data);
      if (operation !== "Update") return submit(record);
      try {
        return submit(record);
      } catch (error) {
        // An update stages against the row's current cells. When this
        // replica has not loaded the row yet (a fresh process, another
        // instance), read it through the caller's session and stage again;
        // later writes wait behind this one, so their order holds.
        if (nativeCoreErrorCode(error) !== "not_observed") throw error;
        // The read's row count is not the answer: this replica may filter
        // out a row it now holds, lacking the rows that grant the session
        // access, and only the authority decides whether the session may
        // write it. Stage again whatever came back.
        return loadRow(client, table, id, context?.readSession, normalized?.branch).then(() => {
          try {
            return submit(record);
          } catch (retryError) {
            // Still not held: the server sent no row, so it does not exist
            // or the session may not read it, and the two are refused alike,
            // with the denial's code, as the authority answers.
            if (nativeCoreErrorCode(retryError) !== "not_observed") throw retryError;
            throw unreadableTargetError("Update", table._table);
          }
        });
      }
    }
    return this.wrapWriteWait(
      result instanceof Promise
        ? this.heldBack(
            new WriteHandle(
              result.then((handle) => handle.txId),
              client,
            ),
            result,
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
    options?: UpdateEditOptions<Partial<Init>>,
  ): WriteHandle;
  update(
    table: TableProxy<any, any, any, any, any>,
    id: string,
    data: Record<string, unknown>,
    options?: LoweredUpdateOptions,
  ): WriteHandle {
    assertWritableTable(table);
    this.getClient(table._schema);
    const { replacements, descriptors } = lowerUpdate(table, data, options);
    options = withoutEditedFrom(options);
    const context = this.getAccessContext();
    if (descriptors.length > 0) {
      return this.prepareMutation(table, id, replacements, "Update", options, (updates, client) =>
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
    const result = this.runWrite(table._schema, (client, db) =>
      db.readied(
        client.delete(
          table._table,
          id,
          normalizeViewOptions(table._schema, table._table, options),
          context?.writeSession,
          context?.attribution,
        ),
      ),
    );
    return this.wrapWriteWait(
      result instanceof Promise
        ? this.heldBack(
            new WriteHandle(
              result.then((handle) => handle.txId),
              client,
            ),
            result,
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

  /**
   * @internal The Inspector's `explain`: for each of `ids` of `table`, which
   * clauses of its read policy (the arms of its top-level `Or`) hold for
   * this Db's session. Backend only.
   */
  explainReadClauses<T, Init>(table: TableProxy<T, Init>, ids: string[]): ExplainedRows {
    const session = this.getAccessContext()?.readSession;
    if (!session) throw new Error("explain needs a Db acting as a user");
    return this.getClient(table._schema).explainReadClauses(table._table, ids, session);
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

  /** @internal Shared construction path; admission remains owned by the transaction. */
  createTransaction<TKind extends TransactionKind>(
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
    return Transaction.createWithAdmission(
      kind,
      (schema) => this.getClient(schema),
      context,
      ownerClient ?? undefined,
      prerequisite,
      this.clientSchema,
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
    return this.rerunOnLocalFirst(
      (settledRefused) => this.transactionOnce(callback, settledRefused),
      (db) => db.transactionOnce(callback),
    );
  }

  /** {@link transaction}, run once: never again on the local-first client. */
  private transactionOnce<TResult>(
    callback: (tx: TransactionScope<"mergeable">) => TResult | Promise<TResult>,
    settledRefused?: () => void,
  ): Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").ownerClient,
      settledRefused,
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
    return this.rerunOnLocalFirst(
      (settledRefused) => this.serializableTransactionOnce(callback, settledRefused),
      (db) => db.serializableTransactionOnce(callback),
    );
  }

  /** {@link serializableTransaction}, run once: never again on the local-first client. */
  private serializableTransactionOnce<TResult>(
    callback: (tx: TransactionScope<"serializable">) => TResult | Promise<TResult>,
    settledRefused?: () => void,
  ): Promise<SerializableWriteResult<Awaited<TResult>>> {
    const transaction = this.beginSerializableTransaction();
    return runInTransaction(
      transaction,
      callback,
      () => getDbTxHandleBinding(transaction, "result").ownerClient,
      settledRefused,
    );
  }

  /**
   * Move this app from the thin client to the local-first one, loading it with
   * `loader` (for example `() => import("jazz-tools/local-first")`). Under a
   * Jazz provider the provider swaps in the local-first `Db` once it holds the
   * app's live queries, and this resolves then. A local-first `Db` resolves at
   * once. (A thin `Db` opened directly, outside a session, upgrades with
   * `upgradeToLocalFirst(thin, open)` from `jazz-tools/thin`, which returns
   * the new `Db` for the app to swap in itself.)
   */
  async upgradeToLocalFirst(loader: LocalFirstLoader): Promise<void> {
    await upgradeDbToLocalFirst(this, loader);
  }

  /**
   * Whether this `Db` reads and writes through a local store: true for a
   * local-first `Db`, and for a thin one once its session has upgraded it
   * (the same `Db` carries on), false for a thin one before. To wait for the
   * upgrade, `await db.upgradeToLocalFirst(loader)`.
   */
  isLocalFirst(): boolean {
    return !isThinDb(this);
  }

  /**
   * Delete browser IndexedDB storage for this Db's active namespace.
   */
  async deleteClientStorage(): Promise<void> {
    await this.connection.deleteClientStorage();
  }

  /**
   * @internal How many writes the server hasn't confirmed in this Db's
   * store: every tab's in the browser, where the worker can say; otherwise
   * this Db's own.
   */
  async storePendingUploads(): Promise<number> {
    const store = await this.connection.storePendingUploads().catch(() => undefined);
    return store ?? this.getSyncStatus().pendingUploads;
  }

  /**
   * @internal Whether this Db still reads and writes: it isn't shutting down
   * or replaced, and its storage wasn't reset under it. A session whose
   * sign-out or sign-in failed keeps a usable Db rather than ending in error.
   */
  isUsable(): boolean {
    return !this.stopping && !this.replaced && this.connection.isUsable();
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
   * Reads of encrypted columns wait up to 30 seconds for encryption readability.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    return this.#readOnce(query, options, Date.now() + LOCAL_FIRST_WAIT_MS);
  }

  async #readOnce<T>(
    query: QueryBuilder<T>,
    options: QueryOptions | undefined,
    deadline: number,
  ): Promise<T[]> {
    try {
      return await this.allInternal(query, lowerPublicDbQueryOptions(options), deadline);
    } catch (error) {
      // On the successor, which may not have synced yet.
      const answer = () =>
        this.runOnSuccessor(() => this.current().#readOnce(query, options, deadline), deadline);
      // Begun before this Db handed over to a successor, and ended by that
      // (its own runtime closing, or this Db draining before the read
      // reached it): the successor answers it. Any other error is the
      // read's own answer.
      if (this.successor && (isRuntimeClosed(error) || error instanceof DbClosedError))
        return answer();
      // The thin client refused it, as it reads an encrypted column: the
      // local-first client answers it once loaded.
      if (error instanceof EncryptionNeedsLocalFirstError) {
        await this.awaitLocalFirst(error);
        return answer();
      }
      throw error;
    }
  }

  #encryptionReadiness(wasmQuery: string): Promise<void> | undefined {
    const app = this.config?.encryption?.app;
    if (!app || isThinDb(this) || this.encryptionReadable) return;
    this.encryptedQueries ??= new EncryptedGuard(resolveSchemaSource(app));
    if (!this.encryptedQueries.queryTouches(wasmQuery)) return;
    return (this.encryptedReadsReady ??= this.encryption.readable().then(
      () => {
        this.encryptionReadable = true;
      },
      (error: unknown) => {
        this.encryptedReadsReady = undefined;
        throw error;
      },
    ));
  }

  private async allInternal<T>(
    query: QueryBuilder<T>,
    options?: InternalDbQueryOptions,
    encryptionDeadline?: number,
  ): Promise<T[]> {
    const client = this.getClient(query._schema);
    const preparation = getOwnerPreparation(client);
    // A write waiting for the local-first client isn't made yet, so this
    // read couldn't see it anyway: it doesn't wait behind it.
    if (preparation?.pending && this.writesWaiting === 0) await preparation.drain();
    // A newly attached browser-worker follower learns the namespace-wide
    // connection state during its init handshake. The core read gate needs
    // that state before it decides whether the first load may wait.
    const mayWaitForServer =
      resolveEffectiveQueryExecutionOptions(this.config, {
        tier: options?.tier,
        firstLoadRemoteWaitMs: options?.firstLoadRemoteWaitMs,
      }).firstLoadRemoteWaitMs !== undefined;
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
    const readable =
      encryptionDeadline === undefined ? undefined : this.#encryptionReadiness(wasmQuery);
    if (encryptionDeadline !== undefined && readable) {
      const remaining = Math.max(0, encryptionDeadline - Date.now());
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const timeout = new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(
                Object.assign(
                  new Error(
                    "NotObserved: encrypted tables cannot be read or written before this client has synced their catalogue identities once",
                  ),
                  { code: "not_observed" },
                ),
              ),
            remaining,
          );
        });
        await Promise.race([readable, timeout]);
      } finally {
        clearTimeout(timer);
      }
    }
    await this.ensureReady(effectiveTier === "remote" ? "server" : "runtime");
    const rows =
      context || usesRelationTraversal
        ? await client.queryInternal(wasmQuery, queryOptions, context?.readSession)
        : await client.queryInternal(wasmQuery, queryOptions);
    return finishRead(query, builtQuery, outputTable, prepared, rows, (physical) =>
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
        const splices = new Map<string, ValueSplices>();
        for (const change of update.delta) {
          if (change.kind === RowChangeKind.Updated && change.splices) {
            splices.set(change.id, change.splices);
          }
        }
        onUpdate(update.all, {
          splices,
          ...(update.reset && update.reason ? { reason: update.reason } : {}),
        });
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
    if (this.replaced) throw this.replaced;
    const { onDelta, onError } =
      typeof callbacks === "function" ? { onDelta: callbacks, onError: undefined } : callbacks;
    let ready: Promise<void> | undefined;
    let first = true;
    const fail = (error: Error) => {
      if (onError) onError(error);
      else console.error("Unhandled Jazz subscription error", error);
    };
    const stop = this.registerMovable((db) => {
      // On the successor the query starts over: its opening result replaces
      // what the subscriber has, rows the upgrade already read in included.
      let opening = !first;
      first = false;
      let parked: (() => void) | undefined;
      const handle = db.subscribeDeltaHere(
        query,
        {
          onDelta: (delta) => {
            if (opening) {
              opening = false;
              onDelta({
                delta: delta.delta,
                all: delta.all ?? [],
                reset: true,
              });
              return;
            }
            onDelta(delta);
          },
          onError: (error) => {
            // The thin client refused it, as it reads an encrypted column:
            // it waits, with nothing emitted, for the local-first client,
            // which opens it when it takes over this Db's live queries.
            if (error instanceof EncryptionNeedsLocalFirstError) {
              parked = db.parkUntilLocalFirst(error, (failure) => {
                stop();
                fail(toError(failure));
              });
              if (parked) return;
            }
            fail(error);
          },
        },
        options,
        session,
      );
      ready ??= handle.ready;
      return () => {
        handle();
        parked?.();
      };
    });
    const handle = (() => stop()) as SubscriptionHandle;
    if (ready) Object.defineProperty(handle, "ready", { value: ready });
    return handle;
  }

  private subscribeDeltaHere<T extends { id: string }>(
    query: QueryBuilder<T>,
    callbacks: DbDeltaSubscriptionCallbacks<T>,
    options?: InternalDbQueryOptions,
    session?: Session,
  ): SubscriptionHandle {
    const { onDelta, onError } = callbacks;
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
    if (queryOptions.splices) {
      assertSpliceableColumns(planningSchema, outputTable, query, queryOptions.splices);
      assertSplicesReadInPart(builtQuery, prepared.physical, queryOptions.splices);
    }
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
      this.liveSubscriptionEnders.delete(endLive);
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
        // A dependency found only now may make a splice column read whole.
        if (queryOptions.splices)
          assertSplicesReadInPart(builtQuery, prepared.physical, queryOptions.splices);
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
          if (!this.stopping) startNativeSubscription(replacement);
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
      this.liveSubscriptionEnders.delete(endLive);
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
        if (unsubscribed || terminalized || activeSubscription === null || this.stopping) {
          return;
        }
        deliveryReady = true;
        for (const delta of bufferedDeltas.splice(0)) {
          if (unsubscribed || terminalized || activeSubscription === null) return;
          deliver(delta);
        }
      })
      .catch((error: unknown) => {
        if (unsubscribed || terminalized || activeSubscription === null || this.stopping) {
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
    const endLive = (error: Error) => {
      if (activeSubscription !== null) terminalizeSubscription(activeSubscription, error);
    };
    this.liveSubscriptionEnders.add(endLive);
    // The native maintained stream owns both the opening snapshot and later
    // changes. Do not fabricate an empty opening or race it with a one-shot
    // cache read: that snapshot may be older than deltas already delivered.
    const requirement = queryOptions.tier === "remote" ? "server" : "runtime";
    // Wait for catalogue identities and held keys before opening a live query.
    const encryptionReady = this.#encryptionReadiness(wasmQuery);
    const connectionReady = this.connection.shouldDeferSubscriptionStart(requirement)
      ? this.ensureReady(requirement, readyAbort.signal)
      : undefined;
    if (connectionReady || encryptionReady) {
      void Promise.all([connectionReady, encryptionReady])
        .then(() => startNativeSubscription(initialSubscription))
        .catch((error: unknown) => {
          if (unsubscribed || readyAbort.signal.aborted || this.stopping) return;
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
   * @internal The session that owned this Db replaced it. Its live queries
   * end with a {@link DbReplacedError} naming the fix, and its reads, writes
   * and listeners throw it, so code that kept the handle fails loudly instead
   * of going silent.
   */
  markReplaced(reason: DbReplacedReason): void {
    this.successor?.markReplaced(reason);
    if (this.replaced) return;
    const error = new DbReplacedError(reason);
    this.replaced = error;
    for (const end of Array.from(this.liveSubscriptionEnders)) end(error);
    this.liveSubscriptionEnders.clear();
    for (const end of Array.from(this.localFirstWaitEnders)) end(error);
  }

  /**
   * @internal The session upgraded this Db to `next`, the local-first Db of
   * the same account. This Db becomes a handle on `next`: its live queries
   * and listeners move there (each query's first result there replaces what
   * it had, so nothing goes back to loading), and its reads and writes go
   * there from now on. Only its own runtime stays behind, for the session to
   * shut down once its writes are uploaded.
   */
  adoptSuccessor(next: Db): void {
    if (this.successor) throw new Error("This Db already has a successor");
    const own = this.currentRuntime();
    // Writes this Db still holds back (waiting for the local-first client,
    // or queued behind one that is) run on `next` first, then those made on
    // `next` from now on.
    const ownClient = this.getCurrentClient();
    const heldBack = ownClient ? getOwnerPreparation(ownClient)?.pending : undefined;
    if (heldBack && this.clientSchema)
      void ownerPreparation(next.getClient(this.clientSchema)).run(() => heldBack);
    // Writes held behind a refused callback transaction: those made on
    // `next` from now on wait for them too. Not in `next`'s write queue,
    // where the transaction's rerun, whose start opens them, will queue.
    const held = this.heldWrites;
    if (held) next.chainHeldWrites((next.heldWrites ?? Promise.resolve()).then(() => held));
    this.successor = next;
    for (const waiter of Array.from(this.successorWaiters)) waiter(next.current());
    this.successorWaiters.clear();
    // A read still waiting for this runtime's server (the server is down)
    // asks `next` instead: only the drain below waits for that server.
    own?.endUnsentReads?.();
    // Upload what this runtime still holds, then stop it. Until then its
    // writes count in the sync status and in waitForSync. A stream still
    // sending its value finishes first, unless the app shuts down.
    const streams = this.letStreamsFinish();
    let draining = true;
    const drainListeners = new Set<() => void>();
    const drained = this.shutdown({ waitForSync: true }).finally(() => {
      draining = false;
      for (const listener of Array.from(drainListeners)) listener();
    });
    void drained.catch(() => {});
    this.drain = {
      done: drained,
      endStreamWait: () => streams.abort(),
      abort: () => {
        if (draining) this.abortGracefulShutdown();
      },
    };
    let combined: SyncStatus | null = null;
    const syncStatus = (): SyncStatus => {
      const ahead = next.getSyncStatus();
      if (!draining) return ahead;
      const own = Db.prototype.getSyncStatus.call(this);
      const status: SyncStatus = {
        link: ahead.link,
        pendingUploads: ahead.pendingUploads + own.pendingUploads,
        oldestPendingAt:
          ahead.oldestPendingAt === null || own.oldestPendingAt === null
            ? (ahead.oldestPendingAt ?? own.oldestPendingAt)
            : Math.min(ahead.oldestPendingAt, own.oldestPendingAt),
        rejected: ahead.rejected + own.rejected,
        lastError:
          own.lastError && (!ahead.lastError || own.lastError.at > ahead.lastError.at)
            ? own.lastError
            : ahead.lastError,
        synced: ahead.synced && own.pendingUploads === 0,
      };
      if (combined && sameSyncStatus(status, combined)) return combined;
      return (combined = status);
    };
    // While this Db's runtime still uploads, its rows count too; a runtime
    // that cannot name them makes the whole answer unknown.
    const combinedRows = new Map<string, readonly string[] | null>();
    const pendingUploadRows = <T, Init>(table: TableProxy<T, Init>): readonly string[] | null => {
      const ahead = next.getPendingUploadRows(table);
      if (!draining) return ahead;
      const own = Db.prototype.getPendingUploadRows.call(this, table) as readonly string[] | null;
      const rows = ahead === null || own === null ? null : [...new Set([...ahead, ...own])].sort();
      const last = combinedRows.get(table._table);
      if (last !== undefined && samePendingUploadRows(last, rows)) return last;
      combinedRows.set(table._table, rows);
      return rows;
    };
    const overrides: Record<string, (...args: never[]) => unknown> = {
      getSyncStatus: syncStatus,
      getPendingUploadRows: pendingUploadRows,
      pendingUploadRowsChangeRegistration: <T, Init>(
        table: TableProxy<T, Init>,
        listener: (ids: readonly string[] | null) => void,
      ) => {
        let published = pendingUploadRows(table);
        const publish = () => {
          const ids = pendingUploadRows(table);
          if (ids === published) return;
          published = ids;
          listener(ids);
        };
        const stopAhead = next.onPendingUploadRowsChange(table, publish);
        const stopOwn = draining ? this.onPendingUploadRowsChangeHere(table, publish) : () => {};
        drainListeners.add(publish);
        return () => {
          stopAhead();
          stopOwn();
          drainListeners.delete(publish);
        };
      },
      syncStatusChangeRegistration: (listener: (status: SyncStatus) => void) => {
        let published = syncStatus();
        const publish = () => {
          const status = syncStatus();
          if (status === published) return;
          published = status;
          listener(status);
        };
        const stopAhead = next.onSyncStatusChange(publish);
        const stopOwn = draining ? this.onSyncStatusChangeHere(publish) : () => {};
        drainListeners.add(publish);
        return () => {
          stopAhead();
          stopOwn();
          drainListeners.delete(publish);
        };
      },
      waitForSync: async (options: { timeoutMs?: number; signal?: AbortSignal } = {}) => {
        await Promise.all([
          next.waitForSync(options),
          draining ? this.waitForOwnSync(options, drained) : undefined,
        ]);
      },
      // The app's offline toggle covers both runtimes while the thin one
      // still uploads.
      disconnect: async () => {
        await Promise.all([next.disconnect(), draining ? this.connection.disconnect() : undefined]);
      },
      reconnect: async () => {
        await Promise.all([next.reconnect(), draining ? this.connection.reconnect() : undefined]);
      },
    };
    for (const name of FORWARDED_TO_SUCCESSOR) {
      Object.defineProperty(this, name, {
        configurable: true,
        value:
          overrides[name] ??
          ((...args: unknown[]) =>
            (next as unknown as Record<string, (...a: unknown[]) => unknown>)[name]!(...args)),
      });
    }
    Object.defineProperty(this, "syncStatusChangeRegistration", {
      configurable: true,
      value: overrides.syncStatusChangeRegistration,
    });
    Object.defineProperty(this, "pendingUploadRowsChangeRegistration", {
      configurable: true,
      value: overrides.pendingUploadRowsChangeRegistration,
    });
    // An app that took the thin Db offline keeps the local-first one offline.
    // The last toggle the app asked for counts, even while it is in flight.
    if (this.offlineRequested ?? this.connection.isExplicitlyOffline())
      void next.disconnect().catch(() => {});
    // Rejections this Db reported before anyone listened go to the listeners
    // on the successor (already logged here as unhandled).
    for (const event of this.pendingMutationErrorEvents.splice(0))
      next.handleMutationError(event, true);
    for (const entry of Array.from(this.movable)) entry.move(next);
    this.movable.clear();
  }

  /**
   * Wait for this Db's own runtime's writes, as {@link waitForSync} does
   * (rejecting with the server's error for a rejected one), while it drains
   * after the upgrade. The drain ending is the wait ending.
   */
  private async waitForOwnSync(
    options: { timeoutMs?: number; signal?: AbortSignal },
    drained: Promise<void>,
  ): Promise<void> {
    const { timeoutMs, signal } = options;
    validateWriteWaitTimeout(timeoutMs);
    signal?.throwIfAborted();
    const control = new AbortController();
    const forward = () => control.abort(signal!.reason);
    signal?.addEventListener("abort", forward, { once: true });
    const timer =
      timeoutMs === undefined
        ? undefined
        : setTimeout(
            () => control.abort(new WriteWaitTimeoutError("global", timeoutMs)),
            timeoutMs,
          );
    try {
      // The thin runtime's writes are answered (a rejection fails the wait),
      // or its drain ends: then it has nothing left to wait for.
      await Promise.race([this.connection.waitForSync(control.signal), drained]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
      signal?.removeEventListener("abort", forward);
    }
  }

  /** The app's last {@link disconnect} (true) or {@link reconnect} (false). */
  private offlineRequested: boolean | undefined;

  /** Set once this Db adopted a successor: its own runtime's last uploads. */
  private drain: { done: Promise<void>; endStreamWait(): void; abort(): void } | null = null;

  /** Set by {@link letStreamsFinish}. */
  private streamsMayFinish: AbortController | null = null;

  /**
   * @internal This Db's graceful shutdown lets streaming writes in flight on
   * its own runtime finish (and waits for their writes) before closing it,
   * until the returned controller aborts. Only the drain after the
   * local-first upgrade asks for this: the app didn't ask for that shutdown,
   * so it must not cut off an upload the app is making. A shutdown the app
   * asks for (a sign-out, an account switch, a close) aborts it, so it is
   * never held by a slow or stalled stream; the stream then fails saying why.
   */
  letStreamsFinish(): AbortController {
    this.streamsMayFinish?.abort();
    this.streamsMayFinish = new AbortController();
    return this.streamsMayFinish;
  }

  /**
   * @internal Stop this Db's own runtime after the upgrade. With
   * `waitForSync`, its writes upload first, the way a local-first client's
   * do when it shuts down; without, they are dropped, as when a thin client
   * closes. Either way a streaming write still sending its value is cut off
   * (it fails saying why) rather than holding the app's shutdown.
   */
  async stopOwnRuntime(options: ShutdownOptions = {}): Promise<void> {
    const drain = this.drain;
    if (!drain) return this.shutdown(options);
    // The app is shutting down: streams in flight no longer hold the drain.
    drain.endStreamWait();
    if (!options.waitForSync) drain.abort();
    await drain.done.catch(() => {});
    // An aborted or failed drain leaves the runtime open: stop it now.
    await Db.prototype.shutdown.call(this);
  }

  /** @internal The Db that does this one's work now: itself, or its successor. */
  current(): Db {
    return this.successor?.current() ?? this;
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes the Db's runtime client.
   *
   * With `waitForSync`, pending writes upload first (see {@link DbPhase}): if
   * that fails, this rejects with a `GracefulShutdownSyncError` and the Db
   * stays open. Otherwise, or once they uploaded, the Db moves to `closing`
   * and on to `closed`, for good.
   *
   * A plain shutdown (no `waitForSync`) always ends `closed`. One asked for
   * while a drain runs waits for that drain, as before, but if the drain
   * fails it tears down anyway: the drain's own caller still gets the
   * `GracefulShutdownSyncError`, but the Db no longer stays open.
   *
   * Idempotent: concurrent or repeated calls share the same in-flight promise.
   */
  shutdown(options: ShutdownOptions = {}): Promise<void> {
    if (this.teardown) return this.teardown;
    if (this.gracefulDrain) {
      if (options.waitForSync) return this.gracefulDrain.done;
      this.gracefulDrain.teardownAnyway = true;
      return this.gracefulDrain.done.catch(() => this.beginTeardown());
    }
    if (!options.waitForSync) return this.beginTeardown();
    // Publish the drain before it starts: waiting can call back into the Db.
    let resolveDrain!: (result: Promise<void>) => void;
    const done = new Promise<void>((resolve) => {
      resolveDrain = resolve;
    });
    const syncAbort = new AbortController();
    let rejectCancelled!: (error: Error) => void;
    const cancelled = new Promise<never>((_resolve, reject) => {
      rejectCancelled = reject;
    });
    const drain = {
      done,
      cancel: () => {
        syncAbort.abort();
        rejectCancelled(new Error("Graceful shutdown cancelled"));
      },
      teardownAnyway: false,
    };
    this.gracefulDrain = drain;
    resolveDrain(
      (async () => {
        try {
          await Promise.race([
            this.runtimeSource.waitForPendingWrites?.(syncAbort.signal) ??
              this.connection.waitForPendingWrites({
                inFlightStreams: this.streamsMayFinish?.signal,
              }),
            cancelled,
          ]);
        } catch (error) {
          syncAbort.abort();
          if (this.gracefulDrain === drain) this.gracefulDrain = null;
          // The Db never left `open`, and takes work again, unless a plain
          // shutdown asked for it meanwhile: that one closes it now, with no
          // moment in between where it reads as open.
          if (drain.teardownAnyway) void this.beginTeardown().catch(() => {});
          throw new GracefulShutdownSyncError(error);
        }
        // Teardown takes over from here: a caller that shares this promise
        // waits for it, and the Db is no longer draining but closing.
        const teardown = this.beginTeardown();
        if (this.gracefulDrain === drain) this.gracefulDrain = null;
        return teardown;
      })(),
    );
    void cancelled.catch(() => {});
    return done;
  }

  /** @internal Credential invalidation must interrupt a graceful sync wait. */
  abortGracefulShutdown(): void {
    this.gracefulDrain?.cancel();
  }

  /** Whether the Db takes no new work: it is draining for a shutdown, or past `open`. */
  private get stopping(): boolean {
    return this.phase !== "open" || this.gracefulDrain !== null;
  }

  protected assertOpen(): void {
    if (this.replaced) throw this.replaced;
    if (this.phase !== "open") throw new DbClosedError(this.phase);
    if (this.gracefulDrain) throw new DbClosedError("draining");
  }

  private readonly shutdownListeners = new Set<() => void>();

  /** @internal Dispose account refresh and invalidation observers with the context. */
  onShutdown(listener: () => void): () => void {
    // A drain may fail and leave the Db open: a listener registered during
    // one waits for the teardown, if it comes.
    if (this.phase !== "open") listener();
    else this.shutdownListeners.add(listener);
    return () => this.shutdownListeners.delete(listener);
  }

  /** Move `open` → `closing`, and to `closed` once teardown ends, however it ends. */
  private beginTeardown(): Promise<void> {
    if (this.teardown) return this.teardown;
    this.phase = "closing";
    // Publish the promise before teardown can synchronously call back into Db.
    let resolveTeardown!: (result: Promise<void>) => void;
    const teardown = new Promise<void>((resolve) => {
      resolveTeardown = resolve;
    });
    this.teardown = teardown;
    resolveTeardown(
      this.runTeardown().finally(() => {
        this.phase = "closed";
      }),
    );
    return teardown;
  }

  private async runTeardown(): Promise<void> {
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
        createRuntimeTokenOptions(secret, clientScope(config), 3600),
      );
      const trustedReservedSession = internalSessionFromVerifiedReservedJwtPayload(
        parseJwtPayload(jwtToken) ?? {},
        "device-account",
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
      createRuntimeTokenOptions(ephemeralSeed, clientScope(config), 3600),
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
      "DbConfig does not accept backendSecret. Run Jazz in your backend with createJazz() from jazz-tools/backend instead.",
    );
  }
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof Worker !== "undefined";
}

const credentialRenewers = new WeakMap<object, number>();

/**
 * @internal Declare that the caller renews this Db's credential whenever the
 * server refuses it, until the returned release is called. Global write waits
 * then outlast a renewable refusal instead of failing (#3923).
 */
export function holdCredentialRenewal(db: object): () => void {
  credentialRenewers.set(db, (credentialRenewers.get(db) ?? 0) + 1);
  let released = false;
  return () => {
    if (released) return;
    released = true;
    credentialRenewers.set(db, (credentialRenewers.get(db) ?? 1) - 1);
  };
}

/** `error` says the runtime closed, also when a connection wrapped it as its cause. */
/** A write held back that failed before it was made: it was never sent. */
function unmadeWriteEvent(error: unknown, number: number): MutationErrorEvent {
  const transactionId = `unmade:${number}` as TxId;
  const reason = error instanceof Error ? error.message : String(error);
  return {
    code: "not_sent",
    reason,
    transaction: {
      transactionId,
      kind: "mergeable",
      sealed: true,
      latestSettlement: { kind: "missing", transactionId },
    },
  };
}

/**
 * Whether `error` says the runtime had closed before the request was sent.
 * A request in flight when it closed (`ThinConnectionLostError`) may have
 * reached the server: that is not this.
 */
function closedBeforeSending(error: unknown): boolean {
  let closed = false;
  for (let current = error; current instanceof Error; current = current.cause) {
    if (current.name === "ThinConnectionLostError") return false;
    if (current instanceof RuntimeClosedError) closed = true;
  }
  return closed;
}

function isRuntimeClosed(error: unknown): boolean {
  for (let current = error; current instanceof Error; current = current.cause)
    if (current instanceof RuntimeClosedError) return true;
  return false;
}

/** The status with why the server refused the credential, while the link is offline for it. */
function withAuthReason(status: SyncStatus, reason: AuthFailureReason | undefined): SyncStatus {
  const { link } = status;
  if (link.state !== "offline" || link.reason !== "auth" || reason === undefined) return status;
  return { ...status, link: { ...link, authReason: reason } };
}
