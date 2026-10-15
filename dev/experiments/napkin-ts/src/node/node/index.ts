// Storage-backed Jazz node core: NodeState, shared errors, and in-memory indexes.
import { SortedMap, SortedSet, cmpBig, cmpNum, cmpStr, type Bytes } from "../../support/index.js";
import {
  type AuthorSubject,
  type BranchKey,
  type BranchSelector,
  type DeletionEvent,
  type GlobalTime,
  type NodeAlias,
  type NodeUuid,
  type PhysicalTableId,
  type RowUuid,
  type SchemaVersionId,
  type TxId,
  type TxTime,
  type Value,
  type ValueType,
  AuthorSubjectSystem,
  CurrentRowBindingRole,
  CurrentRowRecord,
  DescriptorField,
  GlobalTimeOps,
  OwnedRecord,
  PredicateAny,
  QueryFrom,
  RecordDescriptor,
  RowAuthor,
  TxTimeOps,
  USER_COLUMN_PREFIX,
  cmpSupportingRow,
  cmpTxId,
  descriptorPublicName,
  encodeValue,
  globalCurrentBranchKeyPrefix,
  globalCurrentKeyBranchPrefix,
  INLINE_VALUE_MAX_BYTES,
  publicationApplicationName,
  publicationRole,
  queryEquals,
  registeredColumnTransform,
  resultVisibilityForMetadata,
  schemaIdKey,
  txIdKey,
  userColumnField,
  valueEquals,
  versionApplicationCellCount,
  versionCellAt,
} from "../../_extern/node-state.js";
import { compareBytes, toHex } from "../../support/bytes.js";
import { lifecycleMethods } from "./state/lifecycle.js";
import { commitMethods } from "./state/commit.js";
import { contributionMergeMethods } from "./state/contribution_merge.js";
import { durableMethods } from "./state/durable.js";
import { catalogueMethods } from "./state/catalogue.js";
import { readPayloadMethods } from "./state/read_payload.js";
import { evictionMethods } from "./eviction.js";
import { globalStateMethods } from "./global_state.js";
import { openTxMethods, OpenTxState } from "./open_tx.js";
import { recoveryMethods } from "./recovery.js";
import { DatabaseSlot } from "./database_slot.js";
import { NodeAliases } from "./node_aliases.js";

export { NodeAliases } from "./node_aliases.js";
export {
  ClientCacheBudget,
  type ClientCacheBudgetReport,
  ClientCacheClass,
  type EvictColdReport,
} from "./eviction.js";
export { type TransactionBranchRowState, TransactionInsertTargetState } from "./open_tx.js";

/** Construct a tagged `Value`. */
export const V = (kind: string, value?: unknown): Value => ({ kind, value }) as Value;

// ---------------------------------------------------------------- errors

export type NodeErrorKind =
  | "Groove"
  | "ChunkStorage"
  | "LargeValueUploadChunkUnavailable"
  | "LargeValue"
  | "LargeValueReachability"
  | "LargeValueIngressRateLimited"
  | "LargeValueStageExpired"
  | "Record"
  | "MalformedCurrentRow"
  | "Storage"
  | "ClockOverflow"
  | "Query"
  | "QueryLowering"
  | "QueryCapability"
  | "AuthorizationSupportMissingClaim"
  | "PolicyProofCycle"
  | "TableNotFound"
  | "UnsupportedColumnType"
  | "InvalidMergeableCommit"
  | "UnadmittedWriteAuthor"
  | "InvalidBranchKey"
  | "UnsupportedExclusiveRead"
  | "TransactionConflict"
  | "InvalidStoredValue"
  | "InvalidAuthoritySourceClosure"
  | "MissingTransaction"
  | "MalformedViewUpdate"
  | "MaintainedViewMissingBundleWitness"
  | "MissingOpenBatch"
  | "DuplicateOpenBatch"
  | "OpenTransactionIdentityMismatch"
  | "NonMonotoneState"
  | "ConflictingCommitUnit"
  | "ConflictingFate"
  | "UnsupportedCommitUnit"
  | "UnsupportedSyncMessage"
  | "UnauthorizedCatalogueUpdate"
  | "InvalidCatalogueUpdate"
  | "CatalogueActivationFailed"
  | "CatalogueUninitialized"
  | "CatalogueCodec"
  | "HistoricalReadRequiresServer"
  | "AuthorizationDenied"
  | "SubscriptionClosed";

function nodeErrorMessage(kind: NodeErrorKind, d: any): string {
  switch (kind) {
    case "Groove":
    case "ChunkStorage":
    case "LargeValue":
    case "LargeValueReachability":
    case "Record":
    case "Storage":
    case "ClockOverflow":
    case "CatalogueCodec":
    case "Query":
      return String(d?.message ?? d);
    case "LargeValueUploadChunkUnavailable":
      return `large-value upstream upload cannot read local chunk (${d.context}): ${d.source}`;
    case "LargeValueIngressRateLimited":
      return "large-value upload rate limit exceeded";
    case "LargeValueStageExpired":
      return "large-value staging root expired; upload again";
    case "MalformedCurrentRow":
      return `malformed current row in table ${d.table} for ${String(d.rowUuid)}: ${d.source}`;
    case "QueryLowering":
      return `query lowering failed: ${d}`;
    case "QueryCapability":
      return `query capability unsupported: ${d}`;
    case "AuthorizationSupportMissingClaim":
      return `authorization support policy claim is not bound: ${d}`;
    case "PolicyProofCycle":
      return `PolicyProofCycle: table '${d.table}' re-entered at depth ${d.depth}`;
    case "TableNotFound":
      return `table not found: ${d}`;
    case "UnsupportedColumnType":
      return `M1 only supports string user columns, got unsupported column: ${d}`;
    case "InvalidMergeableCommit":
      return `invalid mergeable commit: ${d}`;
    case "UnadmittedWriteAuthor":
      return "durable writes require an admitted account author";
    case "InvalidBranchKey":
      return `invalid branch key: ${d}`;
    case "UnsupportedExclusiveRead":
      return `Reading ${d} is not supported in exclusive transactions yet`;
    case "TransactionConflict":
      return "row visible parent changed since transaction write was staged";
    case "InvalidStoredValue":
      return `invalid stored value: ${d}`;
    case "InvalidAuthoritySourceClosure":
      return `invalid authority source closure for subscription ${String(d.subscription)}: ${d.transition}`;
    case "MissingTransaction":
      return `missing transaction: ${String(d)}`;
    case "MalformedViewUpdate":
      return `malformed view update: ${d}`;
    case "MaintainedViewMissingBundleWitness":
      return `maintained subscription view missing bundle witness: ${d}`;
    case "MissingOpenBatch":
      return `missing open transaction: ${String(d)}`;
    case "DuplicateOpenBatch":
      return `duplicate open batch id: ${String(d)}`;
    case "OpenTransactionIdentityMismatch":
      return "open transaction identity does not match its bound identity";
    case "NonMonotoneState":
      return `non-monotone state update: ${d}`;
    case "ConflictingCommitUnit":
      return `conflicting commit unit for transaction: ${String(d)}`;
    case "ConflictingFate":
      return "conflicting fate transition";
    case "UnsupportedCommitUnit":
      return `unsupported commit unit: ${d}`;
    case "UnsupportedSyncMessage":
      return `unsupported sync message: ${d}`;
    case "UnauthorizedCatalogueUpdate":
      return "unauthorized catalogue update";
    case "InvalidCatalogueUpdate":
      return `invalid catalogue update: ${d}`;
    case "CatalogueActivationFailed":
      return "catalogue activation failed; reopen required";
    case "CatalogueUninitialized":
      return "catalogue bootstrap is not ready";
    case "HistoricalReadRequiresServer":
      return "historical read requires server evaluation";
    case "AuthorizationDenied":
      return "authorization denied";
    case "SubscriptionClosed":
      return "prepared point-read subscription closed";
  }
}

/** Error type returned by the storage-backed node API. */
export class NodeError extends Error {
  constructor(
    readonly kind: NodeErrorKind,
    readonly detail?: any,
  ) {
    super(nodeErrorMessage(kind, detail));
  }
}
export const nodeErr = (kind: NodeErrorKind, detail?: any) => new NodeError(kind, detail);
export const invalidStored = (msg: string) => new NodeError("InvalidStoredValue", msg);
export const invalidCommit = (msg: string) => new NodeError("InvalidMergeableCommit", msg);
export const invalidCatalogue = (msg: string) => new NodeError("InvalidCatalogueUpdate", msg);

/** Wrap a foreign (groove/records/storage) error into the node error domain. */
export function intoNodeError(e: unknown, kind: NodeErrorKind = "Groove"): NodeError {
  return e instanceof NodeError ? e : new NodeError(kind, e);
}

/** Whether encoding a projected row failed only because the read schema lacks the enum case. */
export function isUnrepresentableEnumProjection(error: unknown): boolean {
  return (
    error instanceof NodeError &&
    error.kind === "Record" &&
    (error.detail?.kind === "InvalidEnumDiscriminant" || error.detail?.kind === "UnknownEnumTag")
  );
}

// ---------------------------------------------------------------- enum registry hydration

export type GlobalScalarEnumCaseId = {
  id: any;
  introducingSchema: SchemaVersionId;
  introducingOrdinal: number;
};
export type GlobalEnumCaseId = GlobalScalarEnumCaseId;

const caseEq = (a: GlobalEnumCaseId, b: GlobalEnumCaseId) =>
  String(a.id) === String(b.id) &&
  schemaIdKey(a.introducingSchema) === schemaIdKey(b.introducingSchema) &&
  a.introducingOrdinal === b.introducingOrdinal;

function installCaseIds(
  slot: GlobalEnumCaseId[],
  ids: any[],
  introducingSchema: SchemaVersionId,
  maxOrdinal: number,
  exhausted: string,
): void {
  const expected = ids.map((id, ordinal) => {
    if (ordinal > maxOrdinal) throw invalidStored(exhausted);
    return { id, introducingSchema, introducingOrdinal: ordinal };
  });
  const shared = Math.min(slot.length, expected.length);
  for (let i = 0; i < shared; i++)
    if (!caseEq(slot[i], expected[i]))
      throw invalidStored("enum registry conflicts with authority identities");
  if (slot.length > expected.length)
    throw invalidStored("enum registry conflicts with authority identities");
  slot.push(...expected.slice(slot.length));
}
export const installEnumCaseIds = (
  slot: GlobalScalarEnumCaseId[],
  ids: any[],
  schema: SchemaVersionId,
) => installCaseIds(slot, ids, schema, 0xff, "scalar enum tag exhausted");
export const installPayloadEnumCaseIds = (
  slot: GlobalEnumCaseId[],
  ids: any[],
  schema: SchemaVersionId,
) => installCaseIds(slot, ids, schema, 0xffffffff, "payload enum tag exhausted");

const globalCasePath = (path: string, c: GlobalEnumCaseId) =>
  `${path}/case/${String(c.id).replace(/-/g, "")}`;

function walkNestedEnums(
  valueType: ValueType,
  authoredPath: string,
  path: string,
  visit: (kind: "EnumTag" | "Enum", vt: any, authoredPath: string, path: string) => void,
  recurse: (vt: ValueType, authored: string, path: string) => void,
): void {
  switch (valueType.kind) {
    case "Nullable":
      return recurse(valueType.inner, `${authoredPath}/nullable`, `${path}/nullable`);
    case "Array":
      return recurse(valueType.inner, `${authoredPath}/array`, `${path}/array`);
    case "Tuple":
      valueType.items.forEach((v: ValueType, i: number) =>
        recurse(v, `${authoredPath}/tuple/${i}`, `${path}/tuple/${i}`),
      );
      return;
    case "Record":
      for (const field of valueType.record.fields()) {
        const name = field.name;
        if (name === undefined) throw invalidStored("nested enum record field unnamed");
        recurse(field.valueType, `${authoredPath}/record/${name}`, `${path}/record/${name}`);
      }
      return;
    case "EnumTag":
    case "Enum":
      return visit(valueType.kind, valueType.schema, authoredPath, path);
  }
}

export function hydrateNestedScalarEnumCases(
  valueType: ValueType,
  identities: Map<string, any[]>,
  introducingSchema: SchemaVersionId,
  authoredPath: string,
  path: string,
  output: Map<string, GlobalScalarEnumCaseId[]>,
): void {
  const rec = (vt: ValueType, a: string, p: string) =>
    hydrateNestedScalarEnumCases(vt, identities, introducingSchema, a, p, output);
  walkNestedEnums(
    valueType,
    authoredPath,
    path,
    (kind, schema, a, p) => {
      if (kind === "EnumTag") {
        const ids = identities.get(a);
        if (!ids) throw invalidStored("nested scalar enum authority identities missing");
        if (ids.length !== schema.variants.length)
          throw invalidStored("nested scalar enum authority identity width mismatch");
        let slot = output.get(p);
        if (!slot) output.set(p, (slot = []));
        installEnumCaseIds(slot, ids, introducingSchema);
        return;
      }
      schema.cases.forEach((c: any, ordinal: number) => {
        const id = identities.get(a)?.[ordinal];
        if (id === undefined) throw invalidStored("nested payload enum authority identity missing");
        if (ordinal > 0xffffffff) throw invalidStored("nested payload enum tag exhausted");
        const identity = { id, introducingSchema, introducingOrdinal: ordinal };
        rec(
          { kind: "Record", record: c.payload },
          `${a}/case/${ordinal}`,
          globalCasePath(p, identity),
        );
      });
    },
    rec,
  );
}

/** Register every nested payload enum by its permanent global variant UUID. */
export function hydrateNestedPayloadEnumCases(
  valueType: ValueType,
  identities: Map<string, any[]>,
  introducingSchema: SchemaVersionId,
  authoredPath: string,
  path: string,
  output: Map<string, GlobalEnumCaseId[]>,
): void {
  const rec = (vt: ValueType, a: string, p: string) =>
    hydrateNestedPayloadEnumCases(vt, identities, introducingSchema, a, p, output);
  walkNestedEnums(
    valueType,
    authoredPath,
    path,
    (kind, schema, a, p) => {
      if (kind !== "Enum") return;
      const ids = identities.get(a);
      if (!ids) throw invalidStored("nested payload enum authority identities missing");
      if (ids.length !== schema.cases.length)
        throw invalidStored("nested payload enum authority identity width mismatch");
      let slot = output.get(p);
      if (!slot) output.set(p, (slot = []));
      installPayloadEnumCaseIds(slot, ids, introducingSchema);
      [...slot].forEach((c, ordinal) => {
        const layout = schema.cases[ordinal];
        if (layout)
          rec(
            { kind: "Record", record: layout.payload },
            `${a}/case/${ordinal}`,
            globalCasePath(p, c),
          );
      });
    },
    rec,
  );
}

// ---------------------------------------------------------------- ahead-current index

type AheadKey = [PhysicalTableId, Bytes];
const aheadKey = ([table, key]: AheadKey) => `${table}:${toHex(key)}`;

/** The local delta's rows held in memory, plus row counts per table and branch key. */
export class AheadCurrentKeys {
  private keys = new Map<string, TxId>();
  private rowsPerBranch = new Map<PhysicalTableId, Map<string, number>>();

  get(key: AheadKey): TxId | undefined {
    return this.keys.get(aheadKey(key));
  }
  containsKey(key: AheadKey): boolean {
    return this.keys.has(aheadKey(key));
  }
  insert(key: AheadKey, txId: TxId): TxId | undefined {
    const k = aheadKey(key);
    const previous = this.keys.get(k);
    this.keys.set(k, txId);
    if (previous === undefined) {
      const branch = toHex(globalCurrentKeyBranchPrefix(key[1]));
      let branches = this.rowsPerBranch.get(key[0]);
      if (!branches) this.rowsPerBranch.set(key[0], (branches = new Map()));
      branches.set(branch, (branches.get(branch) ?? 0) + 1);
    }
    return previous;
  }
  remove(key: AheadKey): TxId | undefined {
    const k = aheadKey(key);
    const removed = this.keys.get(k);
    if (removed === undefined) return undefined;
    this.keys.delete(k);
    const branches = this.rowsPerBranch.get(key[0]);
    if (branches) {
      const branch = toHex(globalCurrentKeyBranchPrefix(key[1]));
      const rows = branches.get(branch);
      if (rows !== undefined) {
        if (rows <= 1) branches.delete(branch);
        else branches.set(branch, rows - 1);
      }
      if (branches.size === 0) this.rowsPerBranch.delete(key[0]);
    }
    return removed;
  }
  clear(): void {
    this.keys.clear();
    this.rowsPerBranch.clear();
  }
  /** How many pending rows `tableId`'s local delta holds under `branchKey`. */
  branchRows(tableId: PhysicalTableId, branchKey: BranchKey): number {
    return (
      this.rowsPerBranch.get(tableId)?.get(toHex(globalCurrentBranchKeyPrefix(branchKey))) ?? 0
    );
  }
}

// ---------------------------------------------------------------- constants and small helpers

export const SKEW_TOLERANCE_MS = 30_000;
export const TX_VERSION_TABLE_CACHE_MAX_ENTRIES = 4096;
export const HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES = 4096;
export const LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES = 1 << 20;

let nextGrooveRuntimeTokenValue = 1;
export const nextGrooveRuntimeToken = () => nextGrooveRuntimeTokenValue++;

export function isCatalogueMutation(message: any): boolean {
  return (
    message.kind === "CatalogueSnapshot" ||
    message.kind === "PublishSchema" ||
    message.kind === "PublishSchemaWithLens" ||
    message.kind === "PublishLens"
  );
}

export const recordMaintainedViewStreamBAddBundle = () => {};
export const recordMaintainedViewRemovalStreamBundle = () => {};

export enum LensPathDirection {
  Forward,
  Reverse,
}

export type CompiledLensOp =
  | { kind: "Rename"; from: string; to: string }
  | { kind: "Copy"; from: string; to: string }
  | { kind: "Add"; column: string; default: Value }
  | { kind: "Drop"; column: string };
export type CompiledLensPath = { targetTable: string; ops: CompiledLensOp[] };

export function applyCompiledLensPath(path: CompiledLensPath, cells: Map<string, Value>): string {
  for (const op of path.ops) {
    switch (op.kind) {
      case "Rename": {
        const v = cells.get(op.from);
        if (v !== undefined) {
          cells.delete(op.from);
          cells.set(op.to, v);
        }
        break;
      }
      case "Copy": {
        const v = cells.get(op.from);
        if (v !== undefined) cells.set(op.to, v);
        break;
      }
      case "Add":
        if (!cells.has(op.column)) cells.set(op.column, op.default);
        break;
      case "Drop":
        cells.delete(op.column);
        break;
    }
  }
  return path.targetTable;
}

export function pushCompiledForwardLensOp(op: any, compiled: CompiledLensOp[]): void {
  switch (op.kind) {
    case "RenameTable":
      return;
    case "RenameColumn":
      compiled.push({ kind: "Rename", from: op.from, to: op.to });
      return;
    case "CopyColumn":
      compiled.push({ kind: "Copy", from: op.from, to: op.to });
      return;
    case "AddColumn":
      compiled.push({ kind: "Add", column: op.column, default: op.default });
      return;
    case "DropColumn":
      compiled.push({ kind: "Drop", column: op.column });
      return;
    case "TransformColumn":
      return validateRegisteredTransform(op.transform);
    case "RejectSourceDelta":
      throw invalidCatalogue("lens op is not naturally mappable");
  }
}

export function pushCompiledReverseLensOp(op: any, compiled: CompiledLensOp[]): void {
  switch (op.kind) {
    case "RenameTable":
      return;
    case "RenameColumn":
      compiled.push({ kind: "Rename", from: op.to, to: op.from });
      return;
    case "CopyColumn":
      compiled.push({ kind: "Drop", column: op.to });
      return;
    case "AddColumn":
      compiled.push({ kind: "Drop", column: op.column });
      return;
    case "DropColumn":
      compiled.push({ kind: "Add", column: op.column, default: op.backwardsDefault });
      return;
    case "TransformColumn":
      return validateRegisteredTransform(op.transform);
    case "RejectSourceDelta":
      throw invalidCatalogue("lens op is not naturally mappable");
  }
}

export function validateRegisteredTransform(transform: string): void {
  const semantics = registeredColumnTransform(transform);
  if (!semantics) throw invalidCatalogue("transform column is not registered");
  if (!semantics.bijective || !semantics.canonicalEqualityPreserving)
    throw invalidCatalogue("transform column is not bijective and canonical-preserving");
}

// ---------------------------------------------------------------- catalogue state shapes

export type LargeValueStagingPolicy = {
  incomingBytesPerWindow: number;
  windowMs: number;
  maxAgeMs: number;
};
export const defaultLargeValueStagingPolicy = (): LargeValueStagingPolicy => ({
  incomingBytesPerWindow: 256 * 1024 * 1024,
  windowMs: 1_000,
  maxAgeMs: 10 * 60 * 1_000,
});

export enum CatalogueBootstrapState {
  Uninitialized,
  Ready,
}

/** One authority selection: structural schema, permissions, and revision. */
export class ActiveSchema {
  constructor(
    readonly schema: SchemaVersionId,
    readonly revision: number,
    readonly compiled: any,
  ) {}

  static create(
    pointer: { revision: number; schema: SchemaVersionId },
    compiled: any,
  ): ActiveSchema {
    if (schemaIdKey(compiled.versionId()) !== schemaIdKey(pointer.schema))
      throw invalidCatalogue("active schema permissions target mismatch");
    return new ActiveSchema(pointer.schema, pointer.revision, compiled);
  }

  wirePointer() {
    return { revision: this.revision, schema: this.schema };
  }

  samePermissions(other: ActiveSchema): boolean {
    const a = this.compiled.tables;
    const b = other.compiled.tables;
    return (
      a.length === b.length &&
      a.every(
        (t: any, i: number) =>
          t.name === b[i].name &&
          queryEquals(t.readPolicy, b[i].readPolicy) &&
          t.writePolicies.length === b[i].writePolicies.length &&
          t.writePolicies.every(
            ([k, p]: any, j: number) =>
              k === b[i].writePolicies[j][0] && queryEquals(p, b[i].writePolicies[j][1]),
          ),
      )
    );
  }

  sameAuthorizationSource(other: ActiveSchema, sourceMapping?: any, otherMapping?: any): boolean {
    if (!this.samePermissions(other)) return false;
    if (schemaIdKey(this.schema) === schemaIdKey(other.schema)) return true;
    const others = other.compiled.tables;
    return this.compiled.tables.every((table: any, i: number) => {
      const otherTable = others[i];
      if (!otherTable) return true;
      if (table.branchBy.length || otherTable.branchBy.length) return false;
      const source = sourceMapping?.tables.get(table.name);
      const target = otherMapping?.tables.get(table.name);
      if (!source || !target || source.tableId !== target.tableId) return false;
      const allow = QueryFrom(table.name);
      const deny = allow.clone().filter(PredicateAny([]));
      const policies = [
        ...(table.readPolicy ? [table.readPolicy] : []),
        ...table.writePolicies.map(([, p]: any) => p),
      ];
      return policies.every((p: any) => queryEquals(p, allow) || queryEquals(p, deny));
    });
  }
}

/** Local transaction clock and settled-global application progress. */
export class Clock {
  txTime: TxTime = TxTimeOps.zero;
  reservationHighWater = { value: TxTimeOps.zero as TxTime };
  globalTimeRegister: GlobalTime = GlobalTimeOps.zero;
  locallyMintedGlobalTimes = new SortedSet<GlobalTime>(cmpNum);
  committedGlobalTime: GlobalTime = GlobalTimeOps.zero;
  appliedGlobalTimesAfterFrontier = new SortedSet<GlobalTime>(cmpNum);
  frontierDots = new SortedMap<GlobalTime, TxId[]>(cmpNum);

  allocateGlobalTime(nowMs: number): GlobalTime {
    const globalTime = GlobalTimeOps.tick(this.globalTimeRegister, nowMs);
    this.globalTimeRegister = globalTime;
    this.locallyMintedGlobalTimes.add(globalTime);
    return globalTime;
  }
}

export type ParkedSubscription = { subscribe: any; policyBinding?: any };
export const newParking = () => ({
  parkedShapeRegistrations: new Map<string, any>(),
  parkedBindingDeltas: new Map<string, ParkedSubscription[]>(),
  parkedCommitUnits: new SortedMap<TxId, any>(cmpTxId),
  parkedCatalogueCommitUnits: new SortedSet<TxId>(cmpTxId),
  newestIngestMs: 0,
});
export type Parking = ReturnType<typeof newParking>;
export const cloneParking = (p: Parking): Parking => ({
  parkedShapeRegistrations: new Map(p.parkedShapeRegistrations),
  parkedBindingDeltas: new Map([...p.parkedBindingDeltas].map(([k, v]) => [k, [...v]])),
  parkedCommitUnits: p.parkedCommitUnits.clone(),
  parkedCatalogueCommitUnits: new SortedSet(cmpTxId, p.parkedCatalogueCommitUnits),
  newestIngestMs: p.newestIngestMs,
});

export const newQueryServing = () => ({
  watermarkRestoreSeen: new Set<string>(),
  persistedWatermarks: new Map<string, [GlobalTime, Bytes]>(),
  watermarksInvalidated: false,
  localAvailabilityRecords: new Map<string, any>(),
  localAvailabilityAuthorities: new Map<string, [NodeUuid, number]>(),
  localUnavailableInputs: new Map<string, any>(),
  queryShapeCache: new Map<string, PreparedQueryPlan>(),
  readPolicyAuthorizationRequestCache: new Map<string, any>(),
  compiledQueryProgramCache: new Map<string, any>(),
  queryProgramTemplates: new Map<string, any>(),
  supportedQueryProgramRequests: [] as any[],
  policyAuthorizationGraphCache: new Map<string, any>(),
  policyAuthorizationGraphReplacements: new Map<
    string,
    { previous?: any; lease: WeakRef<object> }[]
  >(),
  policyProofStack: [] as { table: string; lease: WeakRef<object> }[],
  txVersionsCache: new Map<string, { versions: any[] }>(),
  txVersionTablesCacheOrder: [] as TxId[],
  txVersionTablesCacheOrderSet: new Set<string>(),
  versionStorageSourcesCache: new Map<string, string[]>(),
  registeredShapes: new Map<string, any>(),
  registeredShapeOptions: new Map<string, any>(),
  peerShapeOwners: new Map<string, Set<number>>(),
  locallyRegisteredShapes: new Set<string>(),
  outboundShapeOwners: new Map<string, Set<string>>(),
  outboundBindingOwners: new Map<string, Set<number>>(),
  registeredBindings: new Map<string, Map<string, RegisteredBinding>>(),
  authorityResults: new Map<string, AuthorityResultState>(),
  appliedViewUpdateGenerations: new Map<string, number>(),
  retainedRootWindowSources: new Map<string, RetainedRootWindowSource>(),
});
export type QueryServing = ReturnType<typeof newQueryServing>;

export type RegisteredBinding = {
  values: Value[];
  readView: any;
  bindingViewKey: any;
  authorityResultKey: any;
  options: any;
  compilerIdentity: AuthorSubject;
};

/** Compiler-owned description of the root window stage of a retained receiver page. */
export class RetainedRootWindowSource {
  constructor(
    readonly source: { table: string; path: string[] },
    readonly sourceShape: any,
    readonly partition: "Global",
    readonly orderBy: any[],
    readonly tieBreak: "RootOccurrence",
    readonly offset: number,
    readonly limit: number | undefined,
    readonly queryWithoutWindow: any,
  ) {}

  static forShape(shape: any): RetainedRootWindowSource {
    const query = shape.query();
    const queryWithoutWindow = { ...query, offset: 0, limit: undefined };
    return new RetainedRootWindowSource(
      { table: query.table, path: ["Root"] },
      shape,
      "Global",
      query.orderBy,
      "RootOccurrence",
      query.offset,
      query.limit,
      queryWithoutWindow,
    );
  }

  isBounded(): boolean {
    return this.offset !== 0 || this.limit !== undefined;
  }

  containsTarget(target: any): boolean {
    const t = RetainedRootWindowSource.forShape(target);
    if (
      this.source.table !== t.source.table ||
      this.source.path.join("/") !== t.source.path.join("/") ||
      JSON.stringify(this.orderBy) !== JSON.stringify(t.orderBy) ||
      schemaIdKey(this.sourceShape.schemaVersion()) !== schemaIdKey(target.schemaVersion()) ||
      !queryEquals(this.sourceShape.params(), target.params()) ||
      !queryEquals(this.queryWithoutWindow, t.queryWithoutWindow) ||
      this.offset > t.offset
    )
      return false;
    const sourceEnd = this.limit === undefined ? undefined : this.offset + this.limit;
    const targetEnd = t.limit === undefined ? undefined : t.offset + t.limit;
    if (sourceEnd !== undefined && targetEnd !== undefined && targetEnd > sourceEnd) return false;
    return !(sourceEnd !== undefined && targetEnd === undefined);
  }

  relativeWindowFor(target: any): [number, number | undefined] {
    return [target.query().offset - this.offset, target.query().limit];
  }
}

/** Receiver-local replacement key; content and deletion registers stay independent. */
export const coveredInputCoordinateKey = (input: any): string =>
  [
    String(input.physicalTable),
    String(input.row),
    input.version.layer,
    toHex(input.version.branchOrPrefix ?? new Uint8Array()),
  ].join("|");

export type AuthoritySourceClosure = { kind: "Pending" } | { kind: "Claimed"; generation: number };

/** One authority-owned result stream with every receipt making its membership meaningful. */
export class AuthorityResultState {
  appliedViewUpdateGeneration = 0;
  sourceClosure: AuthoritySourceClosure = { kind: "Pending" };
  sourceIncrementals: AuthoritySourceIncremental[] = [];
  liveSettled = false;
  supportingRevision?: Bytes;
  coveredInputVersions = new Map<string, any>();
  compiledCoveredInputSources?: Map<string, string>;
  settledThrough?: GlobalTime;
  authorizationProgress?: number;
  knownStateDeclared = false;
  initialHydration = false;
  deferredPublication = false;
  pendingAuthoritativeReset?: number;
  pendingOpening = false;
}

/** One receiver input delta with an exact prior receipt generation. */
export class AuthoritySourceIncremental {
  constructor(
    public predecessorGeneration: number,
    public generation: number,
    public adds: SortedSet<any>,
    public removes: SortedSet<any>,
  ) {}

  /** Coalesce contiguous wire frames into one exact net transition. */
  static coalesce(
    previous: AuthoritySourceIncremental | undefined,
    predecessorGeneration: number,
    generation: number,
    adds: Iterable<any>,
    removes: Iterable<any>,
  ): AuthoritySourceIncremental {
    const addSet = new SortedSet(cmpSupportingRow, adds);
    const removeSet = new SortedSet(cmpSupportingRow, removes);
    for (const a of addSet)
      if (removeSet.has(a))
        throw invalidStored("covered input incremental delta overlaps additions and removals");
    let result: AuthoritySourceIncremental;
    if (previous) {
      if (previous.generation !== predecessorGeneration)
        throw invalidStored(
          "covered input incremental delta does not continue its retained predecessor",
        );
      result = previous;
    } else {
      result = new AuthoritySourceIncremental(
        predecessorGeneration,
        predecessorGeneration,
        new SortedSet(cmpSupportingRow),
        new SortedSet(cmpSupportingRow),
      );
    }
    for (const fact of addSet) if (!result.removes.delete(fact)) result.adds.add(fact);
    for (const fact of removeSet) if (!result.adds.delete(fact)) result.removes.add(fact);
    result.generation = generation;
    return result;
  }
}

/** Coalesce the contiguous source-delta chain from a receiver's installed receipt. */
export function coalesceAuthoritySourceIncrementals(
  frames: AuthoritySourceIncremental[],
  predecessorGeneration: number,
  successorGeneration: number,
): AuthoritySourceIncremental {
  let cursor = predecessorGeneration;
  let combined: AuthoritySourceIncremental | undefined;
  while (cursor !== successorGeneration) {
    const candidates = frames.filter((f) => f.predecessorGeneration === cursor);
    const frame = candidates[0];
    if (!frame)
      throw invalidStored(
        "covered input receiver missed an incremental predecessor; authority reset required",
      );
    if (candidates.length > 1 || frame.generation <= cursor)
      throw invalidStored("covered input incremental chain is forked or nonmonotonic");
    combined = AuthoritySourceIncremental.coalesce(
      combined,
      frame.predecessorGeneration,
      frame.generation,
      frame.adds,
      frame.removes,
    );
    cursor = frame.generation;
  }
  if (!combined) throw invalidStored("covered input receiver has no incremental successor");
  return combined;
}

export type ParamBindingModeCacheKey = "InlineAllReachableSeeds" | "RetainAllParams";

// ---------------------------------------------------------------- commit-unit trust

export enum CommitUnitTrust {
  Session,
  Relay,
  TrustedBackend,
  TrustedAuthority,
  TrustedAdmin,
}
export const isTrusted = (t: CommitUnitTrust) =>
  t === CommitUnitTrust.TrustedBackend ||
  t === CommitUnitTrust.TrustedAuthority ||
  t === CommitUnitTrust.TrustedAdmin;

export type CommitUnitIngestContext = {
  identity: AuthorSubject;
  trust: CommitUnitTrust;
  admittedWriteAuthorization: boolean;
  versionReceiptsValidated: boolean;
};
const ctxEq = (a: CommitUnitIngestContext, b: CommitUnitIngestContext) =>
  String(a.identity) === String(b.identity) &&
  a.trust === b.trust &&
  a.admittedWriteAuthorization === b.admittedWriteAuthorization &&
  a.versionReceiptsValidated === b.versionReceiptsValidated;

/** Same authenticated authority, ignoring how the receipts were checked. */
export function sameParkedAuthority(
  existing?: CommitUnitIngestContext,
  resent?: CommitUnitIngestContext,
): boolean {
  if (existing && resent)
    return ctxEq(
      { ...existing, versionReceiptsValidated: resent.versionReceiptsValidated },
      resent,
    );
  return existing === resent || (!!existing && !!resent && ctxEq(existing, resent));
}

// ---------------------------------------------------------------- current rows

export type RowProvenance = {
  createdBy: AuthorSubject;
  createdAt: number;
  updatedBy: AuthorSubject;
  updatedAt: number;
};
export type RelationEdge = {
  sourceTable: string;
  sourceRow: RowUuid;
  relation: string;
  targetTable: string;
  targetRow: RowUuid;
};
export type RelationSnapshot = { rootCount: number; rows: CurrentRow[]; edges: RelationEdge[] };

const stripNullable = (value: Value): Value | undefined =>
  value.kind === "Nullable" ? value.value : value;

const PROVENANCE_STORED: Record<string, string> = {
  $createdBy: "created_by",
  $createdAt: "created_at",
  $updatedBy: "updated_by",
  $updatedAt: "updated_at",
};

/** Current-row result backed by an encoded projected record. */
export class CurrentRow {
  private deleted = false;
  constructor(
    readonly tableName: string,
    public record: any,
    public publicationFields: any[],
  ) {}

  static new(table: string, record: any): CurrentRow {
    return CurrentRow.newWithBindingFields(table, record, CurrentRowBindingRole.PhysicalColumn);
  }

  static newWithBindingFields(table: string, record: any, defaultField: any): CurrentRow {
    const fields = record
      .descriptor()
      .fields()
      .map(() => defaultField);
    return CurrentRow.newWithExplicitBindingFields(table, record, fields);
  }

  static newWithExplicitBindingFields(
    table: string,
    record: any,
    bindingFields: any[],
  ): CurrentRow {
    const names = record
      .descriptor()
      .fields()
      .map((field: any, i: number) =>
        bindingFields[i] === CurrentRowBindingRole.LogicalField
          ? descriptorPublicName(field)
          : undefined,
      );
    return CurrentRow.newWithExplicitBindingFieldsAndNames(table, record, bindingFields, names);
  }

  static newWithExplicitBindingFieldsAndNames(
    table: string,
    record: any,
    bindingFields: any[],
    bindingFieldNames: (string | undefined)[],
  ): CurrentRow {
    const fields = record.descriptor().fields();
    if (bindingFields.length !== fields.length)
      throw new Error("native binding fields must align with their record descriptor");
    if (bindingFieldNames.length !== bindingFields.length)
      throw new Error("native binding field names must align with their record descriptor");
    const publication = fields.map((field: any, i: number) => {
      const name: string = field.name;
      if (name === undefined) throw new Error("current row fields are named");
      const publicName = bindingFieldNames[i];
      if (bindingFields[i] === CurrentRowBindingRole.PhysicalColumn) {
        const outputName = publicName ?? CurrentRow.physicalApplicationName(field);
        return outputName !== undefined
          ? { kind: "UnresolvedSourceCell", outputName }
          : { kind: "ResultField", name, visibility: resultVisibilityForMetadata(name) };
      }
      return {
        kind: "ResultField",
        visibility:
          publicName !== undefined ? "ApplicationCell" : resultVisibilityForMetadata(name),
        name: publicName ?? name,
      };
    });
    return CurrentRow.newWithPublicationFields(table, record, publication);
  }

  static newWithPublicationFields(
    table: string,
    record: any,
    publicationFields: any[],
  ): CurrentRow {
    if (publicationFields.length !== record.descriptor().fields().length)
      throw new Error("publication fields must align with record slots");
    return new CurrentRow(table, record, publicationFields);
  }

  intoDeleted(): CurrentRow {
    this.deleted = true;
    return this;
  }
  isDeleted(): boolean {
    return this.deleted;
  }
  bindingFields(): any[] {
    return this.publicationFields.map(publicationRole);
  }
  bindingFieldNames(): (string | undefined)[] {
    return this.publicationFields.map((f) =>
      f.kind === "ResultField" &&
      (f.visibility === "ApplicationCell" || f.visibility === "PublicProvenance")
        ? f.name
        : undefined,
    );
  }
  table(): string {
    return this.tableName;
  }

  rowUuid(): RowUuid {
    const idx =
      this.record.descriptor().fieldIndex("row_uuid") ?? CurrentRowRecord.FIELD_ROW_UUID_IDX;
    return this.record.borrowed().getUuid(idx);
  }

  cellAt(columnPosition: number): Value | undefined {
    const rowIdx = this.record.descriptor().fieldIndex("row_uuid");
    const userCells = rowIdx === undefined ? 0 : rowIdx + 1;
    return stripNullable(this.record.borrowed().getIdx(userCells + columnPosition));
  }

  cell(table: any, column: string): Value | undefined {
    const idx = this.applicationColumnIndex(table, column);
    if (idx === undefined) return undefined;
    let value: Value;
    try {
      value = this.record.borrowed().getIdx(idx);
    } catch {
      return undefined;
    }
    const declared = table.columns.find((c: any) => c.name === column);
    if (!declared) return undefined;
    if (
      declared.columnType.kind === "Nullable" &&
      valueEquals(this.record.descriptor().fields()[idx]?.valueType, declared.columnType)
    )
      return value;
    return stripNullable(value);
  }

  private applicationColumnIndex(table: any, column: string): number | undefined {
    if (!table.columns.some((c: any) => c.name === column)) return undefined;
    return this.applicationColumnIndexByName(column);
  }

  applicationColumnIndexByName(column: string): number | undefined {
    for (const role of [CurrentRowBindingRole.PhysicalColumn, CurrentRowBindingRole.LogicalField]) {
      const index = this.publicationFields.findIndex(
        (f) => publicationRole(f) === role && publicationApplicationName(f) === column,
      );
      if (index >= 0) return index;
    }
    return undefined;
  }

  private static physicalApplicationName(field: any): string | undefined {
    const name = descriptorPublicName(field);
    if (name === undefined) return undefined;
    if (field.name === name)
      return name.startsWith(USER_COLUMN_PREFIX)
        ? name.slice(USER_COLUMN_PREFIX.length)
        : undefined;
    return name;
  }

  encodedRecord(): [any, Bytes] {
    return [this.record.descriptor(), this.record.raw()];
  }

  applicationField(name: string): Value | undefined {
    const index = this.applicationColumnIndexByName(name);
    return index === undefined ? undefined : this.record.borrowed().getIdx(index);
  }

  rawField(field: string): Value | undefined {
    const idx = this.record.descriptor().fieldIndex(field);
    return idx === undefined ? undefined : this.record.borrowed().getIdx(idx);
  }

  private provenanceFieldIndex(column: string): number | undefined {
    const stored = PROVENANCE_STORED[column];
    if (!stored) return undefined;
    const d = this.record.descriptor();
    const direct = d.fieldIndex(column);
    if (direct !== undefined) return direct;
    return d.fieldIndex("schema_version") !== undefined && d.fieldIndex("branch_key") !== undefined
      ? d.fieldIndex(stored)
      : undefined;
  }

  provenanceValue(column: string): Value | undefined {
    const index = this.provenanceFieldIndex(column);
    if (index === undefined) return undefined;
    const record = this.record.borrowed();
    if (column === "$createdBy" || column === "$updatedBy")
      return RowAuthor.fromRecord(record.getRecord(index)).toValue();
    return { kind: "U64", value: record.getU64(index) };
  }

  provenance(): RowProvenance | undefined {
    const idx = ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"].map((c) =>
      this.provenanceFieldIndex(c),
    );
    if (idx.some((i) => i === undefined)) return undefined;
    const r = this.record.borrowed();
    return {
      createdBy: RowAuthor.fromRecord(r.getRecord(idx[0])).asAuthorSubject(),
      createdAt: r.getU64(idx[1]),
      updatedBy: RowAuthor.fromRecord(r.getRecord(idx[2])).asAuthorSubject(),
      updatedAt: r.getU64(idx[3]),
    };
  }

  project(table: any, columns: string[]): CurrentRow {
    const binding = (name: string) => this.applicationColumnIndexByName(name);
    const publicName = (name: string) => {
      const i = binding(name);
      return (
        (i !== undefined ? publicationApplicationName(this.publicationFields[i]) : undefined) ??
        name
      );
    };
    const roleAt = (i: number | undefined) =>
      (i !== undefined
        ? this.publicationFields[i] && publicationRole(this.publicationFields[i])
        : undefined) ?? CurrentRowBindingRole.LogicalField;
    const selected = new Set(columns);
    const projected = table.columns.filter((c: any) => selected.has(c.name));
    const fields = [DescriptorField.new("row_uuid", { kind: "Uuid" })];
    const values: Value[] = [{ kind: "Uuid", value: this.rowUuid() }];
    const bindingFields = [roleAt(this.record.descriptor().fieldIndex("row_uuid"))];
    const names: (string | undefined)[] = [undefined];
    for (const column of projected) {
      const name = publicName(column.name);
      fields.push(
        DescriptorField.new(userColumnField(column.name), {
          kind: "Nullable",
          inner: column.columnType,
        }).withIdentity({
          kind: "Name",
          name,
        }),
      );
      const i = binding(column.name);
      let cell: Value | undefined;
      if (i !== undefined) {
        try {
          cell = stripNullable(this.record.borrowed().getIdx(i));
        } catch {
          cell = undefined;
        }
      }
      if (column.columnType.kind === "Nullable") {
        values.push(
          cell === undefined
            ? { kind: "Nullable" }
            : cell.kind === "Nullable"
              ? { kind: "Nullable", value: cell }
              : { kind: "Nullable", value: { kind: "Nullable", value: cell } },
        );
      } else values.push({ kind: "Nullable", value: cell });
      bindingFields.push(roleAt(i));
      names.push(name);
    }
    const provenance = this.provenance();
    if (provenance) {
      fields.push(
        DescriptorField.new("$createdBy", RowAuthor.valueType()),
        DescriptorField.new("$createdAt", { kind: "U64" }),
        DescriptorField.new("$updatedBy", RowAuthor.valueType()),
        DescriptorField.new("$updatedAt", { kind: "U64" }),
      );
      const author = (s: AuthorSubject) => {
        try {
          return RowAuthor.fromPersistedSubject(s).toValue();
        } catch {
          throw nodeErr("UnadmittedWriteAuthor");
        }
      };
      values.push(
        author(provenance.createdBy),
        { kind: "U64", value: provenance.createdAt },
        author(provenance.updatedBy),
        { kind: "U64", value: provenance.updatedAt },
      );
      for (const c of ["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"]) {
        bindingFields.push(roleAt(this.provenanceFieldIndex(c)));
        names.push(undefined);
      }
    }
    const txAlias = this.projectedTxAlias();
    if (txAlias) {
      fields.push(
        DescriptorField.new("tx_time", { kind: "U64" }),
        DescriptorField.new("tx_node_id", { kind: "U64" }),
      );
      values.push({ kind: "U64", value: txAlias[0] }, { kind: "U64", value: txAlias[1] });
      bindingFields.push(CurrentRowBindingRole.LogicalField, CurrentRowBindingRole.LogicalField);
      names.push(undefined, undefined);
    }
    const descriptor = RecordDescriptor.newWithFields(fields);
    const raw = descriptor.create(values);
    const row = CurrentRow.newWithExplicitBindingFieldsAndNames(
      table.name,
      OwnedRecord.new(raw, descriptor),
      bindingFields,
      names,
    );
    row.publicationFields = row.publicationFields.map((field) => {
      const name = publicationApplicationName(field);
      if (name === undefined) return field;
      const index = this.applicationColumnIndexByName(name);
      return index === undefined ? field : this.publicationFields[index];
    });
    return row;
  }

  /** Project a source row into an explicit relation output, keeping the facade's aliases. */
  projectRelation(table: any, columns: any[]): CurrentRow {
    const fields = [DescriptorField.new("row_uuid", { kind: "Uuid" })];
    const values: Value[] = [{ kind: "Uuid", value: this.rowUuid() }];
    const publication: any[] = [
      { kind: "ResultField", name: "row_uuid", visibility: "HiddenMetadata" },
    ];
    for (const projection of columns) {
      const expr = projection.expr;
      let value: Value;
      let fieldType: ValueType;
      if (
        (expr.kind === "RowId" && expr.ref.kind === "Current") ||
        (expr.kind === "Column" && expr.ref.column === "id")
      ) {
        value = { kind: "Uuid", value: this.rowUuid() };
        fieldType = { kind: "Uuid" };
      } else if (expr.kind === "Column") {
        const position = table.columns.findIndex((c: any) => c.name === expr.ref.column);
        if (position < 0)
          throw invalidStored("relation output column is absent from the read schema");
        fieldType = table.columns[position].columnType;
        const cell = this.cellAt(position);
        if (cell !== undefined) value = cell;
        else if (fieldType.kind === "Nullable") value = { kind: "Nullable" };
        else throw invalidStored("relation output of a non-nullable column has no value");
      } else {
        throw invalidStored("relation output contains an unsupported expression");
      }
      values.push(value);
      fields.push(
        DescriptorField.new(projection.alias, fieldType).withIdentity({
          kind: "Name",
          name: projection.alias,
        }),
      );
      publication.push({
        kind: "ResultField",
        name: projection.alias,
        visibility: "ApplicationCell",
      });
    }
    const descriptor = RecordDescriptor.newWithFields(fields);
    return CurrentRow.newWithPublicationFields(
      table.name,
      OwnedRecord.new(descriptor.create(values), descriptor),
      publication,
    );
  }

  projectedTxAlias(): [number, number] | undefined {
    const fields = this.record.descriptor().fields();
    const stampIdx = fields.findIndex((f: any) => f.name === "tx_time");
    if (stampIdx < 0 || fields[stampIdx + 1]?.name !== "tx_node_id") return undefined;
    try {
      const b = this.record.borrowed();
      const time = b.getU64(stampIdx);
      const alias = b.getU64(stampIdx + 1);
      return time === 0 && alias === 0 ? undefined : [time, alias];
    } catch {
      return undefined;
    }
  }

  /** Compare the row data visible to a subscription, independent of the descriptor. */
  subscriptionEquivalent(other: CurrentRow): boolean {
    let provenanceMatches: boolean;
    try {
      provenanceMatches = JSON.stringify(this.provenance()) === JSON.stringify(other.provenance());
    } catch {
      provenanceMatches = false;
    }
    return this.aggregatePayloadEquivalent(other) && provenanceMatches;
  }

  aggregatePayloadEquivalent(other: CurrentRow): boolean {
    return (
      this.tableName === other.tableName &&
      String(this.rowUuid()) === String(other.rowUuid()) &&
      this.deleted === other.deleted &&
      this.subscriptionCellsEquivalent(other)
    );
  }

  private subscriptionCellsEquivalent(other: CurrentRow): boolean {
    const left = this.canonicalSubscriptionCells();
    const right = other.canonicalSubscriptionCells();
    if (!left || !right || left.length !== right.length) return false;
    return left.every(([n, b], i) => n === right[i][0] && compareBytes(b, right[i][1]) === 0);
  }

  private canonicalSubscriptionCells(): [string, Bytes][] | undefined {
    const cells: [string, Bytes][] = [];
    try {
      for (const [name, value] of this.subscriptionCells()) cells.push([name, encodeValue(value)]);
    } catch {
      return undefined;
    }
    cells.sort((a, b) => cmpStr(a[0], b[0]) || compareBytes(a[1], b[1]));
    return cells;
  }

  subscriptionCells(): [string, Value | undefined][] {
    const borrowed = this.record.borrowed();
    const out: [string, Value | undefined][] = [];
    this.publicationFields.forEach((field, idx) => {
      const name = publicationApplicationName(field);
      if (name === undefined) return;
      let value: Value;
      try {
        value = borrowed.getIdx(idx);
      } catch {
        return;
      }
      out.push([name, stripNullable(value)]);
    });
    return out;
  }
}

/** Cheap read-only handle for historical settled-state reads. */
export class HistoricalRead {
  constructor(
    readonly node: NodeState,
    readonly positionAt: GlobalTime,
  ) {}
  position(): GlobalTime {
    return this.positionAt;
  }
}

export const newSyncMetrics = () => ({
  parkedOrphans: 0,
  parkedOrphansResolved: 0,
  parkedCatalogueOrphans: 0,
  parkedCatalogueOrphansResolved: 0,
  droppedMalformedRelayCommitUnits: 0,
  parkedCatalogueShapes: 0,
  parkedCatalogueShapesResolved: 0,
  droppedDetachedSubscriptionMessages: 0,
  droppedPeerRequestMessages: 0,
  transportBackpressureRetries: 0,
  receiverBulkBundleIngests: 0,
  receiverPerBundleIngests: 0,
  receiverBulkIngestCommits: 0,
  receiverHistoryTableProbes: 0,
  receiverHistoryRebuildRowsAvoided: 0,
  authoritativeResetMissingPayloadFallbacks: 0,
  peerPayloadInventoryMissingFallbacks: 0,
});
export type SyncMetrics = ReturnType<typeof newSyncMetrics>;

export const newQueryEngineReadMetrics = () => ({
  policyAuthorizationGraphs: 0,
  policyAuthorizedSourceJoins: 0,
  sourcePrimaryKeyScans: 0,
  sourceIndexProbes: 0,
  sourceGlobalTimeRangeScans: 0,
  sourceFullScans: 0,
  sourceBranchRegisterFullScans: 0,
  localMaskCountFallbacks: 0,
});
export type QueryEngineReadMetrics = ReturnType<typeof newQueryEngineReadMetrics>;

export type QueryReadProfile = {
  resolveView: number;
  compileProgram: number;
  selectPlan: number;
  executePlan: number;
  decodeMaterialize: number;
  finishRows: number;
  applyProjection: number;
  total: number;
};

export type ContributionMergeRow = { table: string; rowUuid: RowUuid };
export type ContributionMergeRequest = {
  source: BranchSelector;
  target: BranchSelector;
  rows: ContributionMergeRow[];
  madeBy: AuthorSubject;
  permissionSubject?: AuthorSubject;
  nowMs: number;
};

// ---------------------------------------------------------------- mergeable commits

export function valueNeedsLargeValueStaging(value: Value): boolean {
  switch (value.kind) {
    case "String":
      return new TextEncoder().encode(value.value).length > INLINE_VALUE_MAX_BYTES;
    case "Bytes":
      return value.value.length > INLINE_VALUE_MAX_BYTES;
    case "Nullable":
      return value.value !== undefined && valueNeedsLargeValueStaging(value.value);
    default:
      return false;
  }
}

/** Builder for a local mergeable commit. */
export class MergeableCommit {
  branch: BranchSelector = { values: [] };
  madeBy: AuthorSubject = AuthorSubjectSystem;
  permissionSubject?: AuthorSubject;
  cellsMap = new Map<string, Value>();
  authoredColumnsSet?: Set<string>;
  deletionEvent?: DeletionEvent;
  userMetadataJson?: string;
  preparedLargeColumns = new Set<string>();
  stagedLargeValues: any[] = [];
  knownFresh = false;

  constructor(
    public table: string,
    public rowUuid: RowUuid,
    public nowMs: number,
  ) {}

  needsLargeValueStaging(): boolean {
    return [...this.cellsMap.values()].some(valueNeedsLargeValueStaging);
  }
  knownFreshRow(): this {
    this.knownFresh = true;
    return this;
  }
  withBranch(branch: BranchSelector): this {
    this.branch = branch;
    return this;
  }
  withMadeBy(madeBy: AuthorSubject): this {
    this.madeBy = madeBy;
    return this;
  }
  withPermissionSubject(subject: AuthorSubject): this {
    this.permissionSubject = subject;
    return this;
  }
  effectivePermissionSubject(): AuthorSubject {
    return this.permissionSubject ?? this.madeBy;
  }
  cells(cells: Map<string, Value>): this {
    this.cellsMap = new Map(cells);
    return this;
  }
  cell(column: string, value: Value): this {
    this.cellsMap.set(column, value);
    return this;
  }
  stagedLargeCell(column: string, staged: any, nullable: boolean): this {
    const value: Value = { kind: "Large", value: staged.valueRef };
    this.cellsMap.set(column, nullable ? { kind: "Nullable", value } : value);
    this.preparedLargeColumns.add(column);
    this.stagedLargeValues.push(staged.id);
    return this;
  }
  /** Carry an engine-proven physical preimage into a branch-view write. */
  verifiedInheritedLargeCells(inherited: Map<string, Value>): this {
    for (const [column, value] of inherited) {
      const mine = this.cellsMap.get(column);
      if (valueContainsIndirectDescriptor(value) && mine !== undefined && valueEquals(mine, value))
        this.preparedLargeColumns.add(column);
    }
    return this;
  }
  authoredColumns(columns: Set<string>): this {
    this.authoredColumnsSet = columns;
    return this;
  }
  deletion(deletion: DeletionEvent): this {
    this.deletionEvent = deletion;
    return this;
  }
  userMetadata(json: string): this {
    this.userMetadataJson = json;
    return this;
  }

  validate(): void {
    try {
      TxTimeOps.fromPhysicalMs(this.nowMs);
    } catch {
      throw invalidCommit("commit now_ms exceeds packed HLC physical-millisecond range");
    }
    validateMergeableWriteShape(this.cellsMap.size === 0, this.deletionEvent !== undefined);
    for (const [column, value] of this.cellsMap)
      if (valueContainsIndirectDescriptor(value) && !this.preparedLargeColumns.has(column))
        throw invalidCommit(
          "callers must author logical scalar values, not physical large descriptors",
        );
  }
}

export function valueContainsIndirectDescriptor(value: Value): boolean {
  switch (value.kind) {
    case "Large":
      return true;
    case "Tuple":
    case "Array":
      return value.value.some(valueContainsIndirectDescriptor);
    case "Nullable":
      return value.value !== undefined && valueContainsIndirectDescriptor(value.value);
    case "Record":
      try {
        return value.value.toValues().some(valueContainsIndirectDescriptor);
      } catch {
        return false;
      }
    case "Enum":
      try {
        return value.value.record().toValues().some(valueContainsIndirectDescriptor);
      } catch {
        return false;
      }
    default:
      return false;
  }
}

export function collectIndirectDescriptors(value: Value, descriptors: any[]): void {
  switch (value.kind) {
    case "Large":
      if (!descriptors.some((d) => valueEquals(d, value.value))) descriptors.push(value.value);
      return;
    case "Tuple":
    case "Array":
      for (const v of value.value) collectIndirectDescriptors(v, descriptors);
      return;
    case "Nullable":
      if (value.value !== undefined) collectIndirectDescriptors(value.value, descriptors);
      return;
    case "Record":
    case "Enum":
      try {
        const rec = value.kind === "Record" ? value.value : value.value.record();
        for (const v of rec.toValues()) collectIndirectDescriptors(v, descriptors);
      } catch {
        /* undecodable payload holds no descriptors we can see */
      }
      return;
  }
}

export function versionIndirectDescriptors(versions: any[]): any[] {
  const descriptors: any[] = [];
  for (const version of versions)
    for (let position = 0; position < versionApplicationCellCount(version); position++) {
      const value = versionCellAt(version, position);
      if (value !== undefined) collectIndirectDescriptors(value, descriptors);
    }
  return descriptors;
}

export function validateMergeableWriteShape(cellsEmpty: boolean, deletionPresent: boolean): void {
  if (!cellsEmpty && deletionPresent)
    throw invalidCommit("content versions cannot also carry deletion-register events");
  if (cellsEmpty && !deletionPresent)
    throw invalidCommit("mergeable commits must carry content cells or a deletion-register event");
}

// ---------------------------------------------------------------- publications

export class ViewUpdateParts {
  constructor(
    public wireRows: any,
    public subscription: any,
    public settledThrough: GlobalTime,
    public deferSettlement: boolean,
    public resetInputSet: boolean,
    public versionCarriers: any[],
    public peerCompleteTxPayloadRefs: TxId[],
    public authorizationProgress: number | undefined,
    public openingPending: boolean,
    public resultMemberAdds: any[],
    public resultMemberRemoves: any[],
  ) {}
  supportingAdds(): any[] {
    return this.wireRows?.addedRows() ?? [];
  }
  supportingRemoves(): any[] {
    return this.wireRows?.removedRows() ?? [];
  }
}

/** A transaction whose resident Groove publication is visible while persistence is pending. */
export class PublishedTransaction {
  constructor(
    readonly txId: TxId,
    readonly persistence: any,
  ) {}
  changedTables(): string[] {
    return this.persistence.changedTables();
  }
  persist(): Promise<any> {
    return this.persistence.persist();
  }
}

/** A logical operation result and the resident publications it created. */
export class PublicationOutcome<T> {
  constructor(
    public value: T,
    public publications: PublishedTransaction[] = [],
    public postSettlementWork: any[] = [],
  ) {}
  static settled<T>(value: T): PublicationOutcome<T> {
    return new PublicationOutcome(value);
  }
  static published<T>(value: T, publication: PublishedTransaction): PublicationOutcome<T> {
    return new PublicationOutcome(value, [publication]);
  }
  intoParts(): [T, PublishedTransaction[], any[]] {
    return [this.value, this.publications, this.postSettlementWork];
  }
  appendOutcome(this: PublicationOutcome<any[]>, other: PublicationOutcome<any[]>): void {
    this.value.push(...other.value);
    this.publications.push(...other.publications);
    this.postSettlementWork.push(...other.postSettlementWork);
  }
}

export type StagedSchemaLineage = {
  catalogueSeq: number;
  publication: any;
  alias: number;
  mapping: any;
};
export type PendingSchemaLineage = { catalogueSeq: number; publication: any };
export type SchemaLineageActivation = { id: any; catalogueSeq: number };
export type CatalogueBootstrapReady = {
  genesis: SchemaVersionId;
  currentWriteSchema: any;
  activeCatalogueSeq: number;
};
export type CatalogueOpenState = {
  storage: any;
  schemas: Map<string, any>;
  lenses: Map<string, any>;
  schemaVersionAliases: Map<string, number>;
  physicalMappings: Map<string, any>;
  stagedLineages: SortedMap<number, StagedSchemaLineage>;
  pendingLineages: SortedMap<number, PendingSchemaLineage>;
  activeLineagesByTarget: Map<string, StagedSchemaLineage>;
  activeCatalogueSeq: number;
  nextPhysicalTableId: number;
  nextPhysicalColumnId: number;
  catalogueBootstrapMarker: boolean;
  activeSchema: ActiveSchema;
};

export type PreparedQueryPlan =
  | { kind: "Graph"; graph: any; output: any }
  | { kind: "Prepared"; shape: any; params: PreparedQueryParam[]; output: any }
  | { kind: "PeerMaintainedMarker" };
export type PreparedQueryParam = {
  name: string;
  ty: any;
  source: { kind: "User" } | { kind: "Claim"; path: any };
};
export const preparedPlanAppRowSchema = (p: PreparedQueryPlan) =>
  p.kind === "PeerMaintainedMarker" ? undefined : p.output;

// ---------------------------------------------------------------- schema catalogue

export class SchemaCatalogue {
  localSchemaVersionAlias?: number;
  physicalCurrentWinnerProjections = new Map<string, Map<string, [string, string[]]>>();
  lensPathCache = new Map<string, [any, LensPathDirection][] | undefined>();
  compiledLensCache = new Map<string, CompiledLensPath | undefined>();
  physicalWritePlanCache = new Map<string, Map<string, Map<string, any>>>();
  constructor(
    public localSchemaVersionId: SchemaVersionId,
    public schema: any,
    public schemaVersionAliases: Map<string, number>,
    public catalogueSchemas: Map<string, any>,
    public catalogueLenses: Map<string, any>,
    public physicalMappings: Map<string, any>,
    public stagedLineages: SortedMap<number, StagedSchemaLineage>,
    public pendingLineages: SortedMap<number, PendingSchemaLineage>,
    public activeLineagesByTarget: Map<string, StagedSchemaLineage>,
    public activeCatalogueSeq: number,
    public nextPhysicalTableId: number,
    public nextPhysicalColumnId: number,
    public activeSchema: ActiveSchema,
  ) {}

  /** The compiled schema for `version`: the active schema, the local API schema, or a catalogue entry. */
  schemaForVersion(version: SchemaVersionId): any {
    const key = schemaIdKey(version);
    if (key === schemaIdKey(this.activeSchema.schema)) return this.activeSchema.compiled;
    if (key === schemaIdKey(this.localSchemaVersionId)) return this.schema;
    return this.catalogueSchemas.get(key)?.schema;
  }
}

// ---------------------------------------------------------------- node state

/** Storage-backed Jazz node: mergeable history, local reads, and commit-unit sync. */
export class NodeState {
  // Methods contributed by sibling modules and other port areas live on the prototype.
  [method: string]: any;

  selfNodeAlias?: NodeAlias;
  catalogueBootstrapMarker = false;
  clock = new Clock();
  parking = newParking();
  query = newQueryServing();
  openTx = new OpenTxState();
  rejections = { rejectedTransactions: new SortedMap<TxId, any>(cmpTxId) };
  localChunkReader: any;
  chunkResolver: any;
  detachCoveredChunkWaits = { value: false };
  largeValueStagingPolicy = defaultLargeValueStagingPolicy();
  largeValueIngress = { windowStartedMs: 0, admittedBytes: 0 };
  contentRuntimeProvider: any;
  grooveRuntimeToken = nextGrooveRuntimeToken();
  physicalIdentityGeneration = 0;
  nextAuthoritativeResetGeneration = 1;
  authoritativeScalarExitRefresh = false;
  clientLocalLiteralShapes = new Map<string, [WeakRef<object>, any]>();
  authoredCommitDurability: "Global" | "Local" | "None" = "Local";
  relayAuthoritySessionOwner?: any;
  pendingPersistence = new SortedSet<TxId>(cmpTxId);
  nodeAliases = new NodeAliases();
  absentNodeAlias?: NodeUuid;
  aheadCurrentKeys = new AheadCurrentKeys();
  txTouchedDirty = new SortedMap<[TxTime, NodeAlias], any>(
    (a, b) => cmpBig(BigInt(a[0]), BigInt(b[0])) || a[1] - b[1],
  );
  historyTxAuthors = new Map<string, Bytes>();
  historyTxSeqs = new Map<string, GlobalTime>();
  mintingGlobalTime = false;
  syncMetrics = newSyncMetrics();
  queryEngineReadMetrics = newQueryEngineReadMetrics();
  sessionClaims = new Map<string, [AuthorSubject, Map<string, Value>]>();
  sessionClaimRevisions = new Map<string, number>();
  activeSessionClaims?: [AuthorSubject, Map<string, Value>];
  permissionsReady = true;
  catalogueActivationFailed = false;
  initialSyncFlushCadence?: number;
  initialSyncFlushActive = false;
  initialSyncFlushCompleted = false;

  constructor(
    readonly nodeUuidValue: NodeUuid,
    public catalogue: SchemaCatalogue,
    public catalogueBootstrapState: CatalogueBootstrapState,
    public databaseSlot: DatabaseSlot,
    public historyComplete: boolean,
  ) {}

  get database(): any {
    return this.databaseSlot.get();
  }

  reserveTxTimeAfter(highWater: TxTime): void {
    this.mergeTxTime(TxTimeOps.tickAfter(highWater));
  }
}

Object.assign(
  NodeState.prototype,
  lifecycleMethods,
  commitMethods,
  contributionMergeMethods,
  durableMethods,
  catalogueMethods,
  readPayloadMethods,
  evictionMethods,
  globalStateMethods,
  openTxMethods,
  recoveryMethods,
);

export { txIdKey };
