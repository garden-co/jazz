// Maintained (IVM-backed) subscription view: decodes Groove terminal deltas
// into result memberships, version/replacement witnesses, program facts and
// structured app-row trees, and tracks the physical supporting frontier.
import { SortedMap, SortedSet, cmpBytes, cmpStr } from "../../support/collections.js";
import { concatBytes, equalBytes, toHex, utf8 } from "../../support/bytes.js";
import { Blake3Hasher, blake3, blake3Keyed } from "../../support/hash.js";
import { assert } from "../../support/runtime.js";
import {
  type NodeAliases,
  type OwnedRecord,
  type RecordDescriptor,
  type ResultMemberEntry,
  type SupportingRow,
  type TerminalOperation,
  type TxId,
  type Value,
  type ValueType,
  type VersionRow,
  CurrentPayloadEncodePlan,
  EnumValue,
  HISTORY_USER_CELLS,
  NodeErrors,
  ObjectId,
  OutputOccurrenceId,
  OwnedRecord as OwnedRecordApi,
  RealRowMemberEntry,
  RecordDescriptorApi,
  RecordDescriptorFields,
  RowAuthor,
  SyntheticReplacementToken,
  TxId as TxIdApi,
  authoredColumnIdsFromValue,
  cmpResultMember,
  cmpRowUuid,
  cmpSupportingRow,
  cmpTxId,
  debugCoveredInputTrace,
  decodePersistedBranchKey,
  decodePersistedRecordDescriptor,
  defaultBranchCanonicalBytes,
  deletionEventFromValue,
  descriptorEquals,
  encodeAggregatePayloadRecord,
  encodePersistedRecordDescriptor,
  historyValuesFromParts,
  internTable,
  nullableValue,
  programFactKey,
  programSourceKey,
  publicationPublicName,
  recordProjectorRegistryRebound,
  rowUuidKey,
  txIdKey,
  txTimeFromPhysicalMs,
  valueTypeEquals,
  versionTxIdFromAliases,
  BranchKey,
  VersionRowApi,
} from "../../_extern/node-views.js";
import { SupportingFrontier } from "./supporting_frontier.js";
import { TerminalRecordState } from "./terminal_record.js";
import type {
  TerminalRootCarrier,
  TerminalRootLayout,
  TerminalRootPublicField,
} from "./terminal_root.js";

type RowUuid = any;
type TableSchemas = Map<string, any>;
const invalid = (m: string) => NodeErrors.invalidStoredValue(m);

type WitnessLayer = 0 | 1; // Content, Deletion
const layerOf = (row: VersionRow): WitnessLayer => (row.deletion() !== undefined ? 1 : 0);

interface VersionDecodePlan {
  descriptor: RecordDescriptor;
  branchIdx: number | undefined;
  rowIdx: number;
  txTimeIdx: number;
  txNodeIdx: number;
  schemaVersionIdx: number;
  createdByIdx: number;
  createdAtIdx: number;
  updatedByIdx: number;
  updatedAtIdx: number;
  userIndices: Map<string, number>;
  authoredColumnsIdx: number;
}

// ---- retained-byte model -------------------------------------------------
const btreeMapBytes = (len: number) => len * 96;
const btreeSetBytes = (len: number) => len * 64;
const VEC = 24;
const optVecBytes = (v: Uint8Array | undefined) => (v ? VEC + v.length : 0);

function resultMemberEntryBytes(member: ResultMemberEntry): number {
  const base = 128;
  switch (member.kind) {
    case "Row":
    case "TypedRow":
      return (
        base +
        16 +
        member.row.table.length +
        optVecBytes(member.row.branchOrPrefix) +
        optVecBytes(member.row.rowDigest)
      );
    case "Synthetic":
      return base + member.table.length + VEC + member.row.length + 32;
    default:
      return (
        base +
        member.path.length +
        32 +
        member.sourceTable.length +
        member.targetTable.length +
        optVecBytes(member.edgeId) +
        VEC +
        member.revision.length
      );
  }
}
const resultMemberPayloadEntryBytes = (p: any) =>
  64 + resultMemberEntryBytes(p.member) + VEC + p.descriptor.length + VEC + p.record.length;
const versionRowBytes = (row: VersionRow) => 48 + 16 + row.table().length + row.record.raw().length;
const versionKeyBytes = (k: { table: string; raw: Uint8Array }) =>
  48 + 16 + k.table.length + k.raw.length;
const replacementKeyBytes = (k: ReplacementKey) => 40 + 16 + k.table.length;
const weightedVersionBytes = (v: WeightedVersion) =>
  16 + 48 + versionRowBytes(v.payload.row) + versionKeyBytes(v.payload.sortKey);

// ---- retained result maps ------------------------------------------------
class RetainedResultMap<V> {
  readonly entries = new SortedMap<ResultMemberEntry, V>(cmpResultMember);
  entryBytes = 0;
  positiveCount = 0;
  constructor(
    private readonly bytes: (v: V) => number,
    private readonly positive: (v: V) => number = () => 0,
  ) {}
  get size() {
    return this.entries.size;
  }
  get(m: ResultMemberEntry) {
    return this.entries.get(m);
  }
  has(m: ResultMemberEntry) {
    return this.entries.has(m);
  }
  insert(member: ResultMemberEntry, value: V): V | undefined {
    const old = this.entries.set(member, value);
    if (old !== undefined) {
      this.entryBytes -= this.bytes(old);
      this.positiveCount -= this.positive(old);
    } else this.entryBytes += resultMemberEntryBytes(member);
    this.entryBytes += this.bytes(value);
    this.positiveCount += this.positive(value);
    return old;
  }
  remove(member: ResultMemberEntry): V | undefined {
    const value = this.entries.delete(member);
    if (value === undefined) return undefined;
    this.entryBytes -= resultMemberEntryBytes(member) + this.bytes(value);
    this.positiveCount -= this.positive(value);
    return value;
  }
  footprintBytes() {
    return btreeMapBytes(this.entries.size) + this.entryBytes;
  }
}
const weightMap = () =>
  new RetainedResultMap<number>(
    () => 8,
    (w) => (w > 0 ? 1 : 0),
  );
const payloadMap = () => new RetainedResultMap<any>(resultMemberPayloadEntryBytes);

class RetainedResultMembers {
  entries = new SortedSet<ResultMemberEntry>(cmpResultMember);
  entryBytes = 0;
  static from(items: Iterable<ResultMemberEntry>): RetainedResultMembers {
    const m = new RetainedResultMembers();
    for (const i of items) m.insert(i);
    return m;
  }
  get size() {
    return this.entries.size;
  }
  has(m: ResultMemberEntry) {
    return this.entries.has(m);
  }
  insert(member: ResultMemberEntry): boolean {
    const inserted = this.entries.add(member);
    if (inserted) this.entryBytes += resultMemberEntryBytes(member);
    return inserted;
  }
  remove(member: ResultMemberEntry): boolean {
    const removed = this.entries.delete(member);
    if (removed) this.entryBytes -= resultMemberEntryBytes(member);
    return removed;
  }
}

interface WitnessGatedJournal {
  changed: SortedSet<ResultMemberEntry>;
  withheld: SortedSet<ResultMemberEntry>;
  aliasRetargets: number;
  aliasCount: number;
}

// ---- version / replacement indexes ---------------------------------------
interface VersionSortKey {
  table: string;
  rowUuid: RowUuid;
  layer: WitnessLayer;
  raw: Uint8Array;
}
const cmpSortKey = (a: VersionSortKey, b: VersionSortKey) =>
  cmpStr(a.table, b.table) ||
  cmpRowUuid(a.rowUuid, b.rowUuid) ||
  a.layer - b.layer ||
  cmpBytes(a.raw, b.raw);
interface VersionIdentity {
  table: string;
  layer: WitnessLayer;
  raw: Uint8Array;
}
const identityKey = (i: VersionIdentity) => `${i.table}\u0000${i.layer}\u0000${toHex(i.raw)}`;
const versionIdentityFor = (row: VersionRow): VersionIdentity => ({
  table: row.table(),
  layer: layerOf(row),
  raw: row.record.raw(),
});
interface ReplacementKey {
  table: string;
  rowUuid: RowUuid;
  layer: WitnessLayer;
}
const replacementKeyFor = (row: VersionRow): ReplacementKey => ({
  table: row.table(),
  rowUuid: row.rowUuid(),
  layer: layerOf(row),
});
const replacementKeyStr = (k: ReplacementKey) =>
  `${k.table}\u0000${rowUuidKey(k.rowUuid)}\u0000${k.layer}`;

interface VersionPayload {
  row: VersionRow;
  txId: TxId;
  sortKey: VersionSortKey;
}
interface WeightedVersion {
  payload: VersionPayload;
  weight: number;
}

function prepareVersionPayload(
  row: VersionRow,
  identity: VersionIdentity,
  aliases: NodeAliases,
): VersionPayload {
  const txId = versionTxIdFromAliases(row, aliases);
  if (txId === undefined) throw invalid("history tx node alias must exist");
  return {
    row,
    txId,
    sortKey: {
      table: row.table(),
      rowUuid: row.rowUuid(),
      layer: identity.layer,
      raw: identity.raw,
    },
  };
}

class WeightedVersionIndex {
  byTx = new SortedMap<TxId, SortedMap<VersionSortKey, WeightedVersion>>(cmpTxId);
  entryCount = 0;
  entryBytes = 0;

  hasContentWitness(txId: TxId, table: string, rowUuid: RowUuid): boolean {
    const rows = this.byTx.get(txId);
    if (!rows) return false;
    const lower: VersionSortKey = { table, rowUuid, layer: 0, raw: new Uint8Array() };
    const first = rows.range(lower).next();
    if (first.done) return false;
    const k = first.value[0];
    return k.table === table && cmpRowUuid(k.rowUuid, rowUuid) === 0 && k.layer === 0;
  }

  footprintBytes() {
    return btreeMapBytes(this.byTx.size) + btreeMapBytes(this.entryCount) + this.entryBytes;
  }

  applyDelta(payload: VersionPayload, weight: number): void {
    let rows = this.byTx.get(payload.txId);
    if (!rows) {
      if (weight <= 0) return;
      this.byTx.set(payload.txId, (rows = new SortedMap(cmpSortKey)));
    }
    const existing = rows.get(payload.sortKey);
    if (existing) {
      const next = existing.weight + weight;
      this.entryBytes -= weightedVersionBytes(existing);
      if (next > 0) {
        const v = { payload, weight: next };
        this.entryBytes += weightedVersionBytes(v);
        rows.set(payload.sortKey, v);
      } else {
        this.entryBytes -= versionKeyBytes(payload.sortKey);
        this.entryCount--;
        rows.delete(payload.sortKey);
      }
    } else if (weight > 0) {
      const v = { payload, weight };
      this.entryBytes += versionKeyBytes(payload.sortKey) + weightedVersionBytes(v);
      this.entryCount++;
      rows.set(payload.sortKey, v);
    }
    if (rows.size === 0) this.byTx.delete(payload.txId);
  }

  versionsByTx(txId: TxId): VersionRow[] {
    return (this.byTx.get(txId)?.valuesArray() ?? []).map((v) => v.payload.row);
  }
}

class ReplacementIndex {
  contentByKey = new Map<string, Map<string, WeightedVersion>>();
  deletionByKey = new Map<string, Map<string, WeightedVersion>>();
  entryCount = 0;
  entryBytes = 0;
  keyBytes = 0;

  footprintBytes() {
    return (
      btreeMapBytes(this.contentByKey.size + this.deletionByKey.size) +
      btreeMapBytes(this.entryCount) +
      this.keyBytes +
      this.entryBytes
    );
  }

  applyDelta(
    key: ReplacementKey,
    identity: VersionIdentity,
    payload: VersionPayload,
    weight: number,
  ): void {
    const byKey = key.layer === 0 ? this.contentByKey : this.deletionByKey;
    const ks = replacementKeyStr(key);
    let rowVersions = byKey.get(ks);
    if (!rowVersions) byKey.set(ks, (rowVersions = new Map()));
    const wasEmpty = rowVersions.size === 0;
    const ik = identityKey(identity);
    const existing = rowVersions.get(ik);
    if (existing) {
      const next = existing.weight + weight;
      this.entryBytes -= weightedVersionBytes(existing);
      if (next > 0) {
        const v = { payload, weight: next };
        this.entryBytes += weightedVersionBytes(v);
        rowVersions.set(ik, v);
      } else {
        this.entryBytes -= versionKeyBytes(identity);
        this.entryCount--;
        rowVersions.delete(ik);
      }
    } else if (weight > 0) {
      const v = { payload, weight };
      this.entryBytes += versionKeyBytes(identity) + weightedVersionBytes(v);
      this.entryCount++;
      rowVersions.set(ik, v);
    }
    if (wasEmpty && rowVersions.size > 0) this.keyBytes += replacementKeyBytes(key);
    else if (!wasEmpty && rowVersions.size === 0) this.keyBytes -= replacementKeyBytes(key);
    if (rowVersions.size === 0) byKey.delete(ks);
  }

  containsKey(key: ReplacementKey): boolean {
    return (key.layer === 0 ? this.contentByKey : this.deletionByKey).has(replacementKeyStr(key));
  }

  replacementFor(
    table: string,
    rowUuid: RowUuid,
  ): [VersionRow | undefined, VersionRow | undefined] {
    return [
      replacementWinner(this.contentByKey.get(replacementKeyStr({ table, rowUuid, layer: 0 }))),
      replacementWinner(this.deletionByKey.get(replacementKeyStr({ table, rowUuid, layer: 1 }))),
    ];
  }
}

function replacementWinner(
  versions: Map<string, WeightedVersion> | undefined,
): VersionRow | undefined {
  let best: WeightedVersion | undefined;
  for (const v of versions?.values() ?? [])
    if (v.weight > 0 && (!best || cmpTxId(v.payload.txId, best.payload.txId) >= 0)) best = v;
  return best?.payload.row;
}

// ---- public transition/event types ---------------------------------------
export interface ResultTransitions {
  authoritativeMembershipChanged: boolean;
  authoritativeMemberAdds: SortedSet<ResultMemberEntry>;
  adds: ResultMemberEntry[];
  removes: ResultMemberEntry[];
  resultPayloadAdds: [ResultMemberEntry, any][];
  resultPayloadRemoves: ResultMemberEntry[];
  programFactAdds: any[];
  programFactRemoves: any[];
  supportingChanged: boolean;
  terminalOperations: TerminalOperation[];
  observedResultDeltaBatches: number;
  requiresAuthoritativeMembershipReconcile: boolean;
}
export const emptyTransitions = (): ResultTransitions => ({
  authoritativeMembershipChanged: false,
  authoritativeMemberAdds: new SortedSet(cmpResultMember),
  adds: [],
  removes: [],
  resultPayloadAdds: [],
  resultPayloadRemoves: [],
  programFactAdds: [],
  programFactRemoves: [],
  supportingChanged: false,
  terminalOperations: [],
  observedResultDeltaBatches: 0,
  requiresAuthoritativeMembershipReconcile: false,
});

export type DecodedMaintainedEvent =
  | { kind: "ResultCurrent"; member: ResultMemberEntry; payload: any }
  | {
      kind: "AggregateResult";
      member: ResultMemberEntry;
      payload: any;
      synthetic: any;
      valueFields: string[];
    }
  | {
      kind:
        | "VersionContent"
        | "VersionDeletion"
        | "ReplacementContent"
        | "ReplacementDeletion"
        | "SharedVersion";
      source: any;
      row: VersionRow;
    }
  | { kind: "ProgramSourceCoverage"; coverage: any }
  | { kind: "RelationEdge"; edge: any }
  | { kind: "StructuredAppRow"; root: RowUuid; record: OwnedRecord };

type MaintainedTerminalKind =
  | { kind: "ResultCurrent"; schema: any }
  | { kind: "AggregateResult"; schema: any }
  | {
      kind:
        | "VersionContent"
        | "VersionDeletion"
        | "ReplacementContent"
        | "ReplacementDeletion"
        | "SharedContent"
        | "SharedDeletion";
      schema: any;
    }
  | { kind: "ProgramSourceCoverage"; schema: any }
  | { kind: "RelationEdge"; schema: any }
  | { kind: "RootCollectorAppRows"; schema: any; layout: TerminalRootLayout }
  | { kind: "DirectAppRows"; schema: any }
  | { kind: "AggregateAppRows"; schema: any };

const isResultTerminal = (k: MaintainedTerminalKind) =>
  k.kind === "ResultCurrent" || k.kind === "AggregateResult";
const requiresAuthoritativeReconcile = (k: MaintainedTerminalKind) =>
  k.kind === "VersionDeletion" || k.kind === "ReplacementDeletion" || k.kind === "SharedDeletion";

type NetEvent =
  | { kind: "Result"; member: ResultMemberEntry; payload: any }
  | {
      kind: "AggregateResult";
      member: ResultMemberEntry;
      payload: any;
      synthetic: any;
      valueFields: string[];
    }
  | { kind: "Version" | "SharedVersion"; source: any; identity: VersionIdentity; row: VersionRow }
  | {
      kind: "Replacement";
      source: any;
      key: ReplacementKey;
      identity: VersionIdentity;
      row: VersionRow;
    }
  | { kind: "ProgramFact"; fact: any }
  | { kind: "StructuredAppRow"; root: RowUuid; record: OwnedRecord };

/** Sortable identity: variant ordinal then a structural key. */
function netEventIdentity(e: NetEvent): [number, string, ResultMemberEntry?] {
  switch (e.kind) {
    case "Result":
    case "AggregateResult":
      return [0, "", e.member];
    case "Version":
      return [1, `${programSourceKey(e.source)}\u0000${identityKey(e.identity)}`];
    case "Replacement":
      return [
        2,
        `${programSourceKey(e.source)}\u0000${replacementKeyStr(e.key)}\u0000${identityKey(e.identity)}`,
      ];
    case "SharedVersion":
      return [3, `${programSourceKey(e.source)}\u0000${identityKey(e.identity)}`];
    case "ProgramFact":
      return [4, programFactKey(e.fact)];
    case "StructuredAppRow":
      return [5, `${rowUuidKey(e.root)}\u0000${toHex(e.record.raw())}`];
  }
}

/** Root collector keys begin with the root source occurrence UUID. */
function terminalRootUuidFromKey(key: Uint8Array): RowUuid | undefined {
  if (key[0] !== 10 || key.length < 17) return undefined;
  return key.slice(1, 17);
}

const bytesKey = (b: Uint8Array) => toHex(b);

export interface MaintainedSubscriptionViewFootprint {
  resultRows: number;
  resultWeights: number;
  resultPayloads: number;
  versionIdentities: number;
  versionTxEntries: number;
  replacementEntries: number;
  structuredAppRows: number;
  resultWeightsBytes: number;
  resultPayloadsBytes: number;
  versionsBytes: number;
  supportingFrontierBytes: number;
  replacementsBytes: number;
  structuredAppRowsBytes: number;
  totalHeapBytes: number;
}

export class MaintainedSubscriptionView {
  /** Lone-subscriber token; clones start without it. */
  private clientLocalLiteralToken: unknown;
  private readView: any = undefined;
  private witnessTableNames = new Map<string, string>();
  private resultWeights = weightMap();
  private publishedResultMembersSet = new RetainedResultMembers();
  private unreconciledResultMembers: RetainedResultMembers | undefined;
  private witnessGatedJournal: WitnessGatedJournal | undefined;
  private resultPayloads = payloadMap();
  private publishedResultPayloads = payloadMap();
  private structuredAppRows = new SortedMap<Uint8Array, SortedMap<Uint8Array, number>>(cmpBytes);
  private structuredTerminalRecords = new SortedMap<Uint8Array, TerminalRecordState>(cmpBytes);
  private structuredRootKeys = new SortedMap<Uint8Array, RowUuid>(cmpBytes);
  private structuredRootKeyOrder: Uint8Array[] = [];
  private structuredAppRowDescriptor: RecordDescriptor | undefined;
  private retainsStructuredAppRows = true;
  private storageBackedResultMaterialization = false;
  private inlineContentBranchKeys = new Set<string>();
  private programFactWeights = new Map<string, [any, number]>();
  supporting = new SupportingFrontier();
  physicalTables = new SortedMap<string, any>(cmpStr);
  targetedRefreshTables = new Set<string>();
  targetedRefreshUncertain = false;
  private selectedDeletionWitnesses = new SortedMap<SupportingRow, VersionRow>(cmpSupportingRow);
  private versions = new WeightedVersionIndex();
  private replacements = new ReplacementIndex();

  holdClientLocalLiteralToken(token: unknown): void {
    this.clientLocalLiteralToken = token;
  }
  setReadView(readView: any): void {
    this.readView = readView;
  }
  /** Keyed by `${logicalTable}\0${schemaVersionAlias}`. */
  setWitnessTableNames(names: Map<string, string>): void {
    this.witnessTableNames = names;
  }

  private supportingRowForVersion(
    source: any,
    row: VersionRow,
    aliases: NodeAliases,
  ): SupportingRow {
    const physicalTable = this.physicalTables.get(source.table);
    if (physicalTable === undefined)
      throw invalid("support terminal has no physical table identity");
    const tx = versionTxIdFromAliases(row, aliases);
    if (tx === undefined) throw invalid("support input tx node alias must exist");
    const branch: Uint8Array = row.branchKey().canonicalBytes();
    const versionTable =
      this.witnessTableNames.get(`${row.table()}\u0000${row.schemaVersionAlias()}`) ?? row.table();
    return {
      physicalTable,
      versionTable,
      row: row.rowUuid(),
      version: {
        tx,
        schemaVersion: undefined,
        layer: layerOf(row) === 0 ? "Content" : "Deletion",
        batch: tx,
        branchOrPrefix: branch.length > 0 ? branch : undefined,
        rowDigest: undefined,
      },
    };
  }

  usesStorageBackedResultMaterialization(): boolean {
    return this.storageBackedResultMaterialization;
  }
  enableStorageBackedResultMaterialization(): void {
    this.storageBackedResultMaterialization = true;
    this.unreconciledResultMembers = undefined;
    this.witnessGatedJournal = undefined;
  }
  enableInlineContentBranchKey(branchKey: any): void {
    this.inlineContentBranchKeys.add(bytesKey(branchKey.canonicalBytes()));
    this.witnessGatedJournal = undefined;
  }

  static terminalSchemasForProgram(program: any): MaintainedTerminalSchemas {
    return MaintainedTerminalSchemas.forProgram(program);
  }

  applyTypedDeltas(
    sink: string,
    deltas: any,
    schemas: MaintainedTerminalSchemas,
    tables: TableSchemas,
    aliases: NodeAliases,
  ): ResultTransitions {
    const kind = schemas.get(sink);
    const nonEmpty = !deltas.isEmpty();
    const decodePlanCache = new Map<string, VersionDecodePlan>();
    const payloadPlans = new Map<RecordDescriptor, any>();
    const decoded: [DecodedMaintainedEvent, number][] = [];
    for (const [record, weight] of deltas.iter())
      decoded.push([
        decodeTypedTerminalRecord(
          record,
          kind,
          tables,
          aliases,
          decodePlanCache,
          payloadPlans,
          this.readView,
        ),
        weight,
      ]);
    const transitions = this.applyDecodedDeltaResults(decoded, aliases);
    if (nonEmpty && isResultTerminal(kind)) transitions.observedResultDeltaBatches += 1;
    transitions.requiresAuthoritativeMembershipReconcile =
      nonEmpty && requiresAuthoritativeReconcile(kind);
    return transitions;
  }

  applyMultisinkDeltas(
    deltas: any,
    schemas: MaintainedTerminalSchemas,
    tables: TableSchemas,
    aliases: NodeAliases,
  ): ResultTransitions {
    const transitions = emptyTransitions();
    for (const [sink, terminal] of deltas.terminalSinks) {
      const kind = schemas.get(sink);
      if (debugCoveredInputTrace() && terminal.operations.length > 0)
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=terminal_operations sink=${sink} kind=${kind.kind} operations=${terminal.operations.length}`,
        );
      if (kind.kind !== "RootCollectorAppRows") continue;
      const operations = terminal.operations.map((op: TerminalOperation) =>
        rebindTerminalOperationToLayout(op, kind.layout),
      );
      const removedRoots = new Set<string>();
      for (const operation of operations) {
        const rk = bytesKey(operation.rootKey);
        if (operation.path.length > 0 && removedRoots.has(rk)) continue;
        this.applyStructuredTerminalOperation(operation);
        if (operation.path.length === 0) {
          if (operation.edit.kind === "Remove") removedRoots.add(rk);
          else if (operation.edit.kind === "Insert") removedRoots.delete(rk);
        }
        transitions.terminalOperations.push(operation);
      }
    }
    for (const [sink, sinkDeltas] of deltas.sinks) {
      const kind = schemas.get(sink);
      if (debugCoveredInputTrace() && !sinkDeltas.isEmpty())
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=terminal_sink sink=${sink} kind=${kind.kind} records=${sinkDeltas.deltas.length}`,
        );
      if (kind.kind === "RootCollectorAppRows") continue;
      const d = this.applyTypedDeltas(sink, sinkDeltas, schemas, tables, aliases);
      transitions.supportingChanged ||= d.supportingChanged;
      transitions.adds.push(...d.adds);
      transitions.removes.push(...d.removes);
      transitions.programFactAdds.push(...d.programFactAdds);
      transitions.programFactRemoves.push(...d.programFactRemoves);
      transitions.resultPayloadAdds.push(...d.resultPayloadAdds);
      transitions.resultPayloadRemoves.push(...d.resultPayloadRemoves);
      transitions.observedResultDeltaBatches += d.observedResultDeltaBatches;
      transitions.requiresAuthoritativeMembershipReconcile ||=
        d.requiresAuthoritativeMembershipReconcile;
    }
    this.finalizeMultisinkTransitions(transitions, aliases);
    if (
      debugCoveredInputTrace() &&
      (transitions.adds.length > 0 ||
        transitions.programFactAdds.length > 0 ||
        transitions.programFactRemoves.length > 0)
    )
      console.error(
        `JAZZ_COVERED_INPUT_TRACE stage=maintained_transition adds=${transitions.adds.length} removes=${transitions.removes.length} fact_adds=${transitions.programFactAdds.length} fact_removes=${transitions.programFactRemoves.length} terminal_ops=${transitions.terminalOperations.length}`,
      );
    return transitions;
  }

  private finalizeMultisinkTransitions(transitions: ResultTransitions, aliases: NodeAliases): void {
    transitions.resultPayloadAdds = transitions.resultPayloadAdds.filter(
      ([m]) => m.asRow() === undefined,
    );
    transitions.resultPayloadRemoves = transitions.resultPayloadRemoves.filter(
      (m) => m.asRow() === undefined,
    );
    const [adds, removes, payloadAdds, payloadRemoves] =
      this.reconcilePublishableResultMembers(aliases);
    transitions.adds = adds;
    transitions.removes = removes;
    transitions.resultPayloadAdds.push(...payloadAdds);
    transitions.resultPayloadRemoves.push(...payloadRemoves);
  }

  private applyDecodedDeltaResults(
    rows: [DecodedMaintainedEvent, number][],
    aliases: NodeAliases,
  ): ResultTransitions {
    const net = new Map<
      string,
      { id: [number, string, ResultMemberEntry?]; event: NetEvent; weight: number }
    >();
    for (const [event, weight] of rows) {
      let ne: NetEvent;
      switch (event.kind) {
        case "ResultCurrent":
          ne = { kind: "Result", member: event.member, payload: event.payload };
          break;
        case "AggregateResult":
          ne = { ...event, kind: "AggregateResult" };
          break;
        case "VersionContent":
        case "VersionDeletion":
          ne = {
            kind: "Version",
            source: event.source,
            identity: versionIdentityFor(event.row),
            row: event.row,
          };
          break;
        case "ReplacementContent":
        case "ReplacementDeletion":
          ne = {
            kind: "Replacement",
            source: event.source,
            key: replacementKeyFor(event.row),
            identity: versionIdentityFor(event.row),
            row: event.row,
          };
          break;
        case "SharedVersion":
          ne = {
            kind: "SharedVersion",
            source: event.source,
            identity: versionIdentityFor(event.row),
            row: event.row,
          };
          break;
        case "ProgramSourceCoverage":
          ne = {
            kind: "ProgramFact",
            fact: { kind: "ProgramSourceCoverage", coverage: event.coverage },
          };
          break;
        case "RelationEdge":
          ne = { kind: "ProgramFact", fact: { kind: "RelationEdge", edge: event.edge } };
          break;
        case "StructuredAppRow":
          ne = { kind: "StructuredAppRow", root: event.root, record: event.record };
          break;
      }
      const id = netEventIdentity(ne);
      const key = id[0] === 0 ? `0\u0000${JSON.stringify(id[2])}` : `${id[0]}\u0000${id[1]}`;
      const e = net.get(key);
      if (e) e.weight += weight;
      else net.set(key, { id, event: ne, weight });
    }
    const ordered = [...net.values()].sort(
      (a, b) =>
        a.id[0] - b.id[0] ||
        (a.id[0] === 0 ? cmpResultMember(a.id[2], b.id[2]) : cmpStr(a.id[1], b.id[1])),
    );
    const transitions = emptyTransitions();
    for (const { event, weight } of ordered) {
      if (weight === 0) continue;
      if (debugCoveredInputTrace())
        console.error(
          `JAZZ_COVERED_INPUT_TRACE stage=apply_decoded_event event=${event.kind} weight=${weight}`,
        );
      switch (event.kind) {
        case "Result":
          this.applyResultDelta(event.member, event.payload, weight, transitions);
          break;
        case "AggregateResult":
          this.applyAggregateResultDelta(event.member, event.payload, weight, transitions);
          break;
        case "Version": {
          const covered = this.supportingRowForVersion(event.source, event.row, aliases);
          const payload = prepareVersionPayload(event.row, event.identity, aliases);
          if (weight < 0) this.witnessGatedJournal = undefined;
          this.versions.applyDelta(payload, weight);
          this.supporting.apply(0, covered, weight);
          transitions.supportingChanged = true;
          break;
        }
        case "Replacement": {
          const covered = this.supportingRowForVersion(event.source, event.row, aliases);
          const payload = prepareVersionPayload(event.row, event.identity, aliases);
          if (weight < 0 || this.replacements.containsKey(event.key))
            this.witnessGatedJournal = undefined;
          this.replacements.applyDelta(event.key, event.identity, payload, weight);
          this.supporting.apply(1, covered, weight);
          transitions.supportingChanged = true;
          break;
        }
        case "SharedVersion": {
          const covered = this.supportingRowForVersion(event.source, event.row, aliases);
          const key = replacementKeyFor(event.row);
          const payload = prepareVersionPayload(event.row, event.identity, aliases);
          if (weight < 0 || this.replacements.containsKey(key))
            this.witnessGatedJournal = undefined;
          this.versions.applyDelta(payload, weight);
          this.replacements.applyDelta(key, event.identity, payload, weight);
          this.supporting.apply(0, covered, weight);
          this.supporting.apply(1, covered, weight);
          transitions.supportingChanged = true;
          break;
        }
        case "ProgramFact": {
          if (event.fact.kind === "ProgramSourceCoverage") break;
          const present = this.applyProgramFactDelta(event.fact, weight);
          if (present === true) transitions.programFactAdds.push(event.fact);
          else if (present === false) transitions.programFactRemoves.push(event.fact);
          break;
        }
        case "StructuredAppRow": {
          if (!this.retainsStructuredAppRows) break;
          const terminalKey: Uint8Array = event.root.bytes ?? event.root;
          const first = this.structuredRootKeys.set(terminalKey, event.root) === undefined;
          this.applyStructuredAppRowDelta(terminalKey, event.record, weight);
          if (first) this.structuredRootKeyOrder.push(terminalKey);
          break;
        }
      }
    }
    return transitions;
  }

  versionsByTx(txId: TxId): VersionRow[] {
    const versions = this.versions.versionsByTx(txId);
    for (const [fact, version] of this.selectedDeletionWitnesses)
      if (cmpTxId(fact.version.tx, txId) === 0 && !versions.includes(version))
        versions.push(version);
    return versions;
  }

  acknowledgedSupportingRows() {
    return this.supporting.acknowledgedRows();
  }

  /** The only physical table this view reads, when it reads exactly one. */
  singlePhysicalTable(): [string, any] | undefined {
    return this.physicalTables.size === 1 ? this.physicalTables.first() : undefined;
  }

  supportingRows() {
    return this.supporting.rows();
  }
  unpublishedSupportingDelta() {
    return this.supporting.delta();
  }
  forgetPeerSourceClosureBaseline(): void {
    this.supporting.forgetPredecessor();
  }
  acknowledgePeerSourceClosure(): void {
    this.supporting.acknowledge();
  }

  /** Selected-scope tombstones are another contributor to the same physical frontier. */
  replaceSelectedDeletionWitnesses(witnesses: SortedMap<SupportingRow, VersionRow>): boolean {
    const prior = this.selectedDeletionWitnesses;
    const same =
      prior.size === witnesses.size &&
      [...prior].every(
        ([k, v]) =>
          witnesses.get(k) !== undefined &&
          witnesses
            .get(k)
            .record.raw()
            .every((b: number, i: number) => b === v.record.raw()[i]),
      );
    if (!same) this.witnessGatedJournal = undefined;
    let changed = false;
    for (const [row] of prior)
      if (!witnesses.has(row)) {
        this.supporting.apply(3, row, -1);
        changed = true;
      }
    for (const [row] of witnesses)
      if (!prior.has(row)) {
        this.supporting.apply(3, row, 1);
        changed = true;
      }
    this.selectedDeletionWitnesses = witnesses;
    return changed;
  }

  private applyProgramFactDelta(fact: any, weight: number): boolean | undefined {
    const key = programFactKey(fact);
    const before = (this.programFactWeights.get(key)?.[1] ?? 0) > 0;
    const next = (this.programFactWeights.get(key)?.[1] ?? 0) + weight;
    if (next === 0) this.programFactWeights.delete(key);
    else this.programFactWeights.set(key, [fact, next]);
    const after = next > 0;
    return before !== after ? after : undefined;
  }

  replacementFor(
    table: string,
    rowUuid: RowUuid,
  ): [VersionRow | undefined, VersionRow | undefined] {
    return this.replacements.replacementFor(table, rowUuid);
  }

  footprint(): MaintainedSubscriptionViewFootprint {
    const unrec = this.unreconciledResultMembers;
    const resultWeightsBytes =
      this.resultWeights.footprintBytes() +
      (unrec ? btreeSetBytes(unrec.size) + unrec.entryBytes : 0) +
      btreeMapBytes(this.publishedResultMembersSet.size) +
      this.publishedResultMembersSet.entryBytes;
    const resultPayloadsBytes =
      this.resultPayloads.footprintBytes() + this.publishedResultPayloads.footprintBytes();
    const supportingFrontierBytes = this.supporting.retainedBytes();
    let versionsBytes =
      this.versions.footprintBytes() + btreeMapBytes(this.selectedDeletionWitnesses.size);
    for (const v of this.selectedDeletionWitnesses.valuesArray())
      versionsBytes += versionRowBytes(v);
    const replacementsBytes = this.replacements.footprintBytes();
    let witnessTableNamesBytes = btreeMapBytes(this.witnessTableNames.size);
    for (const [k, v] of this.witnessTableNames) witnessTableNamesBytes += k.length + v.length + 56;
    let structuredAppRowsBytes =
      btreeMapBytes(this.structuredAppRows.size) +
      btreeMapBytes(this.structuredTerminalRecords.size);
    let structuredCount = this.structuredTerminalRecords.size;
    for (const records of this.structuredAppRows.valuesArray()) {
      structuredAppRowsBytes += btreeMapBytes(records.size);
      for (const [raw, w] of records) {
        structuredAppRowsBytes += raw.length + 8;
        if (w > 0) structuredCount++;
      }
    }
    for (const r of this.structuredTerminalRecords.valuesArray())
      structuredAppRowsBytes += r.retainedBytes();
    return {
      resultRows: this.resultWeights.positiveCount,
      resultWeights: this.resultWeights.size,
      resultPayloads: this.resultPayloads.size,
      structuredAppRows: structuredCount,
      versionIdentities: this.versions.entryCount + this.selectedDeletionWitnesses.size,
      versionTxEntries: this.versions.entryCount,
      replacementEntries: this.replacements.entryCount,
      resultWeightsBytes,
      resultPayloadsBytes,
      structuredAppRowsBytes,
      versionsBytes,
      supportingFrontierBytes,
      replacementsBytes,
      totalHeapBytes:
        resultWeightsBytes +
        resultPayloadsBytes +
        structuredAppRowsBytes +
        versionsBytes +
        supportingFrontierBytes +
        replacementsBytes +
        witnessTableNamesBytes,
    };
  }

  /** Current positive result memberships. */
  activeResultMembers(): ResultMemberEntry[] {
    return [...this.resultWeights.entries].filter(([, w]) => w > 0).map(([m]) => m);
  }

  /** Memberships that crossed the result/content witness boundary. */
  publishedResultMembers(): SortedSet<ResultMemberEntry> {
    return this.publishedResultMembersSet.entries;
  }

  private structuredAppRowForTerminalKey(terminalKey: Uint8Array): OwnedRecord | undefined {
    const state = this.structuredTerminalRecords.get(terminalKey);
    if (state) {
      try {
        return state.record();
      } catch {
        throw invalid("cannot encode retained terminal tree");
      }
    }
    const descriptor = this.structuredAppRowDescriptor;
    if (!descriptor) return undefined;
    const records = this.structuredAppRows.get(terminalKey);
    if (!records) return undefined;
    for (const [raw, w] of records) if (w > 0) return OwnedRecordApi.new(raw, descriptor);
    return undefined;
  }

  /** One retained collector row for every opaque terminal key, in collector order. */
  structuredAppRowsByTerminalKey(): [Uint8Array, OwnedRecord][] {
    const rows: [Uint8Array, OwnedRecord][] = [];
    for (const key of this.structuredRootKeyOrder) {
      if (!this.structuredRootKeys.has(key)) continue;
      const record = this.structuredAppRowForTerminalKey(key);
      if (record) rows.push([key, record]);
    }
    return rows;
  }

  decodedTerminalRecords(): SortedMap<Uint8Array, TerminalRecordState> {
    return this.structuredTerminalRecords;
  }

  /** Release the app-row collector after a flat subscription's reset. */
  discardStructuredAppRows(): void {
    this.structuredAppRows.clear();
    this.structuredTerminalRecords.clear();
    this.structuredRootKeys.clear();
    this.structuredRootKeyOrder = [];
    this.structuredAppRowDescriptor = undefined;
    this.retainsStructuredAppRows = false;
  }

  private applyStructuredAppRowDelta(
    terminalKey: Uint8Array,
    record: OwnedRecord,
    weight: number,
  ): void {
    this.structuredAppRowDescriptor = record.descriptor();
    const records = this.structuredAppRows.getOrInsert(terminalKey, () => new SortedMap(cmpBytes));
    const raw = record.raw();
    const next = (records.get(raw) ?? 0) + weight;
    if (next === 0) records.delete(raw);
    else records.set(raw, next);
    if (records.size === 0) this.structuredAppRows.delete(terminalKey);
  }

  /** Initialize decoded state from the retained pre-update row. */
  private retainedTerminalRecord(rootKey: Uint8Array): TerminalRecordState {
    let state = this.structuredTerminalRecords.get(rootKey);
    if (state) return state;
    const descriptor = this.structuredAppRowDescriptor;
    if (!descriptor) throw invalid("terminal update arrived before its root collector record");
    const records = this.structuredAppRows.get(rootKey);
    if (!records) throw invalid("terminal descendant operation addressed an absent retained root");
    const candidates = [...records].filter(([, w]) => w > 0);
    if (candidates.length === 0)
      throw invalid("terminal descendant operation addressed a non-positive retained root");
    if (candidates.length > 1)
      throw invalid("terminal descendant operation addressed an ambiguous retained root");
    const [raw, weight] = candidates[0];
    if (weight !== 1) throw invalid("collector terminal root has non-unit multiplicity");
    try {
      state = TerminalRecordState.new(OwnedRecordApi.new(raw, descriptor));
    } catch {
      throw invalid("invalid retained collector terminal record");
    }
    this.structuredAppRows.delete(rootKey);
    this.structuredTerminalRecords.set(rootKey, state);
    return state;
  }

  /** Fold root terminal edits into the retained collector tree. */
  private applyStructuredTerminalOperation(operation: TerminalOperation): void {
    const rootKey: Uint8Array = operation.rootKey;
    if (operation.path.length > 0) {
      if (!this.structuredRootKeys.has(rootKey) && terminalRootUuidFromKey(rootKey) === undefined)
        throw invalid("terminal descendant operation addresses an unknown root key");
      const descriptor = this.structuredAppRowDescriptor;
      if (!descriptor)
        throw invalid("terminal descendant operation arrived before its root collector record");
      if (!descriptorEquals(descriptor, operation.rootDescriptor))
        throw invalid("terminal descendant descriptor disagrees with retained collector layout");
      try {
        this.retainedTerminalRecord(rootKey).apply(operation);
      } catch (e) {
        if ((e as any)?.kind === "InvalidStoredValue") throw e;
        throw invalid("invalid collector descendant terminal operation");
      }
      return;
    }
    const rootWasPresent = this.structuredRootKeys.has(rootKey);
    const edit = operation.edit;
    if (edit.kind === "Insert" || edit.kind === "Update") {
      const record = OwnedRecordApi.new(edit.value, operation.rootDescriptor);
      const index = operation.rootDescriptor.fieldIndex("row_uuid");
      if (index === undefined) throw invalid("root collector terminal operation has no row_uuid");
      this.structuredRootKeys.set(rootKey, record.borrowed().getUuid(index));
    } else if (
      !this.structuredRootKeys.has(rootKey) &&
      terminalRootUuidFromKey(rootKey) === undefined
    )
      throw invalid("root collector terminal edit addresses an unknown root key");
    const sameKey = (k: Uint8Array) => equalBytes(k, rootKey);
    switch (edit.kind) {
      case "Insert": {
        this.structuredTerminalRecords.delete(rootKey);
        this.structuredAppRows.delete(rootKey);
        this.applyStructuredAppRowDelta(
          rootKey,
          OwnedRecordApi.new(edit.value, operation.rootDescriptor),
          1,
        );
        if (rootWasPresent)
          this.structuredRootKeyOrder = this.structuredRootKeyOrder.filter((k) => !sameKey(k));
        this.structuredRootKeyOrder.splice(
          Math.min(edit.index, this.structuredRootKeyOrder.length),
          0,
          rootKey,
        );
        break;
      }
      case "Update":
        try {
          this.retainedTerminalRecord(rootKey).updateRecord(
            OwnedRecordApi.new(edit.value, operation.rootDescriptor),
          );
        } catch (e) {
          if ((e as any)?.kind === "InvalidStoredValue") throw e;
          throw invalid("invalid collector terminal scalar update");
        }
        break;
      case "Remove":
        this.structuredTerminalRecords.delete(rootKey);
        this.structuredRootKeys.delete(rootKey);
        this.structuredRootKeyOrder = this.structuredRootKeyOrder.filter((k) => !sameKey(k));
        this.structuredAppRows.delete(rootKey);
        break;
      case "Move": {
        const previous = this.structuredRootKeyOrder.findIndex(sameKey);
        if (previous < 0)
          throw invalid("root collector terminal move addresses an absent root key");
        this.structuredRootKeyOrder.splice(previous, 1);
        this.structuredRootKeyOrder.splice(
          Math.min(edit.index, this.structuredRootKeyOrder.length),
          0,
          rootKey,
        );
      }
    }
  }

  private reconcilePublishableResultMembers(
    aliases: NodeAliases,
  ): [ResultMemberEntry[], ResultMemberEntry[], [ResultMemberEntry, any][], ResultMemberEntry[]] {
    const adds: ResultMemberEntry[] = [];
    const removes: ResultMemberEntry[] = [];
    const positive = (m: ResultMemberEntry) => (this.resultWeights.get(m) ?? 0) > 0;
    const published = this.publishedResultMembersSet;
    const candidates = this.unreconciledResultMembers;
    this.unreconciledResultMembers = undefined;
    const journal = this.witnessGatedJournal;
    this.witnessGatedJournal = undefined;
    if (candidates) {
      assert(this.storageBackedResultMaterialization);
      for (const member of candidates.entries) {
        const present = positive(member);
        if (!published.has(member) && present) {
          published.insert(member);
          adds.push(member);
        } else if (published.has(member) && !present) {
          published.remove(member);
          removes.push(member);
        }
      }
    } else if (
      journal &&
      !this.storageBackedResultMaterialization &&
      journal.aliasRetargets === aliases.retargetCount() &&
      journal.aliasCount <= aliases.len()
    ) {
      const changed = new SortedSet(cmpResultMember, [...journal.changed, ...journal.withheld]);
      const withheld = new SortedSet<ResultMemberEntry>(cmpResultMember);
      for (const member of changed) {
        const pos = positive(member);
        const publishable =
          pos &&
          (this.resultMemberHasInlineContentSource(member) ||
            this.resultMemberHasBundleWitness(member, aliases));
        const was = published.has(member);
        if (!was && publishable) {
          published.insert(member);
          adds.push(member);
        } else if (was && !publishable) {
          published.remove(member);
          if (pos) withheld.add(member);
          removes.push(member);
        } else if (!was && !publishable && pos) withheld.add(member);
      }
      this.witnessGatedJournal = {
        changed: new SortedSet(cmpResultMember),
        withheld,
        aliasRetargets: aliases.retargetCount(),
        aliasCount: aliases.len(),
      };
    } else {
      const withheld = new SortedSet<ResultMemberEntry>(cmpResultMember);
      const publishable = new SortedSet<ResultMemberEntry>(cmpResultMember);
      for (const [member, weight] of this.resultWeights.entries) {
        if (weight <= 0) continue;
        const ok =
          this.storageBackedResultMaterialization ||
          this.resultMemberHasInlineContentSource(member) ||
          this.resultMemberHasBundleWitness(member, aliases);
        if (ok) publishable.add(member);
        else withheld.add(member);
      }
      for (const m of publishable) if (!published.has(m)) adds.push(m);
      for (const m of published.entries) if (!publishable.has(m)) removes.push(m);
      this.publishedResultMembersSet = RetainedResultMembers.from(publishable);
      if (!this.storageBackedResultMaterialization)
        this.witnessGatedJournal = {
          changed: new SortedSet(cmpResultMember),
          withheld,
          aliasRetargets: aliases.retargetCount(),
          aliasCount: aliases.len(),
        };
    }
    if (this.storageBackedResultMaterialization)
      this.unreconciledResultMembers = new RetainedResultMembers();
    const payloadRemoves = removes.filter((m) => this.publishedResultPayloads.has(m));
    const payloadAdds: [ResultMemberEntry, any][] = [];
    for (const m of adds) {
      const p = this.resultPayloads.get(m);
      if (p !== undefined) payloadAdds.push([m, p]);
    }
    for (const m of payloadRemoves) this.publishedResultPayloads.remove(m);
    for (const [m, p] of payloadAdds) this.publishedResultPayloads.insert(m, p);
    return [adds, removes, payloadAdds, payloadRemoves];
  }

  private resultMemberHasBundleWitness(member: ResultMemberEntry, aliases: NodeAliases): boolean {
    const row = member.asRow();
    if (!row) return true;
    const [table, rowUuid, txId] = row;
    if (this.versions.hasContentWitness(txId, table, rowUuid)) return true;
    for (const [fact, version] of this.selectedDeletionWitnesses)
      if (
        cmpTxId(fact.version.tx, txId) === 0 &&
        version.table() === table &&
        cmpRowUuid(version.rowUuid(), rowUuid) === 0 &&
        !version.isDeleted()
      )
        return true;
    const content = this.replacementFor(table, rowUuid)[0];
    if (!content) return false;
    const tx = versionTxIdFromAliases(content, aliases);
    return tx !== undefined && cmpTxId(tx, txId) === 0;
  }

  private resultMemberHasInlineContentSource(member: ResultMemberEntry): boolean {
    const branch = member.asRealRow()?.branchOrPrefix;
    return branch !== undefined && this.inlineContentBranchKeys.has(bytesKey(branch));
  }

  private applyResultDelta(
    entry: ResultMemberEntry,
    payload: any,
    weight: number,
    t: ResultTransitions,
  ): void {
    this.markResultMemberChanged(entry);
    const old = this.resultWeights.get(entry) ?? 0;
    const next = old + weight;
    if (old <= 0 && next > 0) {
      t.adds.push(entry);
      if (entry.asRealRow()?.rowDigest !== undefined) {
        t.resultPayloadAdds.push([entry, payload]);
        this.resultPayloads.insert(entry, payload);
      }
    }
    if (old > 0 && next <= 0) {
      t.removes.push(entry);
      t.resultPayloadRemoves.push(entry);
      this.resultPayloads.remove(entry);
    }
    if (next === 0) this.resultWeights.remove(entry);
    else this.resultWeights.insert(entry, next);
  }

  private applyAggregateResultDelta(
    member: ResultMemberEntry,
    payload: any,
    weight: number,
    t: ResultTransitions,
  ): void {
    this.markResultMemberChanged(member);
    const [oldMember, oldPayload] = this.aggregatePayloadForStableMember(member);
    const same = (a: ResultMemberEntry | undefined) =>
      a !== undefined && cmpResultMember(a, member) === 0;
    if (weight < 0) {
      if (same(oldMember)) {
        t.removes.push(member);
        this.resultWeights.remove(member);
        const existing = this.resultPayloads.remove(member);
        if (existing !== undefined) {
          t.resultPayloadRemoves.push(member);
          t.programFactRemoves.push({ kind: "ResultPayload", payload: existing });
        }
      }
      return;
    }
    if (oldMember !== undefined && !same(oldMember)) {
      this.markResultMemberChanged(oldMember);
      t.removes.push(oldMember);
      this.resultWeights.remove(oldMember);
      const existing = this.resultPayloads.remove(oldMember) ?? oldPayload;
      if (existing !== undefined) {
        t.resultPayloadRemoves.push(oldMember);
        t.programFactRemoves.push({ kind: "ResultPayload", payload: existing });
      }
    }
    if ((this.resultWeights.get(member) ?? 0) <= 0) t.adds.push(member);
    t.programFactAdds.push({ kind: "ResultPayload", payload });
    t.resultPayloadAdds.push([member, payload]);
    this.resultPayloads.insert(member, payload);
    this.resultWeights.insert(member, 1);
  }

  private markResultMemberChanged(member: ResultMemberEntry): void {
    this.unreconciledResultMembers?.insert(member);
    this.witnessGatedJournal?.changed.add(member);
  }

  private aggregatePayloadForStableMember(member: ResultMemberEntry): [ResultMemberEntry?, any?] {
    if (member.kind !== "Synthetic") return [];
    for (const [candidate, payload] of this.resultPayloads.entries)
      if (
        candidate.kind === "Synthetic" &&
        candidate.table === member.table &&
        equalBytes(candidate.row, member.row)
      )
        return [candidate, payload];
    return [];
  }
}

// ---- terminal layout rebinding ---------------------------------------------
const isRecordArray = (vt: ValueType) => vt.kind === "Array" && vt.element.kind === "Record";

/** Rebind a runtime terminal operation to its early-bound prepared layout. */
function rebindTerminalOperationToLayout(
  operation: TerminalOperation,
  layout: TerminalRootLayout,
): TerminalOperation {
  if (descriptorEquals(operation.rootDescriptor, layout.rootDescriptor)) return operation;
  if (debugCoveredInputTrace())
    console.error(
      `JAZZ_COVERED_INPUT_TRACE terminal_descriptor_mismatch operation=${operation.rootDescriptor} layout=${layout.rootDescriptor}`,
    );
  if (
    !terminalDescriptorCanRebindToLayout(operation.rootDescriptor, layout.rootDescriptor) ||
    !terminalNestedCollectionLayoutAgrees(operation, layout.rootDescriptor)
  )
    throw invalid("structured terminal operation descriptor disagrees with prepared root layout");
  const edit = operation.edit;
  if ((edit.kind === "Insert" || edit.kind === "Update") && operation.path.length === 0)
    edit.value = reencodeTerminalRootRecord(
      operation.rootDescriptor,
      layout.rootDescriptor,
      edit.value,
    );
  operation.rootDescriptor = layout.rootDescriptor;
  return operation;
}

function terminalNestedCollectionLayoutAgrees(
  operation: TerminalOperation,
  target: RecordDescriptor,
): boolean {
  const first = operation.path[0];
  if (!first) return true;
  if (first.kind !== "Collection") return false;
  const si = operation.rootDescriptor.fieldIndex(first.name);
  const ti = target.fieldIndex(first.name);
  if (si === undefined || ti === undefined) return false;
  const st = operation.rootDescriptor.fields()[si].valueType;
  return isRecordArray(st) && valueTypeEquals(st, target.fields()[ti].valueType);
}

function terminalDescriptorCanRebindToLayout(
  source: RecordDescriptor,
  target: RecordDescriptor,
): boolean {
  const s = source.fields();
  const t = target.fields();
  return (
    s.length === t.length &&
    s.every(
      (f: any, i: number) =>
        f.name === t[i].name && terminalFieldCanRebindToLayout(f.valueType, t[i].valueType),
    )
  );
}

function terminalFieldCanRebindToLayout(source: ValueType, target: ValueType): boolean {
  if (valueTypeEquals(source, target)) return true;
  if (target.kind === "Nullable" && terminalFieldCanRebindToLayout(source, target.inner))
    return true;
  try {
    recordProjectorRegistryRebound(
      RecordDescriptorApi.new_([["value", source]]),
      RecordDescriptorApi.new_([["value", target]]),
      [[0, 0]],
    );
    return true;
  } catch {
    return false;
  }
}

function reencodeTerminalRootRecord(
  source: RecordDescriptor,
  target: RecordDescriptor,
  raw: Uint8Array,
): Uint8Array {
  let values: Value[];
  try {
    values = source.bind(raw).toValues();
  } catch {
    throw invalid("invalid structured terminal record");
  }
  return rebindTerminalRecordValues(
    source,
    target,
    values,
    "structured terminal root re-encoding failed",
  );
}

function rebindTerminalRecordValues(
  source: RecordDescriptor,
  target: RecordDescriptor,
  values: Value[],
  encodingFailed: string,
): Uint8Array {
  const t = target.fields();
  const rebound = source
    .fields()
    .map((f: any, i: number) => rebindTerminalValue(values[i], f.valueType, t[i].valueType));
  try {
    return target.create(rebound);
  } catch {
    throw invalid(encodingFailed);
  }
}

function rebindTerminalValue(value: Value, source: ValueType, target: ValueType): Value {
  if (valueTypeEquals(source, target)) return value;
  if (target.kind === "Nullable" && terminalFieldCanRebindToLayout(source, target.inner))
    return { kind: "Nullable", value: rebindTerminalValue(value, source, target.inner) };
  if (!terminalFieldCanRebindToLayout(source, target))
    throw invalid("structured terminal root value disagrees with prepared layout");
  if (value.kind === "Tuple" && source.kind === "Tuple" && target.kind === "Tuple")
    return {
      kind: "Tuple",
      items: value.items.map((v: Value, i: number) =>
        rebindTerminalValue(v, source.items[i], target.items[i]),
      ),
    };
  if (value.kind === "Array" && source.kind === "Array" && target.kind === "Array")
    return {
      kind: "Array",
      items: value.items.map((v: Value) => rebindTerminalValue(v, source.element, target.element)),
    };
  if (value.kind === "Nullable" && source.kind === "Nullable" && target.kind === "Nullable")
    return {
      kind: "Nullable",
      value:
        value.value === undefined
          ? undefined
          : rebindTerminalValue(value.value, source.inner, target.inner),
    };
  if (value.kind === "Record" && source.kind === "Record" && target.kind === "Record") {
    let values: Value[];
    try {
      values = value.record.toValues();
    } catch {
      throw invalid("invalid structured terminal record");
    }
    const raw = rebindTerminalRecordValues(
      source.descriptor,
      target.descriptor,
      values,
      "structured terminal nested re-encoding failed",
    );
    return { kind: "Record", record: OwnedRecordApi.new(raw, target.descriptor) };
  }
  if (value.kind === "Enum" && source.kind === "Enum" && target.kind === "Enum") {
    const tag = value.value.tag();
    let sourceCase: any;
    let targetCase: any;
    try {
      sourceCase = source.enumType.case(tag);
    } catch {
      throw invalid("invalid structured terminal enum tag");
    }
    try {
      targetCase = target.enumType.case(tag);
    } catch {
      throw invalid("prepared terminal enum tag is absent");
    }
    const record = rebindTerminalValue(
      { kind: "Record", record: value.value.intoRecord() },
      { kind: "Record", descriptor: sourceCase.payload },
      { kind: "Record", descriptor: targetCase.payload },
    );
    return { kind: "Enum", value: EnumValue.new_(tag, record.record) };
  }
  return value;
}

// ---- terminal schemas ------------------------------------------------------
export class MaintainedTerminalSchemas {
  constructor(private readonly sinks = new SortedMap<string, MaintainedTerminalKind>(cmpStr)) {}

  private find<K extends MaintainedTerminalKind["kind"]>(kind: K) {
    return this.sinks.valuesArray().find((k) => k.kind === kind) as
      | Extract<MaintainedTerminalKind, { kind: K }>
      | undefined;
  }
  currentPayloadSchema(): any {
    const k = this.find("ResultCurrent");
    if (!k) throw invalid("maintained result has no compiled payload schema");
    return k.schema;
  }
  directAppRowSchema(): any {
    const k = this.find("DirectAppRows");
    if (!k) throw invalid("maintained direct result has no compiler-owned app-row schema");
    return k.schema;
  }
  aggregateAppRowSchema(): any {
    const k = this.find("AggregateAppRows");
    if (!k) throw invalid("maintained aggregate has no compiler-owned app-row schema");
    return k.schema;
  }

  static forProgram(program: any): MaintainedTerminalSchemas {
    const sinks = new SortedMap<string, MaintainedTerminalKind>(cmpStr);
    const sharedSinks = new Set<string>(program.lowered.sharedWitnessSinks.values());
    for (const terminal of program.lowered.terminals) {
      const output = terminal.output;
      if (output.kind === "AppRows") {
        const rows = output.rows;
        const hasRowUuid = rows.descriptor.fieldIndex("row_uuid") !== undefined;
        let kind: MaintainedTerminalKind;
        switch (rows.terminal.kind) {
          case "RootCollector":
            if (!hasRowUuid)
              throw new Error("public root collector app-row terminal has no row_uuid");
            kind = { kind: "RootCollectorAppRows", schema: rows, layout: terminalRootLayout(rows) };
            break;
          case "Aggregate":
            kind = { kind: "AggregateAppRows", schema: rows };
            break;
          default:
            if (!hasRowUuid) throw new Error("direct app-row terminal has no row_uuid");
            kind = { kind: "DirectAppRows", schema: rows };
        }
        sinks.set(terminal.sink, kind);
        continue;
      }
      const fact = output.fact;
      const s = fact.schema;
      let kind: MaintainedTerminalKind | undefined;
      const key = `${fact.key.kind}/${fact.terminal}/${s.kind}`;
      switch (key) {
        case "ResultMembership/Primary/ResultMembership":
          kind = { kind: "ResultCurrent", schema: s.schema };
          break;
        case "ResultMembership/Primary/AggregateResult":
          kind = { kind: "AggregateResult", schema: s.schema };
          break;
        case "ProgramSourceCoverage/Primary/ProgramSourceCoverage":
          kind = { kind: "ProgramSourceCoverage", schema: s.schema };
          break;
        case "RelationEdges/Primary/RelationEdges":
          kind = { kind: "RelationEdge", schema: s.schema };
          break;
        case "VersionWitnesses/VersionWitnessDeletion/VersionWitnesses":
          if (s.schema.deletion) kind = { kind: "VersionDeletion", schema: s.schema.deletion };
          break;
        case "VersionWitnesses/VersionWitnessContent/VersionWitnesses":
          if (s.schema.content) kind = { kind: "VersionContent", schema: s.schema.content };
          break;
        case "ReplacementWitnesses/ReplacementWitnessDeletion/ReplacementWitnesses":
          if (s.schema.deletion) kind = { kind: "ReplacementDeletion", schema: s.schema.deletion };
          break;
        case "ReplacementWitnesses/ReplacementWitnessContent/ReplacementWitnesses":
          if (s.schema.content) kind = { kind: "ReplacementContent", schema: s.schema.content };
          break;
      }
      if (kind && sharedSinks.has(terminal.sink)) {
        if (kind.kind === "VersionContent") kind = { kind: "SharedContent", schema: kind.schema };
        else if (kind.kind === "VersionDeletion")
          kind = { kind: "SharedDeletion", schema: kind.schema };
      }
      if (kind) sinks.set(terminal.sink, kind);
    }
    return new MaintainedTerminalSchemas(sinks);
  }

  get(sink: string): MaintainedTerminalKind {
    const k = this.sinks.get(sink);
    if (!k) throw invalid("maintained view delta arrived for an unknown query-engine terminal");
    return k;
  }

  terminalRootLayout(): TerminalRootLayout | undefined {
    return this.find("RootCollectorAppRows")?.layout;
  }
  hasRootCollector(): boolean {
    return this.find("RootCollectorAppRows") !== undefined;
  }
}

const carrierOf = (c: string): TerminalRootCarrier =>
  c === "CurrentRow" ? "CurrentRow" : "Logical";
const u64le = (n: number) => {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(n), true);
  return b;
};

function terminalRootLayout(rows: any): TerminalRootLayout {
  const canonical = decodePersistedRecordDescriptor(
    encodePersistedRecordDescriptor(rows.descriptor),
  );
  const descriptorFields = canonical.fields().slice();
  const rowFields = rows.descriptor.fields();
  const rootKeySlot = rowFields.findIndex((f: any) => f.name === "row_uuid");
  assert(rootKeySlot >= 0, "structured app-row terminal has a row_uuid slot");
  const publicFields: TerminalRootPublicField[] = [];
  rowFields.forEach((field: any, slot: number) => {
    const name: string | undefined = field.name;
    if (name === undefined) return;
    const publication = rows.publicationFields.get(name);
    if (publication === undefined) return;
    const selectedProvenance =
      publication.kind === "ResultField" && publication.visibility === "PublicProvenance";
    if (slot === rootKeySlot || (rows.hiddenFields.has(name) && !selectedProvenance)) return;
    const publicName = publicationPublicName(publication);
    if (publicName === undefined) return;
    publicFields.push({
      publication,
      name: publicName,
      descriptorFieldName: name,
      slot,
      carrier: carrierOf(rows.fieldCarriers.get(name) ?? rows.carrier),
    });
  });
  for (const f of publicFields)
    if (descriptorFields[f.slot])
      descriptorFields[f.slot] = {
        ...descriptorFields[f.slot],
        identity: { kind: "Name", name: f.name },
      };
  const rootDescriptor = RecordDescriptorFields.newWithFields(descriptorFields);
  const hasher = new Blake3Hasher();
  hasher.update(utf8("jazz terminal root publication v1"));
  const roleDescriptor = RecordDescriptorFields.newWithFields(
    rootDescriptor.fields().map((field: any, slot: number) => {
      let roleName: string;
      const pub = publicFields.find((p) => p.slot === slot);
      if (slot === rootKeySlot) roleName = "metadata/row_uuid";
      else if (pub) {
        const p = pub.publication;
        const role =
          p.kind === "StoredColumn" || p.kind === "UnresolvedSourceCell"
            ? "source"
            : p.visibility === "ApplicationCell"
              ? "result"
              : p.visibility === "PublicProvenance"
                ? "provenance"
                : "hidden";
        roleName = `${role}/${slot}/${pub.name}`;
      } else roleName = `hidden/${slot}`;
      return RecordDescriptorFields.field(roleName, field.valueType);
    }),
  );
  hasher.update(encodePersistedRecordDescriptor(roleDescriptor));
  hasher.update(u64le(rootKeySlot));
  hasher.update(new Uint8Array([rows.carrier === "CurrentRow" ? 0 : 1]));
  hasher.update(new Uint8Array([rows.rootUnionArm ? 1 : 0]));
  for (const f of publicFields) {
    hasher.update(utf8(f.name));
    hasher.update(new Uint8Array([0]));
    hasher.update(u64le(f.slot));
    hasher.update(new Uint8Array([f.carrier === "CurrentRow" ? 0 : 1]));
  }
  return {
    id: `terminal:${toHex(hasher.finalize())}`,
    rootDescriptor,
    rootKeySlot,
    rootKeyFieldName: rowFields[rootKeySlot].name,
    publicFields,
    carrier: carrierOf(rows.carrier),
    rootUnionArm: rows.rootUnionArm,
  };
}

// ---- terminal record decoding ----------------------------------------------
function decodeTypedTerminalRecord(
  record: any,
  kind: MaintainedTerminalKind,
  tables: TableSchemas,
  aliases: NodeAliases,
  decodePlanCache: Map<string, VersionDecodePlan>,
  payloadPlans: Map<RecordDescriptor, any>,
  readView: any,
): DecodedMaintainedEvent {
  const witness = (
    expected: string,
    k:
      | "VersionContent"
      | "VersionDeletion"
      | "ReplacementContent"
      | "ReplacementDeletion"
      | "SharedVersion",
  ): DecodedMaintainedEvent => {
    validateWitnessEventKind(record, expected);
    return {
      kind: k,
      source: kind.schema.source,
      row: decodeTypedVersionWitness(record, kind.schema, tables, decodePlanCache),
    };
  };
  switch (kind.kind) {
    case "SharedContent":
      return witness("version_content", "SharedVersion");
    case "SharedDeletion":
      return witness("version_deletion", "SharedVersion");
    case "VersionContent":
      return witness("version_content", "VersionContent");
    case "VersionDeletion":
      return witness("version_deletion", "VersionDeletion");
    case "ReplacementContent":
      return witness("replacement_content", "ReplacementContent");
    case "ReplacementDeletion":
      return witness("replacement_deletion", "ReplacementDeletion");
    case "AggregateAppRows": {
      if (kind.schema.terminal.kind !== "Aggregate")
        throw invalid("aggregate terminal lost its lowered schema");
      return decodeAggregateResult(record, kind.schema.terminal.schema, "aggregate_result");
    }
    case "AggregateResult": {
      const table = recordString(
        record,
        kind.schema.synthetic.tableField,
        "aggregate result table field must be string",
      );
      return decodeAggregateResult(record, kind.schema, table);
    }
    case "ResultCurrent":
      return decodeResultCurrent(record, kind.schema, tables, aliases, payloadPlans, readView);
    case "ProgramSourceCoverage": {
      const v = record.getIdx(fieldIdx(record, "complete"));
      if (v.kind !== "Bool") throw invalid("program-source coverage complete field must be bool");
      if (v.value !== kind.schema.complete)
        throw invalid("program-source coverage terminal disagrees with compiled schema");
      return {
        kind: "ProgramSourceCoverage",
        coverage: { source: kind.schema.source, complete: v.value },
      };
    }
    case "RelationEdge":
      return {
        kind: "RelationEdge",
        edge: decodeTypedRelationEdge(record, kind.schema, tables, aliases),
      };
    case "RootCollectorAppRows":
    case "DirectAppRows":
      return {
        kind: "StructuredAppRow",
        root: record.getUuid(fieldIdx(record, "row_uuid")),
        record: OwnedRecordApi.new(record.raw().slice(), kind.schema.descriptor),
      };
  }
}

function branchDiscriminator(value: Value, message: string): Uint8Array {
  switch (value.kind) {
    case "Uuid":
      return value.bytes;
    case "Bytes":
      return value.bytes;
    case "Nullable":
      if (value.value === undefined) return new Uint8Array();
      if (value.value.kind === "Uuid" || value.value.kind === "Bytes") return value.value.bytes;
      throw invalid(message);
    default:
      throw invalid(message);
  }
}

function decodeResultCurrent(
  record: any,
  schema: any,
  tables: TableSchemas,
  aliases: NodeAliases,
  payloadPlans: Map<RecordDescriptor, any>,
  readView: any,
): DecodedMaintainedEvent {
  const tableName = recordString(
    record,
    schema.tableField,
    "maintained result membership table field must be string",
  );
  const table = tables.get(tableName);
  if (!table) throw invalid("maintained result membership table_name must exist");
  const rowUuid = record.getUuid(fieldIdx(record, schema.rowField));
  const occurrenceIds = schema.occurrenceIdFields.map((f: string) =>
    ObjectId.fromUuid(record.getUuid(fieldIdx(record, f))),
  );
  if (occurrenceIds.length === 0)
    throw invalid("maintained result membership occurrence must include its root row");
  const unionArms: [number, string][] = schema.occurrenceUnionArmFields.map(
    ([position, field]: [number, string]) => {
      const v = record.getIdx(fieldIdx(record, field));
      if (v.kind !== "String" || v.value === "")
        throw invalid("maintained result union arm must be a non-empty string");
      return [position, v.value];
    },
  );
  const occurrenceId = OutputOccurrenceId.withUnionArms(
    occurrenceIds[0],
    occurrenceIds.slice(1),
    unionArms,
  );
  if (occurrenceId === undefined)
    throw invalid("maintained result union occurrence carrier is malformed");
  if (schema.version.kind !== "Content")
    throw invalid("maintained result membership does not support include-deleted schemas yet");
  const txTime = recordU64(record, schema.version.content.txTimeField);
  const txNode = aliases.nodeForAlias(recordU64(record, schema.version.content.txNodeField));
  if (txNode === undefined) throw invalid("result tx node alias must exist");
  const settlePosition =
    schema.settlePositionField !== undefined
      ? nullableU64(record, schema.settlePositionField)
      : undefined;
  const flatJoinDigest =
    schema.payloadFields.length > 0
      ? flatJoinRowDigest(
          schema.payloadFields,
          schema.payloadFields.map((f: any) => record.getIdx(fieldIdx(record, f.name))),
        )
      : undefined;
  let branchOrPrefix: Uint8Array | undefined;
  if (schema.branchOrPrefixField !== undefined) {
    const bytes = branchDiscriminator(
      record.getIdx(fieldIdx(record, schema.branchOrPrefixField)),
      "result branch discriminator must be UUID or bytes",
    );
    if (bytes.length > 0 && !equalBytes(bytes, defaultBranchCanonicalBytes()))
      branchOrPrefix = bytes;
  }
  let member = RealRowMemberEntry.currentContent([
    table.name,
    rowUuid,
    TxIdApi.new_(txTime, txNode),
  ])
    .withOccurrenceId(occurrenceId)
    .withSettlePosition(settlePosition);
  member.readView = readView;
  member.branchOrPrefix = branchOrPrefix;
  if (flatJoinDigest) member = member.withRowDigest(flatJoinDigest);
  const entry = member.intoResultMember();
  let plan = payloadPlans.get(record.descriptor());
  if (!plan)
    payloadPlans.set(
      record.descriptor(),
      (plan = CurrentPayloadEncodePlan.new_(record.descriptor(), schema)),
    );
  const [descriptor, rowBytes] = plan.encode(record);
  return {
    kind: "ResultCurrent",
    member: entry,
    payload: { member: entry, descriptor, record: rowBytes },
  };
}

/** Decode an aggregate terminal into a synthetic member/payload pair. */
function decodeAggregateResult(record: any, schema: any, table: string): DecodedMaintainedEvent {
  const prepared = encodeAggregatePayloadRecord(record, schema);
  const member = {
    kind: "Synthetic",
    table,
    row: prepared.rowIdentity,
    replacement: SyntheticReplacementToken.fromEncodedRecord(prepared.replacementIdentity),
  };
  return {
    kind: "AggregateResult",
    member,
    payload: { member, descriptor: prepared.descriptor, record: prepared.record },
    synthetic: schema.synthetic,
    valueFields: schema.valueFields.map((f: any) => f.name),
  };
}

const FLAT_JOIN_ROW_DIGEST_DOMAIN = "jazz.flat-join-row-digest.v1";
const FLAT_JOIN_ROW_DIGEST_MAGIC = utf8("JFRD");
const FLAT_JOIN_ROW_DIGEST_VERSION = 1;

function flatJoinRowDigest(fields: any[], values: Value[]): Uint8Array {
  return blake3Keyed(
    blake3(utf8(FLAT_JOIN_ROW_DIGEST_DOMAIN)),
    flatJoinRowDigestPreimage(fields, values),
  );
}

function flatJoinRowDigestPreimage(fields: any[], values: Value[]): Uint8Array {
  if (fields.length !== values.length)
    throw invalid("flat joined result revision field/value arity disagrees");
  const descriptor = RecordDescriptorApi.new_(
    fields.map((f, i) => [`flat_join_payload_${i}`, f.ty]),
  );
  const descriptorBytes = encodePersistedRecordDescriptor(descriptor);
  const canonical = values.map((v, i) => canonicalizeFlatJoinPayloadValue(v, fields[i].ty));
  const recordBytes: Uint8Array = descriptor.create(canonical);
  const u32 = (n: number, message: string) => {
    if (n > 0xffffffff) throw invalid(message);
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, n);
    return b;
  };
  return concatBytes(
    FLAT_JOIN_ROW_DIGEST_MAGIC,
    new Uint8Array([FLAT_JOIN_ROW_DIGEST_VERSION]),
    u32(fields.length, "flat joined result revision has too many fields"),
    u32(descriptorBytes.length, "flat joined result revision descriptor is too large"),
    descriptorBytes,
    u32(recordBytes.length, "flat joined result revision record is too large"),
    recordBytes,
  );
}

function canonicalizeFlatJoinPayloadValue(value: Value, target: ValueType): Value {
  if (value.kind === "Nullable" && target.kind === "Nullable") {
    if (value.value === undefined) return value;
    const inner = canonicalizeFlatJoinPayloadValue(value.value, target.inner);
    if (inner.kind === "Nullable" && inner.value === undefined && target.inner.kind !== "Nullable")
      return { kind: "Nullable", value: undefined };
    return { kind: "Nullable", value: inner };
  }
  if (target.kind === "Nullable")
    return { kind: "Nullable", value: canonicalizeFlatJoinPayloadValue(value, target.inner) };
  if (value.kind === "Nullable" && value.value !== undefined)
    return canonicalizeFlatJoinPayloadValue(value.value, target);
  if (value.kind === "Array" && target.kind === "Array")
    return {
      kind: "Array",
      items: value.items.map((v: Value) => canonicalizeFlatJoinPayloadValue(v, target.element)),
    };
  if (
    value.kind === "Tuple" &&
    target.kind === "Tuple" &&
    value.items.length === target.items.length
  )
    return {
      kind: "Tuple",
      items: value.items.map((v: Value, i: number) =>
        canonicalizeFlatJoinPayloadValue(v, target.items[i]),
      ),
    };
  return value;
}

function decodeTypedRelationEdge(
  record: any,
  schema: any,
  tables: TableSchemas,
  aliases: NodeAliases,
): any {
  const sourceTable = tableNameFromVersionedRef(record, schema.source, tables);
  const targetTable = tableNameFromVersionedRef(record, schema.target, tables);
  const path = recordString(record, schema.pathField, "relation edge path field must be string");
  return {
    path,
    sourceTable: internTable(sourceTable),
    sourceRow: record.getUuid(fieldIdx(record, schema.source.row.rowField)),
    targetTable: internTable(targetTable),
    targetRow: record.getUuid(fieldIdx(record, schema.target.row.rowField)),
    kind: "Relation",
    sourceVersion: decodeRelationEdgeVersion(record, schema.source, aliases),
    targetVersion: decodeRelationEdgeVersion(record, schema.target, aliases),
    depth: undefined,
    edgeId: undefined,
    branch: undefined,
    role: "Terminal",
    order: undefined,
    holeState: undefined,
  };
}

function tableNameFromVersionedRef(record: any, schema: any, tables: TableSchemas): string {
  const name = recordString(
    record,
    schema.row.tableField,
    "relation edge table field must be string",
  );
  if (!tables.has(name)) throw invalid("relation edge table_name must exist");
  return name;
}

function decodeRelationEdgeVersion(record: any, schema: any, aliases: NodeAliases): any {
  if (schema.version?.kind !== "Content") return undefined;
  const txTime = recordU64(record, schema.version.content.txTimeField);
  const txNode = aliases.nodeForAlias(recordU64(record, schema.version.content.txNodeField));
  if (txNode === undefined) throw invalid("relation edge tx node alias must exist");
  let branchOrPrefix: Uint8Array | undefined;
  if (schema.branchOrPrefixField !== undefined) {
    const bytes = branchDiscriminator(
      record.getIdx(fieldIdx(record, schema.branchOrPrefixField)),
      "relation edge branch discriminator must be UUID or bytes",
    );
    if (bytes.length > 0) branchOrPrefix = bytes;
  }
  return {
    tx: TxIdApi.new_(txTime, txNode),
    schemaVersion: undefined,
    layer: "Content",
    batch: undefined,
    branchOrPrefix,
    rowDigest: undefined,
  };
}

function validateWitnessEventKind(record: any, expected: string): void {
  const v = record.getIdx(fieldIdx(record, "event_kind"));
  if (v.kind !== "String") throw invalid("maintained witness event kind must be string");
  if (v.value !== expected)
    throw invalid("maintained witness event kind did not match query-engine terminal schema");
}

function decodeTypedVersionWitness(
  record: any,
  schema: any,
  tables: TableSchemas,
  cache: Map<string, VersionDecodePlan>,
): VersionRow {
  const tableName = recordString(
    record,
    schema.identity.tableField,
    "maintained witness table field must be string",
  );
  const table = tables.get(tableName);
  if (!table) throw invalid("maintained witness table_name must exist");
  const deletion = taggedDeletion(record.getIdx(fieldIdx(record, schema.deletionField)));
  const layer: WitnessLayer = deletion !== undefined ? 1 : 0;
  const cacheKey = `${table.name}\u0000${layer}`;
  let plan = cache.get(cacheKey);
  if (!plan)
    cache.set(cacheKey, (plan = buildVersionDecodePlan(record.descriptor(), schema, table, layer)));
  const txTime = recordU64Idx(record, plan.txTimeIdx);
  let branchKey: any = BranchKey.default_();
  if (plan.branchIdx !== undefined) {
    let v = record.getIdx(plan.branchIdx);
    if (v.kind === "Nullable") v = v.value;
    if (v !== undefined) {
      if (v.kind !== "Bytes") throw invalid("branch key must be bytes");
      try {
        branchKey = decodePersistedBranchKey(table, v.bytes);
      } catch {
        throw invalid("maintained witness branch key is invalid");
      }
    }
  }
  let authoredColumns: any;
  if (layer === 0) {
    const v = nullableValue(record.getIdx(plan.authoredColumnsIdx));
    authoredColumns = v === undefined ? undefined : authoredColumnIdsFromValue(v);
  }
  const author = (idx: number) => {
    try {
      return RowAuthor.fromRecord(record.getRecord(idx)).asAuthorSubject();
    } catch {
      throw NodeErrors.groove({ kind: "NonCanonicalRecord" });
    }
  };
  const physicalMs = (idx: number, message: string) => {
    try {
      return txTimeFromPhysicalMs(recordU64Idx(record, idx));
    } catch {
      throw invalid(message);
    }
  };
  const parts = {
    table: table.name,
    branchKey,
    rowUuid: record.getUuid(plan.rowIdx),
    txNodeAlias: recordU64Idx(record, plan.txNodeIdx),
    schemaVersionAlias: recordU64Idx(record, plan.schemaVersionIdx),
    txTime,
    createdBy: author(plan.createdByIdx),
    createdAt: physicalMs(
      plan.createdAtIdx,
      "maintained witness created_at_ms exceeds packed HLC range",
    ),
    updatedBy: author(plan.updatedByIdx),
    updatedAt: physicalMs(
      plan.updatedAtIdx,
      "maintained witness updated_at_ms exceeds packed HLC range",
    ),
    cells: new Map(),
    authoredColumns,
    deletion,
    seq: 0,
    base: undefined,
    counterSigns: [],
  };
  const values = historyValuesFromParts(table, parts);
  const p = plan;
  const raw = p.descriptor.createWithEncodedFields(
    record.raw().length,
    (index: number, output: number[]) => {
      const userCells = HISTORY_USER_CELLS;
      if (layer === 0 && index >= userCells && index < userCells + table.columns.length) {
        const sourceIndex = p.userIndices.get(table.columns[index - userCells].name)!;
        if (
          valueTypeEquals(
            record.descriptor().fields()[sourceIndex].valueType,
            p.descriptor.fields()[index].valueType,
          )
        ) {
          const [s, e] = record.descriptor().fieldSpan(record.raw(), sourceIndex);
          output.push(...record.raw().subarray(s, e));
          return;
        }
        const value = record.getIdx(sourceIndex);
        nullableValue(value);
        p.descriptor.encodeFieldInto(index, value, output);
      } else p.descriptor.encodeFieldInto(index, values[index], output);
    },
  );
  const version = VersionRowApi.new_(
    internTable(parts.table),
    parts.branchKey,
    OwnedRecordApi.new(raw, p.descriptor),
  );
  version.validateCanonical();
  return version;
}

function buildVersionDecodePlan(
  terminal: RecordDescriptor,
  schema: any,
  table: any,
  layer: WitnessLayer,
): VersionDecodePlan {
  const idx = (field: string) => fieldIdxInDescriptor(terminal, field);
  const userIndices = new Map<string, number>();
  if (layer === 0)
    for (const [column, field] of schema.userFields) userIndices.set(column, idx(field));
  return {
    descriptor: table.historyStorageTable().recordSchema(),
    branchIdx:
      schema.identity.branchOrPrefixField !== undefined
        ? idx(schema.identity.branchOrPrefixField)
        : undefined,
    rowIdx: idx(schema.identity.rowField),
    txTimeIdx: idx(schema.identity.txTimeField),
    txNodeIdx: idx(schema.identity.txNodeField),
    schemaVersionIdx: idx(schema.identity.schemaField),
    createdByIdx: idx(schema.createdByField),
    createdAtIdx: idx(schema.createdAtField),
    updatedByIdx: idx(schema.updatedByField),
    updatedAtIdx: idx(schema.updatedAtField),
    userIndices,
    authoredColumnsIdx: idx(schema.authoredColumnsField),
  };
}

function taggedDeletion(value: Value): any {
  if (value.kind !== "Nullable") throw invalid("tagged _deletion must be nullable");
  if (value.value === undefined) return undefined;
  const v = value.value.kind === "U8" ? { kind: "EnumTag", value: value.value.value } : value.value;
  return deletionEventFromValue(v);
}

function recordString(record: any, field: string, notString: string): string {
  const v = record.getIdx(fieldIdx(record, field));
  if (v.kind !== "String") throw invalid(notString);
  return v.value;
}

function recordU64(record: any, field: string): number {
  return recordU64Idx(record, fieldIdx(record, field));
}

function recordU64Idx(record: any, idx: number): number {
  const v = record.getIdx(idx);
  if (v.kind !== "U64") throw invalid("field must be u64");
  return v.value;
}

function nullableU64(record: any, field: string): number | undefined {
  const v = record.getIdx(fieldIdx(record, field));
  if (v.kind === "U64") return v.value;
  if (v.kind !== "Nullable") throw invalid("field must be nullable u64");
  if (v.value === undefined) return undefined;
  if (v.value.kind !== "U64") throw invalid("nullable field payload must be u64");
  return v.value.value;
}

const fieldIdx = (record: any, field: string): number =>
  fieldIdxInDescriptor(record.descriptor(), field);

function fieldIdxInDescriptor(descriptor: RecordDescriptor, field: string): number {
  const i = descriptor.fieldIndex(field);
  if (i === undefined) throw invalid("maintained view terminal missing field");
  return i;
}
