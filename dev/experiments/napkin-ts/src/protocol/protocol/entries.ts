import type { Reader, Writer } from "../../support/postcard.js";
import { toHex } from "../../support/bytes.js";
import {
  NIL_UUID,
  readUuid,
  writeUuid,
  type GlobalPhysicalTableId,
  type RowUuid,
  type SchemaVersionId,
} from "../../types/ids.js";
import { OutputOccurrenceId, ResultKey } from "../../types/object.js";
import type { GlobalTime } from "../../types/time.js";
import type { ReadViewKey } from "./keys.js";
import {
  readTxId,
  writeTxId,
  type BindingId,
  type DurabilityTier,
  type ShapeId,
  type SnapshotRef,
  type TxId,
} from "../../_extern/types-protocol.js";

export type ResultRowEntry = [string, RowUuid, TxId];

/** Opaque replacement token; Debug never exposes its bytes. */
export class SyntheticReplacementToken {
  constructor(readonly encodedRecord: Uint8Array) {}
  toString() {
    return "SyntheticReplacementToken(..)";
  }
}

export type ResultRowLayer = "Content" | "Deletion" | "ContentOrDeletion";
const LAYERS: ResultRowLayer[] = ["Content", "Deletion", "ContentOrDeletion"];
export const resultRowLayerTag = (l: ResultRowLayer) => LAYERS.indexOf(l);
export const resultRowLayerFromTag = (t: number): ResultRowLayer => {
  const l = LAYERS[t];
  if (l === undefined) throw new Error(`invalid ResultRowLayer tag ${t}`);
  return l;
};

export type ResultRowSource =
  | { kind: "Current" }
  | { kind: "Snapshot"; snapshot: SnapshotRef }
  | { kind: "HistoryCut"; globalTime: GlobalTime }
  | { kind: "Merge"; inputs: ResultRowSource[] }
  | { kind: "LensProjection"; schemaVersion: SchemaVersionId; base: ResultRowSource }
  | { kind: "Overlay"; tx: TxId; base: ResultRowSource };

export interface RealRowMemberEntry {
  table: string;
  rowUuid: RowUuid;
  occurrenceId?: OutputOccurrenceId;
  contentTx?: TxId;
  layer: ResultRowLayer;
  deletionTx?: TxId;
  source: ResultRowSource;
  readView: ReadViewKey;
  schemaVersion?: SchemaVersionId;
  branchOrPrefix?: Uint8Array;
  rowDigest?: Uint8Array;
  batch?: TxId;
  settlePosition?: GlobalTime;
}

export const realRowCurrentContent = ([
  table,
  rowUuid,
  contentTx,
]: ResultRowEntry): RealRowMemberEntry => ({
  table,
  rowUuid,
  occurrenceId: OutputOccurrenceId.singleSource(rowUuid),
  contentTx,
  layer: "Content",
  source: { kind: "Current" },
  readView: NIL_UUID,
});
export const realRowOutputOccurrenceId = (e: RealRowMemberEntry) =>
  e.occurrenceId ?? OutputOccurrenceId.singleSource(e.rowUuid);
export const realRowProjection = (e: RealRowMemberEntry): ResultRowEntry | undefined =>
  e.contentTx && [e.table, e.rowUuid, e.contentTx];

export type ResultMemberEntry =
  | { kind: "Row"; row: RealRowMemberEntry }
  | { kind: "Synthetic"; table: string; row: Uint8Array; replacement: SyntheticReplacementToken }
  | {
      kind: "PathTuple";
      path: string;
      sourceTable: string;
      sourceRow: RowUuid;
      targetTable: string;
      targetRow: RowUuid;
      edgeId?: Uint8Array;
      revision: Uint8Array;
    }
  | { kind: "TypedRow"; row: RealRowMemberEntry; occurrenceKey: ResultKey };

export const resultMemberRow = (entry: ResultRowEntry): ResultMemberEntry => ({
  kind: "Row",
  row: realRowCurrentContent(entry),
});

export function resultMemberFromRealRow(row: RealRowMemberEntry): ResultMemberEntry {
  const o = row.occurrenceId;
  return o && o.hasTypedDiscriminators()
    ? { kind: "TypedRow", row, occurrenceKey: new ResultKey(o) }
    : { kind: "Row", row };
}

export function resultMemberTableName(e: ResultMemberEntry): string {
  switch (e.kind) {
    case "Row":
    case "TypedRow":
      return e.row.table;
    case "Synthetic":
      return e.table;
    case "PathTuple":
      return e.targetTable;
  }
}
export const resultMemberAsRealRow = (e: ResultMemberEntry) =>
  e.kind === "Row" || e.kind === "TypedRow" ? e.row : undefined;
export function resultMemberOutputOccurrenceId(
  e: ResultMemberEntry,
): OutputOccurrenceId | undefined {
  if (e.kind === "TypedRow") return e.occurrenceKey.occurrence;
  const row = resultMemberAsRealRow(e);
  return row && realRowOutputOccurrenceId(row);
}
export const resultMemberAsRow = (e: ResultMemberEntry) => {
  const row = resultMemberAsRealRow(e);
  return row && realRowProjection(row);
};

/** Structural ordering key standing in for the derived `Ord`. */
export const entryKey = (v: unknown): string =>
  JSON.stringify(v, (_k, x) =>
    x instanceof Uint8Array
      ? toHex(x)
      : typeof x === "bigint"
        ? x.toString(16).padStart(16, "0")
        : x instanceof SyntheticReplacementToken
          ? toHex(x.encodedRecord)
          : x instanceof OutputOccurrenceId
            ? x.key()
            : x instanceof ResultKey
              ? x.occurrence.key()
              : x,
  );

export interface RowVersionRefEntry {
  tx: TxId;
  schemaVersion?: SchemaVersionId;
  layer: ResultRowLayer;
  batch?: TxId;
  branchOrPrefix?: Uint8Array;
  rowDigest?: Uint8Array;
}
export const writeRowVersionRefEntry = (w: Writer, e: RowVersionRefEntry) => {
  writeTxId(w, e.tx);
  w.option(e.schemaVersion, writeUuid);
  w.variant(resultRowLayerTag(e.layer));
  w.option(e.batch, writeTxId);
  w.option(e.branchOrPrefix, (w, b) => w.bytes(b));
  w.option(e.rowDigest, (w, b) => w.bytes(b));
};
export const readRowVersionRefEntry = (r: Reader): RowVersionRefEntry => ({
  tx: readTxId(r),
  schemaVersion: r.option(readUuid),
  layer: LAYERS[r.variant(3, "ResultRowLayer")],
  batch: r.option(readTxId),
  branchOrPrefix: r.option((r) => r.bytes().slice()),
  rowDigest: r.option((r) => r.bytes().slice()),
});

export interface SupportingRow {
  physicalTable: GlobalPhysicalTableId;
  versionTable: string;
  row: RowUuid;
  version: RowVersionRefEntry;
}
export const supportingRowIsWireValid = (s: SupportingRow) =>
  s.versionTable.length > 0 && s.version.layer !== "ContentOrDeletion";

// Consecutive supporting rows usually name the same table; reuse the last string.
let lastTableName: string | undefined;
const internTableName = (name: string) =>
  lastTableName === name ? lastTableName : (lastTableName = name);

export const writeSupportingRow = (w: Writer, s: SupportingRow) => {
  writeUuid(w, s.physicalTable);
  w.str(s.versionTable);
  writeUuid(w, s.row);
  writeRowVersionRefEntry(w, s.version);
};
export const readSupportingRow = (r: Reader): SupportingRow => ({
  physicalTable: readUuid(r),
  versionTable: internTableName(r.str()),
  row: readUuid(r),
  version: readRowVersionRefEntry(r),
});

// ---- program facts (node-internal evidence vocabulary) ----

export type RelationEdgeKind = "Include" | "Join" | "Relation" | "Recursive" | "Policy";
export type RelationEdgeRole = "Intermediate" | "Frontier" | "Terminal";
export type PathHoleState = "Matched" | "Hole";
export type PredicateOutputSetRoleEntry = "Base" | "Now";
export type ProgramSourceRole =
  | { kind: "Root" }
  | {
      kind: "Alias" | "RecursiveSeed" | "RecursiveStep" | "CorrelatedChild" | "Policy";
      name: string;
    };

export const MAX_PROGRAM_SOURCE_PATH_SEGMENTS = 32;
export interface ProgramSourceId {
  table: string;
  path: ProgramSourceRole[];
}
export const programSourceIdIsWireValid = (s: ProgramSourceId) =>
  s.table.length > 0 &&
  s.path.length > 0 &&
  s.path.length <= MAX_PROGRAM_SOURCE_PATH_SEGMENTS &&
  s.path.every((role) => role.kind === "Root" || role.name.length > 0);

export interface ResultMemberPayloadEntry {
  member: ResultMemberEntry;
  descriptor: Uint8Array;
  record: Uint8Array;
}
export interface RelationEdgeEntry {
  path: string;
  sourceTable: string;
  sourceRow: RowUuid;
  targetTable: string;
  targetRow: RowUuid;
  kind?: RelationEdgeKind;
  sourceVersion?: RowVersionRefEntry;
  targetVersion?: RowVersionRefEntry;
  depth?: number;
  edgeId?: Uint8Array;
  branch?: Uint8Array;
  role?: RelationEdgeRole;
  order?: Uint8Array;
  holeState?: PathHoleState;
}
export interface VersionWitnessEntry {
  role: string;
  version: RowVersionRefEntry;
  member?: ResultMemberEntry;
}
export interface CoveredInputEntry {
  source: ProgramSourceId;
  versionTable: string;
  sourceRow: RowUuid;
  version: RowVersionRefEntry;
}
export const coveredInputIsWireValid = (c: CoveredInputEntry) =>
  programSourceIdIsWireValid(c.source) && c.versionTable.length > 0;
export interface PolicyWitnessEntry {
  protected: ResultMemberEntry;
  policyPath: string;
  witness: RowVersionRefEntry;
  edgeKind?: RelationEdgeKind;
}
export interface ContributingMembersEntry {
  result: ResultMemberEntry;
  contributor: ResultMemberEntry;
  batch?: TxId;
  role?: string;
}
export interface PredicateReadEntry {
  role: PredicateOutputSetRoleEntry;
  shapeId: ShapeId;
  bindingId: BindingId;
  predicate: Uint8Array;
  frontier: Uint8Array;
}
export interface PointReadEntry {
  present: boolean;
  table: string;
  row: RowUuid;
  version?: RowVersionRefEntry;
  shapeId: ShapeId;
  bindingId: BindingId;
}
export interface PathCorrelationCoverageEntry {
  path: string;
  sourceTable: string;
  sourceRow: RowUuid;
  correlationKey: Uint8Array;
  complete: boolean;
}
export interface ProgramSourceCoverageEntry {
  source: ProgramSourceId;
  complete: boolean;
}
export interface ReadFrontierSettledEntry {
  scope: string;
  tier: DurabilityTier;
  stream?: string;
  frontier: Uint8Array;
}
export interface CompleteTxPayloadCoverageEntry {
  tx: TxId;
  tier: DurabilityTier;
  payloadDigest: Uint8Array;
}
export interface ViewCompleteExclusiveCoverageEntry {
  tx: TxId;
  scope: string;
  result?: ResultMemberEntry;
  tier: DurabilityTier;
  coveredMembersDigest: Uint8Array;
}
export type PolicyDecisionOutcomeEntry =
  | { kind: "Allowed" }
  | { kind: "Denied" }
  | { kind: "IndeterminateRequiresInput"; input: string }
  | { kind: "RequiresCoverage"; scope: string; frontier: Uint8Array };
export interface PolicyDecisionEntry {
  decision: Uint8Array;
  outcome: PolicyDecisionOutcomeEntry;
  reason?: string;
}
export interface PredicateOutputSetEntry {
  role: PredicateOutputSetRoleEntry;
  table: string;
  row: RowUuid;
  version: RowVersionRefEntry;
  shapeId: ShapeId;
  bindingId: BindingId;
}

export type ProgramFactEntry =
  | { kind: "ResultPayload"; entry: ResultMemberPayloadEntry }
  | { kind: "RelationEdge"; entry: RelationEdgeEntry }
  | { kind: "PathCorrelationCoverage"; entry: PathCorrelationCoverageEntry }
  | { kind: "ProgramSourceCoverage"; entry: ProgramSourceCoverageEntry }
  | { kind: "ReadFrontierSettled"; entry: ReadFrontierSettledEntry }
  | { kind: "CompleteTxPayloadCoverage"; entry: CompleteTxPayloadCoverageEntry }
  | { kind: "ViewCompleteExclusiveCoverage"; entry: ViewCompleteExclusiveCoverageEntry }
  | { kind: "PolicyDecision"; entry: PolicyDecisionEntry }
  | { kind: "VersionWitness"; entry: VersionWitnessEntry }
  | { kind: "CoveredInput"; entry: CoveredInputEntry }
  | { kind: "PolicyWitness"; entry: PolicyWitnessEntry }
  | { kind: "ContributingMembers"; entry: ContributingMembersEntry }
  | { kind: "PredicateRead"; entry: PredicateReadEntry }
  | { kind: "PredicateOutputSet"; entry: PredicateOutputSetEntry }
  | { kind: "PointRead"; entry: PointReadEntry };
export type ViewFactEntry = ProgramFactEntry;
export const isPeerSourceClosureFact = (f: ProgramFactEntry) =>
  f.kind === "ProgramSourceCoverage" || f.kind === "CoveredInput";
