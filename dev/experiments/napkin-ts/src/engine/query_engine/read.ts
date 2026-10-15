import type { SortedMap, SortedSet } from "../../support/collections.js";
import { blake3 } from "../../support/hash.js";
import { toHex } from "../../support/bytes.js";
import type {
  AuthorityResultKey,
  BindingViewKey,
  BranchKey,
  DurabilityTier,
  GlobalTime,
  OpenTransactionId,
  SchemaFamilyId,
  SchemaVersionId,
  Snapshot,
  SnapshotRef,
  TxId,
} from "../../_extern/engine.js";
import { structMap, type SourceId, type ProgramPathId } from "./input.js";

export type RequestedReadSet = QueryReadSet;
export type QueryReadSet = { primary: ReadView; factReads: SortedMap<FactReadRole, ReadView> };

export const primaryReadSet = (primary: ReadView): QueryReadSet => ({
  primary,
  factReads: structMap(),
});

export type FactReadRole = "PredicateOutputBase" | "PredicateOutputNow";

export type RequestedReadView = ReadView;
export type ReadView = {
  readSchema: SchemaVersionId;
  policySchema: SchemaVersionId;
  sources: SourceGraph;
};

export function readViewSourceCurrentTier(
  view: ReadView,
  source: SourceId,
): DurabilityTier | undefined {
  const expr = view.sources.get(source);
  return expr && sourceExprCurrentTier(expr);
}

export type RequestedSourceExpr = SourceExpr;
export type SourceGraph = SortedMap<SourceId, SourceExpr>;

/** Requested and resolved stages share one shape; stage metadata is loosely typed. */
export type SourceExpr =
  | { kind: "VisibleCurrent"; projection: SchemaProjection; data: DataSource; tier: DurabilityTier }
  | {
      kind: "BranchView";
      projection: SchemaProjection;
      head: BranchKey;
      base?: BranchViewSourceBase;
      tier: DurabilityTier;
    }
  | { kind: "HistoryCut"; projection: SchemaProjection; data: DataSource; position: GlobalTime }
  | { kind: "SnapshotRef"; projection: SchemaProjection; data: DataSource; snapshot: Snapshot }
  | {
      kind: "SettledBindingView";
      currentDefault: boolean;
      projection: SchemaProjection;
      bindingView: BindingViewKey;
      authorityResultKey?: AuthorityResultKey;
    }
  | { kind: "WithOverlays"; input: SourceExpr; overlays: OverlayStack<any> }
  | { kind: "LensProject"; input: SourceExpr; projection: SchemaProjection }
  | { kind: "Merge"; inputs: SourceExpr[]; mode: SourceMergeMode; resolution: unknown };

export function sourceExprCurrentTier(expr: SourceExpr): DurabilityTier | undefined {
  switch (expr.kind) {
    case "VisibleCurrent":
    case "BranchView":
      return expr.tier;
    case "WithOverlays":
    case "LensProject":
      return sourceExprCurrentTier(expr.input);
    case "Merge": {
      const tiers = expr.inputs.map(sourceExprCurrentTier).filter((t) => t !== undefined);
      if (tiers.length === 0) return undefined;
      return tiers.every((t) => t === tiers[0]) ? tiers[0] : undefined;
    }
    default:
      return undefined;
  }
}

export type BranchViewSourceBase =
  | { kind: "Current"; key: BranchKey }
  | { kind: "Snapshot"; key: BranchKey; snapshot: SnapshotRef };

export type DataSource = { kind: "Current" } | { kind: "Branch"; branch: BranchKey };

export type SchemaProjection = { schemaFamily: any; storage: any; lens: any };

export type SourceMergeMode = "LastWriteWins" | "Union";

export type SchemaFamilySelection =
  | { kind: "Current" }
  | { kind: "ExplicitSchemaFamily"; id: SchemaFamilyId };

export type StorageSchemaSelection =
  | { kind: "CompatiblePartitions" }
  | { kind: "Single"; version: SchemaVersionId }
  | { kind: "Explicit"; versions: SortedSet<SchemaVersionId> };

export type LensSelection =
  | { kind: "Canonical" }
  | { kind: "Fingerprint"; fingerprint: Uint8Array };

export type OverlayStack<O> = { entries: O[] };

export type OverlayRef =
  | { kind: "PendingLocal" }
  | { kind: "DirectBatch"; batch: BatchId }
  | { kind: "AcceptedTransaction"; tx: TxId }
  | { kind: "OpenTransaction"; id: OpenTransactionId };

export type ResolvedOverlay = { overlay: OverlayRef; manifestFingerprint: Uint8Array };

export type BatchId = Uint8Array;

export function formatBatchId(batch: BatchId): string {
  const fingerprint = toHex(blake3(batch));
  return `BatchId { len: ${batch.length}, fingerprint: ${fingerprint.slice(0, 12)} }`;
}

export type CoverageFrontier = { scope: CoverageScope; frontier: FrontierRequirement };

export type CoverageScope =
  | { kind: "Program" }
  | { kind: "Source"; source: SourceId }
  | { kind: "Path"; path: ProgramPathId };

export type FrontierRequirement =
  | { kind: "None" }
  | { kind: "Through"; frontier: ResolvedFrontier };

export type ResolvedFrontier = { tier: DurabilityTier; stream?: string; through: FrontierPosition };

export type FrontierPosition =
  | { kind: "GlobalTime"; time: GlobalTime }
  | { kind: "Snapshot"; fingerprint: Uint8Array }
  | { kind: "Transaction"; tx: TxId }
  | { kind: "StreamPosition"; position: Uint8Array };

export type ResolvedPartitionLens = {
  storageSchema: SchemaVersionId;
  lensPathFingerprint: Uint8Array;
};
