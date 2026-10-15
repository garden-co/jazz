import type { SortedSet } from "../../support/collections.js";
import type { DurabilityTier, RelationProjectColumn } from "../../_extern/engine.js";
import type { ProgramPathId } from "./input.js";
import type { BatchId, CoverageFrontier, CoverageScope } from "./read.js";
import type { ResultId } from "./input.js";

export type RowSetOutputRequest = {
  appRows?: AppRowOutputRequest;
  facts: SortedSet<ProgramFactKey>;
};

export type AppRowOutputRequest = { publicTerminal: boolean; projection: PayloadProjection };

export type PayloadProjection =
  | { kind: "ShapeDefault" }
  | { kind: "Relation"; columns: RelationProjectColumn[] }
  | { kind: "Tree"; tree: AppProjectionTree };

export type AppProjectionTree = { fields: FieldProjection; paths: AppPathProjection[] };

export type FieldProjection = { kind: "All" } | { kind: "Fields"; fields: SortedSet<string> };

export type AppPathProjection = {
  path: ProgramPathId;
  field: string;
  cardinality: PathCardinality;
  fields: FieldProjection;
  children: AppPathProjection[];
  holePolicy: PathHolePolicy;
};

export type PathCardinality = "One" | "Many";
export type PathHolePolicy = "KeepParentWithHoles" | "DropIncompleteParent";

export type ProgramFactKey =
  | {
      kind:
        | "AuthorizedRows"
        | "ResultMembership"
        | "RelationEdges"
        | "PathCorrelationCoverage"
        | "VersionWitnesses"
        | "ReplacementWitnesses"
        | "PolicyWitnesses"
        | "ContributingMembers"
        | "PredicateReads";
    }
  | { kind: "ProgramSourceCoverage"; scope: CoverageScope }
  | { kind: "ReadFrontierSettled"; frontier: CoverageFrontier }
  | { kind: "CompleteTxPayloadCoverage"; batch: BatchId; tier: DurabilityTier }
  | {
      kind: "ViewCompleteExclusiveCoverage";
      view: CoverageScope;
      result?: ResultId;
      tier: DurabilityTier;
    }
  | { kind: "PolicyDecision"; decision: PolicyDecisionFactKey }
  | { kind: "PredicateOutputSet"; role: PredicateOutputSetRole }
  | { kind: "PointReads"; present: boolean };

export type PolicyDecisionFactKey = { role: PolicyDecisionRole; fingerprint: Uint8Array };
export type PolicyDecisionRole = "Read" | "Write";
export type PredicateOutputSetRole = "Base" | "Now";
