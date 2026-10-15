import type { SortedMap, SortedSet } from "../../support/collections.js";
import { JazzError } from "../../support/runtime.js";
import type {
  AuthorSubject,
  ColumnType,
  GraphBuilder,
  PhysicalColumnId,
  RecordDescriptor,
  TableSchema,
} from "../../_extern/engine.js";
import {
  dbg,
  type ClaimPath,
  type ProgramClaimParam,
  type ProvenanceField,
  type RowVisibility,
  type SourceId,
} from "./input.js";
import type { PolicyDecisionRole, ProgramFactKey } from "./output.js";

export type SourceRequest = {
  source: SourceId;
  visibility: RowVisibility;
  authorization: SourceAuthorizationRequest;
  requirements: SourceRequirements;
};

export type SourceAuthorizationRequest =
  | { kind: "System" }
  | { kind: "PolicyFiltered"; permissionSubject: AuthorSubject; plan: PolicyAuthorizationPlan }
  | { kind: "PolicyProof"; permissionSubject: AuthorSubject; plan: PolicyAuthorizationPlan };

export type PolicyAuthorizationPlan = {
  protectedSource: SourceId;
  role: PolicyDecisionRole;
  protectedRowField: string;
  bindingSourceShape?: string;
  bindingUserParams: SortedMap<string, ColumnType>;
  bindingClaimParams: SortedMap<string, ProgramClaimParam>;
};

export type SourceRequirements = {
  appFields: FieldRequirement;
  metadata: SortedSet<SourceMetadataRequirement>;
};

export type SourceMetadataRequirement =
  | {
      kind:
        | "VersionWitnesses"
        | "SettlePosition"
        | "VersionPayloads"
        | "DeletionMarkers"
        | "BatchMembership"
        | "Coverage"
        | "ValidationReads"
        | "PolicyWitnesses";
    }
  | { kind: "Provenance"; field: ProvenanceField }
  | { kind: "AuthorPath"; path: string };

export type FieldRequirement =
  | { kind: "None" }
  | { kind: "All" }
  | { kind: "Fields"; fields: SortedSet<string> };

export interface SourceGraphPreparer {
  /** Rejects with a `SourceResolutionError`. */
  prepareSourceGraph(request: SourceRequest): Promise<ResolvedSource>;
}

export type ResolvedSource = {
  storedColumnIds: SortedMap<string, PhysicalColumnId>;
  tableSchema: TableSchema;
  graph: GraphBuilder;
  rowShape: SourceRowShape;
  routingFields: SortedSet<string>;
  requiresResultPayload: boolean;
  contentVersion?: ContentVersionSource;
  deletionRegister?: DeletionRegisterSource;
  authorizedDeletionPreimage?: AuthorizedDeletionPreimage;
};

export type AuthorizedDeletionPreimage = { graph: GraphBuilder; routingFields: SortedSet<string> };
export type ContentVersionSource = { graph: GraphBuilder; rowUuidField: string };
export type DeletionRegisterSource = { graph: GraphBuilder; rowUuidField: string };

export type SourceRowShape = {
  source: SourceId;
  descriptor: RecordDescriptor;
  rowUuidField: string;
  metadata: SortedMap<SourceMetadataRequirement, SourceMetadataFields>;
};

export type SourceMetadataFields =
  | {
      kind: "VersionWitnesses";
      schemaVersionField: string;
      txTimeField: string;
      txNodeField: string;
      branchOrPrefixField?: string;
    }
  | { kind: "SettlePosition"; settlePositionField: string }
  | {
      kind: "DeletionMarkers";
      deletionStateField: string;
      deletionTxTimeField?: string;
      deletionTxNodeField?: string;
    }
  | {
      kind: "BatchMembership";
      batchIdField: string;
      branchOrPrefixField?: string;
      rowDigestField: string;
      batchKindField: string;
    }
  | { kind: "Coverage"; coverageField: string }
  | { kind: "ValidationReads"; snapshotField: string }
  | { kind: "PolicyWitnesses"; policyPathField: string; edgeKindField: string }
  | { kind: "Provenance"; field: string };

export class SourceResolutionError extends JazzError {
  constructor(
    readonly request: SourceRequest,
    readonly gap: SourceGap,
  ) {
    super("SourceResolution", `source resolution failed: ${dbg(gap)}`);
  }
}

export type SourceGap =
  | {
      kind:
        | "LocalAvailabilityInput"
        | "HistoricalStorageCut"
        | "SnapshotRef"
        | "SchemaProjection"
        | "BranchOverlay"
        | "TransactionReadOverlay"
        | "Coverage";
    }
  | { kind: "PolicyProofCycle"; table: string; depth: number };

export type CapabilityReport = { gaps: UnsupportedReason[]; explain: ExplainPlan };

export type UnsupportedReason =
  | { kind: "Source"; gap: SourceGap }
  | { kind: "UnboundClaim"; path: ClaimPath }
  | { kind: "Operator"; message: string }
  | { kind: "Output"; fact: ProgramFactKey }
  | { kind: "Runtime"; message: string };

export type ExplainPlan = {
  input: string;
  read: string[];
  policy: string[];
  output: string[];
  capabilities: string[];
  physical: string[];
};

export const emptyExplain = (): ExplainPlan => ({
  input: "",
  read: [],
  policy: [],
  output: [],
  capabilities: [],
  physical: [],
});

/** Thrown wherever Rust returns `CapabilityResult::Err`. */
export class CapabilityError extends JazzError {
  constructor(readonly report: CapabilityReport) {
    super("Capability", `unsupported query program: ${dbg(report.gaps)}`);
  }
  static runtime(message: string): CapabilityError {
    return CapabilityError.single({ kind: "Runtime", message });
  }
  static operator(message: string): CapabilityError {
    return CapabilityError.single({ kind: "Operator", message });
  }
  static single(gap: UnsupportedReason): CapabilityError {
    return new CapabilityError({ gaps: [gap], explain: emptyExplain() });
  }
  static explained(gap: UnsupportedReason, capability: string): CapabilityError {
    return new CapabilityError({
      gaps: [gap],
      explain: { ...emptyExplain(), capabilities: [capability] },
    });
  }
}

/** Thrown wherever Rust returns `Result<_, UnsupportedReason>` (or a gap list). */
export class UnsupportedError extends JazzError {
  constructor(readonly gaps: UnsupportedReason[]) {
    super("Unsupported", `unsupported: ${dbg(gaps)}`);
  }
  get gap(): UnsupportedReason {
    return this.gaps[0];
  }
}

export const operatorGap = (message: string): UnsupportedError =>
  new UnsupportedError([{ kind: "Operator", message }]);
export const runtimeGap = (message: string): UnsupportedError =>
  new UnsupportedError([{ kind: "Runtime", message }]);

/** `.map_err(single_gap_report)` */
export function orReport<T>(f: () => T): T {
  try {
    return f();
  } catch (err) {
    if (err instanceof UnsupportedError) throw CapabilityError.single(err.gap);
    throw err;
  }
}

/** Runs f, returning undefined instead of a gap error (Rust `.ok()`). */
export function tryGap<T>(f: () => T): T | undefined {
  try {
    return f();
  } catch (err) {
    if (err instanceof UnsupportedError || err instanceof CapabilityError) return undefined;
    throw err;
  }
}

export function lookupResolvedSource(
  sources: SortedMap<SourceId, ResolvedSource>,
  id: SourceId,
  role: string,
): ResolvedSource {
  const source = sources.get(id);
  if (!source) throw CapabilityError.runtime(`${role} source ${dbg(id)} was not resolved`);
  return source;
}
