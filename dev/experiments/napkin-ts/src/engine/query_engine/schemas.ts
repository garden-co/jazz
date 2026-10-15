import type { SortedMap, SortedSet } from "../../support/collections.js";
import type {
  ColumnType,
  DescriptorField,
  ProgramSourceId,
  RecordDescriptor,
} from "../../_extern/engine.js";
import type { CurrentRowPublicationField } from "./publication.js";
import type { PredicateOutputSetRole, ProgramFactKey } from "./output.js";
import type { CoverageFrontier } from "./read.js";

export type ProgramOutputSchemas = { kind: "RowSet"; terminals: OutputTerminalSchema[] };

export type OutputTerminalSchema =
  | { kind: "AppRows"; schema: AppRowSchema }
  | { kind: "Fact"; fact: ProgramFactOutput };

export type AppRowSchema = {
  publicationFields: SortedMap<string, CurrentRowPublicationField>;
  descriptor: RecordDescriptor;
  hiddenFields: SortedSet<string>;
  carrier: AppRowCarrier;
  fieldCarriers: SortedMap<string, AppRowCarrier>;
  publicFieldNames: SortedMap<string, string>;
  terminal: AppRowTerminal;
  rootUnionArm: boolean;
};

export type AppRowTerminal =
  | { kind: "Direct" }
  | { kind: "RootCollector" }
  | { kind: "Aggregate"; schema: AggregateResultSchema };

export type AppRowCarrier = "CurrentRow" | "Logical";

export type ProgramFactOutput = {
  key: ProgramFactKey;
  terminal: ProgramFactTerminal;
  schema: ProgramFactSchema;
};

export type ProgramFactTerminal =
  | "Primary"
  | "VersionWitnessContent"
  | "VersionWitnessDeletion"
  | "ReplacementWitnessContent"
  | "ReplacementWitnessDeletion";

export type ProgramFactSchema =
  | { kind: "AuthorizedRows"; schema: AuthorizedRowsSchema }
  | { kind: "ResultMembership"; schema: ResultMembershipSchema }
  | { kind: "AggregateResult"; schema: AggregateResultSchema }
  | { kind: "RelationEdges"; schema: RelationEdgeSchema }
  | { kind: "PathCorrelationCoverage"; schema: PathCorrelationCoverageSchema }
  | { kind: "ProgramSourceCoverage"; schema: ProgramSourceCoverageSchema }
  | { kind: "ReadFrontierSettled"; schema: ReadFrontierSettledSchema }
  | { kind: "CompleteTxPayloadCoverage"; schema: CompleteTxPayloadCoverageSchema }
  | { kind: "ViewCompleteExclusiveCoverage"; schema: ViewCompleteExclusiveCoverageSchema }
  | { kind: "VersionWitnesses"; schema: VersionWitnessSchemas }
  | { kind: "ReplacementWitnesses"; schema: VersionWitnessSchemas }
  | { kind: "PolicyDecision"; schema: PolicyDecisionSchema }
  | { kind: "PolicyWitnesses"; schema: PolicyWitnessSchema }
  | { kind: "ContributingMembers"; schema: ContributingMembersSchema }
  | { kind: "PredicateReads"; schema: PredicateReadFactSchema }
  | { kind: "PredicateOutputSet"; schema: PredicateOutputSetSchema }
  | { kind: "PointReads"; schema: PointReadFactSchema };

export type AuthorizedRowsSchema = { rowField: string; routingParamFields: SortedSet<string> };

export type ResultMembershipSchema = {
  tableField: string;
  rowField: string;
  occurrenceIdFields: string[];
  occurrenceUnionArmFields: SortedMap<number, string>;
  payloadFields: TypedOutputField[];
  payloadPublicationFields: SortedMap<string, CurrentRowPublicationField>;
  branchOrPrefixField?: string;
  version: ResultMembershipVersionSchema;
  settlePositionField?: string;
  routingParamFields: SortedSet<string>;
};

export type RowRefSchema = { sourceField: string; tableField: string; rowField: string };

export type VersionedRowRefSchema = {
  row: RowRefSchema;
  version?: ResultMembershipVersionSchema;
  branchOrPrefixField?: string;
};

export type ResultMembershipVersionSchema =
  | { kind: "Content"; content: ContentVersionFields }
  | {
      kind: "ContentOrDeletion";
      content: ContentVersionFields;
      deletion: VersionIdentityFields;
      deletionStateField: string;
    };

export type ContentVersionFields = { txTimeField: string; txNodeField: string };

export type RelationEdgeSchema = {
  source: VersionedRowRefSchema;
  pathField: string;
  target: VersionedRowRefSchema;
  kindField: string;
  depthField?: string;
  edgeIdField?: string;
  branchField?: string;
  roleField?: string;
  orderField?: string;
  holeStateField?: string;
};

export type PathCorrelationCoverageSchema = {
  parent: VersionedRowRefSchema;
  pathField: string;
  correlationField: string;
  expectedCountField?: string;
  readableCountField: string;
  coverageStateField: string;
};

export type VersionWitnessSchemas = {
  roleField: string;
  content?: VersionWitnessSchema;
  deletion?: VersionWitnessSchema;
  routingParamFields: SortedSet<string>;
};

export type VersionWitnessSchema = {
  source: ProgramSourceId;
  descriptor: RecordDescriptor;
  identity: VersionIdentityFields;
  createdByField: string;
  createdAtField: string;
  updatedByField: string;
  updatedAtField: string;
  authoredColumnsField: string;
  deletionField: string;
  userFields: SortedMap<string, string>;
};

export type VersionIdentityFields = {
  tableField: string;
  rowField: string;
  txTimeField: string;
  txNodeField: string;
  batchIdField?: string;
  branchOrPrefixField?: string;
  rowDigestField?: string;
  schemaField: string;
  layerField: string;
};

export type PolicyWitnessSchema = {
  protected: VersionedRowRefSchema;
  policyPathField: string;
  witness: VersionedRowRefSchema;
  edgeKindField: string;
};

export type ProgramSourceCoverageSchema = {
  source: ProgramSourceId;
  complete: boolean;
  routingParamFields: SortedSet<string>;
};

export type ReadFrontierSettledSchema = {
  tierField: string;
  frontierField: string;
  streamField?: string;
  throughField?: string;
  scopeField: string;
  routingParamFields: SortedSet<string>;
};

export type BatchIdentityFields = { batchIdField: string; batchNodeField?: string };

export type CompleteTxPayloadCoverageSchema = {
  batch: BatchIdentityFields;
  tierField: string;
  payloadDigestField: string;
  fateField: string;
};

export type ViewCompleteExclusiveCoverageSchema = {
  batch: BatchIdentityFields;
  viewScopeField: string;
  result?: ResultIdSchema;
  coveredMembersDigestField: string;
  tierField: string;
  fateField: string;
};

export type VisibleBatchMemberFields = {
  tableField: string;
  rowField: string;
  dataBranchField: string;
  rowDigestField: string;
};

export type ContributingMembersSchema = {
  result: ResultIdSchema;
  contributorField: string;
  member: VisibleBatchMemberFields;
  batch: BatchIdentityFields;
  contributingDigestField: string;
};

export type ResultIdSchema =
  | { kind: "RealRow"; row: VersionedRowRefSchema }
  | { kind: "SyntheticRow"; row: SyntheticResultMembershipSchema }
  | { kind: "PathTuple"; pathField: string; rowField: string; revisionField: string };

export type AggregateResultSchema = {
  synthetic: SyntheticResultMembershipSchema;
  groupKeyFields: DescriptorField[];
  groupNames: string[];
  valueFields: DescriptorField[];
  valueNames: string[];
  routingParamFields: SortedSet<string>;
};

export type SyntheticResultMembershipSchema = {
  tableField: string;
  rowField: string;
  replacementField: string;
  routingParamFields: SortedSet<string>;
};

export type TypedOutputField = { name: string; ty: ColumnType };

export type PredicateReadFactSchema = {
  tableField: string;
  shapeIdField: string;
  bindingIdField: string;
  canonicalShapeField: string;
  bindingValueFields: SortedSet<string>;
  baseSnapshotField: string;
  resolvedReadKeyField: string;
  policyKeyField: string;
  sourceVisibilityField: string;
};

export type PredicateOutputSetSchema = {
  role: PredicateOutputSetRole;
  tableField: string;
  rowField: string;
  version: ResultMembershipVersionSchema;
  shapeIdField: string;
  bindingIdField: string;
};

export type PointReadFactSchema = {
  tableField: string;
  rowField: string;
  presenceField: string;
  observedVersionField?: string;
  baseSnapshotField?: string;
};

export type PolicyDecisionSchema = {
  outcomeField: string;
  requiredInputField?: string;
  reasonField?: string;
  facts: ProgramFactOutput[];
};

export type PolicyDecisionOutcome =
  | { kind: "Allowed" }
  | { kind: "Denied" }
  | { kind: "IndeterminateRequiresInput" }
  | { kind: "RequiresCoverage"; frontier: CoverageFrontier };
