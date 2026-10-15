import { extern } from "../support/runtime.js";

// groove
export type GraphBuilder = any;
export type RecordDescriptor = any;
export type DescriptorField = any;
export type Value = any;
export type ColumnType = any;
export type ValueType = any;
export type GroovePredicateExpr = any;
export type GroovePlanExpr = any;
export type ProjectExpr = any;
export type TemplateGraphInput = any;
export const GraphBuilder = extern<any>();
export const RecordDescriptor = extern<any>();
export const DescriptorField = extern<any>();
export const ProjectField = extern<any>();
export const FieldRef = extern<any>();
export const LiteralValue = extern<any>();
export const GroovePredicateExpr = extern<any>();
export const GroovePlanExpr = extern<any>();
export const ProjectExpr = extern<any>();
export const PredicateKind = extern<any>();
export const GrooveAggregateExpr = extern<any>();
export const CollectByField = extern<any>();
export const CollectBySlotBuilder = extern<any>();
export const TopByLimit = extern<any>();
export const TopByOrder = extern<any>();
export const TemplateScalarArgument = extern<{ new (value: any, ty: any): any }>();
export const TypedGraphTemplateCache = extern<{ new (): any }>();
export const MAX_COLLECT_BY_TREE_DEPTH = extern<number>();
export const collectByOrderedScalar = extern<(ty: any) => boolean>();
export const bindTemplateProgram =
  extern<(graphs: any[], inputs: any[], predicates: any[], scalars: any[]) => any[]>();
export const matchTemplateSources =
  extern<(fresh: any[], blueprints: any[], describe: (g: any) => any) => any[] | undefined>();
export const splitTemplateSources =
  extern<(graphs: any[], describe: (g: any) => any) => [any[], any[]]>();
export const decodeValue = extern<(bytes: Uint8Array) => any>();
export const encodeValue = extern<(value: any) => Uint8Array>();

// jazz-types
export type AuthorSubject = any;
export type SchemaFamilyId = any;
export type SchemaVersionId = any;
export type PhysicalColumnId = any;
export type GlobalTime = any;
export const AuthorSubject = extern<{
  metadataPath(field: string): [string, string] | undefined;
}>();
export const coveredInputTrace = extern<() => boolean>();
export const queryTemplateTrace = extern<() => boolean>();

// jazz-model
export type TableSchema = any;
export type OpenTransactionId = any;
export type BindingId = any;
export type ShapeId = any;
export type RecursionBound = any;
export type RelationProjectColumn = any;
export type DurabilityTier = any;
export type Snapshot = any;
export type TxId = any;
export const APP_COLUMN_PREFIX = extern<string>();
export const appStorageColumnName = extern<(column: string) => string>();
export const NIL_UUID = extern<Uint8Array>();

// jazz-protocol
export type BindingViewKey = any;
export type BranchKey = any;
export type SnapshotRef = any;
export type AuthorityResultKey = any;
export type ProgramSourceRole =
  | { kind: "Root" }
  | {
      kind: "Alias" | "RecursiveSeed" | "RecursiveStep" | "CorrelatedChild" | "Policy";
      name: string;
    };
export type ProgramSourceId = { table: string; path: ProgramSourceRole[] };

// Added for graph_lowering/terminals.
export const Value = extern<any>();
export const ValueComparison = extern<any>();
export const FieldIdentity = extern<any>();
export const composeUnionArmPath = extern<(outer: string, inner: string) => string>();
// crate::query / crate::model::policy_claims
export const authorClaimPathKey = extern<(path: string[]) => string | undefined>();
export const authorPolicyClaims = extern<(subject: AuthorSubject) => Map<string, Value>>();
export const policyClaimAtPath = extern<(claims: any, path: string[]) => Value | undefined>();
