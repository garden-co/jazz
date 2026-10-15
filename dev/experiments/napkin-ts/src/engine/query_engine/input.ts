import { SortedMap, SortedSet, cmpStr, type Cmp } from "../../support/collections.js";
import type {
  BindingId,
  ColumnType,
  RecursionBound,
  ShapeId,
  Value,
  ProgramSourceId,
  ProgramSourceRole,
} from "../../_extern/engine.js";
import type { PolicyContext } from "./policy.js";
import type { RequestedReadSet } from "./read.js";
import type { RowSetOutputRequest } from "./output.js";
import type { TypedOutputField } from "./schemas.js";

// Structural identity helpers for Rust derive(Eq, Ord, Hash) keys.
export function skey(value: unknown): string {
  return JSON.stringify(value, (_k, v) =>
    v instanceof Uint8Array ? { b: Array.from(v) } : v instanceof SortedSet ? v.toArray() : v,
  );
}
export const structEq = (a: unknown, b: unknown): boolean => skey(a) === skey(b);
export const cmpStruct: Cmp<any> = (a, b) => cmpStr(skey(a), skey(b));
export const structMap = <K, V>(entries?: Iterable<[K, V]>) =>
  new SortedMap<K, V>(cmpStruct, entries);
export const structSet = <K>(items?: Iterable<K>) => new SortedSet<K>(cmpStruct, items);
export const dbg = (value: unknown): string => skey(value);

export type QueryProgramRequest = {
  authorizationMode: QueryAuthorizationMode;
  reads: RequestedReadSet;
  policy: PolicyContext;
  input: RowSetProgramInput;
  output: RowSetOutputRequest;
};

export type QueryAuthorizationMode = "TrustedServing" | "ClientLocal";

export type RowSetProgramInput = { shape: NormalizedRowSetShape; binding: ProgramBinding };

export type ProgramBinding = {
  id: BindingId;
  sourceShape?: string;
  extraUserParams: SortedMap<string, ColumnType>;
  paramTypes: SortedMap<string, ColumnType>;
  claimParams: SortedMap<string, ProgramClaimParam>;
  values: SortedMap<string, Value>;
};

export type ProgramClaimParam = { path: ClaimPath; ty: ColumnType };

export type NormalizedRowSetShape = {
  identity: NormalizedShapeIdentity;
  root: RowSetNodeId;
  result: ResultId;
  auxiliarySources: SortedSet<SourceId>;
  closurePaths: ClosurePath[];
  joinContributions: JoinContribution[];
  inheritedContributions: InheritedContribution[];
  reachableContributions: ReachableContribution[];
  nodes: SortedMap<RowSetNodeId, RowSetExpr>;
};

export type ClosurePath =
  | { kind: "ImplicitRootReference"; id: string; segment: ClosurePathSegment }
  | {
      kind: "ExplicitInclude";
      id: string;
      segments: ClosurePathSegment[];
      rootGate?: ClosureRootGate;
    };

export type ClosureRootGate = "Required" | "Inner";

export type ClosurePathSegment = { parent: SourceId; target: SourceId; sourceField: string };

export type JoinContribution = {
  parent?: SourceId;
  id: string;
  source: SourceId;
  input: RowSetNodeId;
  membership: PredicateExpr;
};

export type InheritedContribution = {
  id: string;
  source: SourceId;
  input: RowSetNodeId;
  membership: PredicateExpr;
};

export type ReachableContribution = {
  id: string;
  accessSource: SourceId;
  edgeSource: SourceId;
  accessInput: RowSetNodeId;
  rootRefField: string;
};

export type NormalizedShapeIdentity = { shapeId: ShapeId; canonical: Uint8Array };

export type RowSetNodeId = string;

export type SourceId = { table: string; path: SourcePath };
export type SourcePath = { components: SourceRole[] };

export function programSourceId(source: SourceId): ProgramSourceId {
  return {
    table: source.table,
    path: source.path.components.map(
      (role): ProgramSourceRole =>
        role.kind === "Root" ? { kind: "Root" } : { kind: role.kind, name: role.name },
    ),
  };
}

export type ProgramPathId = { owner: SourceId; child: SourceId };

export type ResultId =
  | { kind: "RealRow"; table: string; row: ResultRowRef }
  | { kind: "SyntheticTuple"; identity: SyntheticIdentitySpec }
  | { kind: "PathTuple"; path: ProgramPathId; revision: NormalizedValueRef[] };

export type ResultRowRef =
  | { kind: "Source"; source: SourceId }
  | { kind: "SourceAlternatives"; sources: SortedSet<SourceId> }
  | { kind: "Projected"; row: NormalizedValueRef; sourceDiscriminator?: NormalizedValueRef };

export type SyntheticIdentitySpec = {
  table: string;
  keyColumns: string[];
  revisionColumns: string[];
};

export type SourceRole =
  | { kind: "Root" }
  | {
      kind: "Alias" | "RecursiveSeed" | "RecursiveStep" | "CorrelatedChild" | "Policy";
      name: string;
    };

export type RowSetExpr =
  | { kind: "Source"; source: SourceId; visibility: RowVisibility }
  | { kind: "ValueSource"; shape: string; columns: ValueSourceColumn[]; mode: ValueSourceMode }
  | { kind: "FrontierSource"; frontier: FrontierId; columns: ValueSourceColumn[] }
  | { kind: "Filter"; input: RowSetNodeId; predicate: PredicateExpr }
  | { kind: "Join"; left: RowSetNodeId; right: RowSetNodeId; mode: JoinMode; on: PredicateExpr }
  | {
      kind: "RecursiveRelation";
      seed: RowSetNodeId;
      step: RowSetNodeId;
      frontier: FrontierId;
      frontierKey: NormalizedValueRef;
      dedupeKeys: NormalizedValueRef[];
      bound: RecursionBound;
    }
  | { kind: "Union"; inputs: UnionInput[] }
  | { kind: "Distinct"; input: RowSetNodeId; keys: NormalizedValueRef[] }
  | { kind: "Project"; input: RowSetNodeId; columns: RowProjection[] }
  | {
      kind: "CorrelatedPathProjection";
      input: RowSetNodeId;
      childInput: RowSetNodeId;
      path: ProgramPathId;
      correlation: PredicateExpr;
      requirement: CorrelationRequirement;
    }
  | { kind: "OrderBy"; input: RowSetNodeId; keys: OrderKey[] }
  | {
      kind: "Slice";
      input: RowSetNodeId;
      partitionBy: NormalizedValueRef[];
      limit?: number;
      offset: number;
      tieBreaker: NormalizedValueRef[];
      rankOutput?: TypedOutputField;
    }
  | {
      kind: "Aggregate";
      input: RowSetNodeId;
      groupBy: NormalizedValueRef[];
      outputs: AggregateExpr[];
    };

export type ValueSourceColumn = { name: string; value: NormalizedValueRef; ty: ColumnType };
export type ValueSourceMode = "Binding" | "Inline";
export type RowProjection = { output: TypedOutputField; value: NormalizedValueRef };
export type UnionInput = { node: RowSetNodeId; label: string };

export type PredicateExpr =
  | { kind: "True" }
  | { kind: "False" }
  | { kind: "Compare"; left: NormalizedValueRef; op: ComparisonOp; right: NormalizedValueRef }
  | { kind: "In"; value: NormalizedValueRef; options: NormalizedValueRef[] }
  | { kind: "ArrayContains"; value: NormalizedValueRef; needle: NormalizedValueRef }
  | { kind: "TextContains"; value: NormalizedValueRef; needle: NormalizedValueRef }
  | { kind: "IsNull"; value: NormalizedValueRef }
  | { kind: "IsNotNull"; value: NormalizedValueRef }
  | { kind: "EnumMatch"; value: NormalizedValueRef; caseTag: number; payload: PredicateExpr }
  | { kind: "And"; children: PredicateExpr[] }
  | { kind: "Or"; children: PredicateExpr[] }
  | { kind: "Not"; child: PredicateExpr };

/** Visits every value in order (mutable access via the returned replacement). Throws to stop. */
export function forEachPredicateValue(
  predicate: PredicateExpr,
  f: (value: NormalizedValueRef) => NormalizedValueRef | void,
): void {
  const visit = (holder: any, key: string) => {
    const next = f(holder[key]);
    if (next !== undefined) holder[key] = next;
  };
  switch (predicate.kind) {
    case "True":
    case "False":
      return;
    case "Compare":
      visit(predicate, "left");
      visit(predicate, "right");
      return;
    case "ArrayContains":
    case "TextContains":
      visit(predicate, "value");
      visit(predicate, "needle");
      return;
    case "In":
      visit(predicate, "value");
      for (let i = 0; i < predicate.options.length; i++) visit(predicate.options, String(i));
      return;
    case "IsNull":
    case "IsNotNull":
      visit(predicate, "value");
      return;
    case "EnumMatch":
      visit(predicate, "value");
      forEachPredicateValue(predicate.payload, f);
      return;
    case "And":
    case "Or":
      for (const child of predicate.children) forEachPredicateValue(child, f);
      return;
    case "Not":
      forEachPredicateValue(predicate.child, f);
  }
}

export type ComparisonOp = "Eq" | "Ne" | "Lt" | "Lte" | "Gt" | "Gte";

export type NormalizedValueRef =
  | { kind: "SourceField"; source: SourceId; field: string }
  | { kind: "Param"; name: string }
  | { kind: "Claim"; path: ClaimPath }
  | { kind: "FrontierColumn"; frontier: FrontierId; field: string }
  | { kind: "RowId"; row: RowIdRef }
  | { kind: "Provenance"; source: SourceId; field: ProvenanceField }
  | { kind: "Literal"; bytes: Uint8Array }
  | { kind: "TemplateLiteral"; slot: number };

export type ProvenanceField = "CreatedAt" | "CreatedBy" | "UpdatedAt" | "UpdatedBy";

export type RowIdRef =
  | { kind: "Source"; source: SourceId }
  | { kind: "Frontier"; frontier: FrontierId };

export type FrontierId = string;
export type ClaimPath = string[];

export type JoinMode = "Inner" | "Semi" | "NullExtend";
export type CorrelationRequirement = "Optional" | "AtLeastOne" | "MatchCorrelationCardinality";

export type AggregateExpr = {
  output: TypedOutputField;
  function: AggregateFunction;
  input?: NormalizedValueRef;
};
export type AggregateFunction = "Count" | "Sum" | "Avg" | "Min" | "Max";
export type OrderKey = { value: NormalizedValueRef; direction: SortDirection };
export type SortDirection = "Asc" | "Desc";
/** Ordered: IncludeDeleted > Visible. */
export type RowVisibility = "Visible" | "IncludeDeleted";
