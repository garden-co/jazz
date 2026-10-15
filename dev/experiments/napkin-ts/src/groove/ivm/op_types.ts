// Operator descriptors carried by IVM graph nodes.
import type { DurableStorage, IndexCandidateFilter, ProjectExpr } from "./graph.js";
import {
  type FieldIdentity,
  type RecordDescriptor,
  type Value,
  type ValueType,
  type IndexSchema,
  OwnedRecord,
  encodeRecordDescriptor,
  decodeRecordDescriptor,
  predicateReferencedFields,
} from "../../_extern/groove-ivm-plan.js";
import { unreachable } from "../../support/runtime.js";
import { toHex as bytesToHex } from "../../support/bytes.js";

/** Canonical structural key: stands in for derived Eq/Hash/Ord on descriptors. */
export function structKey(x: unknown): string {
  switch (typeof x) {
    case "undefined":
      return "~";
    case "bigint":
      return `${x}n`;
    case "number":
    case "boolean":
      return String(x);
    case "string":
      return JSON.stringify(x);
    case "object": {
      if (x === null) return "~";
      if (x instanceof Uint8Array) return `b${bytesToHex(x)}`;
      if (Array.isArray(x)) return `[${x.map(structKey).join(",")}]`;
      if (typeof (x as any).structKey === "function") return (x as any).structKey();
      const keys = Object.keys(x).sort();
      return `{${keys.map((k) => `${k}:${structKey((x as any)[k])}`).join(",")}}`;
    }
    default:
      return String(x);
  }
}
export const structEq = (a: unknown, b: unknown) => a === b || structKey(a) === structKey(b);
export const structCmp = (a: unknown, b: unknown) => {
  const ka = structKey(a);
  const kb = structKey(b);
  return ka < kb ? -1 : ka > kb ? 1 : 0;
};

// Sources.
export type VariantProjectionTarget =
  | { kind: "Named"; name: string }
  | { kind: "SchemaIndex"; name: string };

export interface TableSourceOp {
  table: string;
  scan?: StaticScanSpec;
  variantProjection?: VariantProjectionTarget;
}

export interface LookupOp {
  source: TableSourceOp;
  keyDescriptor: RecordDescriptor;
  keyFields: number[];
}

export interface IndexSourceDurableIds {
  index: number;
  intersections: number[];
  candidate?: number;
}

export interface IndexSourceOp {
  table: string;
  index: string;
  durableIds: IndexSourceDurableIds;
  intersections: [string, StaticScanSpec][];
  candidateFilter?: IndexCandidateFilter;
  inputDescriptor: RecordDescriptor;
  variantProjection?: VariantProjectionTarget;
  rowProjection?: VariantProjectionTarget;
  keyFields: number[];
  valueFields: number[];
  unique: boolean;
  appendValueToKey: boolean;
  storeValue: boolean;
  scan?: StaticScanSpec;
}

export type StaticScanSpec =
  | { kind: "Point"; values: LiteralValue[] }
  | { kind: "Prefix"; values: LiteralValue[] }
  | { kind: "PrefixLimit"; prefix: LiteralValue[]; maxItems: number }
  | { kind: "ReversePrefixLimit"; prefix: LiteralValue[]; maxItems: number }
  | { kind: "Range"; start: LiteralValue[]; end: LiteralValue[] };

export interface InlineRecordsOp {
  records: Uint8Array[];
}
export interface FrontierSourceOp {
  binding: FrontierName;
}
export interface BindingSourceOp {
  key: BindingSourceKey;
}

export type BindingSourceKey =
  | { kind: "Prepared"; shape: string }
  | { kind: "RuntimeInput"; runtimeNamespace: number; local: number };

export const BindingSourceKey = {
  prepared: (shape: string): BindingSourceKey => ({ kind: "Prepared", shape }),
  preparedName: (k: BindingSourceKey): string | undefined =>
    k.kind === "Prepared" ? k.shape : undefined,
};

/** Name of a value bound in an evaluation context. */
export type FrontierName = string;

// Stateless transformations.
export interface PersistOp {
  name: string;
  storage: DurableStorage;
  keyFields: number[];
  unique: boolean;
}

export type ValueComparison = "Exact" | "Policy";

export interface FilterOp {
  predicate: PredicateExpr;
  comparison: ValueComparison;
}

export interface MapProjectOp {
  expressions: ProjectionExpr[];
  mapping: [number, number][];
}

export interface StreamingChecksumOp {
  field: string;
  fieldIdx: number;
  outputField: string;
  windowBytes: number;
  maxBytesPerTurn: number;
}

export interface RecursiveEnumRemaps {
  scalar: Map<string, (number | undefined)[]>;
  payload: Map<string, (number | undefined)[]>;
  payloadChildren: Map<string, (string | undefined)[]>;
}
export const emptyRecursiveEnumRemaps = (): RecursiveEnumRemaps => ({
  scalar: new Map(),
  payload: new Map(),
  payloadChildren: new Map(),
});

export interface ProjectionExpr {
  expression: ProjectExpr;
  outputName?: string;
  outputIdentity: FieldIdentity;
}

export interface UnwrapNullableOp {
  field: string;
  fieldIdx: number;
}
export interface UnnestOp {
  arrayField: string;
  arrayFieldIdx: number;
  elementField: string;
}
export interface VariantProjectOp {
  field: string;
  fieldIdx: number;
  tag: number;
}

export interface IndexByOp {
  keyExpressions: PlanExpr[];
  valueExpressions: PlanExpr[];
  explicitIndex?: IndexSchema;
  keyFields: number[];
  valueFields: number[];
  unique: boolean;
  appendValueToKey: boolean;
  storeValue: boolean;
  scan?: StaticScanSpec;
}

// Stateful transformations.
export type JoinOpKind = "Inner" | "Left" | "Right" | "Full";

export interface JoinOp {
  kind: JoinOpKind;
  leftKey: PlanExpr[];
  rightKey: PlanExpr[];
  leftDescriptor: RecordDescriptor;
  rightDescriptor: RecordDescriptor;
  residualPredicate?: PlanExpr;
  comparison: ValueComparison;
}

export interface RecursiveOp {
  frontier: FrontierName;
  maxIters: number;
  truncateAtMaxIters: boolean;
  readTables: string[];
  stepWitnessOutput?: RecordDescriptor;
}

// Aggregate.
export interface ArgMaxByOp {
  groupFields: string[];
  orderFields: string[];
  groupFieldIndices: number[];
  comparisonFieldIndices: number[];
}
export type ArgMinByOp = ArgMaxByOp;

export type TopByLimit = { kind: "Finite"; n: number } | { kind: "Unbounded" };
export type TopByDirection = "Asc" | "Desc";

export interface TopByOrderField {
  field: string;
  direction: TopByDirection;
}

export interface TopByOp {
  groupFields: string[];
  groupFieldIndices: number[];
  orderFields: TopByOrderField[];
  tieFields: string[];
  sortFieldIndices: number[];
  sortDirections: TopByDirection[];
  offset: number;
  limit: TopByLimit;
}

export interface CollectByProjection {
  field: string;
  fieldIdx: number;
  outputName: string;
  unwrapNullable: boolean;
}

export interface CollectBySlot {
  groupFields: string[];
  groupFieldIndices: number[];
  childFields: CollectByProjection[];
  childDescriptor: RecordDescriptor;
  collectionField: string;
  collectionFieldIndex: number;
  slots: CollectBySlot[];
  orderFields: TopByOrderField[];
  tieFields: string[];
  presenceFieldIndex?: number;
  referenceArrayFieldIndex?: number;
  referenceOrder: boolean;
  sortFieldIndices: number[];
  sortDirections: TopByDirection[];
  offset: number;
  limit: TopByLimit;
}

export type CollectByMode = "Collect" | "Root" | "Expand";

export interface CollectByOp {
  mode: CollectByMode;
  groupFields: string[];
  groupFieldIndices: number[];
  parentFields: CollectByProjection[];
  childFields: CollectByProjection[];
  childDescriptor: RecordDescriptor;
  collectionField: string;
  collectionFieldIndex: number;
  slots: CollectBySlot[];
  tupleFields: CollectByProjection[];
  occurrenceIdFields: string[];
  occurrenceIdFieldIndices: number[];
  orderFields: TopByOrderField[];
  tieFields: string[];
  sortFieldIndices: number[];
  sortDirections: TopByDirection[];
  offset: number;
  limit: TopByLimit;
}

/** Every input field the collector reads, sorted and deduplicated. */
export function collectByReadFieldIndices(op: CollectByOp): number[] {
  const fields: number[] = [
    ...op.groupFieldIndices,
    ...op.parentFields.map((f) => f.fieldIdx),
    ...op.childFields.map((f) => f.fieldIdx),
    ...op.tupleFields.map((f) => f.fieldIdx),
    ...op.occurrenceIdFieldIndices,
    ...op.sortFieldIndices,
  ];
  const extend = (slot: CollectBySlot) => {
    fields.push(...slot.groupFieldIndices, ...slot.childFields.map((f) => f.fieldIdx));
    if (slot.presenceFieldIndex !== undefined) fields.push(slot.presenceFieldIndex);
    if (slot.referenceArrayFieldIndex !== undefined) fields.push(slot.referenceArrayFieldIndex);
    fields.push(...slot.sortFieldIndices);
    slot.slots.forEach(extend);
  };
  op.slots.forEach(extend);
  return [...new Set(fields)].sort((a, b) => a - b);
}

export interface AggregateOp {
  groupKey: PlanExpr[];
  groupFieldIndices: number[];
  aggregates: AggregateExpr[];
}

export type AggregateFunction = "Count" | "Sum" | "Avg" | "Min" | "Max";

export interface AggregateExpr {
  function: AggregateFunction;
  expression?: PlanExpr;
  distinct: boolean;
  outputName?: string;
  outputIdentity?: FieldIdentity;
}

export type PlanExpr =
  | { kind: "Field"; name: string }
  | { kind: "Literal"; value: LiteralValue }
  | { kind: "Null"; valueType: ValueType }
  | { kind: "Nullable"; name: string }
  | { kind: "NullableFlat"; name: string }
  | { kind: "EnumTagRemap"; field: string; tags: (number | undefined)[] }
  | { kind: "EnumRemap"; field: string; tags: (number | undefined)[] }
  | {
      kind: "RecursiveEnumRemap";
      field: string;
      remaps: RecursiveEnumRemaps;
      omitUnrepresentable: boolean;
    };

export const PlanExpr = {
  field: (name: string): PlanExpr => ({ kind: "Field", name }),
  literal: (value: LiteralValue): PlanExpr => ({ kind: "Literal", value }),
  null: (valueType: ValueType): PlanExpr => ({ kind: "Null", valueType }),
  nullable: (name: string): PlanExpr => ({ kind: "Nullable", name }),
  nullableFlat: (name: string): PlanExpr => ({ kind: "NullableFlat", name }),
};

export type PredicateKind = "Eq" | "Neq" | "Gt" | "GtEq" | "Lt" | "LtEq";

export function reversedPredicateKind(k: PredicateKind): PredicateKind {
  switch (k) {
    case "Gt":
      return "Lt";
    case "GtEq":
      return "LtEq";
    case "Lt":
      return "Gt";
    case "LtEq":
      return "GtEq";
    default:
      return k;
  }
}

export type PredicateExpr =
  | { kind: "TemplateArgument"; slot: number; fields: string[] }
  | { kind: PredicateKind | "Contains"; field: string; value: LiteralValue }
  | { kind: "EqField" | "NeqField"; field: string; valueField: string }
  | { kind: "ContainsField"; field: string; needleField: string }
  | { kind: "IsNull" | "IsNotNull"; field: string }
  | { kind: "EnumMatch"; field: string; caseTag: number; payload: PredicateExpr }
  | { kind: "And" | "Or"; children: PredicateExpr[] };

export function predicateHasTemplateArguments(p: PredicateExpr): boolean {
  switch (p.kind) {
    case "TemplateArgument":
      return true;
    case "And":
    case "Or":
      return p.children.some(predicateHasTemplateArguments);
    case "EnumMatch":
      return predicateHasTemplateArguments(p.payload);
    default:
      return false;
  }
}

export function predicateAcceptsTemplateArgument(
  p: PredicateExpr,
  argument: PredicateExpr,
): boolean {
  if (p.kind !== "TemplateArgument") return false;
  if (predicateHasTemplateArguments(argument)) return false;
  const referenced = new Set<string>();
  predicateReferencedFields(argument, referenced);
  return [...referenced].every((f) => p.fields.includes(f));
}

export function canonicalizePredicate(p: PredicateExpr): PredicateExpr {
  if (p.kind !== "And" && p.kind !== "Or") return p;
  const children = p.children
    .flatMap((c) => {
      const canon = canonicalizePredicate(c);
      return canon.kind === p.kind ? (canon as any).children : [canon];
    })
    .sort(structCmp);
  return { kind: p.kind, children };
}

export const PredicateExpr = {
  eq: (field: string, value: Value): PredicateExpr => ({
    kind: "Eq",
    field,
    value: literalFromValue(value),
  }),
  gt: (field: string, value: Value): PredicateExpr => ({
    kind: "Gt",
    field,
    value: literalFromValue(value),
  }),
  isNull: (field: string): PredicateExpr => ({ kind: "IsNull", field }),
  isNotNull: (field: string): PredicateExpr => ({ kind: "IsNotNull", field }),
  fromFieldLiteral: (kind: PredicateKind, field: string, value: LiteralValue): PredicateExpr => ({
    kind,
    field,
    value,
  }),
};

export type LiteralValue =
  | {
      kind: "U8" | "U16" | "U32" | "U64" | "U48" | "I64" | "I32" | "F64" | "EnumTag";
      value: number;
    }
  | { kind: "Bool"; value: boolean }
  | { kind: "String"; value: string }
  | { kind: "Bytes" | "Uuid"; value: Uint8Array }
  | { kind: "Tuple" | "Array"; values: LiteralValue[] }
  | { kind: "Nullable"; value?: LiteralValue }
  | { kind: "NativeRecord"; descriptor: Uint8Array; raw: Uint8Array }
  | { kind: "Record" };

export function literalFromValue(value: Value): LiteralValue {
  switch (value.kind) {
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I64":
    case "I32":
    case "F64":
    case "EnumTag":
    case "Bool":
    case "String":
    case "Bytes":
    case "Uuid":
      return { kind: value.kind, value: value.value };
    case "Tuple":
    case "Array":
      return { kind: value.kind, values: value.values.map(literalFromValue) };
    case "Nullable":
      return {
        kind: "Nullable",
        value: value.value === undefined ? undefined : literalFromValue(value.value),
      };
    case "Record":
      return {
        kind: "NativeRecord",
        descriptor: encodeRecordDescriptor(value.record.descriptor()),
        raw: value.record.raw().slice(),
      };
    default:
      return { kind: "Record" };
  }
}

/** Infer the representation used by an untyped literal projection. */
export function literalValueType(l: LiteralValue): ValueType | undefined {
  switch (l.kind) {
    case "EnumTag":
      return { kind: "U8" };
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U48":
    case "I64":
    case "I32":
    case "F64":
    case "Bool":
    case "String":
    case "Bytes":
    case "Uuid":
      return { kind: l.kind };
    case "Tuple": {
      const types = l.values.map(literalValueType);
      return types.every((t) => t !== undefined) ? { kind: "Tuple", items: types } : undefined;
    }
    case "Array": {
      const first = l.values.length ? literalValueType(l.values[0]) : undefined;
      return first && { kind: "Array", inner: first };
    }
    case "Nullable": {
      const inner = l.value && literalValueType(l.value);
      return inner && { kind: "Nullable", inner };
    }
    case "NativeRecord":
      try {
        return { kind: "Record", descriptor: decodeRecordDescriptor(l.descriptor) };
      } catch {
        return undefined;
      }
    case "Record":
      return undefined;
  }
}

export function literalToValue(l: LiteralValue): Value {
  switch (l.kind) {
    case "Tuple":
    case "Array":
      return { kind: l.kind, values: l.values.map(literalToValue) };
    case "Nullable":
      return { kind: "Nullable", value: l.value && literalToValue(l.value) };
    case "NativeRecord":
      return {
        kind: "Record",
        record: new OwnedRecord(l.raw.slice(), decodeRecordDescriptor(l.descriptor)),
      };
    case "Record":
      return unreachable("unsupported literals are rejected during type validation");
    default:
      return { kind: l.kind, value: l.value };
  }
}
