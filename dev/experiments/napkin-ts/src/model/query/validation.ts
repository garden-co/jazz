import {
  authorSubjectMetadataType,
  columnTypeDebug,
  columnTypeEq,
  decodeColumnType,
  decodeSchemaVersionId,
  decodeValue,
  encodeColumnType,
  encodeSchemaVersionId,
  encodeValue,
  newColumnSchema,
  newTableSchema,
  runtimeSchemaVersionId,
  schemaVersionIdBytes,
  type ColumnType,
  type RuntimeSchema,
  type SchemaVersionId,
  type TableSchema,
  type Value,
} from "../../_extern/model-query.js";
import { concatBytes, equalBytes, fromUtf8 } from "../../support/bytes.js";
import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import { uuidV5 } from "../../support/hash.js";
import type { Reader, Writer } from "../../support/postcard.js";
import {
  QUERY_NAMESPACE,
  cloneQuery,
  decodeQuery,
  encodeQuery,
  structEq,
  type FlatJoin,
  type Query,
  type RelationProjectColumn,
  type RelationQuery,
} from "./ast.js";
import {
  canonicalBindingBytes,
  canonicalQueryBytesForSchema,
  normalizeQuery,
  valueMatchesType,
  valueType,
} from "./canonical_request.js";
import type {
  AggregateQuery,
  ArraySubquery,
  BindingId,
  InheritsVia,
  JoinVia,
  Operand,
  OrderBy,
  Predicate,
  ReachableVia,
  ShapeId,
} from "./policy_and_request_contracts.js";
import {
  rejectReservedRelationAliases,
  relationOutputProjection,
  relationOutputProjectionIfPresent,
  relationQueryToQuery,
  relationUnionParts,
} from "./relation_resolution.js";

type Params = SortedMap<string, ColumnType>;
const newParams = () => new SortedMap<string, ColumnType>(cmpStr);

const CT = (kind: string): ColumnType => ({ kind });
const UUID: ColumnType = CT("Uuid");
const nonNull = (t: ColumnType): ColumnType => (t.kind === "Nullable" ? t.inner : t);
const nullable = (t: ColumnType): ColumnType =>
  t.kind === "Nullable" ? t : { kind: "Nullable", inner: t };

/** Validated query shape with inferred parameter types. */
export class ValidatedQuery {
  constructor(
    readonly query: Query,
    readonly schemaVersion: SchemaVersionId,
    readonly params: Params,
    readonly canonical: Uint8Array,
    readonly shapeId: ShapeId,
  ) {}
  bind(values: SortedMap<string, Value>): Binding {
    return validateBindingValues(this.params, values);
  }
}

export function encodeValidatedQuery(w: Writer, v: ValidatedQuery): void {
  encodeQuery(w, v.query);
  encodeSchemaVersionId(w, v.schemaVersion);
  w.seq([...v.params], (w, [name, ty]) => {
    w.str(name);
    encodeColumnType(w, ty);
  });
  w.bytes(v.canonical).bytes(v.shapeId);
}
export function decodeValidatedQuery(r: Reader): ValidatedQuery {
  const query = decodeQuery(r);
  const version = decodeSchemaVersionId(r);
  const params = new SortedMap(
    cmpStr,
    r.seq((r): [string, ColumnType] => [r.str(), decodeColumnType(r)]),
  );
  return new ValidatedQuery(query, version, params, r.bytes().slice(), r.bytes().slice());
}

function validateBindingValues(params: Params, values: SortedMap<string, Value>): Binding {
  for (const [required] of params)
    if (!values.has(required)) throw new QueryError({ kind: "MissingParam", detail: required });
  for (const [name, value] of values) {
    const expected = params.get(name);
    if (expected === undefined) throw new QueryError({ kind: "UnknownParam", detail: name });
    if (!valueMatchesType(value, expected))
      throw new QueryError({ kind: "ParamTypeMismatch", param: name, expected });
  }
  return new Binding(values, canonicalBindingBytes(values));
}

/** Validated binding values for a query shape. */
export class Binding {
  constructor(
    readonly values: SortedMap<string, Value>,
    readonly canonical: Uint8Array,
  ) {}
  bindingId(): BindingId {
    return uuidV5(QUERY_NAMESPACE, this.canonical);
  }
}

export function encodeBinding(w: Writer, b: Binding): void {
  w.seq([...b.values], (w, [name, value]) => {
    w.str(name);
    encodeValue(w, value);
  });
  w.bytes(b.canonical);
}
export function decodeBinding(r: Reader): Binding {
  const values = new SortedMap(
    cmpStr,
    r.seq((r): [string, Value] => [r.str(), decodeValue(r)]),
  );
  return new Binding(values, r.bytes().slice());
}

export const canonicalBindingBytesForValues = (values: SortedMap<string, Value>): Uint8Array =>
  canonicalBindingBytes(values);

/**
 * Decode canonical `jazz-binding-v0` bytes back into binding values (tags 1-15).
 * Only returned when re-encoding reproduces the input exactly.
 */
export function bindingValuesFromCanonicalBytes(bytes: Uint8Array): SortedMap<string, Value> {
  const invalid = () => new QueryError({ kind: "OperandTypeMismatch" });
  let pos = 0;
  const take = (n: number) => {
    if (bytes.length - pos < n) throw invalid();
    pos += n;
    return bytes.subarray(pos - n, pos);
  };
  const view = (n: number) => {
    const b = take(n);
    return new DataView(b.buffer, b.byteOffset, n);
  };
  const byte = () => take(1)[0];
  const len = () => {
    const n = view(8).getBigUint64(0);
    // Every encoded element occupies at least one byte.
    if (n > BigInt(bytes.length - pos)) throw invalid();
    return Number(n);
  };
  const raw = () => take(len()).slice();
  const string = () => {
    try {
      return fromUtf8(raw());
    } catch {
      throw invalid();
    }
  };
  const many = (depth: number) => Array.from({ length: len() }, () => value(depth + 1));
  const flag = () => {
    const b = byte();
    if (b > 1) throw invalid();
    return b === 1;
  };
  const value = (depth: number): Value => {
    if (depth > 64) throw invalid();
    switch (byte()) {
      case 1:
        return { kind: "U8", value: byte() };
      case 2:
        return { kind: "U16", value: view(2).getUint16(0) };
      case 3:
        return { kind: "U32", value: view(4).getUint32(0) };
      case 4:
        return { kind: "U64", value: Number(view(8).getBigUint64(0)) };
      case 5:
        return { kind: "F64", value: view(8).getFloat64(0) };
      case 6:
        return { kind: "Bool", value: flag() };
      case 7:
        return { kind: "String", value: string() };
      case 8:
        return { kind: "Bytes", value: raw() };
      case 9:
        return { kind: "Uuid", value: take(16).slice() };
      case 10:
        return { kind: "EnumTag", value: byte() };
      case 11:
        return { kind: "Tuple", value: many(depth) };
      case 12:
        return { kind: "Array", value: many(depth) };
      case 13:
        return { kind: "Nullable", value: flag() ? value(depth + 1) : undefined };
      case 14:
        return { kind: "I64", value: Number(view(8).getBigInt64(0)) };
      case 15:
        return { kind: "I32", value: view(4).getInt32(0) };
      default:
        throw invalid();
    }
  };
  if (fromUtf8(take(15)) !== "jazz-binding-v0") throw invalid();
  const count = len();
  const values = new SortedMap<string, Value>(cmpStr);
  for (let i = 0; i < count; i++) {
    const name = string();
    if (values.set(name, value(0)) !== undefined) throw invalid();
  }
  if (pos !== bytes.length || !equalBytes(canonicalBindingBytes(values), bytes)) throw invalid();
  return values;
}

export const bindingIdForValues = (values: SortedMap<string, Value>): BindingId =>
  uuidV5(QUERY_NAMESPACE, canonicalBindingBytes(values));

// ---- errors ----

export type QueryErrorData =
  | {
      kind: "ReservedAggregateAlias" | "AggregateOutputNameCollision" | "UnknownTable";
      detail: string;
    }
  | { kind: "UnsupportedRelationQuery" | "MissingParam" | "UnknownParam"; detail: string }
  | { kind: "UnsupportedFlatJoinCombination"; feature: string }
  | { kind: "UnknownColumn"; table: string; column: string }
  | { kind: "OperandTypeMismatch" }
  | { kind: "ContainsNeedleTypeMismatch"; expected: ColumnType; actual: ColumnType }
  | {
      kind: "InCandidateTypeMismatch";
      column: string;
      column_type: ColumnType;
      candidate_type: ColumnType;
    }
  | {
      kind: "ClaimTypeMismatch";
      claim_path: string;
      column: string;
      claim_type: string;
      column_type: string;
    }
  | { kind: "ParamTypeConflict"; param: string }
  | { kind: "ParamTypeMismatch"; param: string; expected: ColumnType }
  | { kind: "JoinNotRefCompatible"; join_table: string; column: string; target_table: string }
  | { kind: "BadIncludePath"; path: string }
  | { kind: "UnsupportedMagicColumn" | "UnsupportedAuthorOrdering"; column: string };

function queryErrorMessage(e: QueryErrorData): string {
  switch (e.kind) {
    case "ReservedAggregateAlias":
      return `aggregate alias ${e.detail} uses a reserved compiler namespace`;
    case "AggregateOutputNameCollision":
      return `aggregate output name ${e.detail} is used more than once`;
    case "UnsupportedFlatJoinCombination":
      return `flat join cannot be combined with ${e.feature}`;
    case "UnknownTable":
      return `unknown table ${e.detail}`;
    case "UnknownColumn":
      return `unknown column ${e.table}.${e.column}`;
    case "OperandTypeMismatch":
      return "operand type mismatch";
    case "ContainsNeedleTypeMismatch":
      return `contains needle has type ${columnTypeDebug(e.actual)}; expected ${columnTypeDebug(e.expected)}`;
    case "InCandidateTypeMismatch":
      return `in candidate for column ${e.column} has type ${columnTypeDebug(e.candidate_type)}, but the column has type ${columnTypeDebug(e.column_type)}`;
    case "ClaimTypeMismatch":
      return `claim ${e.claim_path} has type ${JSON.stringify(e.claim_type)}, but column ${e.column} has type ${JSON.stringify(e.column_type)}`;
    case "UnsupportedRelationQuery":
      return `unsupported relation query: ${e.detail}`;
    case "ParamTypeConflict":
      return `parameter ${e.param} inferred with incompatible type`;
    case "MissingParam":
      return `missing parameter ${e.detail}`;
    case "UnknownParam":
      return `unknown parameter ${e.detail}`;
    case "ParamTypeMismatch":
      return `parameter ${e.param} has wrong type`;
    case "JoinNotRefCompatible":
      return `join column ${e.join_table}.${e.column} does not reference ${e.target_table}`;
    case "BadIncludePath":
      return `bad include path ${e.path}`;
    case "UnsupportedMagicColumn":
      return `unsupported query magic column ${e.column}: permission introspection columns are not executable yet`;
    case "UnsupportedAuthorOrdering":
      return `ordering by author provenance column ${e.column} is unsupported`;
  }
}

export class QueryError extends Error {
  constructor(readonly data: QueryErrorData) {
    super(queryErrorMessage(data));
  }
  get kind() {
    return this.data.kind;
  }
}

const mismatch = () => new QueryError({ kind: "OperandTypeMismatch" });
const relationError = (detail: string) =>
  new QueryError({ kind: "UnsupportedRelationQuery", detail });
const notRefCompatible = (join_table: string, column: string, target_table: string) =>
  new QueryError({ kind: "JoinNotRefCompatible", join_table, column, target_table });
const unknownColumn = (table: string, column: string) =>
  new QueryError({ kind: "UnknownColumn", table, column });

// ---- entry points ----

export const validateQuery = (q: Query, schema: RuntimeSchema): ValidatedQuery =>
  validateQueryWithSchemaVersion(q, schema, runtimeSchemaVersionId(schema));

export const validateQueryWithSchemaVersion = (
  q: Query,
  schema: RuntimeSchema,
  version: SchemaVersionId,
): ValidatedQuery => validateAs(q, schema, version, "Result");

/** Validate one arm of a retained relation UNION; an arm always keeps its explicit projection. */
export const validateUnionArmWithSchemaVersion = (
  q: Query,
  schema: RuntimeSchema,
  version: SchemaVersionId,
): ValidatedQuery => validateAs(q, schema, version, "UnionArm");

type ProjectionRole = "Result" | "UnionArm";

function validateAs(
  q: Query,
  schema: RuntimeSchema,
  version: SchemaVersionId,
  role: ProjectionRole,
): ValidatedQuery {
  const [normalized, params, canonical] = canonicalPartsAs(q, schema, role);
  const shapeId = uuidV5(QUERY_NAMESPACE, concatBytes(canonical, schemaVersionIdBytes(version)));
  return new ValidatedQuery(normalized, version, params, canonical, shapeId);
}

type CanonicalParts = [Query, Params, Uint8Array];

// ---- parameter inference ----

type Fact = { descriptor: number; required: boolean };
type Term = { t: "Param"; i: number } | { t: "Known"; fact: Fact } | { t: "Dynamic" };
type Descriptor = { kind: "Fixed"; ty: ColumnType } | { kind: "Array"; member: Term };
type Relationship = "Compare" | "In" | "Contains";

interface Requirement {
  fact?: Fact;
  dependents: number[];
  constructedDependents: number[];
  notified: number;
  needleSeen: number;
  haystackSeen: number;
  fallbackDeferred: number;
}
interface Dependency {
  relationship: Relationship;
  left: Term;
  right: Term;
  literal: boolean;
  member?: Term;
  constructorQueued: boolean;
}

/** Preparation-phase parameter type inference over a dependency graph of operand relationships. */
class ParameterDomain {
  names = new SortedMap<string, number>(cmpStr);
  requirements: Requirement[] = [];
  descriptors: Descriptor[] = [];
  dependencies: Dependency[] = [];
  pending: number[] = [];
  queued: boolean[] = [];
  affected: number[] = [];
  notification = 0;
  literalDependencies: number[] = [];
  containsDependencies: number[] = [];
  constructors: number[] = [];
  needleSearch: number[] = [];
  haystackSearch: number[] = [];
  needleGeneration = 0;
  haystackGeneration = 0;

  parameter(name: string): Term {
    const existing = this.names.get(name);
    if (existing !== undefined) return { t: "Param", i: existing };
    const i = this.requirements.length;
    this.names.set(name, i);
    this.requirements.push({
      dependents: [],
      constructedDependents: [],
      notified: 0,
      needleSeen: 0,
      haystackSeen: 0,
      fallbackDeferred: 0,
    });
    return { t: "Param", i };
  }

  known(ty: ColumnType): Term & { t: "Known" } {
    const [base, required] = ty.kind === "Nullable" ? [ty.inner, false] : [ty, true];
    const descriptor = this.descriptors.length;
    this.descriptors.push({ kind: "Fixed", ty: base });
    return { t: "Known", fact: { descriptor, required } };
  }

  term(table: TableSchema, o: Operand): Term {
    switch (o.kind) {
      case "Param":
        return this.parameter(o.name);
      case "Column":
        return this.known(plannerColumnType(table, o.name));
      case "Literal":
        return this.known(valueType(o.value));
      case "Claim": {
        const ty = claimType(o.name);
        return ty ? this.known(ty) : { t: "Dynamic" };
      }
    }
  }

  relationship(
    table: TableSchema,
    left: Operand,
    right: Operand,
    relationship: Relationship,
  ): void {
    if (left.kind !== "Param" && right.kind !== "Param") return;
    const l = this.term(table, left);
    const r = this.term(table, right);
    const index = this.dependencies.length;
    for (const term of [l, r])
      if (term.t === "Param") this.requirements[term.i].dependents.push(index);
    const literal = left.kind === "Literal" || right.kind === "Literal";
    if (literal) this.literalDependencies.push(index);
    if (relationship === "Contains") this.containsDependencies.push(index);
    this.dependencies.push({ relationship, left: l, right: r, literal, constructorQueued: false });
    this.queued.push(false);
  }

  fact(term: Term): Fact | undefined {
    return term.t === "Param"
      ? this.requirements[term.i].fact
      : term.t === "Known"
        ? term.fact
        : undefined;
  }

  matchesType(fact: Fact, ty: ColumnType): boolean {
    const [base, required] = ty.kind === "Nullable" ? [ty.inner, false] : [ty, true];
    return fact.required === required && this.descriptorMatchesType(fact.descriptor, base);
  }

  descriptorMatchesType(descriptor: number, ty: ColumnType): boolean {
    const d = this.descriptors[descriptor];
    if (d.kind === "Fixed") return columnTypeEq(d.ty, ty);
    if (ty.kind !== "Array") return false;
    const f = this.fact(d.member);
    return f !== undefined && this.matchesType(f, ty.inner);
  }

  sameDescriptor(left: number, right: number): boolean {
    if (left === right) return true;
    const l = this.descriptors[left];
    const r = this.descriptors[right];
    if (l.kind === "Fixed") return this.descriptorMatchesType(right, l.ty);
    if (r.kind === "Fixed") return this.descriptorMatchesType(left, r.ty);
    const lf = this.fact(l.member);
    const rf = this.fact(r.member);
    return (
      !!lf &&
      !!rf &&
      lf.required === rf.required &&
      this.sameDescriptor(lf.descriptor, rf.descriptor)
    );
  }

  dependsOn(descriptor: number, param: number): boolean {
    const d = this.descriptors[descriptor];
    if (d.kind === "Fixed") return false;
    const m = d.member;
    if (m.t === "Param") {
      const f = this.requirements[m.i].fact;
      return m.i === param || (f !== undefined && this.dependsOn(f.descriptor, param));
    }
    return m.t === "Known" && this.dependsOn(m.fact.descriptor, param);
  }

  enqueue(dependency: number): void {
    if (!this.queued[dependency]) {
      this.queued[dependency] = true;
      this.pending.push(dependency);
    }
  }

  registerConstructorConsumer(descriptor: number, consumer: number): void {
    const d = this.descriptors[descriptor];
    if (d.kind !== "Array") return;
    if (d.member.t === "Param") this.requirements[d.member.i].constructedDependents.push(consumer);
    else if (d.member.t === "Known")
      this.registerConstructorConsumer(d.member.fact.descriptor, consumer);
  }

  require(term: Term, fact: Fact): void {
    if (term.t !== "Param") return;
    const req = this.requirements[term.i];
    const existing = req.fact;
    let changed = false;
    if (existing === undefined) {
      if (this.dependsOn(fact.descriptor, term.i)) throw mismatch();
      req.fact = fact;
      this.registerConstructorConsumer(fact.descriptor, term.i);
      changed = true;
    } else if (
      this.sameDescriptor(existing.descriptor, fact.descriptor) &&
      fact.required &&
      !existing.required
    ) {
      req.fact = { ...existing, required: true };
      changed = true;
    }
    // Heterogeneous comparability is not transitive: independently anchored descriptors stay distinct.
    if (!changed) return;
    // A constructor's member can change without its descriptor changing: notify consumers transitively.
    this.notification++;
    this.affected.push(term.i);
    for (let p = this.affected.pop(); p !== undefined; p = this.affected.pop()) {
      const r = this.requirements[p];
      if (r.notified === this.notification) continue;
      r.notified = this.notification;
      for (const d of r.dependents) this.enqueue(d);
      this.affected.push(...r.constructedDependents);
    }
  }

  unresolvedAlias(dependency: number, param: number): number | undefined {
    const d = this.dependencies[dependency];
    if (d.literal || d.relationship === "Contains") return undefined;
    if (d.left.t !== "Param" || d.right.t !== "Param") return undefined;
    const other = d.left.i === param ? d.right.i : d.left.i;
    return this.requirements[other].fact === undefined ? other : undefined;
  }

  readyConstructor(dependency: number): boolean {
    const d = this.dependencies[dependency];
    return (
      d.relationship === "Contains" &&
      d.left.t === "Param" &&
      this.fact(d.left) === undefined &&
      this.fact(d.right) !== undefined
    );
  }

  private unknownHaystack(index: number, param: number): number | undefined {
    const d = this.dependencies[index];
    if (
      d.relationship === "Contains" &&
      d.right.t === "Param" &&
      d.right.i === param &&
      d.left.t === "Param" &&
      this.requirements[d.left.i].fact === undefined
    )
      return d.left.i;
    return undefined;
  }

  /** Dependency lookahead for a forward constructor that could type this needle's haystack. */
  readyForwardProvider(needle: number): number | undefined {
    this.needleGeneration++;
    this.haystackGeneration++;
    this.needleSearch = [needle];
    this.haystackSearch = [];
    for (let p = this.needleSearch.pop(); p !== undefined; p = this.needleSearch.pop()) {
      const r = this.requirements[p];
      if (r.needleSeen === this.needleGeneration) continue;
      r.needleSeen = this.needleGeneration;
      for (const index of r.dependents) {
        const other = this.unresolvedAlias(index, p);
        if (other !== undefined) this.needleSearch.push(other);
        const haystack = this.unknownHaystack(index, p);
        if (haystack !== undefined) this.haystackSearch.push(haystack);
      }
    }
    // Follow forward providers transitively, including unknown aliases between them.
    for (let p = this.haystackSearch.pop(); p !== undefined; p = this.haystackSearch.pop()) {
      const r = this.requirements[p];
      if (r.needleSeen === this.needleGeneration || r.haystackSeen === this.haystackGeneration)
        continue;
      r.haystackSeen = this.haystackGeneration;
      for (const index of r.dependents) {
        const other = this.unresolvedAlias(index, p);
        if (other !== undefined) this.haystackSearch.push(other);
        const d = this.dependencies[index];
        if (d.left.t === "Param" && d.left.i === p && this.readyConstructor(index)) return index;
        const haystack = this.unknownHaystack(index, p);
        if (haystack !== undefined) this.haystackSearch.push(haystack);
      }
    }
    return undefined;
  }

  enqueueConstructor(index: number): void {
    if (this.readyConstructor(index) && !this.dependencies[index].constructorQueued) {
      this.dependencies[index].constructorQueued = true;
      this.constructors.push(index);
    }
  }

  drainPending(literals: boolean, constructors: boolean): void {
    for (let index = this.pending.shift(); index !== undefined; index = this.pending.shift()) {
      this.queued[index] = false;
      this.settleDependency(index, literals ? 1 : 0);
      if (constructors) this.enqueueConstructor(index);
    }
  }

  settleDependency(index: number, fallbacks: 0 | 1 | 2): void {
    const { left, right, relationship, literal } = this.dependencies[index];
    if (literal && fallbacks === 0) return;
    if (relationship !== "Contains") {
      const lf = this.fact(left);
      const rf = this.fact(right);
      if (lf && !rf) this.require(right, lf);
      else if (!lf && rf) this.require(left, rf);
      else if (lf && rf && !literal && this.sameDescriptor(lf.descriptor, rf.descriptor)) {
        this.require(left, rf);
        this.require(right, lf);
      }
      return;
    }
    const haystack = this.fact(left);
    if (haystack) {
      let member = this.dependencies[index].member;
      if (member === undefined) {
        const d = this.descriptors[haystack.descriptor];
        if (d.kind === "Fixed" && d.ty.kind === "String") member = this.known(CT("String"));
        else if (d.kind === "Fixed" && d.ty.kind === "Array") member = this.known(d.ty.inner);
        else if (d.kind === "Array") member = d.member;
        this.dependencies[index].member = member;
      }
      if (member !== undefined) {
        // Only inferred constructors reconcile their needles' requiredness.
        const mf = this.fact(member);
        const rf = this.fact(right);
        if (
          this.descriptors[haystack.descriptor].kind === "Array" &&
          mf &&
          rf &&
          this.sameDescriptor(mf.descriptor, rf.descriptor)
        )
          this.require(member, rf);
        const fact = this.fact(member);
        if (fact) this.require(right, fact);
      }
    } else if (fallbacks === 2 && this.fact(right) !== undefined && left.t === "Param") {
      const epoch = this.notification + 1;
      if (this.requirements[left.i].fallbackDeferred === epoch) return;
      const provider = this.readyForwardProvider(left.i);
      if (provider !== undefined) {
        this.requirements[left.i].fallbackDeferred = epoch;
        // Run the provider immediately; the epoch guard breaks provider cycles.
        this.dependencies[provider].constructorQueued = true;
        this.constructors.unshift(provider);
        return;
      }
      const descriptor = this.descriptors.length;
      this.descriptors.push({ kind: "Array", member: right });
      this.require(left, { descriptor, required: true });
    }
  }

  materialize(fact: Fact): ColumnType {
    const d = this.descriptors[fact.descriptor];
    let base: ColumnType;
    if (d.kind === "Fixed") base = d.ty;
    else {
      const member = this.fact(d.member);
      if (!member) throw mismatch();
      base = { kind: "Array", inner: this.materialize(member) };
    }
    return fact.required ? base : nullable(base);
  }

  finish(): Params {
    for (let i = 0; i < this.dependencies.length; i++) this.enqueue(i);
    this.drainPending(false, false);
    this.literalDependencies.forEach((d) => this.enqueue(d));
    this.drainPending(true, false);
    this.containsDependencies.forEach((d) => this.enqueueConstructor(d));
    for (
      let index = this.constructors.shift();
      index !== undefined;
      index = this.constructors.shift()
    ) {
      this.dependencies[index].constructorQueued = false;
      if (this.readyConstructor(index)) {
        this.settleDependency(index, 2);
        // Exhaust newly available forward context before the next backward choice.
        this.drainPending(true, true);
      }
    }
    const params = newParams();
    for (const [name, index] of this.names) {
      const fact = this.requirements[index].fact;
      if (!fact) throw mismatch();
      params.set(name, this.materialize(fact));
    }
    return params;
  }
}

type Phase = { kind: "Prepare"; domain: ParameterDomain } | { kind: "Check"; params: Params };

function validateParameterContexts(
  query: Query,
  schema: RuntimeSchema,
  root: TableSchema,
  filterSchema: TableSchema,
  phase: Phase,
): void {
  for (const join of query.joins) validateJoin(schema, root, query.table, join, phase);
  validatePredicates(filterSchema, query.filters, phase);
  for (const r of query.reachable) validateReachable(schema, root, r, phase);
  for (const branch of query.policy_branches) {
    validatePredicates(root, branch.filters, phase);
    for (const join of branch.joins) validateJoin(schema, root, query.table, join, phase);
    for (const r of branch.reachable) validateReachable(schema, root, r, phase);
  }
  validateArraySubqueries(schema, root, query.array_subqueries, phase);
}

function prepareParameterDomain(
  query: Query,
  schema: RuntimeSchema,
  root: TableSchema,
  filterSchema: TableSchema,
  initial: Params,
): Params {
  const domain = new ParameterDomain();
  for (const [name, ty] of initial) domain.require(domain.parameter(name), domain.known(ty).fact);
  validateParameterContexts(query, schema, root, filterSchema, { kind: "Prepare", domain });
  const params = domain.finish();
  // UNION arms remain independent; outer presentation must not narrow an arm's carrier.
  for (const [name, ty] of initial) {
    const got = params.get(name);
    if (got === undefined || !columnTypeEq(got, ty))
      throw new QueryError({ kind: "ParamTypeConflict", param: name });
  }
  validateParameterContexts(query, schema, root, filterSchema, { kind: "Check", params });
  return params;
}

const hasOrdinaryClauses = (q: Query) =>
  q.filters.length > 0 ||
  q.joins.length > 0 ||
  q.flat_join !== undefined ||
  q.policy_branches.length > 0 ||
  q.reachable.length > 0 ||
  q.inherits.length > 0 ||
  q.includes.length > 0 ||
  q.order_by.length > 0 ||
  q.aggregate !== undefined ||
  q.limit !== undefined ||
  q.offset !== 0;

const GENERATED_FIELDS = [
  "filters",
  "joins",
  "flat_join",
  "policy_branches",
  "reachable",
  "inherits",
  "includes",
  "order_by",
  "aggregate",
  "limit",
  "offset",
] as const;

function canonicalPartsAs(
  query: Query,
  schema: RuntimeSchema,
  role: ProjectionRole,
): CanonicalParts {
  const root = schemaTable(schema, query.table);
  const resolvedQuery = cloneQuery(query);
  const initialParams = newParams();
  if (query.relation) {
    const relation = query.relation;
    if (!relationUnionParts(relation.rel)) {
      const resolved = relationQueryToQuery(relation);
      if (resolved.table !== query.table)
        throw relationError("relation query output table does not match its Query envelope");
      // An internal caller may already have materialized the exact clauses generated from the relation.
      const matchesGenerated = GENERATED_FIELDS.every((f) => structEq(query[f], resolved[f]));
      if (hasOrdinaryClauses(query) && !matchesGenerated)
        throw relationError("relation query cannot be combined with ordinary query clauses");
      resolved.array_subqueries = structuredClone(query.array_subqueries);
      resolved.relation = undefined;
      resolved.select = query.select && [...query.select];
      const retain = retainRelationOutputProjection(query, relation, root, role);
      const [normalized, params] = canonicalPartsAs(resolved, schema, "Result");
      if (retain) normalized.relation = structuredClone(relation);
      return [normalized, params, canonicalQueryBytesForSchema(normalized, schema)];
    }
    validateRetainedRelationOuterQuery(query);
    rejectPresentationOverRelationProjection(query);
    validateRetainedRelationUnion(relation, query.table, schema, initialParams);
    const params = prepareParameterDomain(resolvedQuery, schema, root, root, initialParams);
    for (const column of query.select ?? []) validateSelectColumn(root, column);
    const normalized = normalizeQuery(resolvedQuery);
    return [normalized, params, canonicalQueryBytesForSchema(normalized, schema)];
  }
  let flatFilterSchema: TableSchema | undefined;
  if (query.flat_join) {
    const flat = query.flat_join;
    const unsupported = (
      [
        [flat.sources.length === 0, "zero joined sources"],
        [query.joins.length > 0, "existential joins"],
        [query.policy_branches.length > 0, "policy branches"],
        [query.reachable.length > 0, "reachability"],
        [query.inherits.length > 0, "inherited-policy traversals"],
        [query.includes.length > 0, "includes"],
        [query.array_subqueries.length > 0, "array subqueries"],
        [query.select !== undefined, "select projections"],
        [query.order_by.length > 0, "ordering"],
        [query.aggregate !== undefined, "aggregates"],
        [query.limit !== undefined, "limits"],
        [query.offset !== 0, "offsets"],
      ] as [boolean, string][]
    ).find(([present]) => present);
    if (unsupported)
      throw new QueryError({ kind: "UnsupportedFlatJoinCombination", feature: unsupported[1] });
    validateFlatJoin(schema, query.table, flat);
    const sources = flatJoinSourceTables(schema, query.table, flat);
    for (const p of resolvedQuery.filters) {
      qualifyFlatJoinPredicate(p, sources);
      validateFlatJoinFilterRouting(p);
    }
    flatFilterSchema = flatJoinFilterSchema(sources);
  }
  const params = prepareParameterDomain(
    resolvedQuery,
    schema,
    root,
    flatFilterSchema ?? root,
    initialParams,
  );
  for (const inherits of query.inherits) validateInherits(root, inherits);
  for (const branch of resolvedQuery.policy_branches)
    for (const i of branch.inherits) validateInherits(root, i);
  for (const include of query.includes) validateInclude(schema, root, include.path);
  for (const column of query.select ?? []) validateSelectColumn(root, column);
  rejectAuthorOrdering(query.order_by);
  if (query.aggregate) {
    validateAggregate(root, query.aggregate);
    validateAggregateOrderBy(query.table, query.aggregate, query.order_by);
  } else for (const o of query.order_by) plannerColumnType(root, o.column);
  const normalized = normalizeQuery(resolvedQuery);
  return [normalized, params, canonicalQueryBytesForSchema(normalized, schema)];
}

/** Whether a single (non-UNION) relation keeps its explicit output projection as the result shape. */
function retainRelationOutputProjection(
  query: Query,
  relation: RelationQuery,
  outputTable: TableSchema,
  role: ProjectionRole,
): boolean {
  if (relationOutputProjectionIfPresent(relation) === undefined) return false;
  const [outputScope, columns] = relationOutputProjection(relation);
  if (role === "Result" && isFullIdentityProjection(outputTable, outputScope, columns))
    return false;
  rejectPresentationOverRelationProjection(query);
  rejectReservedRelationAliases(columns);
  return true;
}

function isFullIdentityProjection(
  table: TableSchema,
  outputScope: string,
  columns: RelationProjectColumn[],
): boolean {
  const projected = new Set<string>();
  for (const c of columns) {
    const identity =
      c.expr.kind === "RowId"
        ? c.expr.row === "Current" && c.alias === "id"
        : c.expr.column.scope === outputScope && c.expr.column.column === c.alias;
    if (!identity) return false;
    if (c.alias !== "id") projected.add(c.alias);
  }
  return (
    projected.size === table.columns.length && table.columns.every((c) => projected.has(c.name))
  );
}

function rejectPresentationOverRelationProjection(query: Query): void {
  if (query.array_subqueries.length)
    throw relationError(
      "include(...) is not supported on a relation query whose projection renames or narrows its output columns",
    );
  if (query.select !== undefined)
    throw relationError(
      "select(...) is not supported on a relation query whose projection renames or narrows its output columns",
    );
}

function validateRetainedRelationOuterQuery(query: Query): void {
  if (hasOrdinaryClauses(query))
    throw relationError("relation query cannot be combined with ordinary query clauses");
}

/** Validate every UNION arm through the single-relation facade, merging inferred parameters. */
function validateRetainedRelationUnion(
  relation: RelationQuery,
  outputTable: string,
  schema: RuntimeSchema,
  params: Params,
): void {
  const parts = relationUnionParts(relation.rel);
  if (!parts)
    throw relationError(
      "retained relation query must be a union with global terminal operators in builder order",
    );
  if (!parts.inputs.length) throw relationError("union requires at least one input");
  const outputSchema = schemaTable(schema, outputTable);
  for (const term of parts.orderBy ?? []) {
    if (term.column.scope !== undefined && term.column.scope !== outputTable)
      throw relationError("union order_by must be scoped to the union output table");
    rejectAuthorOrdering([{ column: term.column.column, direction: term.direction }]);
    plannerColumnType(outputSchema, term.column.column);
  }
  const labels = new Set<string>();
  let outputProjection: [string, ColumnType][] | undefined;
  for (const arm of parts.inputs) {
    if (
      !arm.label ||
      new TextEncoder().encode(arm.label).length > 4096 ||
      arm.label.includes("\0") ||
      labels.has(arm.label)
    )
      throw relationError("union arm labels must be 1..=4096 bytes, NUL-free, and unique");
    labels.add(arm.label);
    const armRelation: RelationQuery = { rel: structuredClone(arm.input) };
    const [, armColumns] = relationOutputProjection(armRelation);
    const contract = relationProjectionContract(outputSchema, armColumns);
    if (outputProjection === undefined) outputProjection = contract;
    else if (
      outputProjection.length !== contract.length ||
      outputProjection.some(([a, t], i) => a !== contract[i][0] || !columnTypeEq(t, contract[i][1]))
    )
      throw relationError("union arms must use the same output projection");
    const armQuery = relationQueryToQuery(armRelation);
    if (armQuery.table !== outputTable)
      throw relationError("union inputs must output the same table");
    armQuery.relation = armRelation;
    const [, armParams] = canonicalPartsAs(armQuery, schema, "UnionArm");
    for (const [name, ty] of armParams) {
      const existing = params.get(name);
      if (existing === undefined) params.set(name, ty);
      else if (!columnTypeEq(existing, ty))
        throw new QueryError({ kind: "ParamTypeConflict", param: name });
    }
  }
}

function relationProjectionContract(
  table: TableSchema,
  columns: RelationProjectColumn[],
): [string, ColumnType][] {
  return columns.map((c) => {
    if (c.expr.kind === "Column") return [c.alias, plannerColumnType(table, c.expr.column.column)];
    if (c.expr.row === "Current") return [c.alias, UUID];
    throw relationError("outer/frontier row-id relation projections are not unified yet");
  });
}

function rejectAuthorOrdering(orderBy: OrderBy[]): void {
  const order = orderBy.find((o) => o.column === "$createdBy" || o.column === "$updatedBy");
  if (order) throw new QueryError({ kind: "UnsupportedAuthorOrdering", column: order.column });
}

// ---- flat joins ----

type Sources = SortedMap<string, TableSchema>;

function flatJoinSourceTables(schema: RuntimeSchema, rootTable: string, flat: FlatJoin): Sources {
  const sources: Sources = new SortedMap(cmpStr, [
    [flat.root_alias ?? rootTable, schemaTable(schema, rootTable)],
  ]);
  for (const s of flat.sources) {
    const name = s.alias ?? s.table;
    if (sources.set(name, schemaTable(schema, s.table)) !== undefined)
      throw unknownColumn("flat join duplicate source", name);
  }
  return sources;
}

function flatJoinFilterSchema(sources: Sources): TableSchema {
  const columns: { name: string; column_type: ColumnType }[] = [];
  for (const [scope, table] of sources) {
    // Both public identity spellings resolve to the physical row UUID.
    columns.push(newColumnSchema(`${scope}._id`, UUID), newColumnSchema(`${scope}.id`, UUID));
    for (const magic of ["$createdBy", "$updatedBy", "$createdAt", "$updatedAt"]) {
      const ty = executableMagicColumnType(magic);
      if (!ty) throw new Error("listed flat-join magic columns are executable");
      columns.push(newColumnSchema(`${scope}.${magic}`, ty));
    }
    for (const c of table.columns)
      columns.push(newColumnSchema(`${scope}.${c.name}`, c.column_type));
  }
  return newTableSchema("flat join", columns);
}

function qualifyFlatJoinPredicate(p: Predicate, sources: Sources): void {
  rewriteFlatJoinPredicateColumns(p, (c) => qualifyFlatJoinColumn(c, sources));
}

function rewriteFlatJoinPredicateColumns(p: Predicate, rewrite: (column: string) => string): void {
  const operand = (o: Operand) => {
    if (o.kind === "Column") o.name = rewrite(o.name);
  };
  switch (p.kind) {
    case "All":
    case "Any":
      return p.predicates.forEach((c) => rewriteFlatJoinPredicateColumns(c, rewrite));
    case "Not":
      return rewriteFlatJoinPredicateColumns(p.predicate, rewrite);
    case "In":
      operand(p.left);
      return p.values.forEach(operand);
    case "EnumMatch":
      p.column = rewrite(p.column);
      return;
    case "IsNull":
      return operand(p.operand);
    default:
      operand(p.left);
      operand(p.right);
  }
}

export function flatJoinPredicateSources(p: Predicate): SortedSet<string> {
  const sources = new SortedSet<string>(cmpStr);
  rewriteFlatJoinPredicateColumns(structuredClone(p), (c) => {
    sources.add(flatJoinQualifiedField(c)[0]);
    return c;
  });
  return sources;
}

export function unqualifyFlatJoinPredicate(p: Predicate, expectedScope: string): Predicate {
  const out = structuredClone(p);
  rewriteFlatJoinPredicateColumns(out, (c) => {
    const [scope, field] = flatJoinQualifiedField(c);
    if (scope !== expectedScope)
      throw new QueryError({
        kind: "UnsupportedFlatJoinCombination",
        feature: "filters spanning multiple sources",
      });
    return field;
  });
  return out;
}

export function qualifyFlatJoinSourcePredicate(p: Predicate, scope: string): Predicate {
  rewriteFlatJoinPredicateColumns(p, (c) => `${scope}.${c}`);
  return p;
}

function validateFlatJoinFilterRouting(p: Predicate): void {
  if (p.kind === "All") return p.predicates.forEach(validateFlatJoinFilterRouting);
  if (flatJoinPredicateSources(p).size > 1)
    throw new QueryError({
      kind: "UnsupportedFlatJoinCombination",
      feature: "filters spanning multiple sources",
    });
}

function qualifyFlatJoinColumn(column: string, sources: Sources): string {
  let qualified: [string, string] | undefined;
  try {
    qualified = flatJoinQualifiedField(column);
  } catch {}
  if (qualified) {
    const table = sources.get(qualified[0]);
    if (!table) throw unknownColumn("flat join source", qualified[0]);
    flatJoinColumnType(table, qualified[1]);
    return column;
  }
  const matches = [...sources].filter(([, t]) => {
    try {
      flatJoinColumnType(t, column);
      return true;
    } catch {
      return false;
    }
  });
  if (!matches.length) throw unknownColumn("flat join", column);
  if (matches.length > 1)
    throw unknownColumn("ambiguous flat join column; qualify its source", column);
  return `${matches[0][0]}.${column}`;
}

function flatJoinQualifiedField(field: string): [string, string] {
  const magic = field.indexOf(".$");
  if (magic >= 0) return [field.slice(0, magic), field.slice(magic + 1)];
  const dot = field.lastIndexOf(".");
  if (dot < 0) throw unknownColumn("flat join qualified source", field);
  return [field.slice(0, dot), field.slice(dot + 1)];
}

const flatJoinColumnType = (table: TableSchema, column: string): ColumnType =>
  column === "_id" ? UUID : plannerColumnType(table, column);

function validateFlatJoin(schema: RuntimeSchema, rootTable: string, flat: FlatJoin): void {
  const sources = new Map<string, string>([[flat.root_alias ?? rootTable, rootTable]]);
  for (const s of flat.sources) {
    const name = s.alias ?? s.table;
    if (sources.has(name)) throw unknownColumn("flat join duplicate source", name);
    schemaTable(schema, s.table);
    const [leftSource, leftColumn] = flatJoinQualifiedField(s.on.left);
    const [rightSource, rightColumn] = flatJoinQualifiedField(s.on.right);
    const leftTable = sources.get(leftSource);
    if (leftTable === undefined) throw unknownColumn("flat join accumulated source", leftSource);
    if (rightSource !== name) throw unknownColumn("flat join source", rightSource);
    if (
      !flatJoinKeyTypesCompatible(
        flatJoinColumnType(schemaTable(schema, leftTable), leftColumn),
        flatJoinColumnType(schemaTable(schema, s.table), rightColumn),
      )
    )
      throw mismatch();
    sources.set(name, s.table);
  }
}

// ---- joins, aggregates, includes, reachability ----

function validateJoin(
  schema: RuntimeSchema,
  root: TableSchema,
  rootTable: string,
  join: JoinVia,
  phase: Phase,
): void {
  const joinTable = schemaTable(schema, join.table);
  if (join.target === "Uncorrelated") {
    if (
      join.on_column ||
      join.source_column !== undefined ||
      join.source_lookup ||
      join.correlated_filters.length
    )
      throw notRefCompatible(join.table, join.on_column, "uncorrelated existence (no join keys)");
    return validateJoinConditions(schema, joinTable, join, phase);
  }
  if (join.target === "Column") plannerColumnType(joinTable, join.on_column);
  else if (join.on_column !== "id") throw unknownColumn(join.table, join.on_column);
  // Explicit non-reference column equalities are ordinary existence joins.
  const source = join.source_column;
  if (
    join.target === "Column" &&
    !join.source_lookup &&
    source !== undefined &&
    !root.references.has(source) &&
    !joinTable.references.has(join.on_column)
  ) {
    const sourceType = plannerColumnType(root, source);
    const targetType = plannerColumnType(joinTable, join.on_column);
    const refLike = (t: ColumnType) => ["Uuid", "Array"].includes(nonNull(t).kind);
    if (!refLike(sourceType) && !refLike(targetType)) {
      if (!columnTypesComparable(sourceType, targetType)) throw mismatch();
      for (const c of join.correlated_filters)
        if (
          !columnTypesComparable(
            plannerColumnType(root, c.source_column),
            plannerColumnType(joinTable, c.join_column),
          )
        )
          throw mismatch();
      return validateJoinConditions(schema, joinTable, join, phase);
    }
  }
  let targetTable: string;
  const lookup = join.source_lookup;
  if (lookup) {
    plannerColumnType(root, lookup.row_id_source_column);
    const lookupTable = schemaTable(schema, lookup.table);
    if (root.references.get(lookup.row_id_source_column) !== lookup.table)
      throw notRefCompatible(rootTable, lookup.row_id_source_column, lookup.table);
    plannerColumnType(lookupTable, lookup.value_column);
    if (join.source_column !== lookup.value_column)
      throw notRefCompatible(lookup.table, lookup.value_column, "join source column");
    if (lookup.value_column === "id") targetTable = lookup.table;
    else {
      const t = lookupTable.references.get(lookup.value_column);
      if (t === undefined)
        throw notRefCompatible(lookup.table, lookup.value_column, "referenced table");
      targetTable = t;
    }
  } else if (source !== undefined) {
    if (source === "id") targetTable = rootTable;
    else {
      plannerColumnType(root, source);
      const t = root.references.get(source);
      if (t === undefined) throw notRefCompatible(rootTable, source, "referenced table");
      targetTable = t;
    }
  } else targetTable = rootTable;
  // A correlation is an equality: nullability doesn't change comparability.
  for (const c of join.correlated_filters)
    if (
      !columnTypesComparable(
        plannerColumnType(root, c.source_column),
        plannerColumnType(joinTable, c.join_column),
      )
    )
      throw mismatch();
  if (join.target === "Column") {
    const ref = joinTable.references.get(join.on_column);
    const ok =
      ref === undefined
        ? join.on_column === "id" && join.table === targetTable
        : ref === targetTable;
    if (!ok) throw notRefCompatible(join.table, join.on_column, targetTable);
  } else if (join.table !== targetTable)
    throw notRefCompatible(join.table, join.on_column, targetTable);
  validateJoinConditions(schema, joinTable, join, phase);
}

function validateAggregate(table: TableSchema, aggregate: AggregateQuery): void {
  if (aggregate.group_by !== undefined) plannerColumnType(table, aggregate.group_by);
  for (const a of aggregate.aggregates) {
    if (a.alias.startsWith("__jazz_aggregate_"))
      throw new QueryError({ kind: "ReservedAggregateAlias", detail: a.alias });
    if (a.function === "Count") {
      if (a.column !== undefined) columnType(table, a.column);
      continue;
    }
    if (a.column === undefined) throw mismatch();
    const ty = columnType(table, a.column);
    if (a.function === "Sum" || a.function === "Avg" ? !isNumeric(ty) : !isOrderable(ty))
      throw mismatch();
  }
  // Keep all existing aggregate errors ahead of output-name collisions.
  const names = new Set<string>();
  if (aggregate.group_by !== undefined) names.add(aggregate.group_by);
  for (const a of aggregate.aggregates) {
    if (names.has(a.alias))
      throw new QueryError({ kind: "AggregateOutputNameCollision", detail: a.alias });
    names.add(a.alias);
  }
}

function validateAggregateOrderBy(
  table: string,
  aggregate: AggregateQuery,
  orderBy: OrderBy[],
): void {
  for (const o of orderBy)
    if (aggregate.group_by !== o.column && !aggregate.aggregates.some((a) => a.alias === o.column))
      throw unknownColumn(`${table}_aggregate`, o.column);
}

function validateSelectColumn(table: TableSchema, column: string): void {
  if (column === "id" || executableMagicColumnType(column)) return;
  if (column.startsWith("$")) throw unknownColumn(table.name, column);
  columnType(table, column);
}

export function schemaTable(schema: RuntimeSchema, name: string): TableSchema {
  const table = schema.tables.find((t: TableSchema) => t.name === name);
  if (!table) throw new QueryError({ kind: "UnknownTable", detail: name });
  return table;
}

function columnType(table: TableSchema, column: string): ColumnType {
  const c = table.columns.find((c) => c.name === column);
  if (!c) throw unknownColumn(table.name, column);
  return c.column_type;
}

export function plannerColumnType(table: TableSchema, column: string): ColumnType {
  if (column === "id") return UUID;
  return executableMagicColumnType(column) ?? columnType(table, column);
}

function executableMagicColumnType(column: string): ColumnType | undefined {
  const author = authorSubjectMetadataType(column);
  if (author) return author;
  if (column === "$canRead") throw new QueryError({ kind: "UnsupportedMagicColumn", column });
  return column === "$createdAt" || column === "$updatedAt" ? CT("U64") : undefined;
}

function validateInclude(schema: RuntimeSchema, root: TableSchema, path: string): void {
  let current = root;
  for (const segment of path.split(".")) {
    columnType(current, segment);
    const target = current.references.get(segment);
    if (target === undefined) throw new QueryError({ kind: "BadIncludePath", path });
    current = schemaTable(schema, target);
  }
}

function checkUniqueNames(subqueries: ArraySubquery[]): void {
  const names = new Set<string>();
  for (const s of subqueries) {
    if (!s.column_name || names.has(s.column_name))
      throw new QueryError({ kind: "BadIncludePath", path: s.column_name });
    names.add(s.column_name);
  }
}

function validateArraySubqueries(
  schema: RuntimeSchema,
  parent: TableSchema,
  subqueries: ArraySubquery[],
  phase: Phase,
): void {
  checkUniqueNames(subqueries);
  for (const s of subqueries) validateArraySubquery(schema, parent, s, phase);
}

function validateArraySubquery(
  schema: RuntimeSchema,
  parent: TableSchema,
  s: ArraySubquery,
  phase: Phase,
): void {
  const child = schemaTable(schema, s.table);
  if (
    !arrayCorrelationTypesCompatible(
      plannerColumnType(parent, s.outer_column),
      plannerColumnType(child, s.inner_column),
    )
  )
    throw mismatch();
  validatePredicates(child, s.filters, phase);
  for (const column of s.select ?? []) validateSelectColumn(child, column);
  rejectAuthorOrdering(s.order_by);
  for (const o of s.order_by) plannerColumnType(child, o.column);
  checkUniqueNames(s.nested_arrays);
  for (const nested of s.nested_arrays) validateArraySubquery(schema, child, nested, phase);
}

function validateReachable(
  schema: RuntimeSchema,
  root: TableSchema,
  r: ReachableVia,
  phase: Phase,
): void {
  const access = schemaTable(schema, r.access_table);
  plannerColumnType(access, r.access_row_column);
  plannerColumnType(access, r.access_team_column);
  if (r.access_row_column === "id") {
    if (access.name !== root.name)
      throw notRefCompatible(r.access_table, r.access_row_column, root.name);
  } else if (access.name === root.name) {
    if (!columnTypesComparable(plannerColumnType(access, r.access_row_column), UUID))
      throw notRefCompatible(r.access_table, r.access_row_column, root.name);
  } else {
    if (access.references.get(r.access_row_column) !== root.name)
      throw notRefCompatible(r.access_table, r.access_row_column, root.name);
    if (!columnTypesComparable(plannerColumnType(access, r.access_row_column), UUID))
      throw mismatch();
  }
  let teamTable: string;
  if (r.access_team_target === "Uncorrelated")
    throw notRefCompatible(
      r.access_table,
      r.access_team_column,
      "reachability requires a team reference",
    );
  if (r.access_team_target === "Column") {
    const t = access.references.get(r.access_team_column);
    if (t === undefined)
      throw notRefCompatible(r.access_table, r.access_team_column, "referenced table");
    teamTable = t;
  } else {
    if (r.access_team_column !== "id")
      throw notRefCompatible(r.access_table, r.access_team_column, r.access_table);
    teamTable = access.name;
  }
  const edge = schemaTable(schema, r.edge_table);
  for (const column of [r.edge_member_column, r.edge_parent_column]) {
    plannerColumnType(edge, column);
    if (column === "id" && edge.name === teamTable) continue;
    if (edge.references.get(column) !== teamTable)
      throw notRefCompatible(r.edge_table, column, teamTable);
  }
  if (r.seed) {
    const seed = r.seed;
    const seedTable = schemaTable(schema, seed.table);
    if (!columnTypeEq(plannerColumnType(seedTable, seed.team_column), UUID)) throw mismatch();
    if (seed.user_column !== undefined) plannerColumnType(seedTable, seed.user_column);
    const projectsTeam =
      seed.team_column === "id"
        ? seedTable.name === teamTable
        : seedTable.references.get(seed.team_column) === teamTable;
    if (!projectsTeam) throw notRefCompatible(seed.table, seed.team_column, teamTable);
    validatePredicates(seedTable, seed.filters, phase);
  } else if (phase.kind === "Prepare") {
    if (r.from.kind === "Param")
      phase.domain.require(phase.domain.parameter(r.from.name), phase.domain.known(UUID).fact);
  } else {
    const ty = operandType(root, r.from, phase.params);
    if (ty === undefined) checkInferredParam(r.from, UUID, phase.params);
    else if (ty.kind !== "Uuid") throw mismatch();
  }
  validatePredicates(access, r.access_filters, phase);
  validatePredicates(edge, r.edge_filters, phase);
}

function validateInherits(root: TableSchema, inherits: InheritsVia): void {
  plannerColumnType(root, inherits.parent_column);
  if (!root.references.has(inherits.parent_column))
    throw notRefCompatible(root.name, inherits.parent_column, "referenced table");
}

// ---- predicates ----

function validatePredicates(table: TableSchema, predicates: Predicate[], phase: Phase): void {
  for (let i = 0; i < predicates.length; i++)
    predicates[i] = validatePredicate(table, predicates[i], phase);
}

function validateJoinConditions(
  schema: RuntimeSchema,
  joinTable: TableSchema,
  join: JoinVia,
  phase: Phase,
): void {
  validatePredicates(joinTable, join.filters, phase);
  for (const nested of join.nested_joins)
    validateJoin(schema, joinTable, join.table, nested, phase);
}

const ALWAYS_FALSE = (): Predicate => ({ kind: "Any", predicates: [] });

/** Returns the (possibly rewritten) predicate: an empty `In` becomes constant false. */
function validatePredicate(table: TableSchema, p: Predicate, phase: Phase): Predicate {
  switch (p.kind) {
    case "All":
    case "Any":
      validatePredicates(table, p.predicates, phase);
      return p;
    case "Not":
      p.predicate = validatePredicate(table, p.predicate, phase);
      return p;
    case "EnumMatch": {
      const ty = nonNull(plannerColumnType(table, p.column));
      if (ty.kind !== "Enum") throw mismatch();
      let enumCase: any;
      try {
        enumCase = ty.schema.case(ty.schema.tag(p.case));
      } catch {
        throw mismatch();
      }
      const payloadTable = newTableSchema(
        "__enum_payload",
        enumCase.payload.fields().map((f: any) => newColumnSchema(f.name ?? "", f.value_type)),
      );
      p.payload = validatePredicate(payloadTable, p.payload, phase);
      return p;
    }
  }
  if (phase.kind === "Prepare") {
    const { domain } = phase;
    switch (p.kind) {
      case "In":
        if (!p.values.length) {
          operandType(table, p.left, newParams());
          return ALWAYS_FALSE();
        }
        for (const v of p.values) domain.relationship(table, p.left, v, "In");
        return p;
      case "Contains":
        domain.relationship(table, p.left, p.right, "Contains");
        return p;
      case "IsNull":
        if (p.operand.kind === "Param") domain.parameter(p.operand.name);
        return p;
      default:
        domain.relationship(table, p.left, p.right, "Compare");
        return p;
    }
  }
  const { params } = phase;
  switch (p.kind) {
    case "Eq":
    case "Ne":
      validateComparableOperands(table, p.left, p.right, params);
      return p;
    case "In": {
      const leftType = operandType(table, p.left, params);
      if (!p.values.length) return ALWAYS_FALSE();
      for (const v of p.values) {
        let valueTy = operandType(table, v, params);
        if (
          leftType &&
          valueTy &&
          !inOperandTypesCompatible(leftType, valueTy) &&
          p.left.kind !== "Literal" &&
          coerceIntegerLiteralForType(v, leftType)
        )
          valueTy = operandType(table, v, params);
        if (leftType && valueTy) {
          if (!inOperandTypesCompatible(leftType, valueTy) && !inLiteralValueCoercible(leftType, v))
            throw inCandidateTypeMismatch(p.left, leftType, valueTy);
        } else if (leftType) checkInferredParam(v, leftType, params);
        else if (valueTy) checkInferredParam(p.left, valueTy, params);
        else throw mismatch();
      }
      return p;
    }
    case "Gt":
    case "Gte":
    case "Lt":
    case "Lte":
      if (!isOrderable(validateComparableOperands(table, p.left, p.right, params)))
        throw mismatch();
      return p;
    case "Contains": {
      const leftType = operandType(table, p.left, params);
      const rightType = operandType(table, p.right, params);
      if (!leftType) {
        if (!rightType) throw mismatch();
        checkInferredParam(p.left, { kind: "Array", inner: rightType }, params);
        return p;
      }
      const haystack = nonNull(leftType);
      const expected =
        haystack.kind === "String"
          ? CT("String")
          : haystack.kind === "Array"
            ? haystack.inner
            : undefined;
      if (!expected) throw mismatch();
      if (!rightType) checkInferredParam(p.right, expected, params);
      else if (!columnTypeEq(rightType, expected))
        throw new QueryError({ kind: "ContainsNeedleTypeMismatch", expected, actual: rightType });
      return p;
    }
    case "IsNull": {
      // Claims and literals are dynamically typed; either can be checked for nullness.
      if (p.operand.kind === "Claim" || p.operand.kind === "Literal") return p;
      if (operandType(table, p.operand, params)?.kind !== "Nullable") throw mismatch();
      return p;
    }
  }
}

function validateComparableOperands(
  table: TableSchema,
  left: Operand,
  right: Operand,
  params: Params,
): ColumnType {
  // An empty array literal has no element type: permit it only against a schema-resolved array column.
  const isEmptyArray = (o: Operand) =>
    o.kind === "Literal" && o.value.kind === "Array" && o.value.value.length === 0;
  const emptyArrayColumn =
    isEmptyArray(left) && right.kind === "Column"
      ? right.name
      : isEmptyArray(right) && left.kind === "Column"
        ? left.name
        : undefined;
  if (emptyArrayColumn !== undefined) {
    const ty = plannerColumnType(table, emptyArrayColumn);
    if (nonNull(ty).kind === "Array") return ty;
  }
  let leftType = operandType(table, left, params);
  let rightType = operandType(table, right, params);
  if (leftType && rightType && !columnTypesComparable(leftType, rightType)) {
    const coerced =
      left.kind === "Literal" && right.kind !== "Literal"
        ? coerceIntegerLiteralForType(left, rightType)
        : right.kind === "Literal" && left.kind !== "Literal"
          ? coerceIntegerLiteralForType(right, leftType)
          : false;
    if (coerced) {
      leftType = operandType(table, left, params);
      rightType = operandType(table, right, params);
    }
  }
  if (leftType && rightType) {
    if (!columnTypesComparable(leftType, rightType))
      throw (
        claimTypeMismatchError(left, leftType, right, rightType) ??
        claimTypeMismatchError(right, rightType, left, leftType) ??
        mismatch()
      );
    return leftType;
  }
  if (leftType) {
    checkInferredParam(right, leftType, params);
    return leftType;
  }
  if (rightType) {
    checkInferredParam(left, rightType, params);
    return rightType;
  }
  throw mismatch();
}

const NUMERIC = ["U8", "U16", "U32", "U64", "I32", "I64", "F64"];
const isOrderable = (t: ColumnType) => [...NUMERIC, "Uuid", "String"].includes(nonNull(t).kind);
const isNumeric = (t: ColumnType) => NUMERIC.includes(nonNull(t).kind);

function columnTypesComparable(left: ColumnType, right: ColumnType): boolean {
  const l = nonNull(left);
  const r = nonNull(right);
  return (
    columnTypeEq(l, r) ||
    (l.kind === "EnumTag" && r.kind === "U8") ||
    (l.kind === "U8" && r.kind === "EnumTag")
  );
}

const flatJoinKeyTypesCompatible = (l: ColumnType, r: ColumnType) =>
  columnTypesComparable(l, r) ||
  arrayElementTypeCompatible(l, r) ||
  arrayElementTypeCompatible(r, l);

function arrayElementTypeCompatible(array: ColumnType, scalar: ColumnType): boolean {
  const a = nonNull(array);
  return (
    a.kind === "Array" && nonNull(scalar).kind !== "Array" && columnTypesComparable(a.inner, scalar)
  );
}

function inOperandTypesCompatible(left: ColumnType, right: ColumnType): boolean {
  if (columnTypesComparable(left, right)) return true;
  return nonNull(left).kind === "EnumTag" && ["String", "Uuid"].includes(nonNull(right).kind);
}

function arrayCorrelationTypesCompatible(parent: ColumnType, child: ColumnType): boolean {
  if (inOperandTypesCompatible(parent, child)) return true;
  // Correlation expands array lookup keys on either side (forward and reverse references).
  const p = nonNull(parent);
  return (
    (p.kind === "Array" && columnTypesComparable(p.inner, child)) ||
    arrayElementTypeCompatible(child, parent)
  );
}

function inLiteralValueCoercible(left: ColumnType, o: Operand): boolean {
  if (o.kind !== "Literal") return false;
  const t = nonNull(left);
  const v = o.value;
  if (t.kind === "String") return v.kind === "Uuid";
  if (t.kind === "EnumTag") return v.kind === "String" || v.kind === "Uuid";
  if (t.kind === "Array")
    return (
      v.kind === "Array" &&
      v.value.every((x: Value) => inLiteralValueCoercible(t.inner, { kind: "Literal", value: x }))
    );
  return false;
}

function inCandidateTypeMismatch(
  left: Operand,
  column_type: ColumnType,
  candidate_type: ColumnType,
): QueryError {
  return left.kind === "Column"
    ? new QueryError({
        kind: "InCandidateTypeMismatch",
        column: left.name,
        column_type,
        candidate_type,
      })
    : mismatch();
}

/** Widen/narrow an integer literal to its counterpart's width; true when rewritten. */
function coerceIntegerLiteralForType(o: Operand, target: ColumnType): boolean {
  if (o.kind !== "Literal") return false;
  const t = nonNull(target).kind;
  const v = o.value;
  if (v.kind === "I32" && t === "I64") o.value = { kind: "I64", value: v.value };
  else if (
    v.kind === "I64" &&
    t === "I32" &&
    Number(v.value) >= -(2 ** 31) &&
    Number(v.value) < 2 ** 31
  )
    o.value = { kind: "I32", value: Number(v.value) };
  else return false;
  return true;
}

function operandType(table: TableSchema, o: Operand, params: Params): ColumnType | undefined {
  switch (o.kind) {
    case "Column":
      return plannerColumnType(table, o.name);
    case "Literal":
      return valueType(o.value);
    case "Param":
      return params.get(o.name);
    case "Claim":
      return claimType(o.name);
  }
}

function claimType(name: string): ColumnType | undefined {
  if (name.startsWith("user")) {
    const ty = authorSubjectMetadataType(`$createdBy${name.slice(4)}`);
    if (ty) return ty;
  }
  return name === "team" ? UUID : name === "isAdmin" ? CT("Bool") : undefined;
}

function claimTypeMismatchError(
  claim: Operand,
  claimTy: ColumnType,
  other: Operand,
  otherTy: ColumnType,
) {
  if (claim.kind !== "Claim" || other.kind !== "Column") return undefined;
  return new QueryError({
    kind: "ClaimTypeMismatch",
    claim_path: claim.name,
    column: other.name,
    claim_type: columnTypeDebug(claimTy),
    column_type: columnTypeDebug(otherTy),
  });
}

function checkInferredParam(o: Operand, expected: ColumnType, params: Params): void {
  if (o.kind !== "Param") return;
  const existing = params.get(o.name);
  if (existing === undefined) throw mismatch();
  if (!columnTypeEq(existing, expected))
    throw new QueryError({ kind: "ParamTypeConflict", param: o.name });
}
