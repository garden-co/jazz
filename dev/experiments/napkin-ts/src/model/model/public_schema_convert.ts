// Canonical conversion between the public schema and Jazz's internal schema.
// Split across: this file (tables, columns, defaults, policy clause assembly),
// public_schema_convert_rel.ts (ExistsRel lowering) and
// public_schema_convert_predicate.ts (predicates, inherits, literals).
import { SortedMap, SortedSet, cmpStr } from "../../support/collections.js";
import {
  RecordDescriptor,
  EnumSchema,
  EnumCase,
  EnumValue,
  ScalarEnumSchema,
  queryFrom,
  qFilter,
  qPolicyBranch,
  qJoinVia,
  policyAlternativesFromQuery,
  validateQueryRuntime,
  authorSubjectMetadataType,
  parseUuid,
  uuidString,
  objectIdUuidBytes,
  valueTypeEquals,
  type GValue,
  type GValueType,
  type Query,
  type Predicate,
  type Operand,
  type PolicyBranch,
  type InheritsOperation,
} from "../../_extern/model-core.js";
import {
  JazzSchema,
  RuntimeSchema,
  newColumnSchema,
  newTableSchema,
  newCompositeIndexSet,
  writePolicyEntries,
  type ColumnSchema as CoreColumnSchema,
  type TableSchema as CoreTableSchema,
  type MergeStrategy,
} from "../schema.js";
import { validateJsonSchemas, validateJsonValue } from "./public_schema.js";
import { validateMergeStrategy } from "./public_api/types/schema.js";
import type {
  ColumnDescriptor,
  ColumnType,
  Schema,
  TableName,
  TableSchema,
} from "./public_api/types/schema.js";
import type { Value } from "./public_api/types/value.js";
import { debugValue } from "./public_api/types/value.js";
import type { Operation, PolicyExpr } from "./public_api/policy.js";
import {
  appendExistsRelPolicyClause,
  validateExistsRelPolicyJoinConditions,
  uncorrelatedExistsJoin,
} from "./public_schema_convert_rel.js";
import {
  convertPolicyPredicate,
  appendInheritedPolicy,
} from "./public_schema_convert_predicate.js";

export const DIRECT_USER_CLAIM = "user";
export const PUBLIC_USER_SESSION_PATHS = ["user"];
export const DIRECT_AUTH_MODE_CLAIM = "authMode";
export const PUBLIC_AUTH_MODE_SESSION_PATHS = ["authMode", "auth_mode"];
const RESERVED_AGGREGATE_OUTPUT_PREFIX = "__jazz_aggregate_";

/** Error produced while compiling a developer-authored schema for the runtime. */
export class SchemaConversionError extends Error {
  constructor(
    readonly path: string,
    readonly detail: string,
  ) {
    super(`${path}: ${detail}`);
  }
}
export const err = (path: string, message: string) => new SchemaConversionError(path, message);
export const columnErr = (table: TableName, column: string, message: string) =>
  err(`$.${table}.${column}`, message);
export const policyPath = (table: TableName, path: string) => `$.${table}.${path}`;

const nullable = (inner: GValueType): GValueType => ({ kind: "Nullable", inner });
const arrayOf = (inner: GValueType): GValueType => ({ kind: "Array", inner });

export function convertPublicSchema(schema: Schema): JazzSchema {
  const converted = [...schema].map(([name, table]) => convertTable(schema, name, table));
  coerceTypedLiterals(schema, converted);
  validateConvertedSchema(converted);
  return JazzSchema.fromRuntime(schema, new RuntimeSchema(converted));
}

/** Decode and compile the public JSON schema used by native bindings. */
export function decodePublicSchemaJson(json: string, decode: (json: any) => Schema): JazzSchema {
  let schema: Schema;
  try {
    schema = decode(JSON.parse(json));
  } catch (e) {
    throw new Error(`decode public schema: ${(e as Error).message}`);
  }
  try {
    return convertPublicSchema(schema);
  } catch (e) {
    throw new Error(`compile public schema: ${(e as Error).message}`);
  }
}

const BRANCH_TYPES = new Set([
  "String",
  "Uuid",
  "U8",
  "U16",
  "U32",
  "U64",
  "I32",
  "I64",
  "EnumTag",
]);

function validateConvertedSchema(tables: CoreTableSchema[]): void {
  const schema = new RuntimeSchema(tables.slice());
  validateConvertedReferences(tables);
  const branchTypes = new Map<string, GValueType>();
  for (const table of tables) {
    const bound = new Set<string>();
    const p = `$.${table.name}.branchBy`;
    for (const bc of table.branch_by) {
      const column = table.columns.find((c) => c.name === bc);
      if (!column) throw err(p, `unknown branch column ${JSON.stringify(bc)}`);
      if (bound.has(bc))
        throw err(p, `branch column ${JSON.stringify(bc)} is listed more than once`);
      bound.add(bc);
      if (column.column_type.kind === "Nullable")
        throw err(p, `branch column ${JSON.stringify(bc)} must be non-null`);
      if (!BRANCH_TYPES.has(column.column_type.kind))
        throw err(
          p,
          "branch columns require string, UUID, stable enum, or fixed-width integer values",
        );
      const existing = branchTypes.get(bc);
      branchTypes.set(bc, column.column_type);
      if (existing && !valueTypeEquals(existing, column.column_type))
        throw err(p, `branch column ${JSON.stringify(bc)} must have the same type in every table`);
    }
  }
  for (const table of tables) {
    if (table.read_policy)
      try {
        validateQueryRuntime(table.read_policy, schema);
      } catch (e) {
        throw err(
          `$.${table.name}.policies.select.using`,
          `converted read policy is invalid: ${String(e)}`,
        );
      }
    for (const [label, policy] of writePolicyEntries(table.write_policies))
      try {
        validateQueryRuntime(policy, schema);
      } catch (e) {
        throw err(
          `$.${table.name}.policies.${label}`,
          `converted write policy is invalid: ${String(e)}`,
        );
      }
  }
}

function validateConvertedReferences(tables: CoreTableSchema[]): void {
  const names = new Set(tables.map((t) => t.name));
  for (const table of tables)
    for (const [columnName, target] of table.references) {
      const path = `$.${table.name}.${columnName}`;
      if (!names.has(target)) throw err(path, `reference target table '${target}' does not exist`);
      const column = table.columns.find((c) => c.name === columnName);
      if (!column) throw err(path, "reference source column is not declared");
      if (!isUuidReferenceType(column.column_type))
        throw err(path, "reference source column must use UUID or UUID[] type");
    }
}

function isUuidReferenceType(t: GValueType): boolean {
  if (t.kind === "Nullable") return isUuidReferenceType(t.inner);
  if (t.kind === "Uuid") return true;
  return t.kind === "Array" && t.inner.kind === "Uuid";
}

// ---- typed-literal coercion ----

type TypedLiteralTarget = { kind: "Core"; type: GValueType } | { kind: "PublicEnum" };
type ColumnTypes = Map<string, Map<string, TypedLiteralTarget>>;

function coerceTypedLiterals(schema: Schema, tables: CoreTableSchema[]): void {
  const columnTypes: ColumnTypes = new Map(
    tables.map((t) => [
      t.name,
      new Map(
        t.columns.map((c): [string, TypedLiteralTarget] => {
          const pub = schema.get(t.name)?.columns.columns.find((pc) => pc.name === c.name);
          return [
            c.name,
            pub?.column_type.type === "Enum"
              ? { kind: "PublicEnum" }
              : { kind: "Core", type: c.column_type },
          ];
        }),
      ),
    ]),
  );
  for (const t of tables) {
    if (t.read_policy) coerceQuery(t.read_policy, columnTypes);
    for (const [, q] of writePolicyEntries(t.write_policies)) coerceQuery(q, columnTypes);
  }
}

function coerceQuery(q: Query, ct: ColumnTypes): void {
  coercePredicates(q.table, q.filters, ct);
  for (const j of q.joins) coerceJoin(j, ct);
  coerceReachable(q.reachable, ct);
  for (const b of q.policy_branches) {
    coercePredicates(q.table, b.filters, ct);
    for (const j of b.joins) coerceJoin(j, ct);
    coerceReachable(b.reachable, ct);
  }
}

function coerceReachable(reachable: any[], ct: ColumnTypes): void {
  for (const r of reachable) {
    coercePredicates(r.access_table, r.access_filters, ct);
    coercePredicates(r.edge_table, r.edge_filters, ct);
    if (r.seed) coercePredicates(r.seed.table, r.seed.filters, ct);
  }
}

function coerceJoin(j: any, ct: ColumnTypes): void {
  coercePredicates(j.table, j.filters, ct);
  for (const n of j.nested_joins) coerceJoin(n, ct);
}

function coercePredicates(table: string, ps: Predicate[], ct: ColumnTypes): void {
  for (let i = 0; i < ps.length; i++) ps[i] = coercePredicate(table, ps[i], ct);
}

function coercePredicate(table: string, p: Predicate, ct: ColumnTypes): Predicate {
  switch (p.kind) {
    case "All":
    case "Any":
      coercePredicates(table, p.predicates, ct);
      return p;
    case "Not":
      p.predicate = coercePredicate(table, p.predicate, ct);
      return p;
    case "In":
      p.values = p.values.filter((_: Operand, i: number) => {
        const box = { o: p.values[i] };
        const drop = coercePair(table, p.left, box, ct);
        p.values[i] = box.o;
        return !drop;
      });
      return p;
    case "IsNull":
      return p;
    case "EnumMatch":
      p.payload = coercePredicate(table, p.payload, ct);
      return p;
    default: {
      const l = { o: p.left };
      const r = { o: p.right };
      const never = coercePair(table, p.left, r, ct) || coercePair(table, r.o, l, ct);
      p.left = l.o;
      p.right = r.o;
      return never ? { kind: "Any", predicates: [] } : p;
    }
  }
}

/** Returns true when the comparison can never match (string literal vs enum column). */
function coercePair(
  table: string,
  columnOperand: Operand,
  literal: { o: Operand },
  ct: ColumnTypes,
): boolean {
  if (columnOperand.kind !== "Column") return false;
  const column: string = columnOperand.name;
  const target = columnTypeForOperand(table, column, ct);
  if (target && literal.o.kind === "Literal")
    literal.o = { kind: "Literal", value: coerceLiteralForTarget(literal.o.value, target) };
  const schema = columnEnumSchema(table, column, ct);
  if (schema && literal.o.kind === "Literal" && literal.o.value.kind === "String") {
    let d: number | undefined;
    try {
      d = schema.discriminant(literal.o.value.value);
    } catch {
      d = undefined;
    }
    if (d !== undefined) {
      literal.o = { kind: "Literal", value: { kind: "EnumTag", value: d } };
      return false;
    }
  }
  if (schema) return true;
  const t = ct.get(table)?.get(column);
  if (!(t?.kind === "Core" && isStringType(t.type))) return false;
  if (literal.o.kind === "Literal" && literal.o.value.kind === "Uuid")
    literal.o = {
      kind: "Literal",
      value: { kind: "String", value: uuidString(literal.o.value.value) },
    };
  return false;
}

function columnTypeForOperand(
  table: string,
  column: string,
  ct: ColumnTypes,
): TypedLiteralTarget | undefined {
  const meta = authorSubjectMetadataType(column);
  if (meta) return { kind: "Core", type: meta };
  if (column === "id") return { kind: "Core", type: { kind: "Uuid" } };
  if (column === "$createdAt" || column === "$updatedAt")
    return { kind: "Core", type: { kind: "U64" } };
  return ct.get(table)?.get(column);
}

function coerceLiteralForTarget(v: GValue, target: TypedLiteralTarget): GValue {
  if (target.kind === "Core") return coerceLiteralForColumnType(v, target.type);
  if (v.kind === "String") {
    const u = parseUuid(v.value);
    return u !== undefined ? { kind: "Uuid", value: u } : v;
  }
  return v;
}

export function coerceLiteralForColumnType(v: GValue, t: GValueType): GValue {
  if (v.kind === "Uuid" && t.kind === "String")
    return { kind: "String", value: uuidString(v.value) };
  if (v.kind === "String" && t.kind === "Uuid") {
    const u = parseUuid(v.value);
    return u !== undefined ? { kind: "Uuid", value: u } : v;
  }
  if (v.kind === "U64" && t.kind === "I64")
    return BigInt(v.value) <= 2n ** 63n - 1n ? { kind: "I64", value: BigInt(v.value) } : v;
  if (v.kind === "U64" && t.kind === "I32")
    return BigInt(v.value) <= 2147483647n ? { kind: "I32", value: Number(v.value) } : v;
  if (v.kind === "Nullable" && v.value !== undefined && t.kind === "Nullable")
    return { kind: "Nullable", value: coerceLiteralForColumnType(v.value, t.inner) };
  if (v.kind === "Array" && t.kind === "Array")
    return {
      kind: "Array",
      value: v.value.map((e: GValue) => coerceLiteralForColumnType(e, t.inner)),
    };
  if (v.kind === "Tuple" && t.kind === "Tuple" && v.value.length === t.members.length)
    return {
      kind: "Tuple",
      value: v.value.map((e: GValue, i: number) => coerceLiteralForColumnType(e, t.members[i])),
    };
  if (v.kind === "Nullable" && v.value !== undefined)
    return { kind: "Nullable", value: coerceLiteralForColumnType(v.value, t) };
  if (t.kind === "Nullable") return coerceLiteralForColumnType(v, t.inner);
  return v;
}

function columnEnumSchema(table: string, column: string, ct: ColumnTypes): any {
  const t = ct.get(table)?.get(column);
  return t?.kind === "Core" ? enumSchemaOf(t.type) : undefined;
}
function enumSchemaOf(t: GValueType): any {
  if (t.kind === "EnumTag") return t.schema;
  if (t.kind === "Nullable") return enumSchemaOf(t.inner);
  return undefined;
}
const isStringType = (t: GValueType): boolean =>
  t.kind === "String" || (t.kind === "Nullable" && isStringType(t.inner));

// ---- tables and columns ----

function convertTable(schema: Schema, name: TableName, table: TableSchema): CoreTableSchema {
  const references = new SortedMap<string, string>(cmpStr);
  const columns: CoreColumnSchema[] = [];
  const mergeStrategies = new SortedMap<string, MergeStrategy>(cmpStr);
  const columnNames = new Set<string>();
  for (const column of table.columns.columns) {
    const cpath = `$.${name}.columns.${column.name}`;
    if (column.name === "id")
      throw err(
        `$.${name}.columns.id`,
        'column name "id" is reserved for the automatically generated UUID row ID',
      );
    if (column.name.startsWith(RESERVED_AGGREGATE_OUTPUT_PREFIX))
      throw err(cpath, "column name uses the reserved aggregate-output namespace");
    if (column.name.startsWith("$"))
      throw err(cpath, 'column names starting with "$" are reserved for magic columns');
    const converted = convertColumn(name, column);
    const strategy = column.merge_strategy ? convertMergeStrategy(name, column) : undefined;
    if (columnNames.has(column.name)) throw err(cpath, "duplicate column name");
    columnNames.add(column.name);
    if (column.references !== undefined) references.set(column.name, column.references);
    if (strategy) mergeStrategies.set(column.name, strategy);
    columns.push(converted);
  }
  const out = newTableSchema(name, columns);
  out.branch_by = table.branch_by.slice();
  out.references = references;
  out.indexed_columns = new SortedSet(cmpStr, table.indexed_columns ?? []);
  out.composite_indexes = newCompositeIndexSet();
  table.composite_indexes.forEach((group, index) => {
    const ipath = `$.${name}.composite_indexes.${index}`;
    if (group.length < 2) throw err(ipath, "a composite index needs at least two columns");
    const seen = new Set<string>();
    for (const c of group) {
      if (!columnNames.has(c) || seen.has(c))
        throw err(ipath, "composite index columns must be distinct declared columns");
      seen.add(c);
      const t = table.columns.columns.find((d) => d.name === c)?.column_type.type;
      if (
        t === "Bytea" ||
        t === "Array" ||
        t === "Row" ||
        t === "EnumPayload" ||
        t === "CatalogueEnumPayload"
      )
        throw err(ipath, "composite index columns must have ordered scalar values");
    }
    if (!out.composite_indexes.add(group.slice())) throw err(ipath, "duplicate composite index");
  });
  out.merge_strategies = mergeStrategies;
  const conv = (path: string, expr?: PolicyExpr) =>
    expr ? convertPolicy(schema, table, name, path, expr) : undefined;
  out.read_policy = conv("policies.select.using", table.policies.select.using);
  const update_using = conv("policies.update.using", table.policies.update.using);
  const delete_using = conv("policies.delete.using", table.policies.delete.using);
  out.write_policies = {
    insert_check: conv("policies.insert.with_check", table.policies.insert.with_check),
    update_using,
    update_check: conv("policies.update.with_check", table.policies.update.with_check),
    delete_using,
  };
  return out;
}

function convertColumn(table: TableName, column: ColumnDescriptor): CoreColumnSchema {
  try {
    validateJsonSchemas(column.column_type, column.name);
  } catch (e) {
    throw columnErr(table, column.name, (e as Error).message);
  }
  let type = convertColumnType(table, column.name, column.column_type);
  if (column.nullable) type = nullable(type);
  const converted = newColumnSchema(column.name, type);
  if (column.column_type.type === "Json") converted.large_value_kind = "Json";
  if (column.default !== undefined)
    converted.default = convertColumnDefault(table, column, column.default);
  return converted;
}

function convertColumnDefault(table: TableName, column: ColumnDescriptor, value: Value): GValue {
  try {
    validateJsonValue(value, column.column_type, column.name);
  } catch (e) {
    throw columnErr(table, column.name, (e as Error).message);
  }
  if (value.type === "Null") {
    if (!column.nullable)
      throw columnErr(table, column.name, "null default requires a nullable column");
    return { kind: "Nullable", value: undefined };
  }
  const d = convertDefaultForColumnType(table, column.name, column.column_type, value);
  return column.nullable ? { kind: "Nullable", value: d } : d;
}

function convertDefaultForColumnType(
  table: TableName,
  column: string,
  t: ColumnType,
  v: Value,
): GValue {
  if (t.type === "Boolean" && v.type === "Boolean") return { kind: "Bool", value: v.value };
  if ((t.type === "Text" || t.type === "Json") && v.type === "Text")
    return { kind: "String", value: v.value };
  if (t.type === "Enum" && v.type === "Text") {
    if (!t.variants.includes(v.value))
      throw columnErr(
        table,
        column,
        `enum default value ${JSON.stringify(v.value)} is not declared`,
      );
    return { kind: "String", value: v.value };
  }
  if ((t.type === "EnumPayload" || t.type === "CatalogueEnumPayload") && v.type === "Enum") {
    const caseIndex = t.cases.findIndex((c) => c.name === v.case);
    if (caseIndex < 0)
      throw columnErr(
        table,
        column,
        `payload enum default case ${JSON.stringify(v.case)} is not declared`,
      );
    const entry = t.cases[caseIndex];
    if (entry.fields.length !== v.values.length)
      throw columnErr(table, column, "payload enum default field count does not match its case");
    const fields: [string, GValueType][] = [];
    const converted: GValue[] = [];
    entry.fields.forEach((field, i) => {
      let vt = convertColumnType(table, field.name, field.column_type);
      if (field.nullable) vt = nullable(vt);
      const fv = v.values[i];
      let out: GValue;
      if (fv.type === "Null") {
        if (!field.nullable)
          throw err(
            `$.${table}.${column}.${field.name}`,
            "payload enum default cannot use null for a required field",
          );
        out = { kind: "Nullable", value: undefined };
      } else {
        const inner = convertDefaultForColumnType(table, field.name, field.column_type, fv);
        out = field.nullable ? { kind: "Nullable", value: inner } : inner;
      }
      fields.push([field.name, vt]);
      converted.push(out);
    });
    try {
      return {
        kind: "Enum",
        value: EnumValue.create(caseIndex, new RecordDescriptor(fields), converted),
      };
    } catch (e) {
      throw columnErr(table, column, String(e));
    }
  }
  if (t.type === "Timestamp" && v.type === "Timestamp")
    return { kind: "U64", value: BigInt(v.value) };
  if (t.type === "Double" && v.type === "Double") return { kind: "F64", value: v.value };
  if (t.type === "Uuid" && v.type === "Uuid")
    return { kind: "Uuid", value: objectIdUuidBytes(v.value) };
  if (t.type === "Bytea" && v.type === "Bytea") return { kind: "Bytes", value: v.value };
  if (t.type === "Integer" && v.type === "Integer") return { kind: "I32", value: v.value };
  if (t.type === "Integer" && v.type === "BigInt") {
    if (v.value < -(2n ** 31n) || v.value >= 2n ** 31n)
      throw columnErr(table, column, `BIGINT default ${v.value} is outside INTEGER range`);
    return { kind: "I32", value: Number(v.value) };
  }
  if (t.type === "BigInt" && v.type === "Integer") return { kind: "I64", value: BigInt(v.value) };
  if (t.type === "BigInt" && v.type === "BigInt") return { kind: "I64", value: v.value };
  if (t.type === "Array" && v.type === "Array")
    return {
      kind: "Array",
      value: v.value.map((e) => convertDefaultForColumnType(table, column, t.element, e)),
    };
  throw columnErr(
    table,
    column,
    `default ${debugValue(v)} does not match column type ${JSON.stringify(t)}`,
  );
}

export function convertColumnType(table: TableName, column: string, t: ColumnType): GValueType {
  switch (t.type) {
    case "Boolean":
      return { kind: "Bool" };
    case "Text":
    case "Enum":
    case "Json":
      return { kind: "String" };
    case "Timestamp":
      return { kind: "U64" };
    case "Double":
      return { kind: "F64" };
    case "Uuid":
      return { kind: "Uuid" };
    case "Bytea":
      return { kind: "Bytes" };
    case "Integer":
      return { kind: "I32" };
    case "BigInt":
      return { kind: "I64" };
    case "ScalarEnum":
      try {
        return { kind: "EnumTag", schema: new ScalarEnumSchema(t.name, t.variants) };
      } catch (e) {
        throw columnErr(table, column, String(e));
      }
    case "EnumPayload":
    case "CatalogueEnumPayload": {
      const cases = t.cases.map((c) => {
        if (!c.name) throw columnErr(table, column, "enum case names cannot be empty");
        const fields = c.fields.map((f): [string, GValueType] => {
          validatePayloadEnumField(table, column, c.name, f);
          const vt = convertColumnType(table, f.name, f.column_type);
          return [f.name, f.nullable ? nullable(vt) : vt];
        });
        return new EnumCase(c.name, new RecordDescriptor(fields));
      });
      const enumName = t.type === "CatalogueEnumPayload" ? t.name : `${table}_${column}`;
      try {
        return { kind: "Enum", schema: new EnumSchema(enumName, cases) };
      } catch (e) {
        throw columnErr(table, column, String(e));
      }
    }
    case "Array":
      return arrayOf(convertColumnType(table, column, t.element));
    case "TransactionId":
      throw columnErr(
        table,
        column,
        "TransactionId columns are not supported by core schema conversion yet",
      );
    case "Row":
      throw columnErr(
        table,
        column,
        "nested Row columns are not supported by core schema conversion yet",
      );
  }
}

const PAYLOAD_SCALARS = new Set([
  "Boolean",
  "Text",
  "Timestamp",
  "Double",
  "Uuid",
  "Bytea",
  "Json",
  "Integer",
  "BigInt",
]);

function validatePayloadEnumField(
  table: TableName,
  column: string,
  kase: string,
  field: ColumnDescriptor,
): void {
  const path = `$.${table}.${column}.${kase}.${field.name}`;
  if (field.name === "type")
    throw err(path, 'enum payload field name "type" is reserved for the discriminant');
  if (field.name.startsWith("$"))
    throw err(path, 'enum payload field names starting with "$" are reserved for magic columns');
  if (field.references !== undefined)
    throw err(path, "enum payload fields cannot use references; use UUID values instead");
  if (!PAYLOAD_SCALARS.has(field.column_type.type))
    throw err(
      path,
      "payload enum v1 fields must be scalar columns; arrays, rows, and nested enums are not supported",
    );
}

function convertMergeStrategy(table: TableName, column: ColumnDescriptor): MergeStrategy {
  try {
    validateMergeStrategy(column);
  } catch (e) {
    throw columnErr(table, column.name, (e as Error).message);
  }
  return column.merge_strategy === "Counter" ? "Counter" : "GSet";
}

// ---- policy clause assembly ----

export type ExpansionPath = [string, Operation][];

function convertPolicy(
  schema: Schema,
  ts: TableSchema,
  table: TableName,
  path: string,
  expr: PolicyExpr,
): Query {
  return convertPolicyWithNativeSelectInherits(schema, ts, table, path, expr, true, []);
}

/** Expand only the current dependency path; sibling branches may reuse a policy. */
export function convertExpandedInheritedPolicy(
  schema: Schema,
  ts: TableSchema,
  table: TableName,
  path: string,
  expr: PolicyExpr,
  operation: Operation,
  expansion: ExpansionPath,
): Query {
  const start = expansion.findIndex(([t, o]) => t === table && o === operation);
  if (start >= 0) {
    const cycle = [...expansion.slice(start), [table, operation]]
      .map(([t, o]) => `${t}.${o}`)
      .join(" -> ");
    throw err(
      policyPath(table, path),
      `cyclic policy expansion under INHERITS_REFERENCING is unsupported: ${cycle}`,
    );
  }
  expansion.push([table, operation]);
  try {
    return convertPolicyWithNativeSelectInherits(schema, ts, table, path, expr, false, expansion);
  } finally {
    expansion.pop();
  }
}

export function convertPolicyWithNativeSelectInherits(
  schema: Schema,
  ts: TableSchema,
  table: TableName,
  path: string,
  expr: PolicyExpr,
  nativeSelectInherits: boolean,
  expansion: ExpansionPath,
): Query {
  validateExistsRelPolicyJoinConditions(table, path, expr);
  return appendPolicyClause(
    schema,
    ts,
    table,
    path,
    queryFrom(table),
    expr,
    nativeSelectInherits,
    expansion,
  );
}

export function policyRequiresBranch(expr: PolicyExpr): boolean {
  switch (expr.type) {
    case "Inherits":
    case "InheritsReferencing":
    case "Exists":
    case "ExistsRel":
      return true;
    case "And":
    case "Or":
      return expr.exprs.some(policyRequiresBranch);
    case "Not":
      return policyRequiresBranch(expr.expr);
    default:
      return false;
  }
}

const falseBase = (table: string) => qFilter(queryFrom(table), { kind: "Any", predicates: [] });

function appendPolicyClause(
  schema: Schema,
  ts: TableSchema,
  table: TableName,
  path: string,
  query: Query,
  expr: PolicyExpr,
  nativeSelectInherits: boolean,
  expansion: ExpansionPath,
): Query {
  if (query.policy_branches.length) {
    let combined = falseBase(table);
    for (const alt of policyAlternativesFromQuery(query)) {
      const branch = queryFrom(table);
      branch.filters = alt.filters;
      branch.joins = alt.joins;
      branch.reachable = alt.reachable;
      branch.inherits = alt.inherits;
      const appended = appendPolicyClause(
        schema,
        ts,
        table,
        path,
        branch,
        expr,
        nativeSelectInherits,
        expansion,
      );
      for (const a of policyAlternativesFromQuery(appended)) combined = qPolicyBranch(combined, a);
    }
    return combined;
  }
  switch (expr.type) {
    case "And":
      expr.exprs.forEach((e, i) => {
        query = appendPolicyClause(
          schema,
          ts,
          table,
          `${path}.And[${i}]`,
          query,
          e,
          nativeSelectInherits,
          expansion,
        );
      });
      return query;
    case "Inherits":
      return appendInheritedPolicy(
        schema,
        ts,
        table,
        path,
        query,
        expr.operation,
        expr.via_column,
        expr.max_depth,
        nativeSelectInherits,
        expansion,
      );
    case "InheritsReferencing":
      return appendInheritedReferencingPolicy(
        schema,
        table,
        path,
        query,
        expr.operation,
        expr.source_table,
        expr.via_column,
        expansion,
      );
    case "Exists":
      return appendExistsPolicyClause(schema, table, path, query, expr.table, expr.condition);
    case "ExistsRel":
      return appendExistsRelPolicyClause(schema, table, path, query, expr.rel);
    case "Or":
      if (expr.exprs.some(policyRequiresBranch)) {
        const existing = policyAlternativesFromQuery(query);
        let out = falseBase(table);
        expr.exprs.forEach((e, i) => {
          const bq = convertPolicyWithNativeSelectInherits(
            schema,
            ts,
            table,
            `${path}.Or[${i}]`,
            e,
            nativeSelectInherits,
            expansion,
          );
          for (const b of policyAlternativesFromQuery(bq))
            for (const ex of existing)
              out = qPolicyBranch(out, {
                filters: [...ex.filters, ...b.filters],
                joins: [...ex.joins, ...b.joins],
                reachable: [...ex.reachable, ...b.reachable],
                inherits: [...ex.inherits, ...b.inherits],
              });
        });
        return out;
      }
    // fallthrough
    default:
      for (const f of predicateFiltersForExpr(table, path, expr)) query = qFilter(query, f);
      return query;
  }
}

function predicateFiltersForExpr(table: TableName, path: string, expr: PolicyExpr): Predicate[] {
  if (expr.type === "True") return [];
  if (expr.type === "And")
    return expr.exprs.flatMap((e, i) => predicateFiltersForExpr(table, `${path}.And[${i}]`, e));
  return [convertPolicyPredicate(table, path, expr)];
}

function appendInheritedReferencingPolicy(
  schema: Schema,
  table: TableName,
  path: string,
  query: Query,
  operation: Operation,
  sourceTable: string,
  viaColumn: string,
  expansion: ExpansionPath,
): Query {
  const p = policyPath(table, path);
  const source = schema.get(sourceTable);
  if (!source) throw err(p, `INHERITS_REFERENCING source_table '${sourceTable}' was not found`);
  const column = source.columns.columns.find((c) => c.name === viaColumn);
  if (!column)
    throw err(
      p,
      `INHERITS_REFERENCING via_column '${viaColumn}' was not found on source_table '${sourceTable}'`,
    );
  if (column.references === undefined)
    throw err(p, `INHERITS_REFERENCING via_column '${viaColumn}' has no FK reference`);
  if (column.references !== table)
    throw err(
      p,
      `INHERITS_REFERENCING via_column '${viaColumn}' references '${column.references}', expected '${table}'`,
    );
  const sourcePolicy = sourceOperationPolicy(source, operation);
  if (!sourcePolicy)
    throw err(p, `INHERITS_REFERENCING source_table '${sourceTable}' has no ${operation} policy`);
  const subPath = `${path}.InheritsReferencing[${sourceTable}]`;
  try {
    return qJoinVia(query, sourceTable, viaColumn, [
      convertPolicyPredicate(sourceTable, subPath, sourcePolicy),
    ]);
  } catch (e) {
    if (!policyRequiresBranch(sourcePolicy)) throw e;
  }
  const sourceQuery = convertExpandedInheritedPolicy(
    schema,
    source,
    sourceTable,
    subPath,
    sourcePolicy,
    operation,
    expansion,
  );
  return appendInheritedReferencingPolicyBranches(query, sourceTable, viaColumn, sourceQuery);
}

export function sourceOperationPolicy(table: TableSchema, op: Operation): PolicyExpr | undefined {
  switch (op) {
    case "Select":
      return table.policies.select.using;
    case "Insert":
      return table.policies.insert.with_check;
    case "Update":
      return table.policies.update.using ?? table.policies.update.with_check;
    case "Delete":
      return table.policies.delete.using;
  }
}

export const convertInheritsOperation = (op: Operation): InheritsOperation => op;

function appendInheritedReferencingPolicyBranches(
  query: Query,
  sourceTable: string,
  viaColumn: string,
  sourceQuery: Query,
): Query {
  query = qFilter(query, { kind: "Any", predicates: [] });
  for (const branch of policyAlternativesFromQuery(sourceQuery)) {
    if (branch.reachable.length)
      throw err(
        `$.${sourceTable}.InheritsReferencing`,
        "core schema policies do not support INHERITS_REFERENCING through reachability yet",
      );
    const bq = qJoinVia(queryFrom(query.table), sourceTable, viaColumn, branch.filters, {
      nestedJoins: branch.joins,
    });
    for (const b of policyAlternativesFromQuery(bq)) query = qPolicyBranch(query, b);
  }
  return query;
}

export function appendExistsPolicyClause(
  schema: Schema,
  table: TableName,
  path: string,
  query: Query,
  existsTable: string,
  condition: PolicyExpr,
): Query {
  if (!schema.has(existsTable))
    throw err(policyPath(table, path), `EXISTS references unknown table '${existsTable}'`);
  const outer: { join_column: string; source_column: string }[] = [];
  const filters: Predicate[] = [];
  const nestedExists: [number, string, PolicyExpr][] = [];
  const nestedRel: [number, any][] = [];
  collectPolicyConjuncts(condition).forEach((e, i) => {
    if (
      e.type === "Cmp" &&
      e.op === "Eq" &&
      e.value.type === "SessionRef" &&
      e.value.path.length === 2 &&
      e.value.path[0] === "__jazz_outer_row"
    )
      outer.push({ join_column: e.column, source_column: e.value.path[1] });
    else if (e.type === "Exists") nestedExists.push([i, e.table, e.condition]);
    else if (e.type === "ExistsRel") nestedRel.push([i, e.rel]);
    else filters.push(convertPolicyPredicate(existsTable, `${path}.Exists[${i}]`, e));
  });
  if (!outer.length) query.joins.push(uncorrelatedExistsJoin(existsTable, filters, []));
  else {
    const pi = Math.max(
      0,
      outer.findIndex((c) => c.join_column === "id"),
    );
    const [primary] = outer.splice(pi, 1);
    if (primary.join_column === "id")
      query = qJoinVia(query, existsTable, "id", filters, {
        target: "RowId",
        sourceColumn: primary.source_column,
        correlatedFilters: outer,
      });
    else
      query = qJoinVia(query, existsTable, primary.join_column, filters, {
        sourceColumn: primary.source_column,
        correlatedFilters: outer,
      });
  }
  for (const [i, t, c] of nestedExists)
    query = appendExistsPolicyClause(schema, table, `${path}.Exists[${i}]`, query, t, c);
  for (const [i, rel] of nestedRel)
    query = appendExistsRelPolicyClause(schema, table, `${path}.Exists[${i}]`, query, rel);
  return query;
}

function collectPolicyConjuncts(expr: PolicyExpr, out: PolicyExpr[] = []): PolicyExpr[] {
  if (expr.type === "And") for (const c of expr.exprs) collectPolicyConjuncts(c, out);
  else out.push(expr);
  return out;
}

export type { PolicyBranch };
