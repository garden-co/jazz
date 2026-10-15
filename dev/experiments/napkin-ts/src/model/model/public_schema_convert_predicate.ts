// INHERITS lowering, policy predicates, session operands and literals.
import {
  queryFrom,
  qFilter,
  qPolicyBranch,
  qJoinVia,
  qInherits,
  policyAlternativesFromQuery,
  authorClaimPathKey,
  providerClaimPathOperandKey,
  objectIdUuidBytes,
  type GValue,
  type Query,
  type Predicate,
  type Operand,
  type PolicyBranch,
} from "../../_extern/model-core.js";
import type { Schema, TableName, TableSchema } from "./public_api/types/schema.js";
import type { Operation, PolicyExpr, PolicyValue } from "./public_api/policy.js";
import { debugValue, type Value } from "./public_api/types/value.js";
import {
  err,
  policyPath,
  sourceOperationPolicy,
  policyRequiresBranch,
  convertExpandedInheritedPolicy,
  DIRECT_USER_CLAIM,
  PUBLIC_USER_SESSION_PATHS,
  DIRECT_AUTH_MODE_CLAIM,
  PUBLIC_AUTH_MODE_SESSION_PATHS,
  type ExpansionPath,
} from "./public_schema_convert.js";
import { uncorrelatedExistsJoin } from "./public_schema_convert_rel.js";

export function appendInheritedPolicy(
  schema: Schema,
  ts: TableSchema,
  table: TableName,
  path: string,
  query: Query,
  operation: Operation,
  viaColumn: string,
  maxDepth: number | undefined,
  nativeSelectInherits: boolean,
  expansion: ExpansionPath,
): Query {
  const p = policyPath(table, path);
  const column = ts.columns.columns.find((c) => c.name === viaColumn);
  if (!column) throw err(p, `INHERITS via_column '${viaColumn}' was not found`);
  const parentTable = column.references;
  if (parentTable === undefined)
    throw err(p, `INHERITS via_column '${viaColumn}' has no FK reference`);
  const parentSchema = schema.get(parentTable);
  if (!parentSchema)
    throw err(p, `INHERITS via_column '${viaColumn}' references unknown table '${parentTable}'`);
  const parentPolicy = sourceOperationPolicy(parentSchema, operation);
  if (!parentPolicy)
    throw err(
      p,
      `INHERITS via_column '${viaColumn}' references table '${parentTable}' without a ${operation} policy`,
    );
  if (operation === "Select" && !nativeSelectInherits) {
    if (maxDepth !== undefined)
      throw err(
        p,
        "bounded SELECT INHERITS under INHERITS_REFERENCING is unsupported because its expanded join chain cannot preserve max_depth",
      );
    return appendInheritedPolicyExpandedFallback(
      schema,
      parentSchema,
      table,
      path,
      query,
      parentTable,
      viaColumn,
      parentPolicy,
      operation,
      expansion,
    );
  }
  return qInherits(query, viaColumn, operation, maxDepth);
}

function appendInheritedPolicyExpandedFallback(
  schema: Schema,
  parentSchema: TableSchema,
  table: TableName,
  path: string,
  query: Query,
  parentTable: TableName,
  viaColumn: string,
  parentPolicy: PolicyExpr,
  operation: Operation,
  expansion: ExpansionPath,
): Query {
  const subPath = `${path}.Inherits[${parentTable}]`;
  try {
    return qJoinVia(
      query,
      parentTable,
      "id",
      [convertPolicyPredicate(parentTable, subPath, parentPolicy)],
      {
        target: "RowId",
        sourceColumn: viaColumn,
      },
    );
  } catch (e) {
    if (!policyRequiresBranch(parentPolicy)) throw e;
  }
  const parentQuery = convertExpandedInheritedPolicy(
    schema,
    parentSchema,
    parentTable,
    subPath,
    parentPolicy,
    operation,
    expansion,
  );
  query = qFilter(query, { kind: "Any", predicates: [] });
  policyAlternativesFromQuery(parentQuery).forEach((branch: PolicyBranch, index: number) => {
    const bq = inheritedParentBranchToChildQuery(
      table,
      path,
      parentTable,
      viaColumn,
      index,
      branch,
    );
    for (const b of policyAlternativesFromQuery(bq)) query = qPolicyBranch(query, b);
  });
  return query;
}

function inheritedParentBranchToChildQuery(
  table: TableName,
  path: string,
  parentTable: TableName,
  viaColumn: string,
  index: number,
  branch: PolicyBranch,
): Query {
  const bpath = `$.${table}.${path}.InheritsBranch[${index}]`;
  if (branch.reachable.length)
    throw err(
      bpath,
      "core schema policies do not support inherited SELECT branches with reachability yet",
    );
  const rowIdJoin = (q: Query, filters: Predicate[]) =>
    qJoinVia(q, parentTable, "id", filters, { target: "RowId", sourceColumn: viaColumn });
  let query = queryFrom(table);
  if (branch.filters.length) query = rowIdJoin(query, branch.filters);
  for (const join of branch.joins) {
    const sourceColumn = join.source_lookup
      ? join.source_lookup.row_id_source_column
      : join.source_column;
    switch (join.target) {
      case "Uncorrelated":
        query = rowIdJoin(query, []);
        query.joins.push(uncorrelatedExistsJoin(join.table, join.filters, join.nested_joins));
        break;
      case "Column":
      case "RowId":
        if (sourceColumn !== undefined) {
          const lookup = {
            table: parentTable,
            row_id_source_column: viaColumn,
            value_column: sourceColumn,
          };
          query.joins.push({ ...join, source_column: lookup.value_column, source_lookup: lookup });
        } else {
          if (join.target === "RowId" && join.table !== parentTable)
            throw err(
              bpath,
              "core schema policies do not support inherited SELECT row-id joins to non-parent tables yet",
            );
          query = qJoinVia(query, join.table, join.on_column, join.filters, {
            target: join.target,
            sourceColumn: viaColumn,
            correlatedFilters: join.correlated_filters,
            nestedJoins: join.nested_joins,
          });
        }
    }
  }
  return query;
}

const CMP = { Eq: "Eq", Ne: "Ne", Lt: "Lt", Le: "Lte", Gt: "Gt", Ge: "Gte" } as const;

export function convertPolicyPredicate(
  table: TableName,
  path: string,
  expr: PolicyExpr,
): Predicate {
  const column = (name: string): Operand => ({ kind: "Column", name });
  const session = (segments: string[]) => convertSessionPathOperand(table, path, segments);
  const literal = (v: Value): Operand => ({
    kind: "Literal",
    value: convertPolicyLiteral(table, path, v),
  });
  const notNull = (operand: Operand): Predicate => ({
    kind: "Not",
    predicate: { kind: "IsNull", operand },
  });
  switch (expr.type) {
    case "True":
      return { kind: "All", predicates: [] };
    case "False":
      return { kind: "Any", predicates: [] };
    case "And":
    case "Or":
      return {
        kind: expr.type === "And" ? "All" : "Any",
        predicates: expr.exprs.map((e, i) =>
          convertPolicyPredicate(table, `${path}.${expr.type}[${i}]`, e),
        ),
      };
    case "Not":
      return { kind: "Not", predicate: convertPolicyPredicate(table, `${path}.Not`, expr.expr) };
    case "Cmp": {
      const left = column(expr.column);
      if (expr.value.type === "Literal" && expr.value.value.type === "Null") {
        if (expr.op === "Eq") return { kind: "IsNull", operand: left };
        if (expr.op === "Ne") return notNull(left);
      }
      return { kind: CMP[expr.op], left, right: convertPolicyOperand(table, path, expr.value) };
    }
    case "SessionCmp": {
      const left = session(expr.path);
      if (expr.value.type === "Null") {
        if (expr.op === "Eq") return { kind: "IsNull", operand: left };
        if (expr.op === "Ne") return notNull(left);
      }
      const right = literal(expr.value);
      if (expr.op !== "Eq" && expr.op !== "Ne")
        throw err(
          policyPath(table, path),
          "core schema policies only support Eq/Ne SessionCmp operators yet",
        );
      return { kind: expr.op, left, right };
    }
    case "SessionIsNull":
      return { kind: "IsNull", operand: session(expr.path) };
    case "SessionIsNotNull":
      return notNull(session(expr.path));
    case "IsNull":
      return { kind: "IsNull", operand: column(expr.column) };
    case "IsNotNull":
      return notNull(column(expr.column));
    case "Contains":
      return {
        kind: "Contains",
        left: column(expr.column),
        right: convertPolicyOperand(table, path, expr.value),
      };
    case "In":
      return { kind: "Contains", left: session(expr.session_path), right: column(expr.column) };
    case "InList":
      return {
        kind: "In",
        left: column(expr.column),
        values: expr.values.map((v, i) => convertPolicyOperand(table, `${path}.InList[${i}]`, v)),
      };
    case "SessionContains":
      return { kind: "Contains", left: session(expr.path), right: literal(expr.value) };
    case "SessionInList": {
      const values = expr.values.map(
        (v, i): Operand => ({
          kind: "Literal",
          value: convertPolicyLiteral(table, `${path}.SessionInList[${i}]`, v),
        }),
      );
      return { kind: "In", left: session(expr.path), values };
    }
    default:
      throw err(
        policyPath(table, path),
        `core schema policies do not support ${JSON.stringify(expr)} yet`,
      );
  }
}

function convertPolicyOperand(table: TableName, path: string, v: PolicyValue): Operand {
  return v.type === "SessionRef"
    ? convertSessionPathOperand(table, path, v.path)
    : { kind: "Literal", value: convertPolicyLiteral(table, path, v.value) };
}

export function convertSessionPathOperand(
  table: TableName,
  path: string,
  segments: string[],
): Operand {
  const author = authorClaimPathKey(segments);
  if (author !== undefined) return { kind: "Claim", name: author };
  if (segments.length === 1 && PUBLIC_USER_SESSION_PATHS.includes(segments[0]))
    return { kind: "Claim", name: DIRECT_USER_CLAIM };
  if (segments.length === 1 && PUBLIC_AUTH_MODE_SESSION_PATHS.includes(segments[0]))
    return { kind: "Claim", name: DIRECT_AUTH_MODE_CLAIM };
  if (segments.length >= 2 && segments[0] === "claims")
    return { kind: "Claim", name: providerClaimPathOperandKey(segments.slice(1)) };
  throw err(
    policyPath(table, path),
    `core schema policies only support session.user, session.authMode, and provider claims through session.claims; got session.${segments.join(".")}`,
  );
}

export function convertPolicyLiteral(table: TableName, path: string, v: Value): GValue {
  switch (v.type) {
    case "Null":
      return { kind: "Nullable", value: undefined };
    case "Boolean":
      return { kind: "Bool", value: v.value };
    case "Text":
      return { kind: "String", value: v.value };
    case "Integer":
      return { kind: "I32", value: v.value };
    case "BigInt":
      return { kind: "I64", value: v.value };
    case "Double":
      if (!Number.isFinite(v.value))
        throw err(
          policyPath(table, path),
          "core schema policy floating-point literals must be finite numbers",
        );
      return { kind: "F64", value: v.value };
    case "Timestamp":
      return { kind: "U64", value: BigInt(v.value) };
    case "Uuid":
      return { kind: "Uuid", value: objectIdUuidBytes(v.value) };
    case "Bytea":
      return { kind: "Bytes", value: v.value };
    case "Array":
      return {
        kind: "Array",
        value: v.value.map((e, i) => convertPolicyLiteral(table, `${path}.Array[${i}]`, e)),
      };
    default:
      throw err(
        policyPath(table, path),
        `core schema policies do not support ${debugValue(v)} literals yet`,
      );
  }
}
