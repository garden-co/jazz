// SQL generation from schema AST

import type {
  Schema,
  Table,
  Column,
  Lens,
  LensOp,
  PolicyExpr,
  PolicyValue,
  PolicyCmpOp,
  OperationPolicy,
  TablePolicies,
} from "./schema.js";
import { sqlTypeToString } from "./schema.js";
import { assertUserColumnNameAllowed } from "./magic-columns.js";

const BARE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;
const RESERVED_KEYWORDS = new Set([
  "CREATE",
  "TABLE",
  "POLICY",
  "ON",
  "FOR",
  "USING",
  "WITH",
  "CHECK",
  "SESSION",
  "INHERITS",
  "INHERIT",
  "VIA",
  "REFERENCING",
  "SELECT",
  "INSERT",
  "UPDATE",
  "DELETE",
  "AND",
  "OR",
  "IN",
  "CONTAINS",
  "IS",
  "ALTER",
  "ADD",
  "DROP",
  "COLUMN",
  "RENAME",
  "TO",
  "NOT",
  "NULL",
  "DEFAULT",
  "TRUE",
  "FALSE",
  "REFERENCES",
]);

function sqlIdentifier(identifier: string): string {
  if (BARE_IDENTIFIER_RE.test(identifier) && !RESERVED_KEYWORDS.has(identifier.toUpperCase())) {
    return identifier;
  }
  return `"${identifier.replace(/"/g, '""')}"`;
}

function sessionPathToSql(path: string[]): string {
  return path.map(sqlIdentifier).join(".");
}

function columnToSql(column: Column): string {
  const ref = column.references ? ` REFERENCES ${sqlIdentifier(column.references)}` : "";
  const nullability = column.nullable ? "" : " NOT NULL";
  return `    ${sqlIdentifier(column.name)} ${sqlTypeToString(column.sqlType)}${ref}${nullability}`;
}

function tableToSql(table: Table): string {
  const columnDefs = table.columns.map(columnToSql);
  const createTable = `CREATE TABLE ${sqlIdentifier(table.name)} (\n${columnDefs.join(",\n")}\n);`;
  const policyStatements = tablePoliciesToSql(table.name, table.policies);

  if (policyStatements.length === 0) {
    return createTable;
  }

  return `${createTable}\n${policyStatements.join("\n")}`;
}

export function schemaToSql(schema: Schema): string {
  for (const table of schema.tables) {
    for (const column of table.columns) {
      assertUserColumnNameAllowed(column.name);
    }
  }
  return schema.tables.map(tableToSql).join("\n\n") + "\n";
}

function policyValueToSql(value: PolicyValue): string {
  if (value.type === "SessionRef") {
    return `@session.${sessionPathToSql(value.path)}`;
  }
  return formatDefaultValue(value.value);
}

function policyExprToSql(expr: PolicyExpr): string {
  switch (expr.type) {
    case "Cmp":
      return `${sqlIdentifier(expr.column)} ${cmpOpToSql(expr.op)} ${policyValueToSql(expr.value)}`;
    case "SessionCmp":
      return `@session.${sessionPathToSql(expr.path)} ${cmpOpToSql(expr.op)} ${policyValueToSql(expr.value)}`;
    case "IsNull":
      return `${sqlIdentifier(expr.column)} IS NULL`;
    case "SessionIsNull":
      return `@session.${sessionPathToSql(expr.path)} IS NULL`;
    case "IsNotNull":
      return `${sqlIdentifier(expr.column)} IS NOT NULL`;
    case "SessionIsNotNull":
      return `@session.${sessionPathToSql(expr.path)} IS NOT NULL`;
    case "Contains":
      return `${sqlIdentifier(expr.column)} CONTAINS ${policyValueToSql(expr.value)}`;
    case "SessionContains":
      return `@session.${sessionPathToSql(expr.path)} CONTAINS ${policyValueToSql(expr.value)}`;
    case "In":
      return `${sqlIdentifier(expr.column)} IN @session.${sessionPathToSql(expr.session_path)}`;
    case "InList":
      return `${sqlIdentifier(expr.column)} IN (${expr.values.map(policyValueToSql).join(", ")})`;
    case "SessionInList":
      return `@session.${sessionPathToSql(expr.path)} IN (${expr.values.map(policyValueToSql).join(", ")})`;
    case "Exists":
      return `EXISTS (SELECT FROM ${sqlIdentifier(expr.table)} WHERE ${policyExprToSql(expr.condition)})`;
    case "ExistsRel":
      return "EXISTS_REL(<relation_ir>)";
    case "Inherits":
      return expr.max_depth === undefined
        ? `INHERITS ${expr.operation.toUpperCase()} VIA ${sqlIdentifier(expr.via_column)}`
        : `INHERITS ${expr.operation.toUpperCase()} VIA ${sqlIdentifier(expr.via_column)} MAX DEPTH ${expr.max_depth}`;
    case "InheritsReferencing":
      return expr.max_depth === undefined
        ? `INHERITS ${expr.operation.toUpperCase()} REFERENCING ${sqlIdentifier(expr.source_table)} VIA ${sqlIdentifier(expr.via_column)}`
        : `INHERITS ${expr.operation.toUpperCase()} REFERENCING ${sqlIdentifier(expr.source_table)} VIA ${sqlIdentifier(expr.via_column)} MAX DEPTH ${expr.max_depth}`;
    case "And":
      return expr.exprs.map((inner) => `(${policyExprToSql(inner)})`).join(" AND ");
    case "Or":
      return expr.exprs.map((inner) => `(${policyExprToSql(inner)})`).join(" OR ");
    case "Not":
      return `NOT (${policyExprToSql(expr.expr)})`;
    case "True":
      return "TRUE";
    case "False":
      return "FALSE";
  }
}

function cmpOpToSql(op: PolicyCmpOp): string {
  switch (op) {
    case "Eq":
      return "=";
    case "Ne":
      return "!=";
    case "Lt":
      return "<";
    case "Le":
      return "<=";
    case "Gt":
      return ">";
    case "Ge":
      return ">=";
  }
}

function operationPolicyClauses(policy: OperationPolicy): string[] {
  const clauses: string[] = [];
  if (policy.using) {
    clauses.push(`USING (${policyExprToSql(policy.using)})`);
  }
  if (policy.with_check) {
    clauses.push(`WITH CHECK (${policyExprToSql(policy.with_check)})`);
  }
  return clauses;
}

function tablePoliciesToSql(tableName: string, policies: TablePolicies | undefined): string[] {
  if (!policies) {
    return [];
  }

  const statements: string[] = [];
  const ops: Array<{ key: keyof TablePolicies; sqlOp: string }> = [
    { key: "select", sqlOp: "SELECT" },
    { key: "insert", sqlOp: "INSERT" },
    { key: "update", sqlOp: "UPDATE" },
    { key: "delete", sqlOp: "DELETE" },
  ];

  for (const { key, sqlOp } of ops) {
    const opPolicy = policies[key];
    if (!opPolicy) {
      continue;
    }

    const clauses = operationPolicyClauses(opPolicy);
    if (clauses.length === 0) {
      continue;
    }

    statements.push(
      `CREATE POLICY ${sqlIdentifier(`${tableName}_${key}_policy`)} ON ${sqlIdentifier(tableName)} FOR ${sqlOp} ${clauses.join(" ")};`,
    );
  }

  return statements;
}

function formatDefaultValue(value: unknown): string {
  if (value instanceof Uint8Array) {
    const hex = [...value].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    return `'\\\\x${hex}'`;
  }
  if (typeof value === "string") {
    return `'${value.replace(/'/g, "''")}'`;
  }
  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
  }
  if (value instanceof Date) {
    return String(value.getTime());
  }
  if (typeof value === "number") {
    return String(value);
  }
  if (value === null) {
    return "NULL";
  }
  if (Array.isArray(value)) {
    return `ARRAY[${value.map(formatDefaultValue).join(", ")}]`;
  }
  throw new Error(`Unsupported default value type: ${typeof value}`);
}

function lensOpToForwardSql(table: string, op: LensOp): string {
  switch (op.type) {
    case "introduce":
      return `ALTER TABLE ${sqlIdentifier(table)} ADD COLUMN ${sqlIdentifier(op.column)} ${sqlTypeToString(op.sqlType)} DEFAULT ${formatDefaultValue(op.value)};`;
    case "drop":
      return `ALTER TABLE ${sqlIdentifier(table)} DROP COLUMN ${sqlIdentifier(op.column)};`;
    case "rename":
      return `ALTER TABLE ${sqlIdentifier(table)} RENAME COLUMN ${sqlIdentifier(op.column)} TO ${sqlIdentifier(op.value)};`;
  }
}

function lensOpToBackwardSql(table: string, op: LensOp): string {
  switch (op.type) {
    case "introduce":
      return `ALTER TABLE ${sqlIdentifier(table)} DROP COLUMN ${sqlIdentifier(op.column)};`;
    case "drop":
      return `ALTER TABLE ${sqlIdentifier(table)} ADD COLUMN ${sqlIdentifier(op.column)} ${sqlTypeToString(op.sqlType)} DEFAULT ${formatDefaultValue(op.value)};`;
    case "rename":
      return `ALTER TABLE ${sqlIdentifier(table)} RENAME COLUMN ${sqlIdentifier(op.value)} TO ${sqlIdentifier(op.column)};`;
  }
}

export function lensToSql(lens: Lens, direction: "fwd" | "bwd"): string {
  for (const op of lens.operations) {
    if (op.type !== "drop") {
      assertUserColumnNameAllowed(op.column);
    }
  }
  const converter = direction === "fwd" ? lensOpToForwardSql : lensOpToBackwardSql;
  return lens.operations.map((op) => converter(lens.table, op)).join("\n") + "\n";
}

export function lensesToSql(lenses: Lens[], direction: "fwd" | "bwd"): string {
  const ordered = direction === "bwd" ? [...lenses].reverse() : lenses;
  return ordered.map((lens) => lensToSql(lens, direction)).join("");
}
