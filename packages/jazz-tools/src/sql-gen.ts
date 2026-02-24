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

function columnToSql(column: Column): string {
  if (column.inheritPolicy && !column.references) {
    throw new Error(`Column ${column.name} cannot use inheritPolicy without references`);
  }
  const ref = column.references ? ` REFERENCES ${column.references}` : "";
  const inherit = column.inheritPolicy ? " INHERIT POLICY" : "";
  const nullability = column.nullable ? "" : " NOT NULL";
  return `    ${column.name} ${sqlTypeToString(column.sqlType)}${ref}${inherit}${nullability}`;
}

function tableToSql(table: Table): string {
  const columnDefs = table.columns.map(columnToSql);
  const createTable = `CREATE TABLE ${table.name} (\n${columnDefs.join(",\n")}\n);`;
  const policyStatements = tablePoliciesToSql(table.name, table.policies);

  if (policyStatements.length === 0) {
    return createTable;
  }

  return `${createTable}\n${policyStatements.join("\n")}`;
}

export function schemaToSql(schema: Schema): string {
  return schema.tables.map(tableToSql).join("\n\n") + "\n";
}

function policyValueToSql(value: PolicyValue): string {
  if (value.type === "SessionRef") {
    return `@session.${value.path.join(".")}`;
  }
  return formatDefaultValue(value.value);
}

function policyExprToSql(expr: PolicyExpr): string {
  switch (expr.type) {
    case "Cmp":
      return `${expr.column} ${cmpOpToSql(expr.op)} ${policyValueToSql(expr.value)}`;
    case "IsNull":
      return `${expr.column} IS NULL`;
    case "IsNotNull":
      return `${expr.column} IS NOT NULL`;
    case "In":
      return `${expr.column} IN @session.${expr.session_path.join(".")}`;
    case "Exists":
      return `EXISTS (SELECT FROM ${expr.table} WHERE ${policyExprToSql(expr.condition)})`;
    case "ExistsRel":
      return "EXISTS_REL(<relation_ir>)";
    case "Inherits":
      return expr.max_depth === undefined
        ? `INHERITS ${expr.operation.toUpperCase()} VIA ${expr.via_column}`
        : `INHERITS ${expr.operation.toUpperCase()} VIA ${expr.via_column} MAX DEPTH ${expr.max_depth}`;
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
      `CREATE POLICY ${tableName}_${key}_policy ON ${tableName} FOR ${sqlOp} ${clauses.join(" ")};`,
    );
  }

  return statements;
}

function formatDefaultValue(value: unknown): string {
  if (typeof value === "string") {
    return `'${value.replace(/'/g, "''")}'`;
  }
  if (typeof value === "boolean") {
    return value ? "TRUE" : "FALSE";
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
      return `ALTER TABLE ${table} ADD COLUMN ${op.column} ${sqlTypeToString(op.sqlType)} DEFAULT ${formatDefaultValue(op.value)};`;
    case "drop":
      return `ALTER TABLE ${table} DROP COLUMN ${op.column};`;
    case "rename":
      return `ALTER TABLE ${table} RENAME COLUMN ${op.column} TO ${op.value};`;
  }
}

function lensOpToBackwardSql(table: string, op: LensOp): string {
  switch (op.type) {
    case "introduce":
      return `ALTER TABLE ${table} DROP COLUMN ${op.column};`;
    case "drop":
      return `ALTER TABLE ${table} ADD COLUMN ${op.column} ${sqlTypeToString(op.sqlType)} DEFAULT ${formatDefaultValue(op.value)};`;
    case "rename":
      return `ALTER TABLE ${table} RENAME COLUMN ${op.value} TO ${op.column};`;
  }
}

export function lensToSql(lens: Lens, direction: "fwd" | "bwd"): string {
  const converter = direction === "fwd" ? lensOpToForwardSql : lensOpToBackwardSql;
  return lens.operations.map((op) => converter(lens.table, op)).join("\n") + "\n";
}
