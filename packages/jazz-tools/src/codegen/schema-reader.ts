/**
 * Convert TS DSL Schema to WasmSchema JSON format.
 */

import type {
  Schema,
  ScalarSqlType,
  SqlType,
  TablePolicies as DslTablePolicies,
  PolicyExpr as DslPolicyExpr,
  PolicyValue as DslPolicyValue,
} from "../schema.js";
import type {
  WasmSchema,
  ColumnType,
  ColumnDescriptor,
  TableSchema,
  TablePolicies,
  PolicyExpr,
  PolicyValue,
  Value,
} from "../drivers/types.js";

const map: Record<ScalarSqlType, ColumnType> = {
  TEXT: { type: "Text" },
  BOOLEAN: { type: "Boolean" },
  INTEGER: { type: "Integer" },
  REAL: { type: "Real" },
  UUID: { type: "Uuid" },
};

/**
 * Convert a DSL SqlType to WasmColumnType format.
 */
function sqlTypeToWasm(sqlType: SqlType): ColumnType {
  if (typeof sqlType !== "string") {
    if (sqlType.kind === "ENUM") {
      return { type: "Enum", variants: [...sqlType.variants] };
    }
    return { type: "Array", element: sqlTypeToWasm(sqlType.element) };
  }
  return map[sqlType];
}

function literalToWasmValue(value: unknown): Value {
  if (value === null) {
    return { type: "Null" };
  }
  if (typeof value === "string") {
    return { type: "Text", value };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value) || !Number.isInteger(value)) {
      throw new Error("Policy literal numbers must be finite integers");
    }
    if (value >= -2147483648 && value <= 2147483647) {
      return { type: "Integer", value };
    }
    return { type: "BigInt", value };
  }
  if (Array.isArray(value)) {
    return {
      type: "Array",
      value: value.map((inner) => literalToWasmValue(inner)),
    };
  }

  throw new Error(`Unsupported policy literal type: ${typeof value}`);
}

function clonePolicyValue(value: DslPolicyValue): PolicyValue {
  if (value.type === "SessionRef") {
    return { type: "SessionRef", path: [...value.path] };
  }
  return { type: "Literal", value: literalToWasmValue(value.value) };
}

function clonePolicyExpr(expr: DslPolicyExpr): PolicyExpr {
  switch (expr.type) {
    case "Cmp":
      return {
        type: "Cmp",
        column: expr.column,
        op: expr.op,
        value: clonePolicyValue(expr.value),
      };
    case "IsNull":
      return { type: "IsNull", column: expr.column };
    case "IsNotNull":
      return { type: "IsNotNull", column: expr.column };
    case "In":
      return {
        type: "In",
        column: expr.column,
        session_path: [...expr.session_path],
      };
    case "Exists":
      return {
        type: "Exists",
        table: expr.table,
        condition: clonePolicyExpr(expr.condition),
      };
    case "ExistsRel":
      throw new Error(
        "Policy ExistsRel is not supported in schemaToWasm(). Use definePermissions() relation IR path instead.",
      );
    case "Inherits":
      return {
        type: "Inherits",
        operation: expr.operation,
        via_column: expr.via_column,
        max_depth: expr.max_depth,
      };
    case "And":
      return { type: "And", exprs: expr.exprs.map(clonePolicyExpr) };
    case "Or":
      return { type: "Or", exprs: expr.exprs.map(clonePolicyExpr) };
    case "Not":
      return { type: "Not", expr: clonePolicyExpr(expr.expr) };
    case "True":
      return { type: "True" };
    case "False":
      return { type: "False" };
  }
}

function clonePolicies(policies: DslTablePolicies): TablePolicies {
  return {
    select: policies.select
      ? {
          using: policies.select.using ? clonePolicyExpr(policies.select.using) : undefined,
          with_check: policies.select.with_check
            ? clonePolicyExpr(policies.select.with_check)
            : undefined,
        }
      : undefined,
    insert: policies.insert
      ? {
          using: policies.insert.using ? clonePolicyExpr(policies.insert.using) : undefined,
          with_check: policies.insert.with_check
            ? clonePolicyExpr(policies.insert.with_check)
            : undefined,
        }
      : undefined,
    update: policies.update
      ? {
          using: policies.update.using ? clonePolicyExpr(policies.update.using) : undefined,
          with_check: policies.update.with_check
            ? clonePolicyExpr(policies.update.with_check)
            : undefined,
        }
      : undefined,
    delete: policies.delete
      ? {
          using: policies.delete.using ? clonePolicyExpr(policies.delete.using) : undefined,
          with_check: policies.delete.with_check
            ? clonePolicyExpr(policies.delete.with_check)
            : undefined,
        }
      : undefined,
  };
}

/**
 * Convert a TS DSL Schema to WasmSchema format.
 *
 * This produces a JSON-serializable structure that can be passed to the WASM runtime.
 */
export function schemaToWasm(schema: Schema): WasmSchema {
  const tables: Record<string, TableSchema> = {};

  for (const table of schema.tables) {
    const columns: ColumnDescriptor[] = table.columns.map((col) => {
      const descriptor: ColumnDescriptor = {
        name: col.name,
        column_type: sqlTypeToWasm(col.sqlType),
        nullable: col.nullable,
      };
      if (col.references) {
        descriptor.references = col.references;
      }
      return descriptor;
    });

    tables[table.name] = {
      columns,
      policies: table.policies ? clonePolicies(table.policies) : undefined,
    };
  }

  return { tables };
}
