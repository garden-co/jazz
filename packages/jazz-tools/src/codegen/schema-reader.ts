/**
 * Convert TS DSL Schema to WasmSchema JSON format.
 */

import type {
  Schema,
  ScalarSqlType,
  SqlType,
  TablePolicies as DslTablePolicies,
  PolicyExpr as DslPolicyExpr,
  PolicyLiteralValue as DslPolicyLiteralValue,
  PolicyValue as DslPolicyValue,
} from "../schema.js";
import type {
  WasmSchema,
  ColumnType,
  ColumnDescriptor,
  TableSchema,
  TablePolicies,
  PolicyExpr,
  PolicyLiteralValue,
  PolicyValue,
  Value,
} from "../drivers/types.js";
import { toValue } from "../runtime/value-converter.js";

const map: Record<ScalarSqlType, ColumnType> = {
  TEXT: { type: "Text" },
  BOOLEAN: { type: "Boolean" },
  INTEGER: { type: "Integer" },
  REAL: { type: "Double" },
  TIMESTAMP: { type: "Timestamp" },
  UUID: { type: "Uuid" },
  BYTEA: { type: "Bytea" },
};

/**
 * Convert a DSL SqlType to WasmColumnType format.
 */
function sqlTypeToWasm(sqlType: SqlType): ColumnType {
  if (typeof sqlType !== "string") {
    if (sqlType.kind === "ENUM") {
      return { type: "Enum", variants: [...sqlType.variants] };
    }
    if (sqlType.kind === "JSON") {
      return {
        type: "Json",
        schema: sqlType.schema,
      };
    }
    return { type: "Array", element: sqlTypeToWasm(sqlType.element) };
  }
  return map[sqlType];
}

function literalToWasmValue(value: unknown): Value {
  if (value instanceof Uint8Array) {
    return { type: "Bytea", value };
  }
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

function columnMergeStrategyToWasm(
  strategy: Schema["tables"][number]["columns"][number]["mergeStrategy"],
): ColumnDescriptor["merge_strategy"] {
  switch (strategy) {
    case undefined:
      return undefined;
    case "counter":
      return "Counter";
  }
}

function clonePolicyValue(value: DslPolicyValue): PolicyValue {
  if (value.type === "SessionRef") {
    return { type: "SessionRef", path: [...value.path] };
  }
  return { type: "Literal", value: literalToWasmValue(value.value) };
}

function clonePolicyLiteralValue(value: DslPolicyLiteralValue): PolicyLiteralValue {
  return literalToWasmValue(value.value);
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
    case "SessionCmp":
      return {
        type: "SessionCmp",
        path: [...expr.path],
        op: expr.op,
        value: clonePolicyLiteralValue(expr.value),
      };
    case "IsNull":
      return { type: "IsNull", column: expr.column };
    case "SessionIsNull":
      return { type: "SessionIsNull", path: [...expr.path] };
    case "IsNotNull":
      return { type: "IsNotNull", column: expr.column };
    case "SessionIsNotNull":
      return { type: "SessionIsNotNull", path: [...expr.path] };
    case "Contains":
      return {
        type: "Contains",
        column: expr.column,
        value: clonePolicyValue(expr.value),
      };
    case "SessionContains":
      return {
        type: "SessionContains",
        path: [...expr.path],
        value: clonePolicyLiteralValue(expr.value),
      };
    case "In":
      return {
        type: "In",
        column: expr.column,
        session_path: [...expr.session_path],
      };
    case "InList":
      return {
        type: "InList",
        column: expr.column,
        values: expr.values.map(clonePolicyValue),
      };
    case "SessionInList":
      return {
        type: "SessionInList",
        path: [...expr.path],
        values: expr.values.map(clonePolicyLiteralValue),
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
        ...(expr.max_depth === undefined ? {} : { max_depth: expr.max_depth }),
      };
    case "InheritsReferencing":
      return {
        type: "InheritsReferencing",
        operation: expr.operation,
        source_table: expr.source_table,
        via_column: expr.via_column,
        ...(expr.max_depth === undefined ? {} : { max_depth: expr.max_depth }),
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

function cloneOperationPolicy(
  policy: DslTablePolicies[keyof DslTablePolicies],
): TablePolicies["select"] {
  const out: TablePolicies["select"] = {};
  if (!policy) {
    return out;
  }
  if (policy.using) {
    out.using = clonePolicyExpr(policy.using);
  }
  if (policy.with_check) {
    out.with_check = clonePolicyExpr(policy.with_check);
  }
  return out;
}

function clonePolicies(policies: DslTablePolicies): TablePolicies {
  return {
    select: cloneOperationPolicy(policies.select),
    insert: cloneOperationPolicy(policies.insert),
    update: cloneOperationPolicy(policies.update),
    delete: cloneOperationPolicy(policies.delete),
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
      const columnType = sqlTypeToWasm(col.sqlType);
      if (col.mergeStrategy === "counter" && (col.sqlType !== "INTEGER" || col.nullable)) {
        throw new Error(
          "Counter merge strategy is only supported on non-nullable INTEGER columns.",
        );
      }
      const descriptor: ColumnDescriptor = {
        name: col.name,
        column_type: columnType,
        nullable: col.nullable,
      };
      if (col.default !== undefined) {
        descriptor.default = toValue(col.default, columnType);
      }
      if (col.references) {
        descriptor.references = col.references;
      }
      if (col.mergeStrategy) {
        descriptor.merge_strategy = columnMergeStrategyToWasm(col.mergeStrategy);
      }
      if (col.encryptedWith) {
        descriptor.encrypted_with = col.encryptedWith;
      }
      return descriptor;
    });

    tables[table.name] = {
      columns,
      ...(table.indexedColumns ? { indexed_columns: [...table.indexedColumns] } : {}),
      policies: table.policies ? clonePolicies(table.policies) : undefined,
      ...(table.encryptionSpace ? { encryption_space: true } : {}),
    };
  }

  normalizeE2eeIndexes(tables);
  expandE2eeKeysTables(tables);
  validateE2eeSchema(tables);

  return tables;
}

// ============================================================================
// E2EE schema normalization and validation.
//
// Mirrors `crates/jazz-tools/src/query_manager/types/e2ee_schema.rs`; the two
// must emit identical schemas (column order is normative — it feeds the schema
// hash, which is computed by the Rust side).
// ============================================================================

const E2EE_KEYS_TABLE_SUFFIX = "$keys";

const sessionAuthenticated = (): PolicyExpr => ({
  type: "SessionIsNotNull",
  path: ["user_id"],
});

function e2eeKeysTable(spaceTable: string): TableSchema {
  return {
    columns: [
      {
        name: "space_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: spaceTable,
      },
      { name: "key_id", column_type: { type: "Uuid" }, nullable: false },
      { name: "recipient_user_id", column_type: { type: "Uuid" }, nullable: false },
      { name: "recipient_public_key", column_type: { type: "Text" }, nullable: false },
      { name: "sealed_key", column_type: { type: "Bytea" }, nullable: false },
    ],
    policies: {
      select: { using: { type: "True" } },
      insert: { with_check: sessionAuthenticated() },
      // update intentionally absent: key rows are immutable.
      delete: { using: sessionAuthenticated() },
    },
  };
}

function normalizeE2eeIndexes(tables: Record<string, TableSchema>): void {
  for (const table of Object.values(tables)) {
    const hasEncrypted = table.columns.some((c) => c.encrypted_with);
    if (!hasEncrypted || table.indexed_columns) {
      continue;
    }
    table.indexed_columns = table.columns.filter((c) => !c.encrypted_with).map((c) => c.name);
  }
}

function expandE2eeKeysTables(tables: Record<string, TableSchema>): void {
  for (const [name, table] of Object.entries(tables)) {
    const keysName = `${name}${E2EE_KEYS_TABLE_SUFFIX}`;
    if (table.encryption_space && !tables[keysName]) {
      tables[keysName] = e2eeKeysTable(name);
    }
  }
}

function validateE2eeSchema(tables: Record<string, TableSchema>): void {
  for (const [name, table] of Object.entries(tables)) {
    if (name.includes("$")) {
      const base = name.endsWith(E2EE_KEYS_TABLE_SUFFIX)
        ? name.slice(0, -E2EE_KEYS_TABLE_SUFFIX.length)
        : null;
      const isGeneratedCompanion =
        base !== null && !base.includes("$") && tables[base]?.encryption_space === true;
      if (!isGeneratedCompanion) {
        throw new Error(`Table "${name}": "$" is reserved for framework tables.`);
      }
    }

    for (const col of table.columns) {
      const spaceRef = col.encrypted_with;
      if (!spaceRef) continue;
      const refCol = table.columns.find((c) => c.name === spaceRef);
      if (!refCol) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" names unknown ref column "${spaceRef}".`,
        );
      }
      if (refCol.nullable) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" requires a non-nullable ref column "${spaceRef}".`,
        );
      }
      if (!refCol.references) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" requires "${spaceRef}" to be a ref column.`,
        );
      }
      const target = tables[refCol.references];
      if (!target) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" references unknown table "${refCol.references}".`,
        );
      }
      if (!target.encryption_space) {
        throw new Error(
          `Table "${name}": encrypted column "${col.name}" references "${refCol.references}", which is not an encryption space.`,
        );
      }
      if (table.indexed_columns?.includes(col.name)) {
        throw new Error(`Table "${name}": encrypted column "${col.name}" cannot be indexed.`);
      }
    }
  }
}

/**
 * Reject policies that reference encrypted columns. Called after permissions
 * from `permissions.ts` are merged into the wasm schema.
 */
export function validateE2eeSchemaPolicies(tables: Record<string, TableSchema>): void {
  const referencesColumn = (expr: PolicyExpr | undefined, column: string): boolean => {
    if (!expr || typeof expr !== "object") return false;
    if ("column" in expr && expr.column === column) return true;
    if ("via_column" in expr && expr.via_column === column) return true;
    if ("exprs" in expr && Array.isArray(expr.exprs)) {
      return expr.exprs.some((e) => referencesColumn(e, column));
    }
    if ("expr" in expr) return referencesColumn(expr.expr, column);
    if ("condition" in expr) return referencesColumn(expr.condition, column);
    return false;
  };

  for (const [name, table] of Object.entries(tables)) {
    for (const col of table.columns) {
      if (!col.encrypted_with) continue;
      const policies = table.policies ?? {};
      for (const op of ["select", "insert", "update", "delete"] as const) {
        const policy = policies[op];
        for (const clause of [policy?.using, policy?.with_check]) {
          if (clause && referencesColumn(clause, col.name)) {
            throw new Error(`Table "${name}": policy references encrypted column "${col.name}".`);
          }
        }
      }
    }
  }
}
