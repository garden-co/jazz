import type {
  OperationPolicy as WasmOperationPolicy,
  PolicyExpr as WasmPolicyExpr,
  PolicyValue as WasmPolicyValue,
  TablePolicies as WasmTablePolicies,
  Value as WasmValue,
  WasmSchema,
} from "./drivers/types.js";
import type {
  OperationPolicy,
  PolicyExpr,
  PolicyLiteralValue,
  PolicyValue,
  Schema,
  TablePolicies,
} from "./schema.js";

export type CompiledPermissionsMap = Record<string, TablePolicies>;

const UUID_LIKE_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isWasmValueLike(value: unknown): value is WasmValue {
  if (!isRecord(value) || typeof value.type !== "string") {
    return false;
  }

  return [
    "Integer",
    "BigInt",
    "Double",
    "Boolean",
    "Text",
    "Timestamp",
    "Uuid",
    "Bytea",
    "Array",
    "Row",
    "Null",
  ].includes(value.type);
}

function normalizeLegacyTaggedValue(type: string, value: unknown): WasmValue {
  switch (type) {
    case "Null":
      return { type: "Null" };
    case "Array":
      return {
        type: "Array",
        value: Array.isArray(value) ? value.map((entry) => normalizeWasmLiteral(entry)) : [],
      };
    case "Row": {
      const row = isRecord(value) ? value : {};
      return {
        type: "Row",
        value: {
          id: typeof row.id === "string" ? row.id : undefined,
          values: Array.isArray(row.values)
            ? row.values.map((entry) => normalizeWasmLiteral(entry))
            : [],
        },
      };
    }
    case "Bytea":
      return {
        type: "Bytea",
        value:
          value instanceof Uint8Array ? value : new Uint8Array(Array.isArray(value) ? value : []),
      };
    case "Integer":
    case "BigInt":
    case "Double":
    case "Boolean":
    case "Text":
    case "Timestamp":
    case "Uuid":
      return { type, value } as WasmValue;
    default:
      throw new Error(`Unsupported tagged permissions literal "${type}".`);
  }
}

function normalizeWasmLiteral(value: unknown): WasmValue {
  if (value === null) {
    return { type: "Null" };
  }
  if (value instanceof Date) {
    return { type: "Timestamp", value: value.getTime() };
  }
  if (value instanceof Uint8Array) {
    return { type: "Bytea", value };
  }
  if (typeof value === "boolean") {
    return { type: "Boolean", value };
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("Permissions literals only support finite numbers.");
    }
    if (!Number.isInteger(value)) {
      return { type: "Double", value };
    }
    if (value >= -2147483648 && value <= 2147483647) {
      return { type: "Integer", value };
    }
    if (Number.isSafeInteger(value)) {
      return { type: "BigInt", value };
    }
    return { type: "Double", value };
  }
  if (typeof value === "bigint") {
    const asNumber = Number(value);
    if (!Number.isSafeInteger(asNumber)) {
      throw new Error("Permissions bigint literals must fit into a safe JavaScript integer.");
    }
    return { type: "BigInt", value: asNumber };
  }
  if (typeof value === "string") {
    return UUID_LIKE_RE.test(value) ? { type: "Uuid", value } : { type: "Text", value };
  }
  if (Array.isArray(value)) {
    return { type: "Array", value: value.map((entry) => normalizeWasmLiteral(entry)) };
  }
  if (isWasmValueLike(value)) {
    return value;
  }
  if (isRecord(value) && Object.keys(value).length === 1) {
    const [legacyType, legacyValue] = Object.entries(value)[0]!;
    return normalizeLegacyTaggedValue(legacyType, legacyValue);
  }

  throw new Error(
    "Permissions literals must use scalars, arrays, Date, Uint8Array, or tagged Value objects.",
  );
}

function normalizePolicyValueForWasm(value: PolicyValue): WasmPolicyValue {
  if (value.type === "SessionRef") {
    return value;
  }
  return {
    type: "Literal",
    value: normalizeWasmLiteral(value.value),
  };
}

function normalizePolicyLiteralValueForWasm(value: PolicyLiteralValue): WasmValue {
  return normalizeWasmLiteral(value.value);
}

function normalizePolicyExprForWasm(expr: PolicyExpr): WasmPolicyExpr {
  switch (expr.type) {
    case "Cmp":
      return {
        type: "Cmp",
        column: expr.column,
        op: expr.op,
        value: normalizePolicyValueForWasm(expr.value),
      };
    case "SessionCmp":
      return {
        type: "SessionCmp",
        path: expr.path,
        op: expr.op,
        value: normalizePolicyLiteralValueForWasm(expr.value),
      };
    case "IsNull":
    case "IsNotNull":
      return expr;
    case "SessionIsNull":
    case "SessionIsNotNull":
      return expr;
    case "Contains":
      return {
        type: "Contains",
        column: expr.column,
        value: normalizePolicyValueForWasm(expr.value),
      };
    case "SessionContains":
      return {
        type: "SessionContains",
        path: expr.path,
        value: normalizePolicyLiteralValueForWasm(expr.value),
      };
    case "In":
      return expr;
    case "InList":
      return {
        type: "InList",
        column: expr.column,
        values: expr.values.map((value) => normalizePolicyValueForWasm(value)),
      };
    case "SessionInList":
      return {
        type: "SessionInList",
        path: expr.path,
        values: expr.values.map((value) => normalizePolicyLiteralValueForWasm(value)),
      };
    case "Exists":
      return {
        type: "Exists",
        table: expr.table,
        condition: normalizePolicyExprForWasm(expr.condition),
      };
    case "ExistsRel":
    case "Inherits":
    case "InheritsReferencing":
    case "True":
    case "False":
      return expr as unknown as WasmPolicyExpr;
    case "And":
      return {
        type: "And",
        exprs: expr.exprs.map((child) => normalizePolicyExprForWasm(child)),
      };
    case "Or":
      return {
        type: "Or",
        exprs: expr.exprs.map((child) => normalizePolicyExprForWasm(child)),
      };
    case "Not":
      return {
        type: "Not",
        expr: normalizePolicyExprForWasm(expr.expr),
      };
    default: {
      const _never: never = expr;
      return _never;
    }
  }
}

function normalizeOperationPolicyForWasm(
  policy?: OperationPolicy,
): WasmOperationPolicy | undefined {
  if (!policy) {
    return undefined;
  }

  const normalized: WasmOperationPolicy = {};
  if (policy.using) {
    normalized.using = normalizePolicyExprForWasm(policy.using);
  }
  if (policy.with_check) {
    normalized.with_check = normalizePolicyExprForWasm(policy.with_check);
  }
  return normalized;
}

function validatePermissionTables(
  schemaTableNames: readonly string[],
  compiledPermissions: CompiledPermissionsMap,
): void {
  const knownTables = new Set(schemaTableNames);
  const unknownTables = Object.keys(compiledPermissions).filter(
    (tableName) => !knownTables.has(tableName),
  );

  if (unknownTables.length > 0) {
    throw new Error(
      `permissions.ts defines permissions for unknown table(s): ${unknownTables.join(", ")}.`,
    );
  }
}

export function validatePermissionsAgainstSchema(
  schemaTableNames: readonly string[],
  compiledPermissions: CompiledPermissionsMap,
): void {
  validatePermissionTables(schemaTableNames, compiledPermissions);
}

export function normalizePermissionsForWasm(
  compiledPermissions: CompiledPermissionsMap,
): Record<string, WasmTablePolicies> {
  const normalized: Record<string, WasmTablePolicies> = {};
  for (const [tableName, tablePolicies] of Object.entries(compiledPermissions)) {
    normalized[tableName] = {
      select: normalizeOperationPolicyForWasm(tablePolicies.select),
      insert: normalizeOperationPolicyForWasm(tablePolicies.insert),
      update: normalizeOperationPolicyForWasm(tablePolicies.update),
      delete: normalizeOperationPolicyForWasm(tablePolicies.delete),
    };
  }
  return normalized;
}

export function mergePermissionsIntoSchema(
  schema: Schema,
  compiledPermissions: CompiledPermissionsMap,
): Schema {
  validatePermissionTables(
    schema.tables.map((table) => table.name),
    compiledPermissions,
  );

  return {
    tables: schema.tables.map((table) => {
      const external = compiledPermissions[table.name];
      if (!external) {
        return table;
      }

      return {
        ...table,
        policies: external,
      };
    }),
  };
}

export function mergePermissionsIntoWasmSchema(
  schema: WasmSchema,
  compiledPermissions: CompiledPermissionsMap,
): WasmSchema {
  validatePermissionTables(Object.keys(schema), compiledPermissions);
  const normalizedPermissions = normalizePermissionsForWasm(compiledPermissions);

  const merged: WasmSchema = {};
  for (const [tableName, table] of Object.entries(schema)) {
    merged[tableName] = {
      ...table,
      policies: normalizedPermissions[tableName] ?? table.policies,
    };
  }
  return merged;
}
