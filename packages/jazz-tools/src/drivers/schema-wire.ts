import type { WasmSchema } from "./types.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import { isRecord } from "../runtime/util.js";

interface SerializeRuntimeSchemaOptions {
  loadedPolicyBundle?: boolean;
}

const runtimeSchemaCacheKeys = new WeakMap<WasmSchema, Map<boolean, string>>();

export function normalizeRuntimeSchema(schema: unknown): WasmSchema {
  if (schema instanceof Map) {
    return Object.fromEntries(schema.entries()) as WasmSchema;
  }
  if (!isRecord(schema)) {
    throw new Error("Invalid runtime schema value.");
  }
  return schema as WasmSchema;
}

/**
 * Schemas can contain values that JSON.stringify cannot serialize directly:
 * Uint8Array defaults become regular arrays, and BigInt defaults become
 * decimal strings.
 */
export function runtimeSchemaJsonReplacer(key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "number" && !Number.isFinite(value)) {
    throw new Error(`Cannot serialize non-finite number in schema JSON (key "${key}"): ${value}.`);
  }
  return value;
}

function sortSchemaTables(schema: WasmSchema): WasmSchema {
  return Object.fromEntries(
    Object.keys(schema)
      .sort()
      .map((tableName) => [tableName, schema[tableName]]),
  ) as WasmSchema;
}

// The sorted tables serialize once per schema object; the schema source and the runtime
// envelope are spliced around that text. Byte-identical to stringifying each envelope
// whole: the replacer returns objects unchanged, and keys keep insertion order.
const sortedTablesJson = new WeakMap<WasmSchema, string>();

function tablesJson(schema: WasmSchema): string {
  let json = sortedTablesJson.get(schema);
  if (json === undefined) {
    json = JSON.stringify(sortSchemaTables(schema), runtimeSchemaJsonReplacer);
    sortedTablesJson.set(schema, json);
  }
  return json;
}

/**
 * Serialize the developer-authored schema AST for Rust compilation, with the
 * migrations the app ships if it attaches them.
 */
export function serializeSchemaSource(schema: WasmSchema, migrations?: ClientMigrations): string {
  const tables = tablesJson(schema);
  if (!migrations) return `{"tables":${tables}}`;
  return `{"tables":${tables},"migrations":${JSON.stringify(migrations, runtimeSchemaJsonReplacer)}}`;
}

/** `{ __jazzRuntimeSchema: 1, schema, loadedPolicyBundle }`, with the schema's tables sorted. */
export function serializeRuntimeSchema(
  schema: WasmSchema,
  options?: SerializeRuntimeSchemaOptions,
): string {
  const loadedPolicyBundle = options?.loadedPolicyBundle ?? false;
  return `{"__jazzRuntimeSchema":1,"schema":${tablesJson(schema)},"loadedPolicyBundle":${loadedPolicyBundle ? "true" : "false"}}`;
}

export function getRuntimeSchemaCacheKey(
  schema: WasmSchema,
  options?: SerializeRuntimeSchemaOptions,
): string {
  const loadedPolicyBundle = options?.loadedPolicyBundle ?? false;
  let keysByPolicyBundle = runtimeSchemaCacheKeys.get(schema);

  if (!keysByPolicyBundle) {
    keysByPolicyBundle = new Map();
    runtimeSchemaCacheKeys.set(schema, keysByPolicyBundle);
  }

  const cached = keysByPolicyBundle.get(loadedPolicyBundle);
  if (cached !== undefined) {
    return cached;
  }

  const key = serializeRuntimeSchema(schema, options);
  keysByPolicyBundle.set(loadedPolicyBundle, key);
  return key;
}
