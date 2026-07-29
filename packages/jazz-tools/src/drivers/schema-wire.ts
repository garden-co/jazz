import type { WasmSchema } from "./types.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isWasmSchema(value: unknown): value is WasmSchema {
  return isRecord(value);
}

interface RuntimeSchemaEnvelope {
  __jazzRuntimeSchema: 1;
  schema: WasmSchema;
  loadedPolicyBundle: boolean;
}

interface SerializeRuntimeSchemaOptions {
  loadedPolicyBundle?: boolean;
}

const runtimeSchemaCacheKeys = new WeakMap<WasmSchema, Map<boolean, string>>();

function isRuntimeSchemaEnvelope(value: unknown): value is RuntimeSchemaEnvelope {
  return (
    isRecord(value) &&
    value.__jazzRuntimeSchema === 1 &&
    isWasmSchema(value.schema) &&
    typeof value.loadedPolicyBundle === "boolean"
  );
}

export function normalizeRuntimeSchema(schema: unknown): WasmSchema {
  if (schema instanceof Map) {
    return Object.fromEntries(schema.entries()) as WasmSchema;
  }
  if (!isWasmSchema(schema)) {
    throw new Error("Invalid runtime schema value.");
  }
  return schema;
}

/**
 * Schemas can contain Uint8Array values (as defaults for bytea columns).
 * Since they are not serializable by JSON.stringify, we need to replace them
 * with regular arrays.
 */
function runtimeSchemaJsonReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return Array.from(value);
  }
  return value;
}

/**
 * Order object keys deterministically, at every depth.
 *
 * The serialized form is used as a schema identity: two clients that
 * describe the same schema must produce the same string. Property
 * insertion order is not part of what a schema means, but `JSON.stringify`
 * preserves it, so two equivalent objects built in different orders
 * previously serialized differently and read as incompatible schemas.
 *
 * Arrays are left alone deliberately. Column order is positionally
 * significant, so reordering one would change what the schema means
 * rather than normalize how it is written.
 */
function canonicalizeSchemaValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalizeSchemaValue);
  }
  if (value instanceof Uint8Array) {
    return value;
  }
  if (isRecord(value)) {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalizeSchemaValue(value[key])]),
    );
  }
  return value;
}

function sortSchemaTables(schema: WasmSchema): WasmSchema {
  return canonicalizeSchemaValue(schema) as WasmSchema;
}

export function serializeRuntimeSchema(
  schema: WasmSchema,
  options?: SerializeRuntimeSchemaOptions,
): string {
  const envelope: RuntimeSchemaEnvelope = {
    __jazzRuntimeSchema: 1,
    schema: sortSchemaTables(schema),
    loadedPolicyBundle: options?.loadedPolicyBundle ?? false,
  };
  return JSON.stringify(envelope, runtimeSchemaJsonReplacer);
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

export function normalizeRuntimeSchemaJson(schemaJson: string): string {
  const parsed = JSON.parse(schemaJson) as unknown;
  if (isRuntimeSchemaEnvelope(parsed)) {
    return JSON.stringify(parsed);
  }
  if (!isWasmSchema(parsed)) {
    throw new Error("Invalid schema JSON payload.");
  }
  return serializeRuntimeSchema(parsed);
}
