import type { WasmSchema } from "./types.js";
import type { ClientMigrations } from "../runtime/client-migrations.js";
import { isRecord } from "../runtime/util.js";

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

/** Serialize the developer-authored schema AST for Rust compilation. */
export function serializeSchemaSource(schema: WasmSchema): string {
  return JSON.stringify(
    {
      tables: sortSchemaTables(schema),
    },
    runtimeSchemaJsonReplacer,
  );
}

/**
 * Serialize an app for a React Native native session: its schema AST, as
 * {@link serializeSchemaSource} does, and the migrations it ships (`null`
 * for an app that ships none). The native relay opens its store along them,
 * as every other client does, so it stores each column where the server
 * does.
 */
export function serializeNativeAppSource(
  schema: WasmSchema,
  migrations: ClientMigrations | undefined,
): string {
  return JSON.stringify(
    { schema: { tables: sortSchemaTables(schema) }, migrations: migrations ?? null },
    runtimeSchemaJsonReplacer,
  );
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
