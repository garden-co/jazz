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

export function serializeRuntimeSchema(
  schema: WasmSchema,
  options?: SerializeRuntimeSchemaOptions,
): string {
  const envelope: RuntimeSchemaEnvelope = {
    __jazzRuntimeSchema: 1,
    schema,
    loadedPolicyBundle: options?.loadedPolicyBundle ?? false,
  };
  return JSON.stringify(envelope, runtimeSchemaJsonReplacer);
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
