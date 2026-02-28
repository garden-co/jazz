import type { WasmSchema } from "./types.js";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isWasmSchema(value: unknown): value is WasmSchema {
  return isRecord(value);
}

export function serializeRuntimeSchema(schema: WasmSchema): string {
  return JSON.stringify(schema);
}

export function normalizeRuntimeSchemaJson(schemaJson: string): string {
  const parsed = JSON.parse(schemaJson) as unknown;
  if (!isWasmSchema(parsed)) {
    throw new Error("Invalid schema JSON payload.");
  }
  return JSON.stringify(parsed);
}
