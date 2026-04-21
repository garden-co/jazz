import type { WasmSchema } from "./drivers/types.js";

export interface WasmSchemaSource {
  wasmSchema: WasmSchema;
}

export interface QuerySchemaSource {
  _schema: WasmSchema;
}

export type SchemaSourceInput = WasmSchema | WasmSchemaSource | QuerySchemaSource;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isTableSchema(value: unknown): boolean {
  return isRecord(value) && Array.isArray(value.columns);
}

export function isWasmSchema(value: unknown): value is WasmSchema {
  return (
    isRecord(value) &&
    !("_schema" in value) &&
    !("wasmSchema" in value) &&
    Object.values(value).every((table) => isTableSchema(table))
  );
}

export function resolveSchemaSource(input: SchemaSourceInput): WasmSchema {
  if (isWasmSchema(input)) {
    return input;
  }
  if (isRecord(input) && "_schema" in input && isWasmSchema(input._schema)) {
    return input._schema;
  }
  if (isRecord(input) && "wasmSchema" in input && isWasmSchema(input.wasmSchema)) {
    return input.wasmSchema;
  }

  throw new Error(
    "Invalid schema source. Pass a WasmSchema, a generated app object, or a generated query/table object.",
  );
}
