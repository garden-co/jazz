import type { WasmSchema } from "./drivers/types.js";
import { assertSchemaNameAllowed } from "./schema-name.js";
import { commonJsWrapped, commonJsWrapperError } from "./module-interop.js";

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

function assertWasmSchemaNames(schema: WasmSchema): void {
  for (const tableName of Object.keys(schema)) {
    assertSchemaNameAllowed(tableName);
  }
}

export function isWasmSchema(value: unknown): value is WasmSchema {
  return (
    isRecord(value) &&
    !("_schema" in value) &&
    !("wasmSchema" in value) &&
    Object.values(value).every((table) => isTableSchema(table))
  );
}

function resolvesAsSchema(value: unknown): boolean {
  try {
    resolveSchemaSource(value as SchemaSourceInput);
    return true;
  } catch {
    return false;
  }
}

export function resolveSchemaSource(input: SchemaSourceInput): WasmSchema {
  if (isWasmSchema(input)) {
    assertWasmSchemaNames(input);
    return input;
  }
  if (isRecord(input) && "_schema" in input && isWasmSchema(input._schema)) {
    assertWasmSchemaNames(input._schema);
    return input._schema;
  }
  if (isRecord(input) && "wasmSchema" in input && isWasmSchema(input.wasmSchema)) {
    assertWasmSchemaNames(input.wasmSchema);
    return input.wasmSchema;
  }
  if (isRecord(input)) {
    for (const name of Object.keys(input)) {
      assertSchemaNameAllowed(name);
    }
  }
  // A real table named `default` resolved above; this is a module wrapper.
  const wrapped = commonJsWrapped(input);
  if (wrapped !== undefined && resolvesAsSchema(wrapped)) throw commonJsWrapperError("schema.ts");

  throw new Error(
    "Invalid schema source. Pass a WasmSchema, a generated app object, or a generated query/table object.",
  );
}
