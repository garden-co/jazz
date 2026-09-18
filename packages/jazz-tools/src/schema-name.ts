const RESERVED_SCHEMA_NAMES: Record<string, true> = Object.fromEntries(
  [
    ...Object.getOwnPropertyNames(Object.prototype),
    "union",
    "wasmSchema",
    "schemaAst",
    "_schema",
    "exists",
  ].map((name) => [name, true]),
) as Record<string, true>;

export function assertSchemaNameAllowed(name: string): void {
  if (Object.prototype.hasOwnProperty.call(RESERVED_SCHEMA_NAMES, name)) {
    throw new Error(`Schema name "${name}" is reserved for Jazz schema controls.`);
  }
}
