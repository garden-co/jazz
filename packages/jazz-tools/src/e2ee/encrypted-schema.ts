import type { WasmSchema } from "../drivers/types.js";
import type { Table } from "../schema.js";

export type EncryptionDeclaration = Readonly<{
  space: string;
  columns: readonly string[];
}>;

export type EncryptedSchema = Readonly<{
  logical: WasmSchema;
  scopes: ReadonlySet<string>;
  tables: ReadonlyMap<string, EncryptionDeclaration & { scope: string }>;
}>;

// Declarations belong to a schema, never to an account or a device. This also
// keeps query clones and app slices on the same logical/physical mapping.
export const encryptedSchemas = new WeakMap<WasmSchema, EncryptedSchema>();

// A projected encrypted row still needs its space identity for decryption.
// Keep this dependency out of the caller's selected result properties.
export const encryptedRowSpaces = new WeakMap<object, string>();

export function encryptedTableToPhysical(table: Table, declaration?: EncryptionDeclaration): Table {
  if (!declaration) return table;
  return {
    ...table,
    columns: table.columns.map((column) =>
        declaration.columns.includes(column.name)
          ? { name: column.name, sqlType: "BYTEA" as const, nullable: false }
          : column,
      ),
  };
}
