/**
 * Compute schema hashes compatible with Rust `SchemaHash::compute`.
 *
 * Algorithm:
 * - BLAKE3 over canonicalized schema structure
 * - Tables sorted by name
 * - Columns sorted by name
 * - Same type-tag encoding as Rust
 */

import { createHash } from "blake3";
import type { ColumnType, WasmSchema } from "../drivers/types.js";

type WasmColumn = WasmSchema["tables"][string]["columns"][number];

const encoder = new TextEncoder();
const ZERO = Uint8Array.of(0);

function updateUtf8(hasher: ReturnType<typeof createHash>, value: string): void {
  hasher.update(encoder.encode(value));
}

function updateByte(hasher: ReturnType<typeof createHash>, value: number): void {
  hasher.update(Uint8Array.of(value & 0xff));
}

function hashRowDescriptor(
  hasher: ReturnType<typeof createHash>,
  columns: readonly WasmColumn[],
): void {
  const sorted = [...columns].sort((a, b) => a.name.localeCompare(b.name));
  for (const col of sorted) {
    hashColumnDescriptor(hasher, col);
  }
}

function hashColumnDescriptor(hasher: ReturnType<typeof createHash>, col: WasmColumn): void {
  // Name + delimiter
  updateUtf8(hasher, col.name);
  hasher.update(ZERO);

  // Type
  hashColumnType(hasher, col.column_type);

  // Nullable flag
  updateByte(hasher, col.nullable ? 1 : 0);

  // References (FK)
  if (col.references) {
    updateByte(hasher, 1);
    updateUtf8(hasher, col.references);
  } else {
    updateByte(hasher, 0);
  }

  // Trailing delimiter
  hasher.update(ZERO);
}

function hashColumnType(hasher: ReturnType<typeof createHash>, colType: ColumnType): void {
  switch (colType.type) {
    case "Integer":
      updateByte(hasher, 1);
      return;
    case "BigInt":
      updateByte(hasher, 2);
      return;
    case "Boolean":
      updateByte(hasher, 3);
      return;
    case "Text":
      updateByte(hasher, 4);
      return;
    case "Timestamp":
      updateByte(hasher, 5);
      return;
    case "Uuid":
      updateByte(hasher, 6);
      return;
    case "Array":
      updateByte(hasher, 7);
      hashColumnType(hasher, colType.element);
      return;
    case "Row":
      updateByte(hasher, 8);
      hashRowDescriptor(hasher, colType.columns as WasmColumn[]);
      return;
  }
}

/**
 * Compute a 64-char lowercase hex schema hash.
 */
export function computeSchemaHash(schema: WasmSchema): string {
  const hasher = createHash();
  const tableNames = Object.keys(schema.tables).sort();

  for (const tableName of tableNames) {
    const table = schema.tables[tableName];

    // Table name + delimiter
    updateUtf8(hasher, tableName);
    hasher.update(ZERO);

    // Row descriptor hashing (columns sorted by name)
    hashRowDescriptor(hasher, table.columns);
  }

  return hasher.digest("hex");
}
