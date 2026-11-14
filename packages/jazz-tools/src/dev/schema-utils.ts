import { columnTypeSignature } from "../runtime/schema-metadata.js";
import { structuralValuesEqual } from "../runtime/structural-values.js";
import type { ColumnDescriptor, WasmSchema } from "../drivers/types.js";

export { columnTypeSignature };

const SHORT_SCHEMA_HASH_LENGTH = 12;

export function normalizeSchemaHashInput(hash: string, label: string): string {
  const normalized = hash.trim().toLowerCase();
  if (!/^[0-9a-f]{12,64}$/.test(normalized)) {
    throw new Error(`${label} must be a 12-64 character lowercase hex schema hash.`);
  }
  return normalized;
}

export function shortSchemaHash(hash: string): string {
  return normalizeSchemaHashInput(hash, "schema hash").slice(0, SHORT_SCHEMA_HASH_LENGTH);
}

/**
 * Rust's canonical composite-index order: lexicographic over column names
 * compared as UTF-8 bytes, i.e. by Unicode code point. JavaScript's `<` on
 * strings compares UTF-16 code units, which orders U+E000..U+FFFF after
 * astral characters and would diverge from Rust's schema hash.
 */
export function canonicalCompositeIndexOrder(indexes: readonly (readonly string[])[]): string[][] {
  return indexes
    .map((columns) => [...columns])
    .sort((left, right) => {
      for (let index = 0; index < Math.min(left.length, right.length); index++) {
        const order = compareCodePoints(left[index]!, right[index]!);
        if (order !== 0) {
          return order;
        }
      }
      return left.length - right.length;
    });
}

function compareCodePoints(left: string, right: string): number {
  const leftPoints = Array.from(left, (char) => char.codePointAt(0)!);
  const rightPoints = Array.from(right, (char) => char.codePointAt(0)!);
  for (let index = 0; index < Math.min(leftPoints.length, rightPoints.length); index++) {
    if (leftPoints[index] !== rightPoints[index]) {
      return leftPoints[index]! - rightPoints[index]!;
    }
  }
  return leftPoints.length - rightPoints.length;
}

function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    left.merge_strategy === right.merge_strategy &&
    structuralValuesEqual(left.default, right.default) &&
    columnTypeSignature(left.column_type) === columnTypeSignature(right.column_type)
  );
}

function indexedColumnsEqual(
  left: readonly string[] | undefined,
  right: readonly string[] | undefined,
): boolean {
  if (!left && !right) {
    return true;
  }
  if (!left || !right || left.length !== right.length) {
    return false;
  }

  const leftColumns = [...left].sort();
  const rightColumns = [...right].sort();
  return leftColumns.every((column, index) => column === rightColumns[index]);
}

function compositeIndexesEqual(
  left: readonly (readonly string[])[] | undefined,
  right: readonly (readonly string[])[] | undefined,
): boolean {
  const a = (left ?? []).map((columns) => JSON.stringify(columns)).sort();
  const b = (right ?? []).map((columns) => JSON.stringify(columns)).sort();
  return a.length === b.length && a.every((index, position) => index === b[position]);
}

export function tableSchemasEqual(
  left: WasmSchema[string] | undefined,
  right: WasmSchema[string] | undefined,
): boolean {
  if (!left || !right) {
    return false;
  }

  if (left.columns.length !== right.columns.length) {
    return false;
  }

  if (!indexedColumnsEqual(left.indexed_columns, right.indexed_columns)) {
    return false;
  }
  if (!compositeIndexesEqual(left.composite_indexes, right.composite_indexes)) {
    return false;
  }
  if (!indexedColumnsEqual(left.branchBy, right.branchBy)) {
    return false;
  }
  if (left.history !== right.history) {
    return false;
  }

  const leftColumns = [...left.columns].sort((a, b) => a.name.localeCompare(b.name));
  const rightColumns = [...right.columns].sort((a, b) => a.name.localeCompare(b.name));

  return leftColumns.every((column, index) => columnsEqual(column, rightColumns[index]!));
}

export function wasmSchemasEqual(left: WasmSchema, right: WasmSchema): boolean {
  const leftTableNames = Object.keys(left).sort();
  const rightTableNames = Object.keys(right).sort();

  if (leftTableNames.length !== rightTableNames.length) {
    return false;
  }

  return leftTableNames.every((tableName, index) => {
    if (tableName !== rightTableNames[index]) {
      return false;
    }
    return tableSchemasEqual(left[tableName], right[tableName]);
  });
}
