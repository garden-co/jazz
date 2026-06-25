import type {
  ColumnDescriptor,
  ColumnType as WasmColumnType,
  WasmSchema,
} from "../drivers/types.js";

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

export function columnTypeSignature(columnType: WasmColumnType): string {
  return JSON.stringify(columnType);
}

function columnsEqual(left: ColumnDescriptor, right: ColumnDescriptor): boolean {
  return (
    left.name === right.name &&
    left.nullable === right.nullable &&
    left.references === right.references &&
    left.merge_strategy === right.merge_strategy &&
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
