import { blake3 } from "@noble/hashes/blake3.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import type { ColumnDescriptor, ColumnType, Value, WasmSchema } from "./drivers/types.js";

type HashSink = ReturnType<typeof blake3.create>;

const textEncoder = new TextEncoder();

// Keep this encoder byte-for-byte in sync with SchemaHash::compute and its
// helper functions in crates/jazz-tools/src/query_manager/types/branch.rs.
export function computeSchemaHash(schema: WasmSchema): string {
  const hasher = blake3.create();
  const tableNames = Object.keys(schema).sort(compareStrings);

  for (const tableName of tableNames) {
    const tableSchema = schema[tableName];
    if (!tableSchema) {
      continue;
    }

    updateString(hasher, tableName);
    updateByte(hasher, 0);
    hashRowDescriptor(hasher, tableSchema.columns);

    if (tableSchema.indexed_columns) {
      updateByte(hasher, 1);
      for (const column of [...tableSchema.indexed_columns].sort(compareStrings)) {
        updateString(hasher, column);
        updateByte(hasher, 0);
      }
    }
  }

  return bytesToHex(hasher.digest());
}

function hashRowDescriptor(hasher: HashSink, descriptor: readonly ColumnDescriptor[]): void {
  for (const column of descriptor) {
    hashColumnDescriptor(hasher, column);
  }
}

function hashColumnDescriptor(hasher: HashSink, column: ColumnDescriptor): void {
  updateString(hasher, column.name);
  updateByte(hasher, 0);
  hashColumnType(hasher, column.column_type);
  updateByte(hasher, column.nullable ? 1 : 0);

  if (column.references) {
    updateByte(hasher, 1);
    updateString(hasher, column.references);
  } else {
    updateByte(hasher, 0);
  }

  if (column.default) {
    updateByte(hasher, 1);
    hashValue(hasher, column.default);
  } else {
    updateByte(hasher, 0);
  }

  if (column.merge_strategy) {
    updateByte(hasher, 1);
    switch (column.merge_strategy) {
      case "Counter":
        updateByte(hasher, 1);
        break;
      case "GSet":
        updateByte(hasher, 2);
        break;
    }
  } else {
    updateByte(hasher, 0);
  }

  updateByte(hasher, 0);
}

function hashValue(hasher: HashSink, value: Value): void {
  switch (value.type) {
    case "Integer":
      updateByte(hasher, 1);
      updateI32(hasher, value.value);
      break;
    case "BigInt":
      updateByte(hasher, 2);
      updateI64(hasher, value.value);
      break;
    case "Double":
      updateByte(hasher, 10);
      updateF64(hasher, value.value);
      break;
    case "Boolean":
      updateBytes(hasher, Uint8Array.of(3, value.value ? 1 : 0));
      break;
    case "Text":
      updateByte(hasher, 4);
      updateString(hasher, value.value);
      updateByte(hasher, 0);
      break;
    case "Timestamp":
      updateByte(hasher, 5);
      updateU64(hasher, value.value);
      break;
    case "Uuid":
      updateByte(hasher, 6);
      updateBytes(hasher, uuidToBytes(value.value));
      break;
    case "Bytea":
      updateByte(hasher, 11);
      updateLengthPrefixedBytes(hasher, bytesValueToBytes(value.value));
      break;
    case "Array":
      updateByte(hasher, 7);
      updateU64(hasher, value.value.length);
      for (const inner of value.value) {
        hashValue(hasher, inner);
      }
      break;
    case "Row":
      updateByte(hasher, 8);
      updateU64(hasher, value.value.values.length);
      for (const inner of value.value.values) {
        hashValue(hasher, inner);
      }
      break;
    case "Null":
      updateByte(hasher, 9);
      break;
    default:
      throw new Error(
        `Unsupported schema default value type "${(value as { type?: string }).type}".`,
      );
  }
}

function hashColumnType(hasher: HashSink, columnType: ColumnType): void {
  switch (columnType.type) {
    case "Integer":
      updateByte(hasher, 1);
      break;
    case "BigInt":
      updateByte(hasher, 2);
      break;
    case "Double":
      updateByte(hasher, 10);
      break;
    case "Boolean":
      updateByte(hasher, 3);
      break;
    case "Text":
      updateByte(hasher, 4);
      break;
    case "Enum": {
      updateByte(hasher, 9);
      const variants = [...new Set([...columnType.variants].sort(compareStrings))];
      updateU64(hasher, variants.length);
      for (const variant of variants) {
        updateString(hasher, variant);
        updateByte(hasher, 0);
      }
      break;
    }
    case "Timestamp":
      updateByte(hasher, 5);
      break;
    case "Uuid":
      updateByte(hasher, 6);
      break;
    case "Bytea":
      updateByte(hasher, 10);
      break;
    case "Json":
      updateByte(hasher, 11);
      if (columnType.schema !== undefined) {
        updateByte(hasher, 1);
        const encoded = textEncoder.encode(canonicalJson(columnType.schema));
        updateU64(hasher, encoded.length);
        updateBytes(hasher, encoded);
      } else {
        updateByte(hasher, 0);
      }
      break;
    case "Array":
      updateByte(hasher, 7);
      hashColumnType(hasher, columnType.element);
      break;
    case "Row":
      updateByte(hasher, 8);
      hashRowDescriptor(hasher, rowColumns(columnType.columns));
      break;
    default:
      throw new Error(
        `Unsupported schema column type "${(columnType as { type?: string }).type}".`,
      );
  }
}

function rowColumns(columns: unknown): readonly ColumnDescriptor[] {
  if (Array.isArray(columns)) {
    return columns as readonly ColumnDescriptor[];
  }

  if (typeof columns === "object" && columns !== null) {
    const maybeDescriptor = columns as { columns?: unknown };
    if (Array.isArray(maybeDescriptor.columns)) {
      return maybeDescriptor.columns as readonly ColumnDescriptor[];
    }
  }

  throw new Error("Row column type must include a columns array.");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    const encoded = JSON.stringify(value);
    if (encoded === undefined) {
      throw new Error("JSON schema values used for schema hashes must be JSON-serializable.");
    }
    return encoded;
  }

  if (Array.isArray(value)) {
    return `[${value.map((entry) => canonicalJson(entry)).join(",")}]`;
  }

  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, entry]) => entry !== undefined)
    .sort(([left], [right]) => compareStrings(left, right));

  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
}

function updateString(hasher: HashSink, value: string): void {
  updateBytes(hasher, textEncoder.encode(value));
}

function compareStrings(left: string, right: string): number {
  const leftBytes = textEncoder.encode(left);
  const rightBytes = textEncoder.encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);

  for (let index = 0; index < length; index++) {
    const difference = leftBytes[index]! - rightBytes[index]!;
    if (difference !== 0) {
      return difference;
    }
  }

  return leftBytes.length - rightBytes.length;
}

function updateByte(hasher: HashSink, value: number): void {
  updateBytes(hasher, Uint8Array.of(value));
}

function updateLengthPrefixedBytes(hasher: HashSink, bytes: Uint8Array): void {
  updateU64(hasher, bytes.length);
  updateBytes(hasher, bytes);
}

function updateBytes(hasher: HashSink, bytes: Uint8Array): void {
  hasher.update(bytes);
}

function updateI32(hasher: HashSink, value: number): void {
  const bytes = new Uint8Array(4);
  new DataView(bytes.buffer).setInt32(0, value, true);
  updateBytes(hasher, bytes);
}

function updateI64(hasher: HashSink, value: number | bigint): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigInt64(0, BigInt(value), true);
  updateBytes(hasher, bytes);
}

function updateU64(hasher: HashSink, value: number | bigint): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  updateBytes(hasher, bytes);
}

function updateF64(hasher: HashSink, value: number): void {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  updateBytes(hasher, bytes);
}

function uuidToBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "").toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(hex)) {
    throw new Error(`Invalid UUID value "${value}" in schema hash input.`);
  }

  return hexToBytes(hex);
}

function bytesValueToBytes(value: Uint8Array | readonly number[] | string): Uint8Array {
  if (value instanceof Uint8Array) {
    return value;
  }

  if (typeof value === "string") {
    return hexToBytes(value);
  }

  return Uint8Array.from(value);
}

function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(hex)) {
    throw new Error("Expected an even-length hex string.");
  }

  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index++) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
