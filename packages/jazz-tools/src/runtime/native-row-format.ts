import type { ColumnDescriptor, ColumnType, Value, WasmRow } from "../drivers/types.js";
import { isProvenanceMagicTimestampColumn } from "../magic-columns.js";

const textDecoder = new TextDecoder();

function uuidString(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(
    16,
    20,
  )}-${hex.slice(20)}`;
}

function fixedSize(type: ColumnType): number | null {
  switch (type.type) {
    case "Integer":
      return 4;
    case "BigInt":
    case "Double":
    case "Timestamp":
      return 8;
    case "Boolean":
      return 1;
    case "Uuid":
      return 16;
    case "Enum":
      return type.variants.length <= 256 ? 1 : null;
    default:
      return null;
  }
}

type LayoutColumn = {
  fixedOffset?: number;
  fixedTotalSize?: number;
  fixedValueSize?: number;
  variableIndex?: number;
  nullable: boolean;
};

type RowLayout = {
  columns: LayoutColumn[];
  fixedSectionSize: number;
  variableColumnCount: number;
};

const layoutCache = new WeakMap<readonly ColumnDescriptor[], RowLayout>();

function compileLayout(columns: readonly ColumnDescriptor[]): RowLayout {
  const layoutColumns: LayoutColumn[] = [];
  let fixedOffset = 0;
  let variableIndex = 0;

  for (const column of columns) {
    const size = fixedSize(column.column_type);
    if (size === null) {
      layoutColumns.push({ variableIndex, nullable: column.nullable });
      variableIndex += 1;
    } else {
      layoutColumns.push({
        fixedOffset,
        fixedTotalSize: size + (column.nullable ? 1 : 0),
        fixedValueSize: size,
        nullable: column.nullable,
      });
      fixedOffset += size + (column.nullable ? 1 : 0);
    }
  }

  return {
    columns: layoutColumns,
    fixedSectionSize: fixedOffset,
    variableColumnCount: variableIndex,
  };
}

function getLayout(columns: readonly ColumnDescriptor[]): RowLayout {
  const cached = layoutCache.get(columns);
  if (cached) {
    return cached;
  }
  const layout = compileLayout(columns);
  layoutCache.set(columns, layout);
  return layout;
}

function columnBytes(
  row: Uint8Array,
  columns: readonly ColumnDescriptor[],
  layout: RowLayout,
  columnIndex: number,
): { bytes: Uint8Array; isNull: boolean } {
  const columnLayout = layout.columns[columnIndex];
  if (!columnLayout) {
    throw new Error(`Column index ${columnIndex} out of bounds`);
  }

  if (columnLayout.variableIndex === undefined) {
    const offset = columnLayout.fixedOffset!;
    const totalSize = columnLayout.fixedTotalSize!;
    const valueSize = columnLayout.fixedValueSize!;
    if (offset + totalSize > row.byteLength) {
      throw new Error(`Native row is too short for column ${columns[columnIndex]?.name}`);
    }
    if (columnLayout.nullable) {
      return {
        bytes: row.subarray(offset + 1, offset + totalSize),
        isNull: row[offset] === 0,
      };
    }
    return { bytes: row.subarray(offset, offset + valueSize), isNull: false };
  }

  const fixedSizeBytes = layout.fixedSectionSize;
  const offsetTableSize = layout.variableColumnCount > 1 ? (layout.variableColumnCount - 1) * 4 : 0;
  const varDataStart = fixedSizeBytes + offsetTableSize;
  if (varDataStart > row.byteLength) {
    throw new Error("Native row variable section is truncated");
  }

  const varIndex = columnLayout.variableIndex;
  const view = new DataView(row.buffer, row.byteOffset, row.byteLength);
  const startOffset =
    varIndex === 0 ? 0 : view.getUint32(fixedSizeBytes + (varIndex - 1) * 4, true);
  const endOffset =
    varIndex + 1 < layout.variableColumnCount
      ? view.getUint32(fixedSizeBytes + varIndex * 4, true)
      : row.byteLength - varDataStart;
  if (startOffset > endOffset || varDataStart + endOffset > row.byteLength) {
    throw new Error("Native row variable column offsets are invalid");
  }

  const bytes = row.subarray(varDataStart + startOffset, varDataStart + endOffset);
  if (columnLayout.nullable) {
    if (bytes.byteLength === 0) {
      throw new Error("Nullable native row variable column has no null marker");
    }
    return { bytes: bytes.subarray(1), isNull: bytes[0] === 0 };
  }

  return { bytes, isNull: false };
}

function decodeNonNullValue(bytes: Uint8Array, type: ColumnType): Value {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Integer":
      return { type: "Integer", value: view.getInt32(0, true) };
    case "BigInt":
      return { type: "BigInt", value: Number(view.getBigInt64(0, true)) };
    case "Double":
      return { type: "Double", value: view.getFloat64(0, true) };
    case "Boolean":
      return { type: "Boolean", value: bytes[0] !== 0 };
    case "Timestamp":
      return { type: "Timestamp", value: Number(view.getBigUint64(0, true)) };
    case "Uuid":
      return { type: "Uuid", value: uuidString(bytes.subarray(0, 16)) };
    case "Bytea":
      return { type: "Bytea", value: bytes.slice() };
    case "Text":
    case "Json":
      return { type: "Text", value: textDecoder.decode(bytes) };
    case "Enum":
      if (type.variants.length <= 256 && bytes.byteLength === 1) {
        return { type: "Text", value: type.variants[bytes[0]] ?? "" };
      }
      return { type: "Text", value: textDecoder.decode(bytes) };
    case "Array":
      return { type: "Array", value: decodeArray(bytes, type.element) };
    case "Row": {
      if (bytes.byteLength === 0) {
        throw new Error("Native nested row is missing id flag");
      }
      const hasId = bytes[0] === 1;
      const id = hasId ? uuidString(bytes.subarray(1, 17)) : undefined;
      const rowData = bytes.subarray(hasId ? 17 : 1);
      return { type: "Row", value: { id, values: decodeNativeRowValues(type.columns, rowData) } };
    }
  }
}

function timestampToDate(value: number, columnName?: string): Date {
  if (columnName && isProvenanceMagicTimestampColumn(columnName)) {
    return new Date(Math.trunc(value / 1_000));
  }
  return new Date(value);
}

function decodeNativePlainValue(bytes: Uint8Array, type: ColumnType, columnName?: string): unknown {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  switch (type.type) {
    case "Integer":
      return view.getInt32(0, true);
    case "BigInt":
      return Number(view.getBigInt64(0, true));
    case "Double":
      return view.getFloat64(0, true);
    case "Boolean":
      return bytes[0] !== 0;
    case "Timestamp":
      return timestampToDate(Number(view.getBigUint64(0, true)), columnName);
    case "Uuid":
      return uuidString(bytes.subarray(0, 16));
    case "Bytea":
      return bytes.slice();
    case "Text":
      return textDecoder.decode(bytes);
    case "Json":
      return JSON.parse(textDecoder.decode(bytes));
    case "Enum":
      if (type.variants.length <= 256 && bytes.byteLength === 1) {
        return type.variants[bytes[0]] ?? "";
      }
      return textDecoder.decode(bytes);
    case "Array":
      return decodeNativePlainArray(bytes, type.element);
    case "Row": {
      if (bytes.byteLength === 0) {
        throw new Error("Native nested row is missing id flag");
      }
      const hasId = bytes[0] === 1;
      const id = hasId ? uuidString(bytes.subarray(1, 17)) : undefined;
      const rowData = bytes.subarray(hasId ? 17 : 1);
      return decodeNativeRowObject(id, type.columns, rowData);
    }
  }
}

function decodeNativePlainArray(bytes: Uint8Array, elementType: ColumnType): unknown[] {
  if (bytes.byteLength < 4) {
    throw new Error("Native array is missing element count");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const fixed = fixedSize(elementType);
  if (fixed !== null) {
    const values: unknown[] = [];
    let offset = 4;
    for (let i = 0; i < count; i++) {
      values.push(decodeNativePlainValue(bytes.subarray(offset, offset + fixed), elementType));
      offset += fixed;
    }
    return values;
  }

  const offsetsStart = 4;
  const offsetTableSize = count > 0 ? (count - 1) * 4 : 0;
  const values: unknown[] = [];
  const payloadStart = 4 + offsetTableSize;
  for (let i = 0; i < count; i++) {
    const start = i === 0 ? 0 : view.getUint32(offsetsStart + (i - 1) * 4, true);
    const end =
      i + 1 < count ? view.getUint32(offsetsStart + i * 4, true) : bytes.byteLength - payloadStart;
    values.push(
      decodeNativePlainValue(bytes.subarray(payloadStart + start, payloadStart + end), elementType),
    );
  }
  return values;
}

function decodeArray(bytes: Uint8Array, elementType: ColumnType): Value[] {
  if (bytes.byteLength < 4) {
    throw new Error("Native array is missing element count");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint32(0, true);
  const fixed = fixedSize(elementType);
  if (fixed !== null) {
    const values: Value[] = [];
    let offset = 4;
    for (let i = 0; i < count; i++) {
      values.push(decodeNonNullValue(bytes.subarray(offset, offset + fixed), elementType));
      offset += fixed;
    }
    return values;
  }

  const offsetsStart = 4;
  const offsetTableSize = count > 0 ? (count - 1) * 4 : 0;
  const values: Value[] = [];
  const payloadStart = 4 + offsetTableSize;
  for (let i = 0; i < count; i++) {
    const start = i === 0 ? 0 : view.getUint32(offsetsStart + (i - 1) * 4, true);
    const end =
      i + 1 < count ? view.getUint32(offsetsStart + i * 4, true) : bytes.byteLength - payloadStart;
    values.push(
      decodeNonNullValue(bytes.subarray(payloadStart + start, payloadStart + end), elementType),
    );
  }
  return values;
}

export function decodeNativeRowValues(
  columns: readonly ColumnDescriptor[],
  rowData: Uint8Array,
): Value[] {
  const layout = getLayout(columns);
  return columns.map((column, index) => {
    const { bytes, isNull } = columnBytes(rowData, columns, layout, index);
    return isNull ? { type: "Null" } : decodeNonNullValue(bytes, column.column_type);
  });
}

export function decodeNativeRow(
  id: string,
  columns: readonly ColumnDescriptor[],
  data: Uint8Array,
): WasmRow {
  return {
    id,
    values: decodeNativeRowValues(columns, data),
  };
}

export function decodeNativeRowObject(
  id: string | undefined,
  columns: readonly ColumnDescriptor[],
  data: Uint8Array,
): Record<string, unknown> {
  const layout = getLayout(columns);
  const obj: Record<string, unknown> = {};
  if (id !== undefined) {
    obj.id = id;
  }

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    if (!column) continue;
    const { bytes, isNull } = columnBytes(data, columns, layout, i);
    obj[column.name] = isNull
      ? null
      : decodeNativePlainValue(bytes, column.column_type, column.name);
  }

  return obj;
}
