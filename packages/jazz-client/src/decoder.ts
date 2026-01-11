/**
 * Dynamic binary decoder that handles includes using the row buffer format.
 *
 * Row buffer format:
 * - [id (16 bytes)][fixed columns][offset table (N-1 u32s for N var cols)][variable data]
 * - id is the first 16 bytes of every row buffer (ObjectId as u128 LE, converted to Base32 string)
 */

import type { TableMeta, SchemaMeta, IncludeSpec, ColumnMeta } from "./types.js";

// Delta type constants (matching Rust)
const DELTA_ADDED = 1;
const DELTA_UPDATED = 2;
const DELTA_REMOVED = 3;

// Crockford Base32 alphabet (matches Rust ObjectId encoding - lowercase)
const CROCKFORD_ALPHABET = '0123456789abcdefghjkmnpqrstvwxyz';

// Shared TextDecoder instance
const textDecoder = new TextDecoder();

/**
 * Convert a 16-byte binary ObjectId to Base32 string.
 * Matches the Rust ObjectId encoding format.
 */
function objectIdToString(bytes: Uint8Array, offset: number): string {
  // Read as two 64-bit values (little-endian)
  const view = new DataView(bytes.buffer, bytes.byteOffset + offset, 16);
  const lo = view.getBigUint64(0, true);
  const hi = view.getBigUint64(8, true);

  // Combine into 128-bit value
  let value = (hi << 64n) | lo;

  // Encode to Base32 (26 characters for 128 bits)
  const chars = new Array(26);
  for (let i = 25; i >= 0; i--) {
    chars[i] = CROCKFORD_ALPHABET[Number(value & 0x1fn)];
    value >>= 5n;
  }

  return chars.join('');
}

/**
 * Get the fixed byte size of a column type, or null if variable-size.
 */
function getColumnFixedSize(col: ColumnMeta): number | null {
  const baseSize = (() => {
    switch (col.type.kind) {
      case "bool": return 1;
      case "i64": return 8;
      case "f64": return 8;
      case "ref": return 16;
      case "string": return null;
      case "bytes": return null;
      default: return null;
    }
  })();

  if (baseSize === null) return null;
  return col.nullable ? 1 + baseSize : baseSize;
}

/**
 * Column layout information for row buffer decoding.
 */
interface ColumnLayout {
  col: ColumnMeta;
  tableName: string; // Which table this column belongs to
  isFixed: boolean;
  /** For fixed columns: byte offset in fixed section. For variable: index in variable column list. */
  offset: number;
}

/**
 * Row buffer layout computed from table metadata and includes.
 */
interface RowBufferLayout {
  fixedSize: number;
  fixedColumns: ColumnLayout[];
  variableColumns: ColumnLayout[];
  offsetTableSize: number;
}

/**
 * Information about an array column (reverse ref or forward ref in array context) in the row buffer.
 */
interface ArrayColumnLayout {
  name: string;
  sourceTable: string;
  varIndex: number;
  /** Column metadata for decoding each item in the array */
  itemColumns: ColumnMeta[];
  /** Nested include spec for joining related tables in array items */
  nestedInclude?: IncludeSpec;
  /** Reference metadata from the source table for building nested joins */
  sourceTableMeta?: TableMeta;
  /**
   * When true, this is a forward ref in array context that Groove wraps in a single-item array.
   * We should extract items[0] instead of keeping the array.
   */
  isSingleItemUnwrap?: boolean;
}

/**
 * Extended row buffer layout with array columns.
 */
interface RowBufferLayoutExt extends RowBufferLayout {
  arrayColumns: ArrayColumnLayout[];
}

/**
 * Compute the row buffer layout for a table with optional includes.
 * This matches the Rust RowDescriptor layout (fixed columns first, then variable).
 *
 * When reverse refs (arrays) are included, they're added as variable columns.
 * Arrays ARE included in the offset table (unlike the previous incorrect assumption).
 */
function computeLayout(
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec,
  isArrayItemContext = false
): RowBufferLayoutExt {
  const fixedColumns: ColumnLayout[] = [];
  const variableColumns: ColumnLayout[] = [];
  const arrayColumns: ArrayColumnLayout[] = [];
  let fixedOffset = 0;
  let varIndex = 0;

  // id column is always first (ObjectId, 16 bytes, non-nullable)
  const idCol: ColumnMeta = {
    name: "id",
    type: { kind: "ref", table: "" },  // ObjectId is stored same as Ref (16 bytes)
    nullable: false,
  };
  fixedColumns.push({ col: idCol, tableName: tableMeta.name, isFixed: true, offset: fixedOffset });
  fixedOffset += 16;  // ObjectId is 16 bytes

  // For array item context (nested includes), Groove excludes FK columns that are being
  // resolved by inner JOINs. For main query context, Groove includes both FK and expanded columns.
  const resolvedRefColumns = new Set<string>();
  if (isArrayItemContext && include) {
    for (const ref of tableMeta.refs) {
      if (include[ref.column]) {
        resolvedRefColumns.add(ref.column);
      }
    }
  }

  // Process main table columns
  for (const col of tableMeta.columns) {
    // In array item context, skip FK columns that are being resolved by includes
    if (resolvedRefColumns.has(col.name)) {
      continue;
    }
    const fixedSize = getColumnFixedSize(col);
    if (fixedSize !== null) {
      fixedColumns.push({ col, tableName: tableMeta.name, isFixed: true, offset: fixedOffset });
      fixedOffset += fixedSize;
    } else {
      variableColumns.push({ col, tableName: tableMeta.name, isFixed: false, offset: varIndex++ });
    }
  }

  // Process included forward refs (joined tables)
  // Behavior differs based on context:
  // - Main query: Groove expands JOIN columns inline
  // - Array item context: Groove wraps the joined row in a single-item Array
  if (include) {
    for (const ref of tableMeta.refs) {
      if (include[ref.column]) {
        const targetTable = schema.tables[ref.targetTable];
        if (targetTable) {
          if (isArrayItemContext) {
            // In array context, joined tables are wrapped in single-item Arrays, not expanded inline
            // The FK column was already skipped, add a pseudo-array column for the joined data
            arrayColumns.push({
              name: ref.column,
              sourceTable: ref.targetTable,
              varIndex: varIndex++,
              itemColumns: targetTable.columns,
              nestedInclude: undefined, // No further nesting
              sourceTableMeta: targetTable,
              isSingleItemUnwrap: true, // Extract single item, not array
            });
          } else {
            // Main query context: add joined table's id and columns inline
            // First add the joined table's id (16 bytes)
            const joinedIdCol: ColumnMeta = {
              name: "id",
              type: { kind: "ref", table: "" },  // ObjectId is stored same as Ref (16 bytes)
              nullable: false,
            };
            fixedColumns.push({ col: joinedIdCol, tableName: targetTable.name, isFixed: true, offset: fixedOffset });
            fixedOffset += 16;

            // Then add the joined table's other columns
            for (const col of targetTable.columns) {
              const fixedSize = getColumnFixedSize(col);
              if (fixedSize !== null) {
                fixedColumns.push({ col, tableName: targetTable.name, isFixed: true, offset: fixedOffset });
                fixedOffset += fixedSize;
              } else {
                variableColumns.push({ col, tableName: targetTable.name, isFixed: false, offset: varIndex++ });
              }
            }
          }
        }
      }
    }

    // Process included reverse refs (arrays) - these ARE variable columns in the row buffer
    // Arrays are included in the offset table calculation
    for (const reverseRef of tableMeta.reverseRefs) {
      const arrayInclude = include[reverseRef.name];
      if (arrayInclude) {
        const sourceTable = schema.tables[reverseRef.sourceTable];
        // Get columns from the source table for decoding array items
        const itemColumns = sourceTable?.columns ?? [];

        // Check if there's a nested include spec (e.g., { user: true } in IssueAssignees: { user: true })
        const nestedInclude = typeof arrayInclude === 'object' ? arrayInclude as IncludeSpec : undefined;

        arrayColumns.push({
          name: reverseRef.name,
          sourceTable: reverseRef.sourceTable,
          varIndex: varIndex++,
          itemColumns,
          nestedInclude,
          sourceTableMeta: sourceTable,
        });
      }
    }
  }

  // Offset table has N-1 entries for N variable columns
  // Arrays ARE included in the offset table (they're variable-size columns)
  const totalVarColumns = variableColumns.length + arrayColumns.length;
  const offsetTableSize = totalVarColumns > 1 ? (totalVarColumns - 1) * 4 : 0;

  return {
    fixedSize: fixedOffset,
    fixedColumns,
    variableColumns,
    offsetTableSize,
    arrayColumns,
  };
}

/**
 * Read a fixed column value at a known byte offset.
 */
function readFixedValue(
  bytes: Uint8Array,
  view: DataView,
  bufferStart: number,
  col: ColumnMeta,
  colOffset: number
): unknown {
  const absOffset = bufferStart + colOffset;

  if (col.nullable) {
    if (bytes[absOffset] === 0) return null;
    // Value starts after presence byte
    const valueOffset = absOffset + 1;
    switch (col.type.kind) {
      case "bool": return bytes[valueOffset] === 1;
      case "i64": return view.getBigInt64(valueOffset, true);
      case "f64": return view.getFloat64(valueOffset, true);
      case "ref": return objectIdToString(bytes, valueOffset);
      default: throw new Error(`Unknown fixed column type: ${col.type.kind}`);
    }
  } else {
    switch (col.type.kind) {
      case "bool": return bytes[absOffset] === 1;
      case "i64": return view.getBigInt64(absOffset, true);
      case "f64": return view.getFloat64(absOffset, true);
      case "ref": return objectIdToString(bytes, absOffset);
      default: throw new Error(`Unknown fixed column type: ${col.type.kind}`);
    }
  }
}

/**
 * Read a variable column value using the offset table.
 * @param totalVarCount Total number of variable "columns" including arrays
 */
function readVariableValue(
  bytes: Uint8Array,
  view: DataView,
  bufferStart: number,
  layout: RowBufferLayoutExt,
  varIndex: number,
  rowEnd: number,
  col: ColumnMeta | null, // null for array columns
  totalVarCount: number
): unknown {
  const offsetTableStart = bufferStart + layout.fixedSize;
  const varDataStart = offsetTableStart + layout.offsetTableSize;

  // Calculate start position for this variable column
  let start: number;
  if (varIndex === 0) {
    start = varDataStart;
  } else {
    // Read offset from table (offset for column i is at position i-1)
    const rawOffset = view.getUint32(offsetTableStart + (varIndex - 1) * 4, true);
    start = bufferStart + rawOffset;
  }

  // Calculate end position
  let end: number;
  if (varIndex === totalVarCount - 1) {
    end = rowEnd;
  } else {
    const rawOffset = view.getUint32(offsetTableStart + varIndex * 4, true);
    end = bufferStart + rawOffset;
  }

  // For array columns (col is null), return the raw slice
  if (col === null) {
    return bytes.subarray(start, end);
  }

  // Handle nullable variable columns
  if (col.nullable) {
    if (bytes[start] === 0) return null;
    // Data starts after presence byte
    const data = bytes.subarray(start + 1, end);
    if (col.type.kind === "string") {
      return textDecoder.decode(data);
    } else if (col.type.kind === "bytes") {
      return new Uint8Array(data);
    }
  } else {
    const data = bytes.subarray(start, end);
    if (col.type.kind === "string") {
      return textDecoder.decode(data);
    } else if (col.type.kind === "bytes") {
      return new Uint8Array(data);
    }
  }

  throw new Error(`Unknown variable column type: ${col.type.kind}`);
}

/**
 * Read array data from the variable section and return the raw bytes.
 * Returns the slice of bytes containing the array data.
 */
function readArrayData(
  bytes: Uint8Array,
  view: DataView,
  bufferStart: number,
  layout: RowBufferLayoutExt,
  varIndex: number,
  rowEnd: number,
  totalVarCount: number
): Uint8Array {
  const offsetTableStart = bufferStart + layout.fixedSize;
  const varDataStart = offsetTableStart + layout.offsetTableSize;

  // Calculate start position for this variable column
  let start: number;
  if (varIndex === 0) {
    start = varDataStart;
  } else {
    // Read offset from table (offset for column i is at position i-1)
    const rawOffset = view.getUint32(offsetTableStart + (varIndex - 1) * 4, true);
    start = bufferStart + rawOffset;
  }

  // Calculate end position
  let end: number;
  if (varIndex === totalVarCount - 1) {
    end = rowEnd;
  } else {
    const rawOffset = view.getUint32(offsetTableStart + varIndex * 4, true);
    end = bufferStart + rawOffset;
  }

  return bytes.subarray(start, end);
}

/**
 * Decode an array from its buffer format.
 *
 * Array format: [u32 count][u32 offset₂][u32 offset₃]...[item₁][item₂]...
 * For N items, N-1 offsets are stored. Item 0 starts after the offset table.
 * Each item is a row buffer with the structure defined by itemColumns.
 *
 * When nestedInclude is provided, array items can have nested joins (e.g., IssueAssignees.user).
 */
function decodeArrayItems(
  arrayData: Uint8Array,
  itemColumns: ColumnMeta[],
  schema: SchemaMeta,
  nestedInclude?: IncludeSpec,
  sourceTableMeta?: TableMeta
): Record<string, unknown>[] {
  if (arrayData.length < 4) {
    return [];
  }

  const view = new DataView(arrayData.buffer, arrayData.byteOffset, arrayData.byteLength);
  const count = view.getUint32(0, true);

  if (count === 0) {
    return [];
  }

  // Header size: 4 bytes count + (count-1) * 4 bytes offsets
  const headerSize = 4 + (count > 1 ? (count - 1) * 4 : 0);

  const items: Record<string, unknown>[] = [];

  for (let i = 0; i < count; i++) {
    // Get start offset for item i
    let itemStart: number;
    if (i === 0) {
      itemStart = headerSize;
    } else {
      // Offset for item i is at position 4 + (i-1) * 4
      const offsetPos = 4 + (i - 1) * 4;
      itemStart = view.getUint32(offsetPos, true);
    }

    // Get end offset for item i
    let itemEnd: number;
    if (i === count - 1) {
      itemEnd = arrayData.length;
    } else {
      const offsetPos = 4 + i * 4;
      itemEnd = view.getUint32(offsetPos, true);
    }

    // Decode the item row buffer
    const itemBytes = arrayData.subarray(itemStart, itemEnd);
    const itemView = new DataView(itemBytes.buffer, itemBytes.byteOffset, itemBytes.byteLength);

    // If we have nested includes and source table metadata, use full row buffer decoding
    // to properly handle joined columns
    let item: Record<string, unknown>;
    if (nestedInclude && sourceTableMeta) {
      // Pass isArrayItemContext=true so the decoder knows to skip FK columns
      // that are resolved by inner JOINs in ARRAY subqueries
      item = decodeRowBuffer(itemBytes, itemView, 0, itemBytes.length, sourceTableMeta, schema, nestedInclude, true);
    } else {
      // Fall back to simple layout for flat items
      const itemLayout = computeItemLayout(itemColumns);
      const itemOffsetTableSize = itemLayout.variableColumns.length > 1
        ? (itemLayout.variableColumns.length - 1) * 4
        : 0;
      item = decodeItemRowBuffer(itemBytes, itemView, itemLayout, itemOffsetTableSize);
    }
    items.push(item);
  }

  return items;
}

/**
 * Compute layout for array item columns.
 * This is a simplified version for flat item schemas (no nested includes).
 */
function computeItemLayout(columns: ColumnMeta[]): {
  fixedSize: number;
  fixedColumns: Array<{ col: ColumnMeta; offset: number }>;
  variableColumns: Array<{ col: ColumnMeta; offset: number }>;
} {
  const fixedColumns: Array<{ col: ColumnMeta; offset: number }> = [];
  const variableColumns: Array<{ col: ColumnMeta; offset: number }> = [];
  let fixedOffset = 0;
  let varIndex = 0;

  // id column is always first (ObjectId, 16 bytes, non-nullable)
  const idCol: ColumnMeta = {
    name: "id",
    type: { kind: "ref", table: "" },  // ObjectId is stored same as Ref (16 bytes)
    nullable: false,
  };
  fixedColumns.push({ col: idCol, offset: fixedOffset });
  fixedOffset += 16;

  for (const col of columns) {
    const fixedSize = getColumnFixedSize(col);
    if (fixedSize !== null) {
      fixedColumns.push({ col, offset: fixedOffset });
      fixedOffset += fixedSize;
    } else {
      variableColumns.push({ col, offset: varIndex++ });
    }
  }

  return { fixedSize: fixedOffset, fixedColumns, variableColumns };
}

/**
 * Decode a row buffer for an array item.
 */
function decodeItemRowBuffer(
  bytes: Uint8Array,
  view: DataView,
  layout: { fixedSize: number; fixedColumns: Array<{ col: ColumnMeta; offset: number }>; variableColumns: Array<{ col: ColumnMeta; offset: number }> },
  offsetTableSize: number
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  const varDataStart = layout.fixedSize + offsetTableSize;
  const totalVarCount = layout.variableColumns.length;

  // Read fixed columns
  for (const { col, offset } of layout.fixedColumns) {
    const value = readFixedValueSimple(bytes, view, col, offset);
    row[col.name] = value;
  }

  // Read variable columns
  for (const { col, offset: varIndex } of layout.variableColumns) {
    // Calculate start position
    let start: number;
    if (varIndex === 0) {
      start = varDataStart;
    } else {
      const rawOffset = view.getUint32(layout.fixedSize + (varIndex - 1) * 4, true);
      start = rawOffset;
    }

    // Calculate end position
    let end: number;
    if (varIndex === totalVarCount - 1) {
      end = bytes.length;
    } else {
      const rawOffset = view.getUint32(layout.fixedSize + varIndex * 4, true);
      end = rawOffset;
    }

    // Read value
    if (col.nullable) {
      if (bytes[start] === 0) {
        row[col.name] = null;
      } else {
        const data = bytes.subarray(start + 1, end);
        row[col.name] = col.type.kind === "string" ? textDecoder.decode(data) : new Uint8Array(data);
      }
    } else {
      const data = bytes.subarray(start, end);
      row[col.name] = col.type.kind === "string" ? textDecoder.decode(data) : new Uint8Array(data);
    }
  }

  return row;
}

/**
 * Read a fixed column value (simplified version for array items).
 */
function readFixedValueSimple(
  bytes: Uint8Array,
  view: DataView,
  col: ColumnMeta,
  offset: number
): unknown {
  if (col.nullable) {
    if (bytes[offset] === 0) return null;
    const valueOffset = offset + 1;
    switch (col.type.kind) {
      case "bool": return bytes[valueOffset] === 1;
      case "i64": return view.getBigInt64(valueOffset, true);
      case "f64": return view.getFloat64(valueOffset, true);
      case "ref": return objectIdToString(bytes, valueOffset);
      default: throw new Error(`Unknown fixed column type: ${col.type.kind}`);
    }
  } else {
    switch (col.type.kind) {
      case "bool": return bytes[offset] === 1;
      case "i64": return view.getBigInt64(offset, true);
      case "f64": return view.getFloat64(offset, true);
      case "ref": return objectIdToString(bytes, offset);
      default: throw new Error(`Unknown fixed column type: ${col.type.kind}`);
    }
  }
}

/**
 * Decode a row from row buffer format.
 *
 * The row buffer is structured as:
 * - [fixed columns (in schema order, but fixed-size types first)]
 * - [offset table: (N-1) u32s for N variable columns]
 * - [variable data: concatenated variable column values]
 *
 * For JOINed queries, the row buffer contains columns from all joined tables
 * in the order: main table columns, then joined table columns.
 *
 * @param isArrayItemContext - When true, indicates this is decoding an array item
 *   where Groove excludes FK columns that are resolved by inner JOINs.
 */
function decodeRowBuffer(
  bytes: Uint8Array,
  view: DataView,
  bufferStart: number,
  rowEnd: number,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec,
  isArrayItemContext = false
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  const layout = computeLayout(tableMeta, schema, include, isArrayItemContext);
  // Total variable column count includes regular variable columns AND array columns
  const totalVarCount = layout.variableColumns.length + layout.arrayColumns.length;

  // Debug: log once for Issues table

  // Track which table each column belongs to for building nested objects
  const mainTableColumns: Array<{ name: string; value: unknown }> = [];
  const joinedTableColumns: Map<string, Array<{ name: string; value: unknown }>> = new Map();

  // Read fixed columns
  for (const colLayout of layout.fixedColumns) {
    const value = readFixedValue(bytes, view, bufferStart, colLayout.col, colLayout.offset);
    if (colLayout.tableName === tableMeta.name) {
      mainTableColumns.push({ name: colLayout.col.name, value });
    } else {
      if (!joinedTableColumns.has(colLayout.tableName)) {
        joinedTableColumns.set(colLayout.tableName, []);
      }
      joinedTableColumns.get(colLayout.tableName)!.push({ name: colLayout.col.name, value });
    }
  }

  // Read variable columns
  for (const colLayout of layout.variableColumns) {
    const value = readVariableValue(bytes, view, bufferStart, layout, colLayout.offset, rowEnd, colLayout.col, totalVarCount);
    if (colLayout.tableName === tableMeta.name) {
      mainTableColumns.push({ name: colLayout.col.name, value });
    } else if (colLayout.tableName !== '__internal__') {
      if (!joinedTableColumns.has(colLayout.tableName)) {
        joinedTableColumns.set(colLayout.tableName, []);
      }
      joinedTableColumns.get(colLayout.tableName)!.push({ name: colLayout.col.name, value });
    }
  }

  // Build main table row
  for (const { name, value } of mainTableColumns) {
    row[name] = value;
  }

  // Build joined table objects and attach to FK columns
  if (include) {
    for (const ref of tableMeta.refs) {
      if (include[ref.column]) {
        const targetTable = schema.tables[ref.targetTable];
        if (targetTable) {
          const joinedCols = joinedTableColumns.get(targetTable.name);
          if (joinedCols && joinedCols.length > 0) {
            // Build the joined object
            const joinedRow: Record<string, unknown> = {};
            // The id comes from the FK value
            joinedRow.id = row[ref.column];
            for (const { name, value } of joinedCols) {
              joinedRow[name] = value;
            }
            // Replace FK value with joined object
            row[ref.column] = joinedRow;
          }
        }
      }
    }

    // Read and decode array columns (reverse refs and forward refs in array context)
    // Arrays ARE variable columns in the row buffer and ARE in the offset table
    for (const arrayCol of layout.arrayColumns) {
      const arrayData = readArrayData(bytes, view, bufferStart, layout, arrayCol.varIndex, rowEnd, totalVarCount);
      const items = decodeArrayItems(arrayData, arrayCol.itemColumns, schema, arrayCol.nestedInclude, arrayCol.sourceTableMeta);

      // For forward refs in array context, Groove wraps the joined row in a single-item array
      // Extract the single item instead of keeping the array
      if (arrayCol.isSingleItemUnwrap) {
        row[arrayCol.name] = items[0] ?? null;
      } else {
        row[arrayCol.name] = items;
      }
    }
  }

  return row;
}

/**
 * Decode a delta with includes support using row buffer format.
 *
 * Delta format:
 * - [u8 delta_type (1=added, 2=updated, 3=removed)]
 * - For added/updated: [row buffer with id as first 16 bytes]
 * - For removed: [16-byte ObjectId]
 */
export function decodeDeltaWithIncludes<T>(
  buffer: Uint8Array | ArrayBufferLike,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec
): { type: "added" | "updated"; row: T } | { type: "removed"; id: string } {
  try {
  // Handle both Uint8Array (preserves byteOffset) and ArrayBuffer
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: "removed", id };
  }

  // Row buffer starts at offset 1 (after delta type byte)
  // id is the first 16 bytes inside the row buffer
  const bufferStart = 1;
  const rowEnd = bytes.length;

  // Decode the row buffer (id is read as first fixed column)
  const row = decodeRowBuffer(bytes, view, bufferStart, rowEnd, tableMeta, schema, include);

  return {
    type: deltaType === DELTA_ADDED ? "added" : "updated",
    row: row as T
  };
  } catch (error) {
    console.error('decodeDeltaWithIncludes error:', error);
    throw error;
  }
}
