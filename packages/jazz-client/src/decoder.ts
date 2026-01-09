/**
 * Dynamic binary decoder that handles includes using the row buffer format.
 *
 * Row buffer format:
 * - [16-byte ObjectId][row buffer]
 * - Row buffer: [fixed columns][offset table (N-1 u32s for N var cols)][variable data]
 * - ObjectId: 16 bytes binary (u128 LE), converted to Base32 string
 */

import type { TableMeta, SchemaMeta, IncludeSpec, ColumnMeta } from "./types.js";

// Delta type constants (matching Rust)
const DELTA_ADDED = 1;
const DELTA_UPDATED = 2;
const DELTA_REMOVED = 3;

// Crockford Base32 alphabet (matches Rust ObjectId encoding)
const CROCKFORD_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

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
 * Information about a reverse ref (array) in the row buffer.
 */
interface ReverseRefLayout {
  name: string;
  sourceTable: string;
  varIndex: number;
}

/**
 * Extended row buffer layout with reverse refs.
 */
interface RowBufferLayoutExt extends RowBufferLayout {
  reverseRefs: ReverseRefLayout[];
}

/**
 * Compute the row buffer layout for a table with optional includes.
 * This matches the Rust RowDescriptor layout (fixed columns first, then variable).
 *
 * When reverse refs (arrays) are included, they're added as variable columns at the end.
 */
function computeLayout(
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec
): RowBufferLayoutExt {
  const fixedColumns: ColumnLayout[] = [];
  const variableColumns: ColumnLayout[] = [];
  const reverseRefs: ReverseRefLayout[] = [];
  let fixedOffset = 0;
  let varIndex = 0;

  // Process main table columns
  for (const col of tableMeta.columns) {
    const fixedSize = getColumnFixedSize(col);
    if (fixedSize !== null) {
      fixedColumns.push({ col, tableName: tableMeta.name, isFixed: true, offset: fixedOffset });
      fixedOffset += fixedSize;
    } else {
      variableColumns.push({ col, tableName: tableMeta.name, isFixed: false, offset: varIndex++ });
    }
  }

  // Process included forward refs (joined tables)
  if (include) {
    for (const ref of tableMeta.refs) {
      if (include[ref.column]) {
        const targetTable = schema.tables[ref.targetTable];
        if (targetTable) {
          // Add joined table's columns
          for (const col of targetTable.columns) {
            const fixedSize = getColumnFixedSize(col);
            if (fixedSize !== null) {
              fixedColumns.push({ col, tableName: targetTable.name, isFixed: true, offset: fixedOffset });
              fixedOffset += fixedSize;
            } else {
              variableColumns.push({ col, tableName: targetTable.name, isFixed: false, offset: varIndex++ });
            }
          }

          // Groove's JOIN output includes an extra variable-length column after the joined table's columns.
          // TODO: Investigate what this extra variable column actually is.
          // Add a dummy variable column to account for this in the offset table.
          // Use '__internal__' as tableName so it doesn't get included in joined objects.
          const joinMarker: ColumnMeta = { name: `__join_${ref.column}`, type: { kind: 'bytes' }, nullable: false };
          variableColumns.push({ col: joinMarker, tableName: '__internal__', isFixed: false, offset: varIndex++ });
        }
      }
    }

    // Process included reverse refs (arrays) - these add extra variable columns
    for (const reverseRef of tableMeta.reverseRefs) {
      if (include[reverseRef.name]) {
        reverseRefs.push({
          name: reverseRef.name,
          sourceTable: reverseRef.sourceTable,
          varIndex: varIndex++,
        });
      }
    }
  }

  // Offset table has N-1 entries for N variable columns
  // NOTE: Arrays (reverse refs) are NOT included in the offset table - they're appended after
  const offsetTableSize = variableColumns.length > 1 ? (variableColumns.length - 1) * 4 : 0;

  return {
    fixedSize: fixedOffset,
    fixedColumns,
    variableColumns,
    offsetTableSize,
    reverseRefs,
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
 * Decode a row from row buffer format.
 *
 * The row buffer is structured as:
 * - [fixed columns (in schema order, but fixed-size types first)]
 * - [offset table: (N-1) u32s for N variable columns]
 * - [variable data: concatenated variable column values]
 *
 * For JOINed queries, the row buffer contains columns from all joined tables
 * in the order: main table columns, then joined table columns.
 */
function decodeRowBuffer(
  bytes: Uint8Array,
  view: DataView,
  bufferStart: number,
  rowEnd: number,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  const layout = computeLayout(tableMeta, schema, include);
  // Total variable column count - arrays are NOT in the offset table, they're appended after
  const totalVarCount = layout.variableColumns.length;


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

    // Read and decode reverse refs (arrays)
    // Array data format: [u32 count][row1 id (16 bytes)][row1 buffer]...
    // TODO: Arrays (reverse refs) are not included in the Rust row buffer offset table.
    // The ARRAY subquery results may be returned separately or not at all.
    // For now, set arrays to empty until we investigate how Groove encodes ARRAY results.
    for (const revRef of layout.reverseRefs) {
      row[revRef.name] = [];
    }
  }

  return row;
}

/**
 * Decode a delta with includes support using row buffer format.
 *
 * Delta format:
 * - [u8 delta_type (1=added, 2=updated, 3=removed)]
 * - For added/updated: [16-byte ObjectId][row buffer]
 * - For removed: [16-byte ObjectId]
 */
export function decodeDeltaWithIncludes<T>(
  buffer: Uint8Array | ArrayBufferLike,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec
): { type: "added" | "updated"; row: T } | { type: "removed"; id: string } {
  // Handle both Uint8Array (preserves byteOffset) and ArrayBuffer
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: "removed", id };
  }

  // Read ObjectId (16 bytes starting at offset 1)
  const id = objectIdToString(bytes, 1);
  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)
  const rowEnd = bytes.length;

  // Decode the row buffer
  const row = decodeRowBuffer(bytes, view, bufferStart, rowEnd, tableMeta, schema, include);
  row.id = id;

  return {
    type: deltaType === DELTA_ADDED ? "added" : "updated",
    row: row as T
  };
}
