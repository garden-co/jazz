/**
 * Dynamic binary decoder that handles includes
 */

import type { TableMeta, SchemaMeta, IncludeSpec, ColumnMeta } from "./types.js";

// Delta type constants (matching Rust)
const DELTA_ADDED = 1;
const DELTA_UPDATED = 2;
const DELTA_REMOVED = 3;

/**
 * Binary reader for decoding WASM binary data
 */
class BinaryReader {
  public bytes: Uint8Array;
  private view: DataView;
  public offset: number;

  constructor(buffer: ArrayBufferLike, startOffset = 0) {
    this.bytes = new Uint8Array(buffer);
    this.view = new DataView(buffer as ArrayBuffer);
    this.offset = startOffset;
  }

  readObjectId(): string {
    const id = String.fromCharCode(
      this.bytes[this.offset], this.bytes[this.offset+1], this.bytes[this.offset+2],
      this.bytes[this.offset+3], this.bytes[this.offset+4], this.bytes[this.offset+5],
      this.bytes[this.offset+6], this.bytes[this.offset+7], this.bytes[this.offset+8],
      this.bytes[this.offset+9], this.bytes[this.offset+10], this.bytes[this.offset+11],
      this.bytes[this.offset+12], this.bytes[this.offset+13], this.bytes[this.offset+14],
      this.bytes[this.offset+15], this.bytes[this.offset+16], this.bytes[this.offset+17],
      this.bytes[this.offset+18], this.bytes[this.offset+19], this.bytes[this.offset+20],
      this.bytes[this.offset+21], this.bytes[this.offset+22], this.bytes[this.offset+23],
      this.bytes[this.offset+24], this.bytes[this.offset+25]
    );
    this.offset += 26;
    return id;
  }

  readU8(): number {
    return this.bytes[this.offset++];
  }

  readU32(): number {
    const val = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return val;
  }

  readI32(): number {
    const val = this.view.getInt32(this.offset, true);
    this.offset += 4;
    return val;
  }

  readI64(): bigint {
    const val = this.view.getBigInt64(this.offset, true);
    this.offset += 8;
    return val;
  }

  readF64(): number {
    const val = this.view.getFloat64(this.offset, true);
    this.offset += 8;
    return val;
  }

  readBool(): boolean {
    return this.bytes[this.offset++] === 1;
  }

  readString(): string {
    const len = this.readU32();
    const decoder = new TextDecoder();
    const str = decoder.decode(new Uint8Array(this.bytes.buffer, this.offset, len));
    this.offset += len;
    return str;
  }

  readNullableRef(): string | null {
    if (this.bytes[this.offset] === 0) {
      this.offset++;
      return null;
    }
    return this.readObjectId();
  }

  /**
   * Check if there's more data to read
   */
  hasMore(): boolean {
    return this.offset < this.bytes.length;
  }
}

/**
 * Read a column value based on its type
 */
function readColumnValue(reader: BinaryReader, col: ColumnMeta): unknown {
  if (col.nullable && col.type.kind !== "ref") {
    const present = reader.readU8();
    if (present === 0) return null;
  }

  switch (col.type.kind) {
    case "bool":
      return reader.readBool();
    case "i64":
      return reader.readI64();
    case "f64":
      return reader.readF64();
    case "string":
      return reader.readString();
    case "bytes":
      // Read as string length + bytes
      const len = reader.readU32();
      const bytes = new Uint8Array(reader.bytes.buffer, reader.offset, len);
      reader.offset += len;
      return bytes;
    case "ref":
      if (col.nullable) {
        return reader.readNullableRef();
      }
      return reader.readObjectId();
    default:
      throw new Error(`Unknown column type: ${(col.type as { kind: string }).kind}`);
  }
}

/**
 * Read a full row (id + columns) for a table.
 *
 * For JOINed queries, Groove outputs flat values: [left_table_columns..., right_table_columns...]
 * The joined table's columns are WITHOUT an id prefix - the id comes from the FK column.
 *
 * @param isJoinedTable - If true, this is reading a joined table's columns (no id prefix)
 * @param fkId - For joined tables, the id comes from the FK value (passed from parent)
 */
function readRow(
  reader: BinaryReader,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec,
  resolvedRefColumns?: Set<string>,
  isJoinedTable = false,
  fkId?: string
): Record<string, unknown> {
  const row: Record<string, unknown> = {};

  // Read id - for joined tables, use the FK value passed from parent
  if (isJoinedTable && fkId) {
    row.id = fkId;
  } else {
    row.id = reader.readObjectId();
  }

  // Read columns
  // For joined tables, we don't skip any columns since we need the FK values for nested tables
  for (const col of tableMeta.columns) {
    // DON'T skip FK columns - JoinedRow includes ALL columns
    row[col.name] = readColumnValue(reader, col);
  }

  // Read included forward refs (joined table columns come after primary table columns)
  // For JOINed queries, the joined table's columns are flat (no nested id)
  // IMPORTANT: Read in the order of tableMeta.refs to match SQL/binary order
  if (include) {
    for (const ref of tableMeta.refs) {
      if (include[ref.column]) {
        const targetTable = schema.tables[ref.targetTable];
        if (targetTable) {
          // The FK value we just read is the joined table's id
          const joinedId = row[ref.column] as string;
          // Read joined table columns flat (no id prefix), use FK as id
          row[ref.column] = readRow(reader, targetTable, schema, undefined, undefined, true, joinedId);
        }
      }
    }

    // Read included reverse refs (arrays of nested rows)
    // IMPORTANT: Read in the order of tableMeta.reverseRefs to match SQL/binary order
    for (const reverseRef of tableMeta.reverseRefs) {
      if (include[reverseRef.name]) {
        const sourceTable = schema.tables[reverseRef.sourceTable];
        if (sourceTable) {
          const count = reader.readU32();
          const arr: Record<string, unknown>[] = [];

          // Get the nested include for this array
          const nestedInclude = include[reverseRef.name] === true ? undefined : include[reverseRef.name] as IncludeSpec;

          for (let i = 0; i < count; i++) {
            // Array elements are full rows (with id prefix)
            arr.push(readRow(reader, sourceTable, schema, nestedInclude));
          }
          row[reverseRef.name] = arr;
        }
      }
    }
  }

  return row;
}

/**
 * Decode a delta with includes support
 */
export function decodeDeltaWithIncludes<T>(
  buffer: ArrayBufferLike,
  tableMeta: TableMeta,
  schema: SchemaMeta,
  include?: IncludeSpec
): { type: "added" | "updated"; row: T } | { type: "removed"; id: string } {
  const reader = new BinaryReader(buffer);
  const deltaType = reader.readU8();

  if (deltaType === DELTA_REMOVED) {
    const id = reader.readObjectId();
    return { type: "removed", id };
  }

  // Read the row - for JOINed queries, all columns are flat including FK columns
  const row = readRow(reader, tableMeta, schema, include);

  return {
    type: deltaType === DELTA_ADDED ? "added" : "updated",
    row: row as T
  };
}
