// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

// Shared decoder for UTF-8 strings (used for variable-length strings)
const decoder = new TextDecoder();

// Delta type constants
export const DELTA_ADDED = 1;
export const DELTA_UPDATED = 2;
export const DELTA_REMOVED = 3;

/**
 * Fast ObjectId decoding using String.fromCharCode.
 * Since Base32 is ASCII-only, this is faster than TextDecoder.
 */
function decodeObjectId(bytes: Uint8Array, offset: number): string {
  return String.fromCharCode(
    bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3], bytes[offset+4],
    bytes[offset+5], bytes[offset+6], bytes[offset+7], bytes[offset+8], bytes[offset+9],
    bytes[offset+10], bytes[offset+11], bytes[offset+12], bytes[offset+13], bytes[offset+14],
    bytes[offset+15], bytes[offset+16], bytes[offset+17], bytes[offset+18], bytes[offset+19],
    bytes[offset+20], bytes[offset+21], bytes[offset+22], bytes[offset+23], bytes[offset+24],
    bytes[offset+25]
  );
}

/** Delta type for incremental updates */
export type Delta<T> =
  | { type: 'added'; row: T }
  | { type: 'updated'; row: T }
  | { type: 'removed'; id: string };

/**
 * Decoder state for reading from a binary buffer.
 * Used for composing decoders for nested/joined rows.
 */
export class BinaryReader {
  readonly bytes: Uint8Array;
  readonly view: DataView;
  offset: number;

  constructor(buffer: ArrayBufferLike, startOffset = 0) {
    this.bytes = new Uint8Array(buffer);
    this.view = new DataView(buffer as ArrayBuffer);
    this.offset = startOffset;
  }

  readObjectId(): string {
    const id = decodeObjectId(this.bytes, this.offset);
    this.offset += 26;
    return id;
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
    const str = decoder.decode(new Uint8Array(this.bytes.buffer, this.offset, len));
    this.offset += len;
    return str;
  }

  readBytes(): Uint8Array {
    const len = this.readU32();
    const bytes = new Uint8Array(this.bytes.buffer, this.offset, len);
    this.offset += len;
    return bytes;
  }

  /** Read nullable value. Returns null if not present. */
  readNullable<T>(readValue: () => T): T | null {
    if (this.bytes[this.offset++] === 0) return null;
    return readValue();
  }

  /**
   * Read a nullable ObjectId ref.
   * Uses byte-0 detection instead of presence flag since Base32 can't contain byte 0.
   */
  readNullableRef(): string | null {
    if (this.bytes[this.offset] === 0) {
      this.offset++;
      return null;
    }
    return this.readObjectId();
  }

  /**
   * Read an array of values.
   * @param readElement Function to read each element
   */
  readArray<T>(readElement: () => T): T[] {
    const count = this.readU32();
    const arr = new Array(count);
    for (let i = 0; i < count; i++) {
      arr[i] = readElement();
    }
    return arr;
  }
}

/**
 * Decode binary rows for Users table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of User rows
 */
export function decodeUserRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; email: string; avatarColor: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // name: string
    const nameLen = view.getUint32(offset, true);
    offset += 4;
    row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
    offset += nameLen;

    // email: string
    const emailLen = view.getUint32(offset, true);
    offset += 4;
    row.email = decoder.decode(new Uint8Array(buffer, offset, emailLen));
    offset += emailLen;

    // avatarColor: string
    const avatarColorLen = view.getUint32(offset, true);
    offset += 4;
    row.avatarColor = decoder.decode(new Uint8Array(buffer, offset, avatarColorLen));
    offset += avatarColorLen;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single User row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeUserRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; name: string; email: string; avatarColor: string }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // name: string
  const nameLen = view.getUint32(offset, true);
  offset += 4;
  row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
  offset += nameLen;

  // email: string
  const emailLen = view.getUint32(offset, true);
  offset += 4;
  row.email = decoder.decode(new Uint8Array(buffer, offset, emailLen));
  offset += emailLen;

  // avatarColor: string
  const avatarColorLen = view.getUint32(offset, true);
  offset += 4;
  row.avatarColor = decoder.decode(new Uint8Array(buffer, offset, avatarColorLen));
  offset += avatarColorLen;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a User delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeUserDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; email: string; avatarColor: string }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeUserRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a User row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readUser(reader: BinaryReader): { id: string; name: string; email: string; avatarColor: string } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const email = reader.readString();
  const avatarColor = reader.readString();
  return { id, name, email, avatarColor };
}

/**
 * Decode binary rows for Projects table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Project rows
 */
export function decodeProjectRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; color: string; description: string | null }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // name: string
    const nameLen = view.getUint32(offset, true);
    offset += 4;
    row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
    offset += nameLen;

    // color: string
    const colorLen = view.getUint32(offset, true);
    offset += 4;
    row.color = decoder.decode(new Uint8Array(buffer, offset, colorLen));
    offset += colorLen;

    // description: string (nullable)
    const descriptionPresent = view.getUint8(offset++);
    if (descriptionPresent === 0) {
      row.description = null;
    } else {
      const descriptionLen = view.getUint32(offset, true);
      offset += 4;
      row.description = decoder.decode(new Uint8Array(buffer, offset, descriptionLen));
      offset += descriptionLen;
    }

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single Project row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeProjectRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; name: string; color: string; description: string | null }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // name: string
  const nameLen = view.getUint32(offset, true);
  offset += 4;
  row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
  offset += nameLen;

  // color: string
  const colorLen = view.getUint32(offset, true);
  offset += 4;
  row.color = decoder.decode(new Uint8Array(buffer, offset, colorLen));
  offset += colorLen;

  // description: string (nullable)
  const descriptionPresent = view.getUint8(offset++);
  if (descriptionPresent === 0) {
    row.description = null;
  } else {
    const descriptionLen = view.getUint32(offset, true);
    offset += 4;
    row.description = decoder.decode(new Uint8Array(buffer, offset, descriptionLen));
    offset += descriptionLen;
  }

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a Project delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeProjectDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string; description: string | null }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeProjectRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Project row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readProject(reader: BinaryReader): { id: string; name: string; color: string; description: string | null } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const color = reader.readString();
  const description = reader.readNullable(() => reader.readString());
  return { id, name, color, description };
}

/**
 * Decode binary rows for Issues table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Issue rows
 */
export function decodeIssueRows(buffer: ArrayBufferLike): Array<{ id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // title: string
    const titleLen = view.getUint32(offset, true);
    offset += 4;
    row.title = decoder.decode(new Uint8Array(buffer, offset, titleLen));
    offset += titleLen;

    // description: string (nullable)
    const descriptionPresent = view.getUint8(offset++);
    if (descriptionPresent === 0) {
      row.description = null;
    } else {
      const descriptionLen = view.getUint32(offset, true);
      offset += 4;
      row.description = decoder.decode(new Uint8Array(buffer, offset, descriptionLen));
      offset += descriptionLen;
    }

    // status: string
    const statusLen = view.getUint32(offset, true);
    offset += 4;
    row.status = decoder.decode(new Uint8Array(buffer, offset, statusLen));
    offset += statusLen;

    // priority: string
    const priorityLen = view.getUint32(offset, true);
    offset += 4;
    row.priority = decoder.decode(new Uint8Array(buffer, offset, priorityLen));
    offset += priorityLen;

    // project: ref
    row.project = decodeObjectId(bytes, offset);
    offset += 26;

    // createdAt: i64
    row.createdAt = view.getBigInt64(offset, true);
    offset += 8;

    // updatedAt: i64
    row.updatedAt = view.getBigInt64(offset, true);
    offset += 8;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single Issue row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeIssueRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // title: string
  const titleLen = view.getUint32(offset, true);
  offset += 4;
  row.title = decoder.decode(new Uint8Array(buffer, offset, titleLen));
  offset += titleLen;

  // description: string (nullable)
  const descriptionPresent = view.getUint8(offset++);
  if (descriptionPresent === 0) {
    row.description = null;
  } else {
    const descriptionLen = view.getUint32(offset, true);
    offset += 4;
    row.description = decoder.decode(new Uint8Array(buffer, offset, descriptionLen));
    offset += descriptionLen;
  }

  // status: string
  const statusLen = view.getUint32(offset, true);
  offset += 4;
  row.status = decoder.decode(new Uint8Array(buffer, offset, statusLen));
  offset += statusLen;

  // priority: string
  const priorityLen = view.getUint32(offset, true);
  offset += 4;
  row.priority = decoder.decode(new Uint8Array(buffer, offset, priorityLen));
  offset += priorityLen;

  // project: ref
  row.project = decodeObjectId(bytes, offset);
  offset += 26;

  // createdAt: i64
  row.createdAt = view.getBigInt64(offset, true);
  offset += 8;

  // updatedAt: i64
  row.updatedAt = view.getBigInt64(offset, true);
  offset += 8;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a Issue delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeIssueDelta(buffer: ArrayBufferLike): Delta<{ id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeIssueRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Issue row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readIssue(reader: BinaryReader): { id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint } {
  const id = reader.readObjectId();
  const title = reader.readString();
  const description = reader.readNullable(() => reader.readString());
  const status = reader.readString();
  const priority = reader.readString();
  const project = reader.readObjectId();
  const createdAt = reader.readI64();
  const updatedAt = reader.readI64();
  return { id, title, description, status, priority, project, createdAt, updatedAt };
}

/**
 * Decode binary rows for Labels table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Label rows
 */
export function decodeLabelRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // name: string
    const nameLen = view.getUint32(offset, true);
    offset += 4;
    row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
    offset += nameLen;

    // color: string
    const colorLen = view.getUint32(offset, true);
    offset += 4;
    row.color = decoder.decode(new Uint8Array(buffer, offset, colorLen));
    offset += colorLen;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single Label row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeLabelRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; name: string; color: string }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // name: string
  const nameLen = view.getUint32(offset, true);
  offset += 4;
  row.name = decoder.decode(new Uint8Array(buffer, offset, nameLen));
  offset += nameLen;

  // color: string
  const colorLen = view.getUint32(offset, true);
  offset += 4;
  row.color = decoder.decode(new Uint8Array(buffer, offset, colorLen));
  offset += colorLen;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a Label delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeLabelDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeLabelRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Label row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readLabel(reader: BinaryReader): { id: string; name: string; color: string } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const color = reader.readString();
  return { id, name, color };
}

/**
 * Decode binary rows for IssueLabels table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of IssueLabel rows
 */
export function decodeIssueLabelRows(buffer: ArrayBufferLike): Array<{ id: string; issue: string; label: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // issue: ref
    row.issue = decodeObjectId(bytes, offset);
    offset += 26;

    // label: ref
    row.label = decodeObjectId(bytes, offset);
    offset += 26;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single IssueLabel row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeIssueLabelRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; issue: string; label: string }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // issue: ref
  row.issue = decodeObjectId(bytes, offset);
  offset += 26;

  // label: ref
  row.label = decodeObjectId(bytes, offset);
  offset += 26;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a IssueLabel delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeIssueLabelDelta(buffer: ArrayBufferLike): Delta<{ id: string; issue: string; label: string }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeIssueLabelRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a IssueLabel row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readIssueLabel(reader: BinaryReader): { id: string; issue: string; label: string } {
  const id = reader.readObjectId();
  const issue = reader.readObjectId();
  const label = reader.readObjectId();
  return { id, issue, label };
}

/**
 * Decode binary rows for IssueAssignees table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of IssueAssignee rows
 */
export function decodeIssueAssigneeRows(buffer: ArrayBufferLike): Array<{ id: string; issue: string; user: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    const row: any = {};

    // Read ObjectId (26 bytes Base32)
    row.id = decodeObjectId(bytes, offset);
    offset += 26;

    // issue: ref
    row.issue = decodeObjectId(bytes, offset);
    offset += 26;

    // user: ref
    row.user = decodeObjectId(bytes, offset);
    offset += 26;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single IssueAssignee row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeIssueAssigneeRow(buffer: ArrayBufferLike, startOffset = 0): { row: { id: string; issue: string; user: string }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = startOffset;

  const row: any = {};

  // Read ObjectId (26 bytes Base32)
  row.id = decodeObjectId(bytes, offset);
  offset += 26;

  // issue: ref
  row.issue = decodeObjectId(bytes, offset);
  offset += 26;

  // user: ref
  row.user = decodeObjectId(bytes, offset);
  offset += 26;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a IssueAssignee delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeIssueAssigneeDelta(buffer: ArrayBufferLike): Delta<{ id: string; issue: string; user: string }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeIssueAssigneeRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a IssueAssignee row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readIssueAssignee(reader: BinaryReader): { id: string; issue: string; user: string } {
  const id = reader.readObjectId();
  const issue = reader.readObjectId();
  const user = reader.readObjectId();
  return { id, issue, user };
}
