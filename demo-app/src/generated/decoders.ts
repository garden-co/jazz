// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

// Shared decoder for UTF-8 strings
const decoder = new TextDecoder();

// Delta type constants
export const DELTA_ADDED = 1;
export const DELTA_UPDATED = 2;
export const DELTA_REMOVED = 3;

// Crockford Base32 alphabet (matches Rust ObjectId encoding - lowercase)
const CROCKFORD_ALPHABET = '0123456789abcdefghjkmnpqrstvwxyz';

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
    const id = objectIdToString(this.bytes, this.offset);
    this.offset += 16;
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

  /** Read nullable value. Returns null if not present (presence byte = 0). */
  readNullable<T>(readValue: () => T): T | null {
    if (this.bytes[this.offset++] === 0) return null;
    return readValue();
  }

  /**
   * Read a nullable ObjectId ref.
   * Nullable refs have a presence byte before the 16-byte ObjectId.
   */
  readNullableRef(): string | null {
    if (this.bytes[this.offset++] === 0) {
      this.offset += 16; // Skip the zeroed ObjectId bytes
      return null;
    }
    return this.readObjectId();
  }
}

/**
 * Decode binary rows for Users table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 16 bytes
 * - Variable columns: 3
 * - Offset table: 8 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 16;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varDataStart = bufferStart + 16 + 8;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const email = decoder.decode(bytes.subarray(varOffset1, varOffset2));
    const avatarColor = decoder.decode(bytes.subarray(varOffset2, rowEnd));

    rows[i] = { id, name, email, avatarColor };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a User delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeUserDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; email: string; avatarColor: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const offsetTableStart = bufferStart + 16;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varDataStart = bufferStart + 16 + 8;
  const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  const email = decoder.decode(bytes.subarray(varOffset1, varOffset2));
  const avatarColor = decoder.decode(bytes.subarray(varOffset2, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, email, avatarColor }
  };
}

/**
 * Read a User row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeUserRows/decodeUserDelta instead.
 */
export function readUser(reader: BinaryReader): { id: string; name: string; email: string; avatarColor: string } {
  throw new Error('readUser requires row boundary context - use decodeUserRows or decodeUserDelta instead');
}

/**
 * Decode binary rows for Projects table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 16 bytes
 * - Variable columns: 3
 * - Offset table: 8 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 16;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varDataStart = bufferStart + 16 + 8;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const color = decoder.decode(bytes.subarray(varOffset1, varOffset2));
    let description: string | null = null;
    if (bytes[varOffset2] === 1) {
      description = decoder.decode(bytes.subarray(varOffset2 + 1, rowEnd));
    }

    rows[i] = { id, name, color, description };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Project delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeProjectDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string; description: string | null }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const offsetTableStart = bufferStart + 16;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varDataStart = bufferStart + 16 + 8;
  const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  const color = decoder.decode(bytes.subarray(varOffset1, varOffset2));
  let description: string | null = null;
  if (bytes[varOffset2] === 1) {
    description = decoder.decode(bytes.subarray(varOffset2 + 1, rowEnd));
  }

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, color, description }
  };
}

/**
 * Read a Project row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeProjectRows/decodeProjectDelta instead.
 */
export function readProject(reader: BinaryReader): { id: string; name: string; color: string; description: string | null } {
  throw new Error('readProject requires row boundary context - use decodeProjectRows or decodeProjectDelta instead');
}

/**
 * Decode binary rows for Issues table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 48 bytes
 * - Variable columns: 4
 * - Offset table: 12 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);
    const project = objectIdToString(bytes, bufferStart + 16);
    const createdAt = view.getBigInt64(bufferStart + 32, true);
    const updatedAt = view.getBigInt64(bufferStart + 40, true);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 48;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varOffset3 = bufferStart + view.getUint32(offsetTableStart + 8, true);
    const varDataStart = bufferStart + 48 + 12;

    const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    let description: string | null = null;
    if (bytes[varOffset1] === 1) {
      description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
    }
    const status = decoder.decode(bytes.subarray(varOffset2, varOffset3));
    const priority = decoder.decode(bytes.subarray(varOffset3, rowEnd));

    rows[i] = { id, title, description, status, priority, project, createdAt, updatedAt };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Issue delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeIssueDelta(buffer: ArrayBufferLike): Delta<{ id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const project = objectIdToString(bytes, bufferStart + 16);
  const createdAt = view.getBigInt64(bufferStart + 32, true);
  const updatedAt = view.getBigInt64(bufferStart + 40, true);
  const offsetTableStart = bufferStart + 48;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varOffset3 = bufferStart + view.getUint32(offsetTableStart + 8, true);
  const varDataStart = bufferStart + 48 + 12;
  const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  let description: string | null = null;
  if (bytes[varOffset1] === 1) {
    description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
  }
  const status = decoder.decode(bytes.subarray(varOffset2, varOffset3));
  const priority = decoder.decode(bytes.subarray(varOffset3, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, title, description, status, priority, project, createdAt, updatedAt }
  };
}

/**
 * Read a Issue row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeIssueRows/decodeIssueDelta instead.
 */
export function readIssue(reader: BinaryReader): { id: string; title: string; description: string | null; status: string; priority: string; project: string; createdAt: bigint; updatedAt: bigint } {
  throw new Error('readIssue requires row boundary context - use decodeIssueRows or decodeIssueDelta instead');
}

/**
 * Decode binary rows for Labels table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 16 bytes
 * - Variable columns: 2
 * - Offset table: 4 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 16;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varDataStart = bufferStart + 16 + 4;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const color = decoder.decode(bytes.subarray(varOffset1, rowEnd));

    rows[i] = { id, name, color };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Label delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeLabelDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const offsetTableStart = bufferStart + 16;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varDataStart = bufferStart + 16 + 4;
  const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  const color = decoder.decode(bytes.subarray(varOffset1, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, color }
  };
}

/**
 * Read a Label row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeLabelRows/decodeLabelDelta instead.
 */
export function readLabel(reader: BinaryReader): { id: string; name: string; color: string } {
  throw new Error('readLabel requires row boundary context - use decodeLabelRows or decodeLabelDelta instead');
}

/**
 * Decode binary rows for IssueLabels table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 48 bytes
 * - Variable columns: 0
 * - Offset table: 0 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);
    const issue = objectIdToString(bytes, bufferStart + 16);
    const label = objectIdToString(bytes, bufferStart + 32);

    rows[i] = { id, issue, label };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a IssueLabel delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeIssueLabelDelta(buffer: ArrayBufferLike): Delta<{ id: string; issue: string; label: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const issue = objectIdToString(bytes, bufferStart + 16);
  const label = objectIdToString(bytes, bufferStart + 32);

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, issue, label }
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
 *
 * Row buffer layout:
 * - Fixed size: 48 bytes
 * - Variable columns: 0
 * - Offset table: 0 bytes
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
    // Read row size (row buffer with id as first 16 bytes)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;
    const bufferStart = rowStart; // Row buffer starts here (id is first 16 bytes)

    // Fixed columns
    const id = objectIdToString(bytes, bufferStart + 0);
    const issue = objectIdToString(bytes, bufferStart + 16);
    const user = objectIdToString(bytes, bufferStart + 32);

    rows[i] = { id, issue, user };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a IssueAssignee delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeIssueAssigneeDelta(buffer: ArrayBufferLike): Delta<{ id: string; issue: string; user: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row buffer (id is first 16 bytes)
  const bufferStart = 1; // After delta type byte
  const rowEnd = bytes.length;

  const id = objectIdToString(bytes, bufferStart + 0);
  const issue = objectIdToString(bytes, bufferStart + 16);
  const user = objectIdToString(bytes, bufferStart + 32);

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, issue, user }
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
