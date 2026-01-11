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
 * - Fixed size: 33 bytes
 * - Variable columns: 3
 * - Offset table: 8 bytes
 */
export function decodeUserRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }> {
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
    const age = view.getBigInt64(bufferStart + 16, true);
    const score = view.getFloat64(bufferStart + 24, true);
    const isAdmin = bytes[bufferStart + 32] === 1;

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 33;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varDataStart = bufferStart + 33 + 8;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const email = decoder.decode(bytes.subarray(varOffset1, varOffset2));
    let avatar: string | null = null;
    if (bytes[varOffset2] === 1) {
      avatar = decoder.decode(bytes.subarray(varOffset2 + 1, rowEnd));
    }

    rows[i] = { id, name, email, avatar, age, score, isAdmin };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a User delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeUserDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }> {
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
  const age = view.getBigInt64(bufferStart + 16, true);
  const score = view.getFloat64(bufferStart + 24, true);
  const isAdmin = bytes[bufferStart + 32] === 1;
  const offsetTableStart = bufferStart + 33;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varDataStart = bufferStart + 33 + 8;
  const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  const email = decoder.decode(bytes.subarray(varOffset1, varOffset2));
  let avatar: string | null = null;
  if (bytes[varOffset2] === 1) {
    avatar = decoder.decode(bytes.subarray(varOffset2 + 1, rowEnd));
  }

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, email, avatar, age, score, isAdmin }
  };
}

/**
 * Read a User row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeUserRows/decodeUserDelta instead.
 */
export function readUser(reader: BinaryReader): { id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean } {
  throw new Error('readUser requires row boundary context - use decodeUserRows or decodeUserDelta instead');
}

/**
 * Decode binary rows for Projects table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 32 bytes
 * - Variable columns: 3
 * - Offset table: 8 bytes
 */
export function decodeProjectRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; description: string | null; owner: string; color: string }> {
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
    const owner = objectIdToString(bytes, bufferStart + 16);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 32;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varDataStart = bufferStart + 32 + 8;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    let description: string | null = null;
    if (bytes[varOffset1] === 1) {
      description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
    }
    const color = decoder.decode(bytes.subarray(varOffset2, rowEnd));

    rows[i] = { id, name, description, owner, color };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Project delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeProjectDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; description: string | null; owner: string; color: string }> {
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
  const owner = objectIdToString(bytes, bufferStart + 16);
  const offsetTableStart = bufferStart + 32;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varDataStart = bufferStart + 32 + 8;
  const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  let description: string | null = null;
  if (bytes[varOffset1] === 1) {
    description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
  }
  const color = decoder.decode(bytes.subarray(varOffset2, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, description, owner, color }
  };
}

/**
 * Read a Project row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeProjectRows/decodeProjectDelta instead.
 */
export function readProject(reader: BinaryReader): { id: string; name: string; description: string | null; owner: string; color: string } {
  throw new Error('readProject requires row boundary context - use decodeProjectRows or decodeProjectDelta instead');
}

/**
 * Decode binary rows for Tasks table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 66 bytes
 * - Variable columns: 4
 * - Offset table: 12 bytes
 */
export function decodeTaskRows(buffer: ArrayBufferLike): Array<{ id: string; title: string; description: string | null; status: string; priority: string; project: string; assignee: string | null; createdAt: bigint; updatedAt: bigint; isCompleted: boolean }> {
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
    const assignee = bytes[bufferStart + 32] === 0 ? null : objectIdToString(bytes, bufferStart + 32 + 1);
    const createdAt = view.getBigInt64(bufferStart + 49, true);
    const updatedAt = view.getBigInt64(bufferStart + 57, true);
    const isCompleted = bytes[bufferStart + 65] === 1;

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 66;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varOffset3 = bufferStart + view.getUint32(offsetTableStart + 8, true);
    const varDataStart = bufferStart + 66 + 12;

    const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    let description: string | null = null;
    if (bytes[varOffset1] === 1) {
      description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
    }
    const status = decoder.decode(bytes.subarray(varOffset2, varOffset3));
    const priority = decoder.decode(bytes.subarray(varOffset3, rowEnd));

    rows[i] = { id, title, description, status, priority, project, assignee, createdAt, updatedAt, isCompleted };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Task delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeTaskDelta(buffer: ArrayBufferLike): Delta<{ id: string; title: string; description: string | null; status: string; priority: string; project: string; assignee: string | null; createdAt: bigint; updatedAt: bigint; isCompleted: boolean }> {
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
  const assignee = bytes[bufferStart + 32] === 0 ? null : objectIdToString(bytes, bufferStart + 32 + 1);
  const createdAt = view.getBigInt64(bufferStart + 49, true);
  const updatedAt = view.getBigInt64(bufferStart + 57, true);
  const isCompleted = bytes[bufferStart + 65] === 1;
  const offsetTableStart = bufferStart + 66;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varOffset3 = bufferStart + view.getUint32(offsetTableStart + 8, true);
  const varDataStart = bufferStart + 66 + 12;
  const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  let description: string | null = null;
  if (bytes[varOffset1] === 1) {
    description = decoder.decode(bytes.subarray(varOffset1 + 1, varOffset2));
  }
  const status = decoder.decode(bytes.subarray(varOffset2, varOffset3));
  const priority = decoder.decode(bytes.subarray(varOffset3, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, title, description, status, priority, project, assignee, createdAt, updatedAt, isCompleted }
  };
}

/**
 * Read a Task row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeTaskRows/decodeTaskDelta instead.
 */
export function readTask(reader: BinaryReader): { id: string; title: string; description: string | null; status: string; priority: string; project: string; assignee: string | null; createdAt: bigint; updatedAt: bigint; isCompleted: boolean } {
  throw new Error('readTask requires row boundary context - use decodeTaskRows or decodeTaskDelta instead');
}

/**
 * Decode binary rows for Tags table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 16 bytes
 * - Variable columns: 2
 * - Offset table: 4 bytes
 */
export function decodeTagRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; color: string }> {
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
 * Decode a Tag delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeTagDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string }> {
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
 * Read a Tag row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeTagRows/decodeTagDelta instead.
 */
export function readTag(reader: BinaryReader): { id: string; name: string; color: string } {
  throw new Error('readTag requires row boundary context - use decodeTagRows or decodeTagDelta instead');
}

/**
 * Decode binary rows for TaskTags table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 48 bytes
 * - Variable columns: 0
 * - Offset table: 0 bytes
 */
export function decodeTaskTagRows(buffer: ArrayBufferLike): Array<{ id: string; task: string; tag: string }> {
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
    const task = objectIdToString(bytes, bufferStart + 16);
    const tag = objectIdToString(bytes, bufferStart + 32);

    rows[i] = { id, task, tag };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a TaskTag delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeTaskTagDelta(buffer: ArrayBufferLike): Delta<{ id: string; task: string; tag: string }> {
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
  const task = objectIdToString(bytes, bufferStart + 16);
  const tag = objectIdToString(bytes, bufferStart + 32);

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, task, tag }
  };
}

/**
 * Read a TaskTag row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readTaskTag(reader: BinaryReader): { id: string; task: string; tag: string } {
  const id = reader.readObjectId();
  const task = reader.readObjectId();
  const tag = reader.readObjectId();
  return { id, task, tag };
}

/**
 * Decode binary rows for Categories table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 33 bytes
 * - Variable columns: 1
 * - Offset table: 0 bytes
 */
export function decodeCategoryRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; parent: string | null }> {
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
    const parent = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 33;
    const varDataStart = bufferStart + 33 + 0;

    const name = decoder.decode(bytes.subarray(varDataStart, rowEnd));

    rows[i] = { id, name, parent };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Category delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeCategoryDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; parent: string | null }> {
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
  const parent = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);
  const offsetTableStart = bufferStart + 33;
  const varDataStart = bufferStart + 33 + 0;
  const name = decoder.decode(bytes.subarray(varDataStart, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, parent }
  };
}

/**
 * Read a Category row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeCategoryRows/decodeCategoryDelta instead.
 */
export function readCategory(reader: BinaryReader): { id: string; name: string; parent: string | null } {
  throw new Error('readCategory requires row boundary context - use decodeCategoryRows or decodeCategoryDelta instead');
}

/**
 * Decode binary rows for Comments table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 74 bytes
 * - Variable columns: 1
 * - Offset table: 0 bytes
 */
export function decodeCommentRows(buffer: ArrayBufferLike): Array<{ id: string; content: string; author: string; task: string | null; parentComment: string | null; createdAt: bigint }> {
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
    const author = objectIdToString(bytes, bufferStart + 16);
    const task = bytes[bufferStart + 32] === 0 ? null : objectIdToString(bytes, bufferStart + 32 + 1);
    const parentComment = bytes[bufferStart + 49] === 0 ? null : objectIdToString(bytes, bufferStart + 49 + 1);
    const createdAt = view.getBigInt64(bufferStart + 66, true);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 74;
    const varDataStart = bufferStart + 74 + 0;

    const content = decoder.decode(bytes.subarray(varDataStart, rowEnd));

    rows[i] = { id, content, author, task, parentComment, createdAt };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Comment delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [row buffer with id] or just ObjectId for removed
 */
export function decodeCommentDelta(buffer: ArrayBufferLike): Delta<{ id: string; content: string; author: string; task: string | null; parentComment: string | null; createdAt: bigint }> {
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
  const author = objectIdToString(bytes, bufferStart + 16);
  const task = bytes[bufferStart + 32] === 0 ? null : objectIdToString(bytes, bufferStart + 32 + 1);
  const parentComment = bytes[bufferStart + 49] === 0 ? null : objectIdToString(bytes, bufferStart + 49 + 1);
  const createdAt = view.getBigInt64(bufferStart + 66, true);
  const offsetTableStart = bufferStart + 74;
  const varDataStart = bufferStart + 74 + 0;
  const content = decoder.decode(bytes.subarray(varDataStart, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, content, author, task, parentComment, createdAt }
  };
}

/**
 * Read a Comment row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeCommentRows/decodeCommentDelta instead.
 */
export function readComment(reader: BinaryReader): { id: string; content: string; author: string; task: string | null; parentComment: string | null; createdAt: bigint } {
  throw new Error('readComment requires row boundary context - use decodeCommentRows or decodeCommentDelta instead');
}
