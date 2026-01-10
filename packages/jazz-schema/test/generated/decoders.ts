// Generated from SQL schema by @jazz/schema
// DO NOT EDIT MANUALLY

// Shared decoder for UTF-8 strings
const decoder = new TextDecoder();

// Delta type constants
export const DELTA_ADDED = 1;
export const DELTA_UPDATED = 2;
export const DELTA_REMOVED = 3;

// Crockford Base32 alphabet (matches Rust ObjectId encoding)
const CROCKFORD_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

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
 * - Fixed size: 17 bytes
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
    // Read row size (includes 16-byte ObjectId + row buffer)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;

    // Read ObjectId (16 bytes binary -> Base32 string)
    const id = objectIdToString(bytes, offset);
    offset += 16;
    const bufferStart = offset; // Start of row buffer (after ObjectId)

    // Fixed columns
    const age = view.getBigInt64(bufferStart + 0, true);
    const score = view.getFloat64(bufferStart + 8, true);
    const isAdmin = bytes[bufferStart + 16] === 1;

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 17;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
    const varDataStart = bufferStart + 17 + 8;

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
 * Format: u8 type (1=added, 2=updated, 3=removed) + [16-byte ObjectId][row buffer] or just ObjectId
 */
export function decodeUserDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row
  const id = objectIdToString(bytes, 1);
  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)
  const rowEnd = bytes.length;

  const age = view.getBigInt64(bufferStart + 0, true);
  const score = view.getFloat64(bufferStart + 8, true);
  const isAdmin = bytes[bufferStart + 16] === 1;
  const offsetTableStart = bufferStart + 17;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varOffset2 = bufferStart + view.getUint32(offsetTableStart + 4, true);
  const varDataStart = bufferStart + 17 + 8;
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
 * Decode binary rows for Folders table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 33 bytes
 * - Variable columns: 1
 * - Offset table: 0 bytes
 */
export function decodeFolderRows(buffer: ArrayBufferLike): Array<{ id: string; name: string; owner: string; parent: string | null }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    // Read row size (includes 16-byte ObjectId + row buffer)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;

    // Read ObjectId (16 bytes binary -> Base32 string)
    const id = objectIdToString(bytes, offset);
    offset += 16;
    const bufferStart = offset; // Start of row buffer (after ObjectId)

    // Fixed columns
    const owner = objectIdToString(bytes, bufferStart + 0);
    const parent = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 33;
    const varDataStart = bufferStart + 33 + 0;

    const name = decoder.decode(bytes.subarray(varDataStart, rowEnd));

    rows[i] = { id, name, owner, parent };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Folder delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [16-byte ObjectId][row buffer] or just ObjectId
 */
export function decodeFolderDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; owner: string; parent: string | null }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row
  const id = objectIdToString(bytes, 1);
  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)
  const rowEnd = bytes.length;

  const owner = objectIdToString(bytes, bufferStart + 0);
  const parent = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);
  const offsetTableStart = bufferStart + 33;
  const varDataStart = bufferStart + 33 + 0;
  const name = decoder.decode(bytes.subarray(varDataStart, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, name, owner, parent }
  };
}

/**
 * Read a Folder row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeFolderRows/decodeFolderDelta instead.
 */
export function readFolder(reader: BinaryReader): { id: string; name: string; owner: string; parent: string | null } {
  throw new Error('readFolder requires row boundary context - use decodeFolderRows or decodeFolderDelta instead');
}

/**
 * Decode binary rows for Notes table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 50 bytes
 * - Variable columns: 2
 * - Offset table: 4 bytes
 */
export function decodeNoteRows(buffer: ArrayBufferLike): Array<{ id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  let offset = 0;

  // Read row count
  const rowCount = view.getUint32(offset, true);
  offset += 4;

  const rows = new Array(rowCount);

  for (let i = 0; i < rowCount; i++) {
    // Read row size (includes 16-byte ObjectId + row buffer)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;

    // Read ObjectId (16 bytes binary -> Base32 string)
    const id = objectIdToString(bytes, offset);
    offset += 16;
    const bufferStart = offset; // Start of row buffer (after ObjectId)

    // Fixed columns
    const author = objectIdToString(bytes, bufferStart + 0);
    const folder = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);
    const createdAt = view.getBigInt64(bufferStart + 33, true);
    const updatedAt = view.getBigInt64(bufferStart + 41, true);
    const isPublic = bytes[bufferStart + 49] === 1;

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 50;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varDataStart = bufferStart + 50 + 4;

    const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const content = decoder.decode(bytes.subarray(varOffset1, rowEnd));

    rows[i] = { id, title, content, author, folder, createdAt, updatedAt, isPublic };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Note delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [16-byte ObjectId][row buffer] or just ObjectId
 */
export function decodeNoteDelta(buffer: ArrayBufferLike): Delta<{ id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row
  const id = objectIdToString(bytes, 1);
  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)
  const rowEnd = bytes.length;

  const author = objectIdToString(bytes, bufferStart + 0);
  const folder = bytes[bufferStart + 16] === 0 ? null : objectIdToString(bytes, bufferStart + 16 + 1);
  const createdAt = view.getBigInt64(bufferStart + 33, true);
  const updatedAt = view.getBigInt64(bufferStart + 41, true);
  const isPublic = bytes[bufferStart + 49] === 1;
  const offsetTableStart = bufferStart + 50;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varDataStart = bufferStart + 50 + 4;
  const title = decoder.decode(bytes.subarray(varDataStart, varOffset1));
  const content = decoder.decode(bytes.subarray(varOffset1, rowEnd));

  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row: { id, title, content, author, folder, createdAt, updatedAt, isPublic }
  };
}

/**
 * Read a Note row using a BinaryReader.
 * NOTE: This table has variable columns - use decodeNoteRows/decodeNoteDelta instead.
 */
export function readNote(reader: BinaryReader): { id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean } {
  throw new Error('readNote requires row boundary context - use decodeNoteRows or decodeNoteDelta instead');
}

/**
 * Decode binary rows for Tags table (batch format)
 *
 * Row buffer layout:
 * - Fixed size: 0 bytes
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
    // Read row size (includes 16-byte ObjectId + row buffer)
    const rowSize = view.getUint32(offset, true);
    offset += 4;
    const rowStart = offset;
    const rowEnd = rowStart + rowSize;

    // Read ObjectId (16 bytes binary -> Base32 string)
    const id = objectIdToString(bytes, offset);
    offset += 16;
    const bufferStart = offset; // Start of row buffer (after ObjectId)

    // Variable columns (using offset table)
    const offsetTableStart = bufferStart + 0;
    const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
    const varDataStart = bufferStart + 0 + 4;

    const name = decoder.decode(bytes.subarray(varDataStart, varOffset1));
    const color = decoder.decode(bytes.subarray(varOffset1, rowEnd));

    rows[i] = { id, name, color };
    offset = rowEnd;
  }

  return rows;
}

/**
 * Decode a Tag delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + [16-byte ObjectId][row buffer] or just ObjectId
 */
export function decodeTagDelta(buffer: ArrayBufferLike): Delta<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer as ArrayBuffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    const id = objectIdToString(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the row
  const id = objectIdToString(bytes, 1);
  const bufferStart = 17; // 1 (delta type) + 16 (ObjectId)
  const rowEnd = bytes.length;

  const offsetTableStart = bufferStart + 0;
  const varOffset1 = bufferStart + view.getUint32(offsetTableStart + 0, true);
  const varDataStart = bufferStart + 0 + 4;
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
