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

  constructor(buffer: ArrayBuffer, startOffset = 0) {
    this.bytes = new Uint8Array(buffer);
    this.view = new DataView(buffer);
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
export function decodeUserRows(buffer: ArrayBuffer): Array<{ id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

    // avatar: string (nullable)
    const avatarPresent = view.getUint8(offset++);
    if (avatarPresent === 0) {
      row.avatar = null;
    } else {
      const avatarLen = view.getUint32(offset, true);
      offset += 4;
      row.avatar = decoder.decode(new Uint8Array(buffer, offset, avatarLen));
      offset += avatarLen;
    }

    // age: i64
    row.age = view.getBigInt64(offset, true);
    offset += 8;

    // score: f64
    row.score = view.getFloat64(offset, true);
    offset += 8;

    // isAdmin: bool
    row.isAdmin = view.getUint8(offset++) === 1;

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
export function decodeUserRow(buffer: ArrayBuffer, startOffset = 0): { row: { id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

  // avatar: string (nullable)
  const avatarPresent = view.getUint8(offset++);
  if (avatarPresent === 0) {
    row.avatar = null;
  } else {
    const avatarLen = view.getUint32(offset, true);
    offset += 4;
    row.avatar = decoder.decode(new Uint8Array(buffer, offset, avatarLen));
    offset += avatarLen;
  }

  // age: i64
  row.age = view.getBigInt64(offset, true);
  offset += 8;

  // score: f64
  row.score = view.getFloat64(offset, true);
  offset += 8;

  // isAdmin: bool
  row.isAdmin = view.getUint8(offset++) === 1;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a User delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeUserDelta(buffer: ArrayBuffer): Delta<{ id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean }> {
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
export function readUser(reader: BinaryReader): { id: string; name: string; email: string; avatar: string | null; age: bigint; score: number; isAdmin: boolean } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const email = reader.readString();
  const avatar = reader.readNullable(() => reader.readString());
  const age = reader.readI64();
  const score = reader.readF64();
  const isAdmin = reader.readBool();
  return { id, name, email, avatar, age, score, isAdmin };
}

/**
 * Decode binary rows for Folders table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Folder rows
 */
export function decodeFolderRows(buffer: ArrayBuffer): Array<{ id: string; name: string; owner: string; parent: string | null }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

    // owner: ref
    row.owner = decodeObjectId(bytes, offset);
    offset += 26;

    // parent: ref (nullable)
    const parentPresent = view.getUint8(offset++);
    if (parentPresent === 0) {
      row.parent = null;
    } else {
      row.parent = decodeObjectId(bytes, offset);
      offset += 26;
    }

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single Folder row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeFolderRow(buffer: ArrayBuffer, startOffset = 0): { row: { id: string; name: string; owner: string; parent: string | null }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

  // owner: ref
  row.owner = decodeObjectId(bytes, offset);
  offset += 26;

  // parent: ref (nullable)
  const parentPresent = view.getUint8(offset++);
  if (parentPresent === 0) {
    row.parent = null;
  } else {
    row.parent = decodeObjectId(bytes, offset);
    offset += 26;
  }

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a Folder delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeFolderDelta(buffer: ArrayBuffer): Delta<{ id: string; name: string; owner: string; parent: string | null }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeFolderRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Folder row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readFolder(reader: BinaryReader): { id: string; name: string; owner: string; parent: string | null } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const owner = reader.readObjectId();
  const parent = reader.readNullable(() => reader.readObjectId());
  return { id, name, owner, parent };
}

/**
 * Decode binary rows for Notes table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Note rows
 */
export function decodeNoteRows(buffer: ArrayBuffer): Array<{ id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

    // content: string
    const contentLen = view.getUint32(offset, true);
    offset += 4;
    row.content = decoder.decode(new Uint8Array(buffer, offset, contentLen));
    offset += contentLen;

    // author: ref
    row.author = decodeObjectId(bytes, offset);
    offset += 26;

    // folder: ref (nullable)
    const folderPresent = view.getUint8(offset++);
    if (folderPresent === 0) {
      row.folder = null;
    } else {
      row.folder = decodeObjectId(bytes, offset);
      offset += 26;
    }

    // createdAt: i64
    row.createdAt = view.getBigInt64(offset, true);
    offset += 8;

    // updatedAt: i64
    row.updatedAt = view.getBigInt64(offset, true);
    offset += 8;

    // isPublic: bool
    row.isPublic = view.getUint8(offset++) === 1;

    rows[i] = row;
  }

  return rows;
}

/**
 * Decode a single Note row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeNoteRow(buffer: ArrayBuffer, startOffset = 0): { row: { id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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

  // content: string
  const contentLen = view.getUint32(offset, true);
  offset += 4;
  row.content = decoder.decode(new Uint8Array(buffer, offset, contentLen));
  offset += contentLen;

  // author: ref
  row.author = decodeObjectId(bytes, offset);
  offset += 26;

  // folder: ref (nullable)
  const folderPresent = view.getUint8(offset++);
  if (folderPresent === 0) {
    row.folder = null;
  } else {
    row.folder = decodeObjectId(bytes, offset);
    offset += 26;
  }

  // createdAt: i64
  row.createdAt = view.getBigInt64(offset, true);
  offset += 8;

  // updatedAt: i64
  row.updatedAt = view.getBigInt64(offset, true);
  offset += 8;

  // isPublic: bool
  row.isPublic = view.getUint8(offset++) === 1;

  return { row, bytesRead: offset - startOffset };
}

/**
 * Decode a Note delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeNoteDelta(buffer: ArrayBuffer): Delta<{ id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeNoteRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Note row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readNote(reader: BinaryReader): { id: string; title: string; content: string; author: string; folder: string | null; createdAt: bigint; updatedAt: bigint; isPublic: boolean } {
  const id = reader.readObjectId();
  const title = reader.readString();
  const content = reader.readString();
  const author = reader.readObjectId();
  const folder = reader.readNullable(() => reader.readObjectId());
  const createdAt = reader.readI64();
  const updatedAt = reader.readI64();
  const isPublic = reader.readBool();
  return { id, title, content, author, folder, createdAt, updatedAt, isPublic };
}

/**
 * Decode binary rows for Tags table (batch format)
 * @param buffer ArrayBuffer from WASM
 * @returns Array of Tag rows
 */
export function decodeTagRows(buffer: ArrayBuffer): Array<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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
 * Decode a single Tag row from binary (no header)
 * @param buffer ArrayBuffer containing a single row
 * @param startOffset Byte offset to start reading from
 * @returns Decoded row and bytes consumed
 */
export function decodeTagRow(buffer: ArrayBuffer, startOffset = 0): { row: { id: string; name: string; color: string }; bytesRead: number } {
  const bytes = new Uint8Array(buffer);
  const view = new DataView(buffer);
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
 * Decode a Tag delta from binary
 * Format: u8 type (1=added, 2=updated, 3=removed) + row data or id
 * @param buffer ArrayBuffer containing a single delta
 * @returns Decoded delta
 */
export function decodeTagDelta(buffer: ArrayBuffer): Delta<{ id: string; name: string; color: string }> {
  const bytes = new Uint8Array(buffer);
  const deltaType = bytes[0];

  if (deltaType === DELTA_REMOVED) {
    // Removed: just the ObjectId
    const id = decodeObjectId(bytes, 1);
    return { type: 'removed', id };
  }

  // Added or Updated: decode the full row
  const { row } = decodeTagRow(buffer, 1);
  return {
    type: deltaType === DELTA_ADDED ? 'added' : 'updated',
    row
  };
}

/**
 * Read a Tag row using a BinaryReader.
 * Use this for nested/joined row decoding.
 */
export function readTag(reader: BinaryReader): { id: string; name: string; color: string } {
  const id = reader.readObjectId();
  const name = reader.readString();
  const color = reader.readString();
  return { id, name, color };
}
