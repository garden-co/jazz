/**
 * Tests for binary row decoders
 *
 * These tests use the row buffer format expected by the generated decoders.
 * The format matches what the Rust encoder produces.
 *
 * Row buffer format:
 * - [id (16 bytes u128 LE)][fixed columns][offset table (N-1 u32s for N var cols)][variable data]
 *
 * Batch format:
 * - [u32 count][u32 size₁][row buffer₁]...
 *
 * Delta format:
 * - [u8 type][row buffer] for added/updated
 * - [u8 type][16 byte ObjectId] for removed
 */

import { describe, expect, it } from "vitest";
import {
  BinaryReader,
  DELTA_ADDED,
  DELTA_REMOVED,
  DELTA_UPDATED,
  decodeFolderRows,
  decodeNoteDelta,
  decodeNoteRows,
  decodeUserDelta,
  decodeUserRows,
} from "./generated/decoders.js";

// Crockford Base32 alphabet (lowercase, matches Rust)
const CROCKFORD_ALPHABET = "0123456789abcdefghjkmnpqrstvwxyz";

// Helper to create a test ObjectId (26 Base32 chars)
function makeObjectId(n: number): string {
  const base = "00000000000000000000000000"; // 26 zeros
  const str = n.toString();
  // Ensure total length is always 26 by slicing the base appropriately
  return base.slice(0, 26 - str.length) + str;
}

// Helper to convert Base32 string to BigInt
function base32ToU128(s: string): bigint {
  let value = 0n;
  for (const c of s.toLowerCase()) {
    const idx = CROCKFORD_ALPHABET.indexOf(c);
    value = (value << 5n) | BigInt(idx >= 0 ? idx : 0);
  }
  return value;
}

// Helper to encode an ObjectId to 16 bytes (u128 LE)
function encodeObjectId(id: string): Uint8Array {
  const value = base32ToU128(id);
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  view.setBigUint64(0, value & 0xffffffffffffffffn, true); // Low 64 bits
  view.setBigUint64(8, value >> 64n, true); // High 64 bits
  return bytes;
}

// Helper to encode i64 (8 bytes LE)
function encodeI64(n: bigint): Uint8Array {
  const result = new Uint8Array(8);
  new DataView(result.buffer).setBigInt64(0, n, true);
  return result;
}

// Helper to encode f64 (8 bytes LE)
function encodeF64(n: number): Uint8Array {
  const result = new Uint8Array(8);
  new DataView(result.buffer).setFloat64(0, n, true);
  return result;
}

// Helper to encode bool (1 byte)
function encodeBool(b: boolean): Uint8Array {
  return new Uint8Array([b ? 1 : 0]);
}

// Helper to concat Uint8Arrays
function concat(...arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}

// Helper to encode u32 header
function encodeU32(n: number): Uint8Array {
  const result = new Uint8Array(4);
  new DataView(result.buffer).setUint32(0, n, true);
  return result;
}

// UTF-8 encoder
const textEncoder = new TextEncoder();

/**
 * Encode a User row buffer.
 *
 * Layout:
 * - Fixed size: 33 bytes
 *   - id: 16 bytes (offset 0)
 *   - age: 8 bytes i64 (offset 16)
 *   - score: 8 bytes f64 (offset 24)
 *   - isAdmin: 1 byte bool (offset 32)
 * - Offset table: 8 bytes (2 u32s for offsets within row buffer)
 * - Variable columns (start at offset 41):
 *   - name: plain string
 *   - email: plain string
 *   - avatar: nullable (presence byte + string or just presence byte if null)
 */
function encodeUserRowBuffer(user: {
  id: string;
  name: string;
  email: string;
  avatar: string | null;
  age: bigint;
  score: number;
  isAdmin: boolean;
}): Uint8Array {
  const nameBytes = textEncoder.encode(user.name);
  const emailBytes = textEncoder.encode(user.email);
  const avatarBytes = user.avatar
    ? textEncoder.encode(user.avatar)
    : new Uint8Array(0);

  // Fixed section
  const fixedSection = concat(
    encodeObjectId(user.id),
    encodeI64(user.age),
    encodeF64(user.score),
    encodeBool(user.isAdmin),
  );
  // fixedSection is 33 bytes

  // Offset table starts at byte 33
  // Variable data starts at byte 41 (33 + 8)
  const varDataStart = 41;
  const nameEnd = varDataStart + nameBytes.length;
  const emailEnd = nameEnd + emailBytes.length;
  // avatar is nullable - presence byte + optional data

  // Offset table: offsets are relative to row buffer start
  const offsetTable = concat(encodeU32(nameEnd), encodeU32(emailEnd));

  // Variable data
  let varData: Uint8Array;
  if (user.avatar !== null) {
    varData = concat(
      nameBytes,
      emailBytes,
      new Uint8Array([1]), // presence byte
      avatarBytes,
    );
  } else {
    varData = concat(
      nameBytes,
      emailBytes,
      new Uint8Array([0]), // null presence byte
    );
  }

  return concat(fixedSection, offsetTable, varData);
}

/**
 * Encode a Folder row buffer.
 *
 * Layout:
 * - Fixed size: 49 bytes
 *   - id: 16 bytes (offset 0)
 *   - owner: 16 bytes ref (offset 16)
 *   - parent: 17 bytes nullable ref (1 presence + 16 bytes) (offset 32)
 * - Offset table: 0 bytes (only 1 variable column)
 * - Variable columns (start at offset 49):
 *   - name: plain string
 */
function encodeFolderRowBuffer(folder: {
  id: string;
  name: string;
  owner: string;
  parent: string | null;
}): Uint8Array {
  const nameBytes = textEncoder.encode(folder.name);

  // Fixed section
  let fixedSection: Uint8Array;
  if (folder.parent !== null) {
    fixedSection = concat(
      encodeObjectId(folder.id),
      encodeObjectId(folder.owner),
      new Uint8Array([1]), // presence byte
      encodeObjectId(folder.parent),
    );
  } else {
    fixedSection = concat(
      encodeObjectId(folder.id),
      encodeObjectId(folder.owner),
      new Uint8Array([0]), // null presence byte
      new Uint8Array(16), // zeroed ObjectId placeholder
    );
  }
  // fixedSection is 49 bytes

  // No offset table for 1 variable column
  // Variable data starts at byte 49
  return concat(fixedSection, nameBytes);
}

/**
 * Encode a Note row buffer.
 *
 * Layout:
 * - Fixed size: 66 bytes
 *   - id: 16 bytes (offset 0)
 *   - author: 16 bytes ref (offset 16)
 *   - folder: 17 bytes nullable ref (1 presence + 16 bytes) (offset 32)
 *   - createdAt: 8 bytes i64 (offset 49)
 *   - updatedAt: 8 bytes i64 (offset 57)
 *   - isPublic: 1 byte bool (offset 65)
 * - Offset table: 4 bytes (1 u32 for 2 variable columns)
 * - Variable columns (start at offset 70):
 *   - title: plain string
 *   - content: plain string
 */
function encodeNoteRowBuffer(note: {
  id: string;
  title: string;
  content: string;
  author: string;
  folder: string | null;
  createdAt: bigint;
  updatedAt: bigint;
  isPublic: boolean;
}): Uint8Array {
  const titleBytes = textEncoder.encode(note.title);
  const contentBytes = textEncoder.encode(note.content);

  // Fixed section
  let fixedSection: Uint8Array;
  if (note.folder !== null) {
    fixedSection = concat(
      encodeObjectId(note.id),
      encodeObjectId(note.author),
      new Uint8Array([1]), // presence byte
      encodeObjectId(note.folder),
      encodeI64(note.createdAt),
      encodeI64(note.updatedAt),
      encodeBool(note.isPublic),
    );
  } else {
    fixedSection = concat(
      encodeObjectId(note.id),
      encodeObjectId(note.author),
      new Uint8Array([0]), // null presence byte
      new Uint8Array(16), // zeroed ObjectId placeholder
      encodeI64(note.createdAt),
      encodeI64(note.updatedAt),
      encodeBool(note.isPublic),
    );
  }
  // fixedSection is 66 bytes

  // Offset table starts at byte 66
  // Variable data starts at byte 70 (66 + 4)
  const varDataStart = 70;
  const titleEnd = varDataStart + titleBytes.length;

  // Offset table: offset is relative to row buffer start
  const offsetTable = encodeU32(titleEnd);

  // Variable data
  const varData = concat(titleBytes, contentBytes);

  return concat(fixedSection, offsetTable, varData);
}

/**
 * Encode a batch of rows with count and size headers.
 */
function encodeBatch(rowBuffers: Uint8Array[]): Uint8Array {
  const parts: Uint8Array[] = [encodeU32(rowBuffers.length)];
  for (const rowBuffer of rowBuffers) {
    parts.push(encodeU32(rowBuffer.length));
    parts.push(rowBuffer);
  }
  return concat(...parts);
}

describe("Binary Decoders", () => {
  describe("decodeUserRows", () => {
    it("decodes empty rows", () => {
      const buffer = encodeU32(0).buffer; // 0 rows
      const rows = decodeUserRows(buffer);
      expect(rows).toEqual([]);
    });

    it("decodes a single user row", () => {
      const id = makeObjectId(1);
      const rowBuffer = encodeUserRowBuffer({
        id,
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: 25n,
        score: 100.5,
        isAdmin: true,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const rows = decodeUserRows(buffer);

      expect(rows).toHaveLength(1);
      expect(rows[0]).toEqual({
        id,
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: 25n,
        score: 100.5,
        isAdmin: true,
      });
    });

    it("decodes user with non-null avatar", () => {
      const id = makeObjectId(2);
      const rowBuffer = encodeUserRowBuffer({
        id,
        name: "Bob",
        email: "bob@example.com",
        avatar: "https://example.com/avatar.png",
        age: 30n,
        score: 200.0,
        isAdmin: false,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const rows = decodeUserRows(buffer);

      expect(rows).toHaveLength(1);
      expect(rows[0].avatar).toBe("https://example.com/avatar.png");
      expect(rows[0].isAdmin).toBe(false);
    });

    it("decodes multiple rows", () => {
      const id1 = makeObjectId(1);
      const id2 = makeObjectId(2);

      const row1 = encodeUserRowBuffer({
        id: id1,
        name: "Alice",
        email: "alice@example.com",
        avatar: null,
        age: 25n,
        score: 100.0,
        isAdmin: true,
      });

      const row2 = encodeUserRowBuffer({
        id: id2,
        name: "Bob",
        email: "bob@example.com",
        avatar: null,
        age: 30n,
        score: 200.0,
        isAdmin: false,
      });

      const buffer = encodeBatch([row1, row2]).buffer;
      const rows = decodeUserRows(buffer);

      expect(rows).toHaveLength(2);
      expect(rows[0].name).toBe("Alice");
      expect(rows[1].name).toBe("Bob");
    });
  });

  describe("decodeUserDelta", () => {
    it("decodes DELTA_ADDED", () => {
      const id = makeObjectId(5);
      const rowBuffer = encodeUserRowBuffer({
        id,
        name: "Eve",
        email: "eve@example.com",
        avatar: null,
        age: 22n,
        score: 50.0,
        isAdmin: false,
      });

      const deltaBuffer = concat(
        new Uint8Array([DELTA_ADDED]),
        rowBuffer,
      ).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("added");
      if (delta.type === "added") {
        expect(delta.row.name).toBe("Eve");
      }
    });

    it("decodes DELTA_UPDATED", () => {
      const id = makeObjectId(6);
      const rowBuffer = encodeUserRowBuffer({
        id,
        name: "Frank Updated",
        email: "frank@example.com",
        avatar: null,
        age: 40n,
        score: 300.0,
        isAdmin: true,
      });

      const deltaBuffer = concat(
        new Uint8Array([DELTA_UPDATED]),
        rowBuffer,
      ).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("updated");
      if (delta.type === "updated") {
        expect(delta.row.name).toBe("Frank Updated");
      }
    });

    it("decodes DELTA_REMOVED", () => {
      const id = makeObjectId(7);
      const deltaBuffer = concat(
        new Uint8Array([DELTA_REMOVED]),
        encodeObjectId(id),
      ).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("removed");
      if (delta.type === "removed") {
        expect(delta.id).toBe(id);
      }
    });
  });

  describe("BinaryReader", () => {
    it("reads ObjectId correctly", () => {
      const id = makeObjectId(1);
      const data = encodeObjectId(id);

      const reader = new BinaryReader(data.buffer);
      expect(reader.readObjectId()).toBe(id);
    });

    it("reads various fixed types", () => {
      const data = concat(
        encodeObjectId(makeObjectId(1)),
        encodeI64(123456789n),
        encodeF64(3.14159),
        encodeBool(true),
        encodeU32(42),
      );

      const reader = new BinaryReader(data.buffer);

      expect(reader.readObjectId()).toBe(makeObjectId(1));
      expect(reader.readI64()).toBe(123456789n);
      expect(reader.readF64()).toBeCloseTo(3.14159);
      expect(reader.readBool()).toBe(true);
      expect(reader.readU32()).toBe(42);
    });

    it("reads nullable ref values", () => {
      const refId = makeObjectId(500);
      const data = concat(
        new Uint8Array([0]), // null
        new Uint8Array(16), // zeroed ObjectId placeholder for null
        new Uint8Array([1]), // present
        encodeObjectId(refId),
      );

      const reader = new BinaryReader(data.buffer);

      expect(reader.readNullableRef()).toBe(null);
      expect(reader.readNullableRef()).toBe(refId);
    });
  });

  describe("decodeNoteRows (with refs)", () => {
    it("decodes a note with non-nullable author ref", () => {
      const noteId = makeObjectId(100);
      const authorId = makeObjectId(101);

      const rowBuffer = encodeNoteRowBuffer({
        id: noteId,
        title: "My Note",
        content: "Note content",
        author: authorId,
        folder: null,
        createdAt: 1000000n,
        updatedAt: 1000001n,
        isPublic: true,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const notes = decodeNoteRows(buffer);

      expect(notes).toHaveLength(1);
      expect(notes[0].id).toBe(noteId);
      expect(notes[0].title).toBe("My Note");
      expect(notes[0].author).toBe(authorId);
      expect(notes[0].folder).toBeNull();
      expect(notes[0].isPublic).toBe(true);
    });

    it("decodes a note with non-null folder ref", () => {
      const noteId = makeObjectId(102);
      const authorId = makeObjectId(103);
      const folderId = makeObjectId(104);

      const rowBuffer = encodeNoteRowBuffer({
        id: noteId,
        title: "Note in folder",
        content: "Content",
        author: authorId,
        folder: folderId,
        createdAt: 2000000n,
        updatedAt: 2000001n,
        isPublic: false,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const notes = decodeNoteRows(buffer);

      expect(notes).toHaveLength(1);
      expect(notes[0].author).toBe(authorId);
      expect(notes[0].folder).toBe(folderId);
    });
  });

  describe("decodeFolderRows (with self-ref)", () => {
    it("decodes folder with null parent", () => {
      const folderId = makeObjectId(200);
      const ownerId = makeObjectId(201);

      const rowBuffer = encodeFolderRowBuffer({
        id: folderId,
        name: "Root Folder",
        owner: ownerId,
        parent: null,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const folders = decodeFolderRows(buffer);

      expect(folders).toHaveLength(1);
      expect(folders[0].name).toBe("Root Folder");
      expect(folders[0].owner).toBe(ownerId);
      expect(folders[0].parent).toBeNull();
    });

    it("decodes folder with non-null parent (self-reference)", () => {
      const folderId = makeObjectId(202);
      const ownerId = makeObjectId(203);
      const parentId = makeObjectId(204);

      const rowBuffer = encodeFolderRowBuffer({
        id: folderId,
        name: "Subfolder",
        owner: ownerId,
        parent: parentId,
      });

      const buffer = encodeBatch([rowBuffer]).buffer;
      const folders = decodeFolderRows(buffer);

      expect(folders).toHaveLength(1);
      expect(folders[0].name).toBe("Subfolder");
      expect(folders[0].parent).toBe(parentId);
    });
  });

  describe("decodeNoteDelta", () => {
    it("decodes added note with refs", () => {
      const noteId = makeObjectId(300);
      const authorId = makeObjectId(301);

      const rowBuffer = encodeNoteRowBuffer({
        id: noteId,
        title: "Delta Note",
        content: "Delta Content",
        author: authorId,
        folder: null,
        createdAt: 3000000n,
        updatedAt: 3000001n,
        isPublic: true,
      });

      const buffer = concat(new Uint8Array([DELTA_ADDED]), rowBuffer).buffer;
      const delta = decodeNoteDelta(buffer);

      expect(delta.type).toBe("added");
      if (delta.type === "added") {
        expect(delta.row.title).toBe("Delta Note");
        expect(delta.row.author).toBe(authorId);
        expect(delta.row.folder).toBeNull();
      }
    });

    it("decodes removed note", () => {
      const noteId = makeObjectId(302);

      const buffer = concat(
        new Uint8Array([DELTA_REMOVED]),
        encodeObjectId(noteId),
      ).buffer;
      const delta = decodeNoteDelta(buffer);

      expect(delta.type).toBe("removed");
      if (delta.type === "removed") {
        expect(delta.id).toBe(noteId);
      }
    });
  });
});
