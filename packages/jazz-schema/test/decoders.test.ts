/**
 * Tests for binary row decoders
 */

import { describe, it, expect } from "vitest";
import {
  decodeUserRows,
  decodeUserRow,
  decodeUserDelta,
  decodeNoteRows,
  decodeNoteRow,
  decodeNoteDelta,
  decodeFolderRows,
  decodeFolderRow,
  BinaryReader,
  readUser,
  readNote,
  readFolder,
  DELTA_ADDED,
  DELTA_UPDATED,
  DELTA_REMOVED,
} from "./generated/decoders.js";

// Helper to create a test ObjectId (26 Base32 chars)
function makeObjectId(n: number): string {
  const base = "00000000000000000000000000";
  const suffix = n.toString().padStart(6, "0");
  return base.slice(0, 20) + suffix;
}

// Helper to encode an ObjectId to bytes
function encodeObjectId(id: string): Uint8Array {
  const bytes = new Uint8Array(26);
  for (let i = 0; i < 26; i++) {
    bytes[i] = id.charCodeAt(i);
  }
  return bytes;
}

// Helper to encode a string (u32 length + UTF-8 bytes)
function encodeString(s: string): Uint8Array {
  const encoder = new TextEncoder();
  const strBytes = encoder.encode(s);
  const result = new Uint8Array(4 + strBytes.length);
  new DataView(result.buffer).setUint32(0, strBytes.length, true);
  result.set(strBytes, 4);
  return result;
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

describe("Binary Decoders", () => {
  describe("decodeUserRows", () => {
    it("decodes empty rows", () => {
      const buffer = encodeU32(0).buffer; // 0 rows
      const rows = decodeUserRows(buffer);
      expect(rows).toEqual([]);
    });

    it("decodes a single user row", () => {
      const id = makeObjectId(1);
      const row = concat(
        encodeObjectId(id),
        encodeString("Alice"),
        encodeString("alice@example.com"),
        new Uint8Array([0]), // avatar is null
        encodeI64(25n),
        encodeF64(100.5),
        encodeBool(true)
      );

      const buffer = concat(encodeU32(1), row).buffer;
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
      const row = concat(
        encodeObjectId(id),
        encodeString("Bob"),
        encodeString("bob@example.com"),
        new Uint8Array([1]), // avatar is present
        encodeString("https://example.com/avatar.png"),
        encodeI64(30n),
        encodeF64(200.0),
        encodeBool(false)
      );

      const buffer = concat(encodeU32(1), row).buffer;
      const rows = decodeUserRows(buffer);

      expect(rows).toHaveLength(1);
      expect(rows[0].avatar).toBe("https://example.com/avatar.png");
      expect(rows[0].isAdmin).toBe(false);
    });

    it("decodes multiple rows", () => {
      const id1 = makeObjectId(1);
      const id2 = makeObjectId(2);

      const row1 = concat(
        encodeObjectId(id1),
        encodeString("Alice"),
        encodeString("alice@example.com"),
        new Uint8Array([0]),
        encodeI64(25n),
        encodeF64(100.0),
        encodeBool(true)
      );

      const row2 = concat(
        encodeObjectId(id2),
        encodeString("Bob"),
        encodeString("bob@example.com"),
        new Uint8Array([0]),
        encodeI64(30n),
        encodeF64(200.0),
        encodeBool(false)
      );

      const buffer = concat(encodeU32(2), row1, row2).buffer;
      const rows = decodeUserRows(buffer);

      expect(rows).toHaveLength(2);
      expect(rows[0].name).toBe("Alice");
      expect(rows[1].name).toBe("Bob");
    });
  });

  describe("decodeUserRow", () => {
    it("decodes a single row without header", () => {
      const id = makeObjectId(3);
      const rowBytes = concat(
        encodeObjectId(id),
        encodeString("Charlie"),
        encodeString("charlie@example.com"),
        new Uint8Array([0]),
        encodeI64(35n),
        encodeF64(150.0),
        encodeBool(false)
      );

      const { row, bytesRead } = decodeUserRow(rowBytes.buffer);

      expect(row.name).toBe("Charlie");
      expect(row.id).toBe(id);
      expect(bytesRead).toBe(rowBytes.length);
    });

    it("decodes from an offset", () => {
      const prefix = new Uint8Array([0, 0, 0, 0]); // 4 bytes padding
      const id = makeObjectId(4);
      const rowBytes = concat(
        encodeObjectId(id),
        encodeString("Diana"),
        encodeString("diana@example.com"),
        new Uint8Array([0]),
        encodeI64(28n),
        encodeF64(175.0),
        encodeBool(true)
      );

      const buffer = concat(prefix, rowBytes).buffer;
      const { row, bytesRead } = decodeUserRow(buffer, 4);

      expect(row.name).toBe("Diana");
      expect(bytesRead).toBe(rowBytes.length);
    });
  });

  describe("decodeUserDelta", () => {
    it("decodes DELTA_ADDED", () => {
      const id = makeObjectId(5);
      const rowBytes = concat(
        encodeObjectId(id),
        encodeString("Eve"),
        encodeString("eve@example.com"),
        new Uint8Array([0]),
        encodeI64(22n),
        encodeF64(50.0),
        encodeBool(false)
      );

      const deltaBuffer = concat(new Uint8Array([DELTA_ADDED]), rowBytes).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("added");
      if (delta.type === "added") {
        expect(delta.row.name).toBe("Eve");
      }
    });

    it("decodes DELTA_UPDATED", () => {
      const id = makeObjectId(6);
      const rowBytes = concat(
        encodeObjectId(id),
        encodeString("Frank Updated"),
        encodeString("frank@example.com"),
        new Uint8Array([0]),
        encodeI64(40n),
        encodeF64(300.0),
        encodeBool(true)
      );

      const deltaBuffer = concat(new Uint8Array([DELTA_UPDATED]), rowBytes).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("updated");
      if (delta.type === "updated") {
        expect(delta.row.name).toBe("Frank Updated");
      }
    });

    it("decodes DELTA_REMOVED", () => {
      const id = makeObjectId(7);
      const deltaBuffer = concat(new Uint8Array([DELTA_REMOVED]), encodeObjectId(id)).buffer;
      const delta = decodeUserDelta(deltaBuffer);

      expect(delta.type).toBe("removed");
      if (delta.type === "removed") {
        expect(delta.id).toBe(id);
      }
    });
  });

  describe("BinaryReader", () => {
    it("reads various types", () => {
      const data = concat(
        encodeObjectId(makeObjectId(1)),
        encodeString("test"),
        encodeI64(123456789n),
        encodeF64(3.14159),
        encodeBool(true),
        encodeU32(42)
      );

      const reader = new BinaryReader(data.buffer);

      expect(reader.readObjectId()).toBe(makeObjectId(1));
      expect(reader.readString()).toBe("test");
      expect(reader.readI64()).toBe(123456789n);
      expect(reader.readF64()).toBeCloseTo(3.14159);
      expect(reader.readBool()).toBe(true);
      expect(reader.readU32()).toBe(42);
    });

    it("reads nullable values", () => {
      const data = concat(
        new Uint8Array([0]), // null
        new Uint8Array([1]), // present
        encodeString("value")
      );

      const reader = new BinaryReader(data.buffer);

      expect(reader.readNullable(() => reader.readString())).toBe(null);
      expect(reader.readNullable(() => reader.readString())).toBe("value");
    });

    it("reads arrays", () => {
      const data = concat(
        encodeU32(3),
        encodeString("a"),
        encodeString("b"),
        encodeString("c")
      );

      const reader = new BinaryReader(data.buffer);
      const arr = reader.readArray(() => reader.readString());

      expect(arr).toEqual(["a", "b", "c"]);
    });
  });

  describe("decodeNoteRows (with refs)", () => {
    it("decodes a note with non-nullable author ref", () => {
      const noteId = makeObjectId(100);
      const authorId = makeObjectId(101);

      const noteBytes = concat(
        encodeObjectId(noteId),
        encodeString("My Note"),
        encodeString("Note content"),
        encodeObjectId(authorId), // author is non-nullable ref
        new Uint8Array([0]), // folder is null (nullable ref)
        encodeI64(1000000n),
        encodeI64(1000001n),
        encodeBool(true)
      );

      const buffer = concat(encodeU32(1), noteBytes).buffer;
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

      const noteBytes = concat(
        encodeObjectId(noteId),
        encodeString("Note in folder"),
        encodeString("Content"),
        encodeObjectId(authorId),
        new Uint8Array([1]), // Presence byte for non-null folder
        encodeObjectId(folderId),
        encodeI64(2000000n),
        encodeI64(2000001n),
        encodeBool(false)
      );

      const buffer = concat(encodeU32(1), noteBytes).buffer;
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

      const folderBytes = concat(
        encodeObjectId(folderId),
        encodeString("Root Folder"),
        encodeObjectId(ownerId), // owner is non-nullable
        new Uint8Array([0]) // parent is null
      );

      const buffer = concat(encodeU32(1), folderBytes).buffer;
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

      const folderBytes = concat(
        encodeObjectId(folderId),
        encodeString("Subfolder"),
        encodeObjectId(ownerId),
        new Uint8Array([1]), // Presence byte for non-null parent
        encodeObjectId(parentId)
      );

      const buffer = concat(encodeU32(1), folderBytes).buffer;
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

      const noteBytes = concat(
        encodeObjectId(noteId),
        encodeString("Delta Note"),
        encodeString("Delta Content"),
        encodeObjectId(authorId),
        new Uint8Array([0]), // null folder
        encodeI64(3000000n),
        encodeI64(3000001n),
        encodeBool(true)
      );

      const buffer = concat(new Uint8Array([DELTA_ADDED]), noteBytes).buffer;
      const delta = decodeNoteDelta(buffer);

      expect(delta.type).toBe("added");
      if (delta.type === "added") {
        expect(delta.row.title).toBe("Delta Note");
        expect(delta.row.author).toBe(authorId);
        expect(delta.row.folder).toBeNull();
      }
    });
  });

  describe("readUser (composable reader)", () => {
    it("reads a user using BinaryReader", () => {
      const id = makeObjectId(10);
      const userBytes = concat(
        encodeObjectId(id),
        encodeString("Grace"),
        encodeString("grace@example.com"),
        new Uint8Array([1]),
        encodeString("avatar.png"),
        encodeI64(45n),
        encodeF64(999.0),
        encodeBool(true)
      );

      const reader = new BinaryReader(userBytes.buffer);
      const user = readUser(reader);

      expect(user.id).toBe(id);
      expect(user.name).toBe("Grace");
      expect(user.email).toBe("grace@example.com");
      expect(user.avatar).toBe("avatar.png");
      expect(user.age).toBe(45n);
      expect(user.score).toBe(999.0);
      expect(user.isAdmin).toBe(true);
    });

    it("can compose readers for nested data", () => {
      // Simulate nested data: array of users
      const id1 = makeObjectId(11);
      const id2 = makeObjectId(12);

      const user1Bytes = concat(
        encodeObjectId(id1),
        encodeString("User1"),
        encodeString("user1@example.com"),
        new Uint8Array([0]),
        encodeI64(20n),
        encodeF64(100.0),
        encodeBool(false)
      );

      const user2Bytes = concat(
        encodeObjectId(id2),
        encodeString("User2"),
        encodeString("user2@example.com"),
        new Uint8Array([0]),
        encodeI64(30n),
        encodeF64(200.0),
        encodeBool(true)
      );

      const arrayData = concat(
        encodeU32(2), // 2 elements
        user1Bytes,
        user2Bytes
      );

      const reader = new BinaryReader(arrayData.buffer);
      const users = reader.readArray(() => readUser(reader));

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe("User1");
      expect(users[1].name).toBe("User2");
    });
  });

  describe("readNullableRef", () => {
    it("reads null ref (byte 0)", () => {
      const data = new Uint8Array([0]); // null indicator
      const reader = new BinaryReader(data.buffer);
      expect(reader.readNullableRef()).toBeNull();
      expect(reader.offset).toBe(1);
    });

    it("reads non-null ref (presence byte + 26 bytes)", () => {
      const refId = makeObjectId(500);
      const data = concat(
        new Uint8Array([1]), // Presence byte
        encodeObjectId(refId)
      );
      const reader = new BinaryReader(data.buffer);
      expect(reader.readNullableRef()).toBe(refId);
      expect(reader.offset).toBe(27); // 1 (presence) + 26 (ObjectId)
    });
  });

  describe("readNote (with refs)", () => {
    it("reads note with null folder", () => {
      const noteId = makeObjectId(600);
      const authorId = makeObjectId(601);

      const noteBytes = concat(
        encodeObjectId(noteId),
        encodeString("Reader Note"),
        encodeString("Content via reader"),
        encodeObjectId(authorId),
        new Uint8Array([0]), // null folder
        encodeI64(6000000n),
        encodeI64(6000001n),
        encodeBool(false)
      );

      const reader = new BinaryReader(noteBytes.buffer);
      const note = readNote(reader);

      expect(note.id).toBe(noteId);
      expect(note.title).toBe("Reader Note");
      expect(note.author).toBe(authorId);
      expect(note.folder).toBeNull();
    });

    it("reads note with non-null folder", () => {
      const noteId = makeObjectId(602);
      const authorId = makeObjectId(603);
      const folderId = makeObjectId(604);

      const noteBytes = concat(
        encodeObjectId(noteId),
        encodeString("Folder Note"),
        encodeString("Content"),
        encodeObjectId(authorId),
        new Uint8Array([1]), // Presence byte for non-null folder
        encodeObjectId(folderId),
        encodeI64(6000002n),
        encodeI64(6000003n),
        encodeBool(true)
      );

      const reader = new BinaryReader(noteBytes.buffer);
      const note = readNote(reader);

      expect(note.folder).toBe(folderId);
    });
  });

  describe("readFolder (with self-ref)", () => {
    it("reads folder with null parent", () => {
      const folderId = makeObjectId(700);
      const ownerId = makeObjectId(701);

      const folderBytes = concat(
        encodeObjectId(folderId),
        encodeString("Top Level"),
        encodeObjectId(ownerId),
        new Uint8Array([0])
      );

      const reader = new BinaryReader(folderBytes.buffer);
      const folder = readFolder(reader);

      expect(folder.name).toBe("Top Level");
      expect(folder.owner).toBe(ownerId);
      expect(folder.parent).toBeNull();
    });

    it("reads folder with parent", () => {
      const folderId = makeObjectId(702);
      const ownerId = makeObjectId(703);
      const parentId = makeObjectId(704);

      const folderBytes = concat(
        encodeObjectId(folderId),
        encodeString("Child"),
        encodeObjectId(ownerId),
        new Uint8Array([1]), // Presence byte for non-null parent
        encodeObjectId(parentId)
      );

      const reader = new BinaryReader(folderBytes.buffer);
      const folder = readFolder(reader);

      expect(folder.parent).toBe(parentId);
    });
  });
});
