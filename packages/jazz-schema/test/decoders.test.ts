/**
 * Tests for binary row decoders
 */

import { describe, it, expect } from "vitest";
import {
  decodeUserRows,
  decodeUserRow,
  decodeUserDelta,
  decodeNoteRows,
  BinaryReader,
  readUser,
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
});
