import { describe, expect, test } from "vitest";
import { CoPlainTextPackImplementation } from "../pack/coPlainText.js";
import type {
  AppOpPayload,
  DeletionOpPayload,
  ListOpPayload,
  OpID,
  PreOpPayload,
} from "../coValues/coList.js";
import { ENCODING_MAP_PRIMITIVES_VALUES } from "../pack/objToArr.js";
import { packOpID } from "../pack/opID.js";

describe("CoPlainTextPackImplementation", () => {
  const packer = new CoPlainTextPackImplementation();

  // Helper to create a mock OpID
  const createOpID = (sessionID: string, txIndex: number): OpID => ({
    sessionID: sessionID as any,
    txIndex,
    changeIdx: 0,
    branch: undefined,
  });

  const serializeOpRef = (ref: OpID | "start" | "end") =>
    typeof ref === "string" ? ref : packOpID(ref);

  describe("packChanges", () => {
    test("should pack sequential character insertions with same 'after'", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "H", after: "start" },
        { op: "app", value: "e", after: "start" },
        { op: "app", value: "l", after: "start" },
        { op: "app", value: "l", after: "start" },
        { op: "app", value: "o", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2); // first element array + concatenated string
      expect(Array.isArray(result[0])).toBe(true);
      // First element is now packed as ["H", "start", 1, true]
      expect((result[0] as any)[0]).toBe("H"); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBe(
        ENCODING_MAP_PRIMITIVES_VALUES.undefined,
      ); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect(result[1]).toBe("ello"); // Remaining characters concatenated
    });

    test("should pack with OpID as 'after' reference", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: opID },
        { op: "app", value: "b", after: opID },
        { op: "app", value: "c", after: opID },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("a"); // value
      expect((result[0] as any)[1]).toBe(packOpID(opID)); // after
      expect((result[0] as any)[2]).toBe(
        ENCODING_MAP_PRIMITIVES_VALUES.undefined,
      ); // op (1 = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect(result[1]).toBe("bc");
    });

    test("should handle emoji and special characters", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "👋", after: "start" },
        { op: "app", value: "🌍", after: "start" },
        { op: "app", value: "✨", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("👋"); // value at index 0
      expect(result[1]).toBe("🌍✨");
    });

    test("should handle complex grapheme clusters", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "👨‍👩‍👧‍👦", after: "start" }, // Family emoji (single grapheme)
        { op: "app", value: "b", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("a"); // value at index 0
      expect(result[1]).toBe("👨‍👩‍👧‍👦b");
    });

    test("should NOT pack when operations have different 'after' references", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: opID1 },
        { op: "app", value: "b", after: opID2 },
        { op: "app", value: "c", after: opID1 },
      ];

      const result = packer.packChanges(changes);

      // Returns array of arrays format without compacting
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag
    });

    test("should NOT pack when first operation is not 'app'", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID },
        { op: "app", value: "a", after: "start" },
      ];

      const result = packer.packChanges(changes as any);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
    });

    test("should NOT pack when operations contain 'pre' operation", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "pre", value: "b", before: "end" },
        { op: "app", value: "c", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag
    });

    test("should NOT pack when operations contain 'del' operation", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "b", after: "start" },
        { op: "del", insertion: opID },
      ];

      const result = packer.packChanges(changes as any);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
    });

    test("should handle single character", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Single operation is packed as array of arrays
      expect(result.length).toBe(1);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("a"); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBeUndefined(); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag for single operation
    });

    test("should handle empty array", () => {
      const changes: ListOpPayload<string>[] = [];

      const result = packer.packChanges(changes);

      expect(result).toEqual([]);
    });

    test("should pack long text efficiently", () => {
      const text = "The quick brown fox jumps over the lazy dog";
      const changes: ListOpPayload<string>[] = text
        .split("")
        .map((char) => ({ op: "app", value: char, after: "start" }));

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("T"); // value at index 0
      expect(result[1]).toBe(text.slice(1));
    });

    test("should handle whitespace characters", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: " ", after: "start" },
        { op: "app", value: "\t", after: "start" },
        { op: "app", value: "\n", after: "start" },
        { op: "app", value: "b", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("a"); // value at index 0
      expect(result[1]).toBe(" \t\nb");
    });

    test("should handle Unicode characters", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "こ", after: "start" },
        { op: "app", value: "ん", after: "start" },
        { op: "app", value: "に", after: "start" },
        { op: "app", value: "ち", after: "start" },
        { op: "app", value: "は", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("こ"); // value at index 0
      expect(result[1]).toBe("んにちは");
    });

    test("should NOT pack prepend operations", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "a", before: "end" },
        { op: "pre", value: "b", before: "end" },
        { op: "pre", value: "c", before: "end" },
      ];

      const result = packer.packChanges(changes);

      // Prepend operations are not compacted, returns array of arrays
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[2]).toBe(2); // op (2 = "pre")
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag
    });

    test("should NOT pack when mixing prepend with append", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "a", before: "end" },
        { op: "app", value: "b", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Mixed operations - returns array of arrays
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
      expect((result[0] as any)[2]).toBe(2); // op (2 = "pre")
      expect((result[1] as any)[2]).toBeUndefined(); // op (0 is null so we use the default value = "app")
    });
  });

  describe("unpackChanges", () => {
    test("should unpack packed text changes correctly", () => {
      // First element is now an array: ["H", "start", 1, true]
      const packed = [["H", "start", 1, true], "ello"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(5);
      // First element retains the compacted flag
      expect(result[0]?.op).toBe("app");
      expect((result[0] as AppOpPayload<string>).value).toBe("H");
      expect((result[0] as AppOpPayload<string>).after).toBe("start");
      expect(result[1]).toEqual({ op: "app", value: "e", after: "start" });
      expect(result[2]).toEqual({ op: "app", value: "l", after: "start" });
      expect(result[3]).toEqual({ op: "app", value: "l", after: "start" });
      expect(result[4]).toEqual({ op: "app", value: "o", after: "start" });
    });

    test("should unpack with OpID as 'after' reference", () => {
      const opID = createOpID("session1", 5);
      // First element is now an array
      const packed = [["a", opID, 1, true], "bcd"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(4);
      expect(serializeOpRef((result[0] as AppOpPayload<string>).after)).toBe(
        packOpID(opID),
      );
      expect(serializeOpRef((result[1] as AppOpPayload<string>).after)).toBe(
        packOpID(opID),
      );
      expect(serializeOpRef((result[2] as AppOpPayload<string>).after)).toBe(
        packOpID(opID),
      );
      expect(serializeOpRef((result[3] as AppOpPayload<string>).after)).toBe(
        packOpID(opID),
      );
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe("abcd");
    });

    test("should correctly unpack emoji graphemes", () => {
      // First element is now an array
      const packed = [["👋", "start", 1, true], "🌍✨"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect((result[0] as AppOpPayload<string>).value).toBe("👋");
      expect((result[1] as AppOpPayload<string>).value).toBe("🌍");
      expect((result[2] as AppOpPayload<string>).value).toBe("✨");
    });

    test("should correctly unpack complex grapheme clusters", () => {
      // First element is now an array
      const packed = [["a", "start", 1, true], "👨‍👩‍👧‍👦b"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect((result[0] as AppOpPayload<string>).value).toBe("a");
      expect((result[1] as AppOpPayload<string>).value).toBe("👨‍👩‍👧‍👦");
      expect((result[2] as AppOpPayload<string>).value).toBe("b");
    });

    test("should handle empty string in packed format", () => {
      // First element is now an array
      const packed = [["a", "start", 1, true], ""];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(1);
      expect((result[0] as AppOpPayload<string>).value).toBe("a");
    });

    test("should pass through unpacked changes unchanged", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "b", after: "start" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should handle single unpacked operation", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should handle empty array", () => {
      const result = packer.unpackChanges([]);

      expect(result).toEqual([]);
    });

    test("should unpack long text correctly", () => {
      const text = "The quick brown fox jumps over the lazy dog";
      // First element is now an array
      const packed = [["T", "start", 1, true], text.slice(1)];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(text.length);
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe(text);
    });

    test("should unpack whitespace characters correctly", () => {
      // First element is now an array
      const packed = [["a", "start", 1, true], " \t\nb"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(5);
      expect((result[0] as AppOpPayload<string>).value).toBe("a");
      expect((result[1] as AppOpPayload<string>).value).toBe(" ");
      expect((result[2] as AppOpPayload<string>).value).toBe("\t");
      expect((result[3] as AppOpPayload<string>).value).toBe("\n");
      expect((result[4] as AppOpPayload<string>).value).toBe("b");
    });

    test("should unpack Unicode characters correctly", () => {
      // First element is now an array
      const packed = [["こ", "start", 1, true], "んにちは"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(5);
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe("こんにちは");
    });

    test("should handle combining diacritics correctly", () => {
      // First element is now an array
      // é as combining characters (e + combining acute)
      const packed = [["e", "start", 1, true], "\u0301"];

      const result = packer.unpackChanges(packed as any);

      // splitGraphemes treats combining diacritics as separate graphemes
      expect(result.length).toBe(2);
      expect((result[0] as AppOpPayload<string>).value).toBe("e");
      expect((result[1] as AppOpPayload<string>).value).toBe("\u0301");
    });

    test("should pass through prepend operations unchanged", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "a", before: "end" },
        { op: "pre", value: "b", before: "end" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });
  });

  describe("pack/unpack roundtrip", () => {
    test("should maintain text integrity through pack/unpack cycle", () => {
      const text = "Hello, World!";
      const original: ListOpPayload<string>[] = text
        .split("")
        .map((char) => ({ op: "app", value: char, after: "start" }));

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      const reconstructedText = unpacked
        .map((op) => (op as AppOpPayload<string>).value)
        .join("");
      expect(reconstructedText).toBe(text);
    });

    test("should maintain emoji integrity through pack/unpack cycle", () => {
      const text = "Hello 👋🌍✨!";
      const original: ListOpPayload<string>[] = Array.from(text).map(
        (char) => ({ op: "app", value: char, after: "start" }),
      );

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      const reconstructedText = unpacked
        .map((op) => (op as AppOpPayload<string>).value)
        .join("");
      expect(reconstructedText).toBe(text);
    });

    test("should maintain data integrity for unpacked operations", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);

      const original: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: opID1 },
        { op: "app", value: "b", after: opID2 },
      ];

      const packed = packer.packChanges(original); // Should not compact (different after references)
      const unpacked = packer.unpackChanges(packed as any);

      // Compare by value, not by reference, since it's now packed/unpacked
      expect(unpacked.length).toBe(original.length);
      for (let i = 0; i < unpacked.length; i++) {
        expect(unpacked[i]?.op).toBe(original[i]?.op);
        expect((unpacked[i] as AppOpPayload<string>).value).toBe(
          (original[i] as AppOpPayload<string>).value,
        );
        expect(
          serializeOpRef((unpacked[i] as AppOpPayload<string>).after),
        ).toBe(serializeOpRef((original[i] as AppOpPayload<string>).after));
      }
    });

    test("should work with multiple pack/unpack cycles", () => {
      const text = "Test";
      const original: ListOpPayload<string>[] = text
        .split("")
        .map((char) => ({ op: "app", value: char, after: "start" }));

      const packed1 = packer.packChanges(original);
      const unpacked1 = packer.unpackChanges(packed1 as any);
      const packed2 = packer.packChanges(unpacked1);
      const unpacked2 = packer.unpackChanges(packed2 as any);

      const reconstructedText = unpacked2
        .map((op) => (op as AppOpPayload<string>).value)
        .join("");
      expect(reconstructedText).toBe(text);
    });

    test("should handle complex mixed content", () => {
      const text = "Hello 世界 👨‍👩‍👧‍👦!";
      const original: ListOpPayload<string>[] = Array.from(text).map(
        (char) => ({ op: "app", value: char, after: "start" }),
      );

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      const reconstructedText = unpacked
        .map((op) => (op as AppOpPayload<string>).value)
        .join("");
      expect(reconstructedText).toBe(text);
    });

    test("should maintain prepend operations through pack/unpack cycle", () => {
      const opID = createOpID("session1", 0);
      const original: ListOpPayload<string>[] = [
        { op: "pre", value: "a", before: "end" },
        { op: "pre", value: "b", before: opID },
        { op: "pre", value: "c", before: "end" },
      ];

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      // Check that all values are correct
      expect(unpacked.length).toBe(original.length);
      for (let i = 0; i < unpacked.length; i++) {
        expect(unpacked[i]?.op).toBe(original[i]?.op);
        expect((unpacked[i] as PreOpPayload<string>).value).toBe(
          (original[i] as PreOpPayload<string>).value,
        );
        expect(
          serializeOpRef((unpacked[i] as PreOpPayload<string>).before),
        ).toBe(serializeOpRef((original[i] as PreOpPayload<string>).before));
      }
    });
  });

  describe("packChanges - deletion operations", () => {
    test("should pack multiple sequential deletion operations", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);
      const opID3 = createOpID("session1", 2);
      const opID4 = createOpID("session1", 3);

      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID1 },
        { op: "del", insertion: opID2 },
        { op: "del", insertion: opID3 },
        { op: "del", insertion: opID4 },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(4); // first element array + 3 OpIDs
      expect(Array.isArray(result[0])).toBe(true);
      // First element is now packed as [opID1, true, 3]
      expect((result[0] as any)[0]).toBe(opID1); // insertion
      expect((result[0] as any)[1]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
      expect(result[1]).toBe(opID2);
      expect(result[2]).toBe(opID3);
      expect(result[3]).toBe(opID4);
    });

    test("should pack two deletion operations", () => {
      const opID1 = createOpID("session1", 5);
      const opID2 = createOpID("session1", 6);

      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID1 },
        { op: "del", insertion: opID2 },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(opID1); // insertion
      expect((result[0] as any)[1]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
      expect(result[1]).toBe(opID2);
    });

    test("should NOT pack single deletion operation", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [{ op: "del", insertion: opID }];

      const result = packer.packChanges(changes);

      // Single operation is packed as array of arrays
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(1);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(opID); // insertion
      expect((result[0] as any)[1]).toBe(
        ENCODING_MAP_PRIMITIVES_VALUES.undefined,
      ); // no compacted flag (trailing)
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
    });

    test("should NOT pack mixed deletion and insertion operations", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);

      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID1 },
        { op: "app", value: "a", after: "start" },
        { op: "del", insertion: opID2 },
      ];

      const result = packer.packChanges(changes);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
    });

    test("should NOT pack when first operation is deletion but others are insertions", () => {
      const opID = createOpID("session1", 0);

      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID },
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "b", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
    });

    test("should pack large batch of deletions", () => {
      const changes: ListOpPayload<string>[] = [];
      const opIDs: OpID[] = [];

      for (let i = 0; i < 50; i++) {
        const opID = createOpID(`session${i}`, i);
        opIDs.push(opID);
        changes.push({ op: "del", insertion: opID });
      }

      const result = packer.packChanges(changes);

      expect(result.length).toBe(50); // first element array + 49 OpIDs
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(opIDs[0]); // insertion
      expect((result[0] as any)[1]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")

      for (let i = 1; i < 50; i++) {
        expect(result[i]).toBe(opIDs[i]);
      }
    });

    test("should handle empty deletions array", () => {
      const changes: ListOpPayload<string>[] = [];
      const result = packer.packChanges(changes);
      expect(result).toEqual([]);
    });
  });

  describe("unpackChanges - deletion operations", () => {
    test("should unpack packed deletion operations correctly", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);
      const opID3 = createOpID("session1", 2);

      // First element is now an array: ["del", opID1, true]
      const packed = [[opID1, true, 3], opID2, opID3];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect(result[0]?.op).toBe("del");
      expect((result[0] as DeletionOpPayload).insertion).toBe(opID1);
      expect(result[1]).toEqual({ op: "del", insertion: opID2 });
      expect(result[2]).toEqual({ op: "del", insertion: opID3 });
    });

    test("should unpack two deletion operations", () => {
      const opID1 = createOpID("session1", 5);
      const opID2 = createOpID("session1", 6);

      // First element is now an array
      const packed = [[opID1, true, 3], opID2];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(2);
      expect((result[0] as DeletionOpPayload).insertion).toBe(opID1);
      expect((result[1] as DeletionOpPayload).insertion).toBe(opID2);
    });

    test("should handle single packed deletion", () => {
      const opID = createOpID("session1", 0);

      // First element is now an array
      const packed = [[opID, true, 3]];

      const result = packer.unpackChanges(packed as any);

      // Single operation - should still work
      expect(result.length).toBe(1);
      expect((result[0] as DeletionOpPayload).insertion).toBe(opID);
    });

    test("should pass through unpacked deletion operations unchanged", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);

      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID1 },
        { op: "del", insertion: opID2 },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should unpack large batch of deletions", () => {
      const opIDs: OpID[] = [];
      for (let i = 0; i < 50; i++) {
        opIDs.push(createOpID(`session${i}`, i));
      }

      // First element is now an array
      const packed = [[opIDs[0]!, true, 3], ...opIDs.slice(1)];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(50);
      for (let i = 0; i < 50; i++) {
        expect(result[i]?.op).toBe("del");
        expect((result[i] as DeletionOpPayload).insertion).toBe(opIDs[i]);
      }
    });
  });

  describe("pack/unpack roundtrip - deletion operations", () => {
    test("should maintain deletion operations integrity through pack/unpack cycle", () => {
      const opIDs = [
        createOpID("session1", 0),
        createOpID("session1", 1),
        createOpID("session1", 2),
        createOpID("session1", 3),
      ];

      const original: ListOpPayload<string>[] = opIDs.map((opID) => ({
        op: "del",
        insertion: opID,
      }));

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      expect(unpacked.length).toBe(original.length);
      for (let i = 0; i < original.length; i++) {
        expect(unpacked[i]?.op).toBe("del");
        expect((unpacked[i] as DeletionOpPayload).insertion).toBe(opIDs[i]);
      }
    });

    test("should work with multiple pack/unpack cycles for deletions", () => {
      const opIDs = [
        createOpID("session1", 0),
        createOpID("session1", 1),
        createOpID("session1", 2),
      ];

      const original: ListOpPayload<string>[] = opIDs.map((opID) => ({
        op: "del",
        insertion: opID,
      }));

      const packed1 = packer.packChanges(original);
      const unpacked1 = packer.unpackChanges(packed1 as any);
      const packed2 = packer.packChanges(unpacked1);
      const unpacked2 = packer.unpackChanges(packed2 as any);

      expect(unpacked2.length).toBe(original.length);
      for (let i = 0; i < original.length; i++) {
        expect(unpacked2[i]?.op).toBe("del");
        expect((unpacked2[i] as DeletionOpPayload).insertion).toBe(opIDs[i]);
      }
    });

    test("should handle mixed pack/unpack with both insertions and deletions separately", () => {
      // Test insertions
      const insertions: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "b", after: "start" },
      ];

      const packedInsertions = packer.packChanges(insertions);
      const unpackedInsertions = packer.unpackChanges(packedInsertions as any);

      expect(unpackedInsertions.length).toBe(2);
      expect((unpackedInsertions[0] as AppOpPayload<string>).value).toBe("a");
      expect((unpackedInsertions[1] as AppOpPayload<string>).value).toBe("b");

      // Test deletions
      const opIDs = [createOpID("session1", 0), createOpID("session1", 1)];
      const deletions: ListOpPayload<string>[] = opIDs.map((opID) => ({
        op: "del",
        insertion: opID,
      }));

      const packedDeletions = packer.packChanges(deletions);
      const unpackedDeletions = packer.unpackChanges(packedDeletions as any);

      expect(unpackedDeletions.length).toBe(2);
      expect((unpackedDeletions[0] as DeletionOpPayload).insertion).toBe(
        opIDs[0],
      );
      expect((unpackedDeletions[1] as DeletionOpPayload).insertion).toBe(
        opIDs[1],
      );
    });
  });

  describe("space efficiency - deletion operations", () => {
    test("packed deletion format should be more compact than unpacked", () => {
      const opIDs: OpID[] = [];
      for (let i = 0; i < 100; i++) {
        opIDs.push(createOpID(`session${i}`, i));
      }

      const changes: ListOpPayload<string>[] = opIDs.map((opID) => ({
        op: "del",
        insertion: opID,
      }));

      const packed = packer.packChanges(changes);
      const unpackedSize = JSON.stringify(changes).length;
      const packedSize = JSON.stringify(packed).length;

      // Packed should be smaller (removes repeated "op":"del" for each operation)
      expect(packedSize).toBeLessThan(unpackedSize);
    });

    test("deletion packing efficiency increases with more operations", () => {
      const batchSizes = [5, 10, 20, 50];

      const savings = batchSizes.map((size) => {
        const opIDs: OpID[] = [];
        for (let i = 0; i < size; i++) {
          opIDs.push(createOpID(`session${i}`, i));
        }

        const changes: ListOpPayload<string>[] = opIDs.map((opID) => ({
          op: "del",
          insertion: opID,
        }));

        const packed = packer.packChanges(changes);
        const unpackedSize = JSON.stringify(changes).length;
        const packedSize = JSON.stringify(packed).length;

        return ((unpackedSize - packedSize) / unpackedSize) * 100;
      });

      // Savings should generally increase with batch size
      for (let i = 1; i < savings.length; i++) {
        const currentSaving = savings[i];
        const previousSaving = savings[i - 1];
        if (currentSaving !== undefined && previousSaving !== undefined) {
          expect(currentSaving).toBeGreaterThanOrEqual(previousSaving * 0.9); // Allow small variance
        }
      }
    });
  });

  describe("space efficiency", () => {
    test("packed format should be more compact than unpacked for long text", () => {
      const text = "The quick brown fox jumps over the lazy dog ".repeat(10);
      const changes: ListOpPayload<string>[] = text
        .split("")
        .map((char) => ({ op: "app", value: char, after: "start" }));

      const packed = packer.packChanges(changes);
      const unpackedSize = JSON.stringify(changes).length;
      const packedSize = JSON.stringify(packed).length;

      // Packed should be significantly smaller
      expect(packedSize).toBeLessThan(unpackedSize);
      // Should be at least 50% smaller for long text
      expect(packedSize).toBeLessThan(unpackedSize * 0.5);
    });

    test("packed format saves space proportionally to text length", () => {
      const testTexts = [
        "Hi",
        "Hello",
        "Hello, World!",
        "The quick brown fox jumps over the lazy dog",
      ];

      const savings = testTexts.map((text) => {
        const changes: ListOpPayload<string>[] = text
          .split("")
          .map((char) => ({ op: "app", value: char, after: "start" }));

        const packed = packer.packChanges(changes);
        const unpackedSize = JSON.stringify(changes).length;
        const packedSize = JSON.stringify(packed).length;

        return ((unpackedSize - packedSize) / unpackedSize) * 100;
      });

      // Savings should increase with text length
      for (let i = 1; i < savings.length; i++) {
        const currentSaving = savings[i];
        const previousSaving = savings[i - 1];
        if (currentSaving !== undefined && previousSaving !== undefined) {
          expect(currentSaving).toBeGreaterThanOrEqual(previousSaving);
        }
      }
    });
  });
});
