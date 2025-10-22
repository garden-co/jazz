import { describe, expect, test } from "vitest";
import { CoPlainTextPackImplementation } from "../coValues/pack/coPlainText.js";
import type { AppOpPayload, ListOpPayload, OpID } from "../coValues/coList.js";

describe("CoPlainTextPackImplementation", () => {
  const packer = new CoPlainTextPackImplementation();

  // Helper to create a mock OpID
  const createOpID = (sessionID: string, txIndex: number): OpID => ({
    sessionID: sessionID as any,
    txIndex,
    changeIdx: 0,
  });

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
      expect(result.length).toBe(2); // first element + concatenated string
      expect((result[0] as any).compacted).toBe(true);
      expect((result[0] as any).op).toBe("app");
      expect((result[0] as any).value).toBe("H");
      expect((result[0] as any).after).toBe("start");
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
      expect((result[0] as any).compacted).toBe(true);
      expect((result[0] as any).after).toBe(opID);
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
      expect((result[0] as any).value).toBe("👋");
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
      expect((result[0] as any).value).toBe("a");
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

      expect(result).toBe(changes); // Returns original array
      expect((result[0] as any).compacted).toBeUndefined();
    });

    test("should NOT pack when first operation is not 'app'", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID },
        { op: "app", value: "a", after: "start" },
      ];

      const result = packer.packChanges(changes as any);

      expect(result).toBe(changes);
    });

    test("should NOT pack when operations contain 'pre' operation", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "pre", value: "b", before: "end" },
        { op: "app", value: "c", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(result).toBe(changes);
      expect((result[0] as any).compacted).toBeUndefined();
    });

    test("should NOT pack when operations contain 'del' operation", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
        { op: "app", value: "b", after: "start" },
        { op: "del", insertion: opID },
      ];

      const result = packer.packChanges(changes as any);

      expect(result).toBe(changes);
    });

    test("should handle single character", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Single operation doesn't get packed
      expect(result.length).toBe(1);
      expect((result[0] as any).compacted).toBeUndefined();
      expect((result[0] as any).value).toBe("a");
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
      expect((result[0] as any).value).toBe("T");
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
      expect((result[0] as any).value).toBe("a");
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
      expect((result[0] as any).value).toBe("こ");
      expect(result[1]).toBe("んにちは");
    });
  });

  describe("unpackChanges", () => {
    test("should unpack packed text changes correctly", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "H",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, "ello"];

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
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "a",
        after: opID,
        compacted: true,
      };

      const packed = [firstOp, "bcd"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(4);
      expect((result[0] as AppOpPayload<string>).after).toBe(opID);
      expect((result[1] as AppOpPayload<string>).after).toBe(opID);
      expect((result[2] as AppOpPayload<string>).after).toBe(opID);
      expect((result[3] as AppOpPayload<string>).after).toBe(opID);
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe("abcd");
    });

    test("should correctly unpack emoji graphemes", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "👋",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, "🌍✨"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect((result[0] as AppOpPayload<string>).value).toBe("👋");
      expect((result[1] as AppOpPayload<string>).value).toBe("🌍");
      expect((result[2] as AppOpPayload<string>).value).toBe("✨");
    });

    test("should correctly unpack complex grapheme clusters", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "a",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, "👨‍👩‍👧‍👦b"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect((result[0] as AppOpPayload<string>).value).toBe("a");
      expect((result[1] as AppOpPayload<string>).value).toBe("👨‍👩‍👧‍👦");
      expect((result[2] as AppOpPayload<string>).value).toBe("b");
    });

    test("should handle empty string in packed format", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "a",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, ""];

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
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "T",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, text.slice(1)];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(text.length);
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe(text);
    });

    test("should unpack whitespace characters correctly", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "a",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, " \t\nb"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(5);
      expect((result[0] as AppOpPayload<string>).value).toBe("a");
      expect((result[1] as AppOpPayload<string>).value).toBe(" ");
      expect((result[2] as AppOpPayload<string>).value).toBe("\t");
      expect((result[3] as AppOpPayload<string>).value).toBe("\n");
      expect((result[4] as AppOpPayload<string>).value).toBe("b");
    });

    test("should unpack Unicode characters correctly", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "こ",
        after: "start",
        compacted: true,
      };

      const packed = [firstOp, "んにちは"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(5);
      expect(
        result.map((r) => (r as AppOpPayload<string>).value).join(""),
      ).toBe("こんにちは");
    });

    test("should handle combining diacritics correctly", () => {
      const firstOp: AppOpPayload<string> & { compacted: true } = {
        op: "app",
        value: "e",
        after: "start",
        compacted: true,
      };

      // é as combining characters (e + combining acute)
      const packed = [firstOp, "\u0301"];

      const result = packer.unpackChanges(packed as any);

      // splitGraphemes treats combining diacritics as separate graphemes
      expect(result.length).toBe(2);
      expect((result[0] as AppOpPayload<string>).value).toBe("e");
      expect((result[1] as AppOpPayload<string>).value).toBe("\u0301");
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

      const packed = packer.packChanges(original); // Should not pack
      const unpacked = packer.unpackChanges(packed as any);

      expect(unpacked).toBe(original);
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
