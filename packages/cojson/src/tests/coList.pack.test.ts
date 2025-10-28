import { describe, expect, test } from "vitest";
import { CoListPackImplementation } from "../pack/coList.js";
import type {
  AppOpPayload,
  ListOpPayload,
  OpID,
  PreOpPayload,
} from "../coValues/coList.js";
import { ENCODING_MAP_PRIMITIVES_VALUES } from "../pack/objToArr.js";

describe("CoListPackImplementation", () => {
  const packer = new CoListPackImplementation<string>();

  // Helper to create a mock OpID
  const createOpID = (sessionID: string, txIndex: number): OpID => ({
    sessionID: sessionID as any,
    txIndex,
    changeIdx: 0,
  });

  describe("packChanges", () => {
    test("should pack sequential append operations with same 'after'", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
        { op: "app", value: "item2", after: "start" },
        { op: "app", value: "item3", after: "start" },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3); // first element array + 2 additional values
      expect(Array.isArray(result[0])).toBe(true);
      // First element is now packed as ["item1", "start", 1, true]
      expect((result[0] as any)[0]).toBe("item1"); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBe(0); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect(result[1]).toBe("item2");
      expect(result[2]).toBe("item3");
    });

    test("should pack with OpID as 'after' reference", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "a", after: opID },
        { op: "app", value: "b", after: opID },
        { op: "app", value: "c", after: opID },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("a"); // value
      expect((result[0] as any)[1]).toBe(opID); // after
      expect((result[0] as any)[2]).toBe(0); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect(result[1]).toBe("b");
      expect(result[2]).toBe("c");
    });

    test("should NOT pack when operations have different 'after' references", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: opID1 },
        { op: "app", value: "item2", after: opID2 },
        { op: "app", value: "item3", after: opID1 },
      ];

      const result = packer.packChanges(changes);

      // Returns array of arrays format without compacting
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      // Each element is packed as ["item1", opID1, 1] without compacted flag
      expect((result[0] as any)[0]).toBe("item1"); // value
      expect((result[0] as any)[1]).toBe(opID1); // after
      expect((result[0] as any)[2]).toBeUndefined(); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag
    });

    test("should NOT pack when first operation is not 'app'", () => {
      const opID = createOpID("session1", 0);
      const changes: ListOpPayload<string>[] = [
        { op: "del", insertion: opID },
        { op: "app", value: "item2", after: "start" },
      ];

      const result = packer.packChanges(changes as any);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(opID); // insertion
      expect((result[0] as any)[2]).toBe(3); // op (3 = "del")
    });

    test("should NOT pack when operations contain 'pre' operation", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
        { op: "pre", value: "item2", before: "end" },
        { op: "app", value: "item3", after: "start" },
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
        { op: "app", value: "item1", after: "start" },
        { op: "app", value: "item2", after: "start" },
        { op: "del", insertion: opID },
      ];

      const result = packer.packChanges(changes as any);

      // Returns array of arrays format
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
    });

    test("should handle single operation (no packing needed)", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Single operation is packed as array of arrays
      expect(result.length).toBe(1);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe("item1"); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBeUndefined(); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag for single operation
    });

    test("should handle empty array", () => {
      const changes: ListOpPayload<string>[] = [];

      const result = packer.packChanges(changes);

      expect(result).toEqual([]);
    });

    test("should pack with JSON objects as values", () => {
      type TaskItem = { id: number; title: string; done: boolean };
      const taskPacker = new CoListPackImplementation<TaskItem>();

      const changes: ListOpPayload<TaskItem>[] = [
        {
          op: "app",
          value: { id: 1, title: "Task 1", done: false },
          after: "start",
        },
        {
          op: "app",
          value: { id: 2, title: "Task 2", done: true },
          after: "start",
        },
        {
          op: "app",
          value: { id: 3, title: "Task 3", done: false },
          after: "start",
        },
      ];

      const result = taskPacker.packChanges(changes);

      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toEqual({
        id: 1,
        title: "Task 1",
        done: false,
      }); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBe(0); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      expect(result[1]).toEqual({ id: 2, title: "Task 2", done: true });
      expect(result[2]).toEqual({ id: 3, title: "Task 3", done: false });
    });

    test("should pack large batch of operations", () => {
      const changes: ListOpPayload<number>[] = Array.from(
        { length: 100 },
        (_, i) => ({
          op: "app",
          value: i,
          after: "start",
        }),
      );

      const numberPacker = new CoListPackImplementation<number>();
      const result = numberPacker.packChanges(changes);

      expect(result.length).toBe(100); // first element array + 99 additional values
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(0); // value
      expect((result[0] as any)[1]).toBe("start"); // after
      expect((result[0] as any)[2]).toBe(0); // op (0 is null so we use the default value = "app")
      expect((result[0] as any)[3]).toBe(ENCODING_MAP_PRIMITIVES_VALUES.true); // compacted
      for (let i = 1; i < result.length; i++) {
        expect(result[i]).toBe(i);
      }
    });

    test("should NOT pack prepend operations", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "item1", before: "end" },
        { op: "pre", value: "item2", before: "end" },
        { op: "pre", value: "item3", before: "end" },
      ];

      const result = packer.packChanges(changes);

      // Prepend operations are not compacted, returns array of arrays
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[2]).toBe(2); // op (2 = "pre")
      expect((result[0] as any)[3]).toBeUndefined(); // no compacted flag
    });

    test("should NOT pack when mixing prepend with other operations", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "item1", before: "end" },
        { op: "app", value: "item2", after: "start" },
      ];

      const result = packer.packChanges(changes);

      // Mixed operations - returns array of arrays
      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(2);
      expect((result[0] as any)[2]).toBe(2); // op (2 = "pre")
      expect((result[1] as any)[2]).toBeUndefined(); // op (1 = "app")
    });
  });

  describe("unpackChanges", () => {
    test("should unpack packed changes correctly", () => {
      // First element is now an array: ["item1", "start", 1, true]
      const packed = [["item1", "start", 1, true], "item2", "item3"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      // First element retains the compacted flag
      expect(result[0]?.op).toBe("app");
      expect((result[0] as AppOpPayload<string>).value).toBe("item1");
      expect((result[0] as AppOpPayload<string>).after).toBe("start");
      expect(result[1]).toEqual({ op: "app", value: "item2", after: "start" });
      expect(result[2]).toEqual({ op: "app", value: "item3", after: "start" });
    });

    test("should unpack with OpID as 'after' reference", () => {
      const opID = createOpID("session1", 5);
      // First element is now an array: ["app", "a", opID, true]
      const packed = [["a", opID, 1, true], "b", "c", "d"];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(4);
      expect((result[0] as AppOpPayload<string>).after).toBe(opID);
      expect((result[1] as AppOpPayload<string>).after).toBe(opID);
      expect((result[2] as AppOpPayload<string>).after).toBe(opID);
      expect((result[3] as AppOpPayload<string>).after).toBe(opID);
    });

    test("should pass through unpacked changes unchanged", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
        { op: "app", value: "item2", after: "start" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should handle single unpacked operation", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should handle empty array", () => {
      const result = packer.unpackChanges([]);

      expect(result).toEqual([]);
    });

    test("should unpack prepend operations", () => {
      const changes: ListOpPayload<string>[] = [
        { op: "pre", value: "item1", before: "end" },
        { op: "pre", value: "item2", before: "end" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });
  });

  describe("pack/unpack roundtrip", () => {
    test("should maintain data integrity through pack/unpack cycle", () => {
      const original: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: "start" },
        { op: "app", value: "item2", after: "start" },
        { op: "app", value: "item3", after: "start" },
        { op: "app", value: "item4", after: "start" },
        { op: "app", value: "item5", after: "start" },
      ];

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      // Check that all values are correct
      expect(unpacked.length).toBe(original.length);
      for (let i = 0; i < unpacked.length; i++) {
        expect(unpacked[i]?.op).toBe(original[i]?.op);
        expect((unpacked[i] as AppOpPayload<string>).value).toBe(
          (original[i] as AppOpPayload<string>).value,
        );
        expect((unpacked[i] as AppOpPayload<string>).after).toBe(
          (original[i] as AppOpPayload<string>).after,
        );
      }
    });

    test("should maintain data integrity for unpacked operations", () => {
      const opID1 = createOpID("session1", 0);
      const opID2 = createOpID("session1", 1);

      const original: ListOpPayload<string>[] = [
        { op: "app", value: "item1", after: opID1 },
        { op: "app", value: "item2", after: opID2 },
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
        expect((unpacked[i] as AppOpPayload<string>).after).toBe(
          (original[i] as AppOpPayload<string>).after,
        );
      }
    });

    test("should work with multiple pack/unpack cycles", () => {
      const original: ListOpPayload<number>[] = [
        { op: "app", value: 1, after: "start" },
        { op: "app", value: 2, after: "start" },
        { op: "app", value: 3, after: "start" },
      ];

      const numberPacker = new CoListPackImplementation<number>();

      const packed1 = numberPacker.packChanges(original);
      const unpacked1 = numberPacker.unpackChanges(packed1 as any);
      const packed2 = numberPacker.packChanges(unpacked1);
      const unpacked2 = numberPacker.unpackChanges(packed2 as any);

      // Check that all values are correct
      expect(unpacked2.length).toBe(original.length);
      for (let i = 0; i < unpacked2.length; i++) {
        expect(unpacked2[i]?.op).toBe(original[i]?.op);
        expect((unpacked2[i] as AppOpPayload<number>).value).toBe(
          (original[i] as AppOpPayload<number>).value,
        );
        expect((unpacked2[i] as AppOpPayload<number>).after).toBe(
          (original[i] as AppOpPayload<number>).after,
        );
      }
    });

    test("should maintain prepend operations through pack/unpack cycle", () => {
      const opID = createOpID("session1", 0);
      const original: ListOpPayload<string>[] = [
        { op: "pre", value: "item1", before: "end" },
        { op: "pre", value: "item2", before: opID },
        { op: "pre", value: "item3", before: "end" },
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
        expect((unpacked[i] as PreOpPayload<string>).before).toBe(
          (original[i] as PreOpPayload<string>).before,
        );
      }
    });
  });

  describe("space efficiency", () => {
    test("packed format should be more compact than unpacked", () => {
      const changes: ListOpPayload<string>[] = Array.from(
        { length: 50 },
        (_, i) => ({
          op: "app",
          value: `item${i}`,
          after: "start",
        }),
      );

      const packed = packer.packChanges(changes);
      const unpackedSize = JSON.stringify(changes).length;
      const packedSize = JSON.stringify(packed).length;

      // Packed should be smaller due to not repeating "op" and "after" fields
      expect(packedSize).toBeLessThan(unpackedSize);
    });
  });
});
