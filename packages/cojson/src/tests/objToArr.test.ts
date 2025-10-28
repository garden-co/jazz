import { describe, expect, test } from "vitest";
import {
  packObjectToArr,
  unpackArrToObject,
  unpackArrOfObjectsCoList,
  packArrOfObjectsCoList,
  ENCODING_MAP_PRIMITIVES_VALUES,
} from "../pack/objToArr.js";
import type { JsonObject, JsonValue } from "../jsonValue.js";

describe("objToArr utilities", () => {
  describe("packObjectToArr", () => {
    test("should pack simple object to array using keys", () => {
      const keys = ["name", "age", "city"];
      const obj: JsonObject = {
        name: "Alice",
        age: 30,
        city: "NYC",
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["Alice", 30, "NYC"]);
    });

    test("should handle missing keys with null converted to 0", () => {
      const keys = ["name", "age", "city"];
      const obj: JsonObject = {
        name: "Bob",
        city: "LA",
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["Bob", 0, "LA"]);
    });

    test("should remove trailing nulls to save space", () => {
      const keys = ["name", "age", "city", "country"];
      const obj: JsonObject = {
        name: "Charlie",
        age: 25,
      };

      const result = packObjectToArr(keys, obj);

      // Should only have name and age, trailing nulls removed one by one
      // The implementation removes all trailing nulls, so we get [name, age]
      expect(result).toEqual(["Charlie", 25]);
    });

    test("should convert intermediate nulls to 0", () => {
      const keys = ["name", "age", "city"];
      const obj: JsonObject = {
        name: "David",
        city: "Chicago",
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["David", 0, "Chicago"]);
    });

    test("should handle empty object", () => {
      const keys = ["name", "age"];
      const obj: JsonObject = {};

      const result = packObjectToArr(keys, obj);

      // Empty object results in [] because all trailing nulls are removed
      expect(result).toEqual([]);
    });

    test("should handle object with all null values", () => {
      const keys = ["name", "age", "city"];
      const obj: JsonObject = {
        name: null,
        age: null,
        city: null,
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual([]);
    });

    test("should handle nested objects as values", () => {
      const keys = ["id", "data", "meta"];
      const obj: JsonObject = {
        id: 1,
        data: { nested: "value" },
        meta: { info: "test" },
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual([1, { nested: "value" }, { info: "test" }]);
    });

    test("should handle arrays as values", () => {
      const keys = ["id", "tags", "scores"];
      const obj: JsonObject = {
        id: 1,
        tags: ["a", "b", "c"],
        scores: [10, 20, 30],
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual([1, ["a", "b", "c"], [10, 20, 30]]);
    });

    test("should handle boolean and number values", () => {
      const keys = ["active", "count", "score"];
      const obj: JsonObject = {
        active: true,
        count: 0,
        score: 99.5,
      };

      const result = packObjectToArr(keys, obj);

      // Note: The implementation uses || operator which converts falsy values to null
      // So count: 0 becomes null
      expect(result).toEqual([ENCODING_MAP_PRIMITIVES_VALUES.true, 0, 99.5]);
    });

    test("should handle string values with special characters", () => {
      const keys = ["text", "unicode", "emoji"];
      const obj: JsonObject = {
        text: "Hello\nWorld",
        unicode: "こんにちは",
        emoji: "👋🌍",
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["Hello\nWorld", "こんにちは", "👋🌍"]);
    });

    test("should handle single key", () => {
      const keys = ["name"];
      const obj: JsonObject = {
        name: "Single",
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["Single"]);
    });

    test("should handle extra properties in object that are not in keys", () => {
      const keys = ["name", "age"];
      const obj: JsonObject = {
        name: "Eve",
        age: 28,
        city: "Boston", // Not in keys, should be ignored
        country: "USA", // Not in keys, should be ignored
      };

      const result = packObjectToArr(keys, obj);

      expect(result).toEqual(["Eve", 28]);
    });

    test("should handle falsy values correctly", () => {
      const keys = ["zero", "emptyString", "falseBool", "nullVal"];
      const obj: JsonObject = {
        zero: 0,
        emptyString: "",
        falseBool: false,
        nullVal: null,
      };

      const result = packObjectToArr(keys, obj);

      // Note: The implementation uses || operator which converts all falsy values to null
      // So 0, "", false all become null, and the trailing null is removed
      expect(result).toEqual([0, "", ENCODING_MAP_PRIMITIVES_VALUES.false]);
    });
  });

  describe("unpackArrToObject", () => {
    test("should unpack array to object using keys", () => {
      const keys = ["name", "age", "city"];
      const arr: JsonValue[] = ["Alice", 30, "NYC"];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        name: "Alice",
        age: 30,
        city: "NYC",
      });
    });

    test("should skip null and 0 values", () => {
      const keys = ["name", "age", "city"];
      const arr: JsonValue[] = ["Bob", 0, "LA"];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        name: "Bob",
        city: "LA",
      });
    });

    test("should skip undefined values", () => {
      const keys = ["name", "age", "city"];
      const arr: JsonValue[] = ["Charlie", undefined as any, "Chicago"];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        name: "Charlie",
        city: "Chicago",
      });
    });

    test("should handle array shorter than keys", () => {
      const keys = ["name", "age", "city", "country"];
      const arr: JsonValue[] = ["David", 35];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        name: "David",
        age: 35,
      });
    });

    test("should handle empty array", () => {
      const keys = ["name", "age"];
      const arr: JsonValue[] = [];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({});
    });

    test("should handle nested objects", () => {
      const keys = ["id", "data", "meta"];
      const arr: JsonValue[] = [1, { nested: "value" }, { info: "test" }];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        id: 1,
        data: { nested: "value" },
        meta: { info: "test" },
      });
    });

    test("should handle arrays as values", () => {
      const keys = ["id", "tags", "scores"];
      const arr: JsonValue[] = [1, ["a", "b", "c"], [10, 20, 30]];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        id: 1,
        tags: ["a", "b", "c"],
        scores: [10, 20, 30],
      });
    });

    test("should preserve falsy values except null, undefined, and 0", () => {
      const keys = ["zero", "emptyString", "falseBool"];
      const arr: JsonValue[] = [0, "", false];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        emptyString: "",
        falseBool: false,
      });
    });

    test("should handle single key-value pair", () => {
      const keys = ["name"];
      const arr: JsonValue[] = ["Single"];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        name: "Single",
      });
    });

    test("should handle special characters in values", () => {
      const keys = ["text", "unicode", "emoji"];
      const arr: JsonValue[] = ["Hello\nWorld", "こんにちは", "👋🌍"];

      const result = unpackArrToObject(keys, arr);

      expect(result).toEqual({
        text: "Hello\nWorld",
        unicode: "こんにちは",
        emoji: "👋🌍",
      });
    });
  });

  describe("pack/unpack roundtrip", () => {
    test("should maintain data integrity through pack/unpack cycle", () => {
      const keys = ["name", "age", "city", "active"];
      const original: JsonObject = {
        name: "Alice",
        age: 30,
        city: "NYC",
        active: true,
      };

      const packed = packObjectToArr(keys, original);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(original);
    });

    test("should handle missing values in roundtrip", () => {
      const keys = ["name", "age", "city", "country"];
      const original: JsonObject = {
        name: "Bob",
        age: 25,
      };

      const packed = packObjectToArr(keys, original);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(original);
    });

    test("should handle complex nested structures in roundtrip", () => {
      const keys = ["id", "data", "meta", "tags"];
      const original: JsonObject = {
        id: 1,
        data: { nested: { deep: "value" } },
        meta: { created: "2024-01-01", updated: "2024-01-02" },
        tags: ["tag1", "tag2", "tag3"],
      };

      const packed = packObjectToArr(keys, original);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(original);
    });

    test("should work with multiple roundtrips", () => {
      const keys = ["a", "b", "c", "d"];
      const original: JsonObject = {
        a: "test",
        b: 123,
        c: true,
        d: { nested: "obj" },
      };

      const packed1 = packObjectToArr(keys, original);
      const unpacked1 = unpackArrToObject(keys, packed1);
      const packed2 = packObjectToArr(keys, unpacked1);
      const unpacked2 = unpackArrToObject(keys, packed2);

      expect(unpacked2).toEqual(original);
    });
  });

  describe("unpackArrToObjectWithKeys", () => {
    test("should unpack array of app operations", () => {
      const arr: JsonValue[][] = [
        ["value1", "after1", 1],
        ["value2", "after2", 1],
      ];

      const result = unpackArrOfObjectsCoList(arr);

      expect(result).toEqual([
        { op: "app", value: "value1", after: "after1" },
        { op: "app", value: "value2", after: "after2" },
      ]);
    });

    test("should handle empty array", () => {
      const arr: JsonValue[][] = [];

      const result = unpackArrOfObjectsCoList(arr);

      expect(result).toEqual([]);
    });

    test("should unpack array of del operations", () => {
      const arr: JsonValue[][] = [
        ["insertion1", 0, 3],
        ["insertion2", 0, 3],
      ];

      const result = unpackArrOfObjectsCoList(arr);

      expect(result).toEqual([
        { op: "del", insertion: "insertion1" },
        { op: "del", insertion: "insertion2" },
      ]);
    });

    test("should handle mixed operations", () => {
      const arr: JsonValue[][] = [
        ["value1", "after1", 1],
        ["insertion1", 0, 3],
      ];

      const result = unpackArrOfObjectsCoList(arr);

      expect(result).toEqual([
        { op: "app", value: "value1", after: "after1" },
        { op: "del", insertion: "insertion1" },
      ]);
    });

    test("should handle operations with compacted flag", () => {
      const arr: JsonValue[][] = [
        ["value1", "after1", 1, true],
        ["insertion1", true, 3],
      ];

      const result = unpackArrOfObjectsCoList(arr);

      expect(result).toEqual([
        { op: "app", value: "value1", after: "after1", compacted: true },
        { op: "del", insertion: "insertion1", compacted: true },
      ]);
    });
  });

  describe("packArrToObjectWithKeys", () => {
    test("should pack array of app operations", () => {
      const arr = [
        { op: "app" as const, value: "value1", after: "after1" },
        { op: "app" as const, value: "value2", after: "after2" },
      ];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([
        ["value1", "after1"],
        ["value2", "after2"],
      ]);
    });

    test("should handle empty array", () => {
      const arr: { op: "app" | "del" }[] = [];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([]);
    });

    test("should pack array of del operations", () => {
      const arr = [
        { op: "del" as const, insertion: "insertion1" },
        { op: "del" as const, insertion: "insertion2" },
      ];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([
        ["insertion1", 0, 3],
        ["insertion2", 0, 3],
      ]);
    });

    test("should handle mixed operations", () => {
      const arr = [
        { op: "app" as const, value: "value1", after: "after1" },
        { op: "del" as const, insertion: "insertion1" },
      ];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([
        ["value1", "after1"],
        ["insertion1", 0, 3],
      ]);
    });

    test("should handle operations with compacted flag", () => {
      const arr = [
        {
          op: "app" as const,
          value: "value1",
          after: "after1",
          compacted: true,
        },
        { op: "del" as const, insertion: "insertion1", compacted: true },
      ];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([
        ["value1", "after1", 0, ENCODING_MAP_PRIMITIVES_VALUES.true],
        ["insertion1", ENCODING_MAP_PRIMITIVES_VALUES.true, 3],
      ]);
    });

    test("should remove trailing nulls for each item", () => {
      const arr = [
        { op: "app" as const, value: "value1", after: "after1" },
        {
          op: "app" as const,
          value: "value2",
          after: "after2",
          compacted: true,
        },
      ];

      const result = packArrOfObjectsCoList(arr);

      expect(result).toEqual([
        ["value1", "after1"],
        ["value2", "after2", 0, ENCODING_MAP_PRIMITIVES_VALUES.true],
      ]);
    });
  });

  describe("packArrToObjectWithKeys/unpackArrToObjectWithKeys roundtrip", () => {
    test("should maintain data integrity through pack/unpack cycle for app operations", () => {
      const original = [
        { op: "app" as const, value: "value1", after: "after1" },
        { op: "app" as const, value: "value2", after: "after2" },
        { op: "app" as const, value: "value3", after: "after3" },
      ];

      const packed = packArrOfObjectsCoList(original);
      const unpacked = unpackArrOfObjectsCoList(packed);

      expect(unpacked).toEqual(original);
    });

    test("should maintain data integrity for del operations in roundtrip", () => {
      const original = [
        { op: "del" as const, insertion: "insertion1" },
        { op: "del" as const, insertion: "insertion2" },
        { op: "del" as const, insertion: "insertion3" },
      ];

      const packed = packArrOfObjectsCoList(original);
      const unpacked = unpackArrOfObjectsCoList(packed);

      expect(unpacked).toEqual(original);
    });

    test("should work with mixed operations in roundtrip", () => {
      const original = [
        { op: "app" as const, value: "value1", after: "after1" },
        { op: "del" as const, insertion: "insertion1" },
        {
          op: "app" as const,
          value: "value2",
          after: "after2",
          compacted: true,
        },
      ];

      const packed = packArrOfObjectsCoList(original);
      const unpacked = unpackArrOfObjectsCoList(packed);

      expect(unpacked).toEqual(original);
    });

    test("should work with multiple roundtrips", () => {
      const original = [
        { op: "app" as const, value: "value1", after: "after1" },
        { op: "del" as const, insertion: "insertion1" },
      ];

      const packed1 = packArrOfObjectsCoList(original);
      const unpacked1 = unpackArrOfObjectsCoList(packed1);
      const packed2 = packArrOfObjectsCoList(unpacked1 as any);
      const unpacked2 = unpackArrOfObjectsCoList(packed2);

      expect(unpacked2).toEqual(original);
    });
  });

  describe("space efficiency", () => {
    test("packed format should be more compact for repeated structures", () => {
      const original = Array.from({ length: 50 }, (_, i) => ({
        op: "app" as const,
        value: `User${i}`,
        after: "someAfterValue",
      }));

      const packed = packArrOfObjectsCoList(original);

      const originalSize = JSON.stringify(original).length;
      const packedSize = JSON.stringify(packed).length;

      // Packed should be smaller due to not repeating keys
      expect(packedSize).toBeLessThan(originalSize);
    });

    test("trailing null removal saves space", () => {
      const keys = ["a", "b", "c", "d", "e"];
      const objWithTrailingNulls: JsonObject = { a: 1, b: 2 };

      const packed = packObjectToArr(keys, objWithTrailingNulls);

      // All trailing nulls are removed, so we get [1, 2]
      expect(packed.length).toBe(2);
      expect(JSON.stringify(packed).length).equal(
        JSON.stringify([1, 2]).length,
      );
    });
  });

  describe("edge cases", () => {
    test("should handle very long key arrays", () => {
      const keys = Array.from({ length: 50 }, (_, i) => `key${i}`);
      const obj: JsonObject = {
        key0: "value0",
        key25: "value25",
        key49: "value49",
      };

      const packed = packObjectToArr(keys, obj);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(obj);
    });

    test("should handle objects with number-like string keys", () => {
      const keys = ["0", "1", "2"];
      const obj: JsonObject = {
        "0": "zero",
        "1": "one",
        "2": "two",
      };

      const packed = packObjectToArr(keys, obj);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(obj);
    });

    test("should handle deeply nested objects", () => {
      const keys = ["data"];
      const obj: JsonObject = {
        data: {
          level1: {
            level2: {
              level3: {
                level4: {
                  value: "deep",
                },
              },
            },
          },
        },
      };

      const packed = packObjectToArr(keys, obj);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(obj);
    });

    test("should handle arrays with mixed types", () => {
      const keys = ["mixed"];
      const obj: JsonObject = {
        mixed: [1, "two", true, null, { nested: "obj" }, [1, 2, 3]],
      };

      const packed = packObjectToArr(keys, obj);
      const unpacked = unpackArrToObject(keys, packed);

      expect(unpacked).toEqual(obj);
    });
  });
});
