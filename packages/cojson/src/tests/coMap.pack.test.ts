import { describe, expect, test } from "vitest";
import {
  CoMapPackImplementation,
  getCoMapOperationType,
  packArrToObjectCoMap,
  unpackArrToObjectCoMap,
  packArrOfObjectsCoMap,
  unpackArrOfObjectsCoMap,
  ENCODING_MAP_COMAP_OPERATION_TYPES,
} from "../pack/coMap.js";
import type {
  MapOpPayload,
  MapOpPayloadSet,
  MapOpPayloadDel,
} from "../coValues/coMap.js";
import type { JsonValue } from "../jsonValue.js";

describe("CoMapPackImplementation", () => {
  const packer = new CoMapPackImplementation<string, string>();

  describe("packChanges", () => {
    test("should pack set operations", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "set", key: "name", value: "Alice" },
        { op: "set", key: "age", value: "30" },
        { op: "set", key: "city", value: "NYC" },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);

      // First operation
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      ); // op
      expect((result[0] as any)[1]).toBe("name"); // key
      expect((result[0] as any)[2]).toBe("Alice"); // value

      // Second operation
      expect(Array.isArray(result[1])).toBe(true);
      expect((result[1] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      ); // op
      expect((result[1] as any)[1]).toBe("age"); // key
      expect((result[1] as any)[2]).toBe("30"); // value

      // Third operation
      expect(Array.isArray(result[2])).toBe(true);
      expect((result[2] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      ); // op
      expect((result[2] as any)[1]).toBe("city"); // key
      expect((result[2] as any)[2]).toBe("NYC"); // value
    });

    test("should pack del operations", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "del", key: "name" },
        { op: "del", key: "age" },
        { op: "del", key: "city" },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);

      // First operation
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      ); // op
      expect((result[0] as any)[1]).toBe("name"); // key

      // Second operation
      expect(Array.isArray(result[1])).toBe(true);
      expect((result[1] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      ); // op
      expect((result[1] as any)[1]).toBe("age"); // key

      // Third operation
      expect(Array.isArray(result[2])).toBe(true);
      expect((result[2] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      ); // op
      expect((result[2] as any)[1]).toBe("city"); // key
    });

    test("should pack mixed operations (set and del)", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "set", key: "name", value: "Bob" },
        { op: "del", key: "age" },
        { op: "set", key: "city", value: "LA" },
      ];

      const result = packer.packChanges(changes);

      expect(Array.isArray(result)).toBe(true);
      expect(result.length).toBe(3);

      // First operation - set
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[0] as any)[1]).toBe("name");
      expect((result[0] as any)[2]).toBe("Bob");

      // Second operation - del
      expect((result[1] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      );
      expect((result[1] as any)[1]).toBe("age");
      expect((result[1] as any)[2]).toBeUndefined(); // del has no value

      // Third operation - set
      expect((result[2] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[2] as any)[1]).toBe("city");
      expect((result[2] as any)[2]).toBe("LA");
    });

    test("should handle empty array", () => {
      const changes: MapOpPayload<string, string>[] = [];

      const result = packer.packChanges(changes);

      expect(result).toEqual([]);
    });

    test("should handle single set operation", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "set", key: "name", value: "Charlie" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(1);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[0] as any)[1]).toBe("name");
      expect((result[0] as any)[2]).toBe("Charlie");
    });

    test("should handle single del operation", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "del", key: "name" },
      ];

      const result = packer.packChanges(changes);

      expect(result.length).toBe(1);
      expect(Array.isArray(result[0])).toBe(true);
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      );
      expect((result[0] as any)[1]).toBe("name");
      expect((result[0] as any)[2]).toBeUndefined();
    });

    test("should pack with JSON objects as values", () => {
      type UserProfile = { id: number; email: string; active: boolean };
      const profilePacker = new CoMapPackImplementation<string, UserProfile>();

      const changes: MapOpPayload<string, UserProfile>[] = [
        {
          op: "set",
          key: "user1",
          value: { id: 1, email: "alice@example.com", active: true },
        },
        {
          op: "set",
          key: "user2",
          value: { id: 2, email: "bob@example.com", active: false },
        },
        { op: "del", key: "user3" },
      ];

      const result = profilePacker.packChanges(changes);

      expect(result.length).toBe(3);

      // First operation
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[0] as any)[1]).toBe("user1");
      expect((result[0] as any)[2]).toEqual({
        id: 1,
        email: "alice@example.com",
        active: true,
      });

      // Second operation
      expect((result[1] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[1] as any)[1]).toBe("user2");
      expect((result[1] as any)[2]).toEqual({
        id: 2,
        email: "bob@example.com",
        active: false,
      });

      // Third operation
      expect((result[2] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      );
      expect((result[2] as any)[1]).toBe("user3");
    });

    test("should pack large batch of operations", () => {
      const changes: MapOpPayload<string, number>[] = Array.from(
        { length: 100 },
        (_, i) => ({
          op: i % 2 === 0 ? "set" : "del",
          key: `key${i}`,
          ...(i % 2 === 0 ? { value: i } : {}),
        }),
      ) as MapOpPayload<string, number>[];

      const numberPacker = new CoMapPackImplementation<string, number>();
      const result = numberPacker.packChanges(changes);

      expect(result.length).toBe(100);

      // Check first few operations
      expect((result[0] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      );
      expect((result[0] as any)[1]).toBe("key0");
      expect((result[0] as any)[2]).toBe(0);

      expect((result[1] as any)[0]).toBe(
        ENCODING_MAP_COMAP_OPERATION_TYPES.del,
      );
      expect((result[1] as any)[1]).toBe("key1");
    });

    test("should pack with null values", () => {
      const nullPacker = new CoMapPackImplementation<string, string | null>();

      const changes: MapOpPayload<string, string | null>[] = [
        { op: "set", key: "nullable", value: null },
        { op: "set", key: "present", value: "value" },
      ];

      const result = nullPacker.packChanges(changes);

      expect(result.length).toBe(2);
      // Trailing nulls are removed during packing, so the first operation
      // will have length 2 (op and key only)
      expect((result[0] as any).length).toBe(3);
      expect((result[1] as any)[2]).toBe("value");
    });

    test("should pack with numeric values", () => {
      const numberPacker = new CoMapPackImplementation<string, number>();

      const changes: MapOpPayload<string, number>[] = [
        { op: "set", key: "zero", value: 0 },
        { op: "set", key: "negative", value: -42 },
        { op: "set", key: "float", value: 3.14 },
      ];

      const result = numberPacker.packChanges(changes);

      expect(result.length).toBe(3);
      expect((result[0] as any)[2]).toBe(0);
      expect((result[1] as any)[2]).toBe(-42);
      expect((result[2] as any)[2]).toBe(3.14);
    });
  });

  describe("unpackChanges", () => {
    test("should unpack packed set changes correctly", () => {
      const packed = [
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "name", "Alice"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "age", "30"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "city", "NYC"],
      ];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ op: "set", key: "name", value: "Alice" });
      expect(result[1]).toEqual({ op: "set", key: "age", value: "30" });
      expect(result[2]).toEqual({ op: "set", key: "city", value: "NYC" });
    });

    test("should unpack packed del changes correctly", () => {
      const packed = [
        [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "name"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "age"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "city"],
      ];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ op: "del", key: "name" });
      expect(result[1]).toEqual({ op: "del", key: "age" });
      expect(result[2]).toEqual({ op: "del", key: "city" });
    });

    test("should unpack mixed operations", () => {
      const packed = [
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "name", "Bob"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "age"],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "city", "LA"],
      ];

      const result = packer.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ op: "set", key: "name", value: "Bob" });
      expect(result[1]).toEqual({ op: "del", key: "age" });
      expect(result[2]).toEqual({ op: "set", key: "city", value: "LA" });
    });

    test("should pass through already unpacked changes", () => {
      const changes: MapOpPayload<string, string>[] = [
        { op: "set", key: "name", value: "Alice" },
        { op: "del", key: "age" },
      ];

      const result = packer.unpackChanges(changes);

      expect(result).toBe(changes);
    });

    test("should handle empty array", () => {
      const result = packer.unpackChanges([]);

      expect(result).toEqual([]);
    });

    test("should unpack with JSON objects as values", () => {
      type UserProfile = { id: number; email: string; active: boolean };
      const profilePacker = new CoMapPackImplementation<string, UserProfile>();

      const packed = [
        [
          ENCODING_MAP_COMAP_OPERATION_TYPES.set,
          "user1",
          { id: 1, email: "alice@example.com", active: true },
        ],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "user2"],
      ];

      const result = profilePacker.unpackChanges(packed as any);

      expect(result.length).toBe(2);
      expect(result[0]).toEqual({
        op: "set",
        key: "user1",
        value: { id: 1, email: "alice@example.com", active: true },
      });
      expect(result[1]).toEqual({ op: "del", key: "user2" });
    });

    test("should unpack with null values", () => {
      const nullPacker = new CoMapPackImplementation<string, string | null>();

      const packed = [
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "nullable", null],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "present", "value"],
      ];

      const result = nullPacker.unpackChanges(packed as any);

      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ op: "set", key: "nullable", value: null });
      expect(result[1]).toEqual({ op: "set", key: "present", value: "value" });
    });

    test("should unpack with numeric values", () => {
      const numberPacker = new CoMapPackImplementation<string, number>();

      const packed = [
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "zero", 0],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "negative", -42],
        [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "float", 3.14],
      ];

      const result = numberPacker.unpackChanges(packed as any);

      expect(result.length).toBe(3);
      expect(result[0]).toEqual({ op: "set", key: "zero", value: 0 });
      expect(result[1]).toEqual({ op: "set", key: "negative", value: -42 });
      expect(result[2]).toEqual({ op: "set", key: "float", value: 3.14 });
    });
  });

  describe("pack/unpack roundtrip", () => {
    test("should maintain data integrity through pack/unpack cycle", () => {
      const original: MapOpPayload<string, string>[] = [
        { op: "set", key: "name", value: "Alice" },
        { op: "set", key: "age", value: "30" },
        { op: "del", key: "city" },
        { op: "set", key: "country", value: "USA" },
        { op: "del", key: "zip" },
      ];

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      expect(unpacked.length).toBe(original.length);
      for (let i = 0; i < unpacked.length; i++) {
        expect(unpacked[i]).toEqual(original[i]);
      }
    });

    test("should work with multiple pack/unpack cycles", () => {
      const original: MapOpPayload<string, number>[] = [
        { op: "set", key: "count", value: 42 },
        { op: "del", key: "old" },
        { op: "set", key: "score", value: 99 },
      ];

      const numberPacker = new CoMapPackImplementation<string, number>();

      const packed1 = numberPacker.packChanges(original);
      const unpacked1 = numberPacker.unpackChanges(packed1 as any);
      const packed2 = numberPacker.packChanges(unpacked1);
      const unpacked2 = numberPacker.unpackChanges(packed2 as any);

      expect(unpacked2.length).toBe(original.length);
      for (let i = 0; i < unpacked2.length; i++) {
        expect(unpacked2[i]).toEqual(original[i]);
      }
    });

    test("should maintain complex objects through roundtrip", () => {
      type ComplexValue = {
        nested: { deep: { value: string } };
        array: number[];
        mixed: (string | number | boolean)[];
      };
      const complexPacker = new CoMapPackImplementation<string, ComplexValue>();

      const original: MapOpPayload<string, ComplexValue>[] = [
        {
          op: "set",
          key: "data",
          value: {
            nested: { deep: { value: "test" } },
            array: [1, 2, 3],
            mixed: ["a", 1, true],
          },
        },
        { op: "del", key: "old" },
      ];

      const packed = complexPacker.packChanges(original);
      const unpacked = complexPacker.unpackChanges(packed as any);

      expect(unpacked).toEqual(original);
    });

    test("should handle large datasets in roundtrip", () => {
      const original: MapOpPayload<string, string>[] = Array.from(
        { length: 1000 },
        (_, i) => ({
          op: i % 3 === 0 ? "del" : "set",
          key: `key${i}`,
          ...(i % 3 !== 0 ? { value: `value${i}` } : {}),
        }),
      ) as MapOpPayload<string, string>[];

      const packed = packer.packChanges(original);
      const unpacked = packer.unpackChanges(packed as any);

      expect(unpacked.length).toBe(original.length);
      expect(unpacked).toEqual(original);
    });
  });

  describe("space efficiency", () => {
    test("packed format should be more compact than unpacked", () => {
      const changes: MapOpPayload<string, string>[] = Array.from(
        { length: 50 },
        (_, i) => ({
          op: "set",
          key: `key${i}`,
          value: `value${i}`,
        }),
      );

      const packed = packer.packChanges(changes);
      const unpackedSize = JSON.stringify(changes).length;
      const packedSize = JSON.stringify(packed).length;

      // Packed should be smaller due to using numeric operation types
      expect(packedSize).toBeLessThan(unpackedSize);
    });

    test("packed format should save space with mixed operations", () => {
      const changes: MapOpPayload<string, string>[] = Array.from(
        { length: 100 },
        (_, i) => ({
          op: i % 2 === 0 ? "set" : "del",
          key: `key${i}`,
          ...(i % 2 === 0 ? { value: `value${i}` } : {}),
        }),
      ) as MapOpPayload<string, string>[];

      const packed = packer.packChanges(changes);
      const unpackedSize = JSON.stringify(changes).length;
      const packedSize = JSON.stringify(packed).length;

      expect(packedSize).toBeLessThan(unpackedSize);
    });
  });
});

describe("getCoMapOperationType", () => {
  test("should return 'set' for operation type 1", () => {
    const arr = [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "key", "value"];
    const result = getCoMapOperationType(arr, "set");

    expect(result).toBe("set");
  });

  test("should return 'del' for operation type 2", () => {
    const arr = [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "key"];
    const result = getCoMapOperationType(arr, "set");

    expect(result).toBe("del");
  });

  test("should return default value for unknown operation type", () => {
    const arr = [999, "key", "value"];
    const result = getCoMapOperationType(arr, "set");

    expect(result).toBe("set");
  });

  test("should return default value for empty array", () => {
    const arr: any[] = [];
    const result = getCoMapOperationType(arr, "set");

    expect(result).toBe("set");
  });
});

describe("packArrToObjectCoMap", () => {
  test("should pack set operation", () => {
    const operation: MapOpPayloadSet<string, string> = {
      op: "set",
      key: "name",
      value: "Alice",
    };

    const result = packArrToObjectCoMap(operation);

    expect(result[0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.set);
    expect(result[1]).toBe("name");
    expect(result[2]).toBe("Alice");
  });

  test("should pack del operation", () => {
    const operation: MapOpPayloadDel<string> = {
      op: "del",
      key: "name",
    };

    const result = packArrToObjectCoMap(operation);

    expect(result[0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.del);
    expect(result[1]).toBe("name");
    expect(result.length).toBe(2); // No value for del
  });

  test("should pack set operation with object value", () => {
    const operation: MapOpPayloadSet<string, { id: number; name: string }> = {
      op: "set",
      key: "user",
      value: { id: 1, name: "Bob" },
    };

    const result = packArrToObjectCoMap(operation);

    expect(result[0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.set);
    expect(result[1]).toBe("user");
    expect(result[2]).toEqual({ id: 1, name: "Bob" });
  });

  test("should pack set operation with null value", () => {
    const operation: MapOpPayloadSet<string, null> = {
      op: "set",
      key: "nullable",
      value: null,
    };

    const result = packArrToObjectCoMap(operation);

    expect(result[0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.set);
    expect(result[1]).toBe("nullable");
    expect(result[2]).toBe(null);
    expect(result.length).toBe(3);
  });
});

describe("unpackArrToObjectCoMap", () => {
  test("should unpack set operation", () => {
    const packed = [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "name", "Alice"];

    const result = unpackArrToObjectCoMap(packed);

    expect(result.op).toBe("set");
    expect((result as MapOpPayloadSet<string, string>).key).toBe("name");
    expect((result as MapOpPayloadSet<string, string>).value).toBe("Alice");
  });

  test("should unpack del operation", () => {
    const packed = [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "name"];

    const result = unpackArrToObjectCoMap(packed);

    expect(result.op).toBe("del");
    expect((result as MapOpPayloadDel<string>).key).toBe("name");
    // MapOpPayloadDel doesn't have a value property
    expect("value" in result).toBe(false);
  });

  test("should unpack set operation with object value", () => {
    const packed = [
      ENCODING_MAP_COMAP_OPERATION_TYPES.set,
      "user",
      { id: 1, name: "Bob" },
    ];

    const result = unpackArrToObjectCoMap(packed);

    expect(result.op).toBe("set");
    expect((result as MapOpPayloadSet<string, any>).key).toBe("user");
    expect((result as MapOpPayloadSet<string, any>).value).toEqual({
      id: 1,
      name: "Bob",
    });
  });

  test("should unpack set operation with null value", () => {
    const packed = [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "nullable", null];

    const result = unpackArrToObjectCoMap(packed);

    expect(result.op).toBe("set");
    expect((result as MapOpPayloadSet<string, null>).key).toBe("nullable");
    expect((result as MapOpPayloadSet<string, null>).value).toBe(null);
  });
});

describe("packArrOfObjectsCoMap", () => {
  test("should pack array of set operations", () => {
    const operations: MapOpPayload<string, string>[] = [
      { op: "set", key: "name", value: "Alice" },
      { op: "set", key: "age", value: "30" },
    ];

    const result = packArrOfObjectsCoMap(operations);

    expect(result.length).toBe(2);
    expect(result[0]![0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.set);
    expect(result[0]![1]).toBe("name");
    expect(result[0]![2]).toBe("Alice");
    expect(result[1]![0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.set);
    expect(result[1]![1]).toBe("age");
    expect(result[1]![2]).toBe("30");
  });

  test("should pack array of del operations", () => {
    const operations: MapOpPayload<string, string>[] = [
      { op: "del", key: "name" },
      { op: "del", key: "age" },
    ];

    const result = packArrOfObjectsCoMap(operations);

    expect(result.length).toBe(2);
    expect(result[0]![0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.del);
    expect(result[0]![1]).toBe("name");
    expect(result[1]![0]).toBe(ENCODING_MAP_COMAP_OPERATION_TYPES.del);
    expect(result[1]![1]).toBe("age");
  });

  test("should pack empty array", () => {
    const operations: MapOpPayload<string, string>[] = [];

    const result = packArrOfObjectsCoMap(operations);

    expect(result).toEqual([]);
  });
});

describe("unpackArrOfObjectsCoMap", () => {
  test("should unpack array of set operations", () => {
    const packed = [
      [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "name", "Alice"],
      [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "age", "30"],
    ];

    const result = unpackArrOfObjectsCoMap(packed);

    expect(result.length).toBe(2);
    expect(result[0]).toEqual({ op: "set", key: "name", value: "Alice" });
    expect(result[1]).toEqual({ op: "set", key: "age", value: "30" });
  });

  test("should unpack array of del operations", () => {
    const packed = [
      [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "name"],
      [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "age"],
    ];

    const result = unpackArrOfObjectsCoMap(packed);

    expect(result.length).toBe(2);
    expect(result[0]).toEqual({ op: "del", key: "name" });
    expect(result[1]).toEqual({ op: "del", key: "age" });
  });

  test("should unpack empty array", () => {
    const packed: any[] = [];

    const result = unpackArrOfObjectsCoMap(packed);

    expect(result).toEqual([]);
  });

  test("should unpack mixed operations", () => {
    const packed = [
      [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "name", "Bob"],
      [ENCODING_MAP_COMAP_OPERATION_TYPES.del, "age"],
      [ENCODING_MAP_COMAP_OPERATION_TYPES.set, "city", "LA"],
    ];

    const result = unpackArrOfObjectsCoMap(packed);

    expect(result.length).toBe(3);
    expect(result[0]).toEqual({ op: "set", key: "name", value: "Bob" });
    expect(result[1]).toEqual({ op: "del", key: "age" });
    expect(result[2]).toEqual({ op: "set", key: "city", value: "LA" });
  });
});

describe("pack/unpack roundtrip for individual functions", () => {
  test("packArrToObjectCoMap and unpackArrToObjectCoMap roundtrip", () => {
    const original: MapOpPayloadSet<string, string> = {
      op: "set",
      key: "name",
      value: "Alice",
    };

    const packed = packArrToObjectCoMap(original);
    const unpacked = unpackArrToObjectCoMap(packed);

    expect(unpacked).toEqual(original);
  });

  test("packArrOfObjectsCoMap and unpackArrOfObjectsCoMap roundtrip", () => {
    const original: MapOpPayload<string, string>[] = [
      { op: "set", key: "name", value: "Alice" },
      { op: "del", key: "age" },
      { op: "set", key: "city", value: "NYC" },
    ];

    const packed = packArrOfObjectsCoMap(original);
    const unpacked = unpackArrOfObjectsCoMap(packed);

    expect(unpacked).toEqual(original);
  });
});

describe("edge cases", () => {
  const packer = new CoMapPackImplementation<string, string>();

  test("should handle keys with special characters", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "user:123", value: "Alice" },
      { op: "set", key: "meta@data", value: "test" },
      { op: "del", key: "old-key" },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
  });

  test("should handle unicode keys and values", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "名前", value: "太郎" },
      { op: "set", key: "emoji", value: "🎉🎊" },
      { op: "del", key: "古いキー" },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
  });

  test("should handle very long keys", () => {
    const longKey = "a".repeat(1000);
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: longKey, value: "value" },
      { op: "del", key: longKey },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
  });

  test("should handle very long values", () => {
    const longValue = "x".repeat(10000);
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "data", value: longValue },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
  });

  test("should handle empty string keys and values", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "", value: "" },
      { op: "del", key: "" },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
  });
});

describe("detailed roundtrip tests - data integrity", () => {
  test("should preserve boolean values exactly", () => {
    const boolPacker = new CoMapPackImplementation<string, boolean>();
    const changes: MapOpPayload<string, boolean>[] = [
      { op: "set", key: "isTrue", value: true },
      { op: "set", key: "isFalse", value: false },
      { op: "del", key: "removed" },
      { op: "set", key: "anotherTrue", value: true },
    ];

    const packed = boolPacker.packChanges(changes);
    const unpacked = boolPacker.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect(unpacked[0]?.op).toBe("set");
    expect((unpacked[0] as MapOpPayloadSet<string, boolean>).value).toBe(true);
    expect((unpacked[0] as MapOpPayloadSet<string, boolean>).value).not.toBe(1);
    expect(unpacked[1]?.op).toBe("set");
    expect((unpacked[1] as MapOpPayloadSet<string, boolean>).value).toBe(false);
    expect((unpacked[1] as MapOpPayloadSet<string, boolean>).value).not.toBe(0);
  });

  test("should preserve numeric types and special values", () => {
    const numPacker = new CoMapPackImplementation<string, number>();
    const changes: MapOpPayload<string, number>[] = [
      { op: "set", key: "zero", value: 0 },
      { op: "set", key: "negativeZero", value: -0 },
      { op: "set", key: "positive", value: 42 },
      { op: "set", key: "negative", value: -42 },
      { op: "set", key: "float", value: 3.14159 },
      { op: "set", key: "negativeFloat", value: -2.71828 },
      { op: "set", key: "large", value: 1e10 },
      { op: "set", key: "small", value: 1e-10 },
    ];

    const packed = numPacker.packChanges(changes);
    const unpacked = numPacker.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect(unpacked.length).toBe(changes.length);

    // Verify each value is exactly preserved
    expect((unpacked[0] as MapOpPayloadSet<string, number>).value).toBe(0);
    expect((unpacked[0] as MapOpPayloadSet<string, number>).value).not.toBe(
      false,
    );
    expect((unpacked[1] as MapOpPayloadSet<string, number>).value).toBe(-0);
    expect((unpacked[2] as MapOpPayloadSet<string, number>).value).toBe(42);
    expect((unpacked[3] as MapOpPayloadSet<string, number>).value).toBe(-42);
    expect((unpacked[4] as MapOpPayloadSet<string, number>).value).toBe(
      3.14159,
    );
    expect((unpacked[5] as MapOpPayloadSet<string, number>).value).toBe(
      -2.71828,
    );
    expect((unpacked[6] as MapOpPayloadSet<string, number>).value).toBe(1e10);
    expect((unpacked[7] as MapOpPayloadSet<string, number>).value).toBe(1e-10);
  });

  test("should preserve null vs undefined distinction", () => {
    const nullPacker = new CoMapPackImplementation<string, string | null>();
    const changes: MapOpPayload<string, string | null>[] = [
      { op: "set", key: "nullValue", value: null },
      { op: "set", key: "stringValue", value: "test" },
      { op: "del", key: "deleted" },
    ];

    const packed = nullPacker.packChanges(changes);
    const unpacked = nullPacker.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect((unpacked[0] as MapOpPayloadSet<string, string | null>).value).toBe(
      null,
    );
    expect(
      (unpacked[0] as MapOpPayloadSet<string, string | null>).value,
    ).not.toBe(undefined);
    expect(
      (unpacked[0] as MapOpPayloadSet<string, string | null>).value,
    ).not.toBe(0);
    expect(
      (unpacked[0] as MapOpPayloadSet<string, string | null>).value,
    ).not.toBe(false);
    expect(
      (unpacked[0] as MapOpPayloadSet<string, string | null>).value,
    ).not.toBe("");
  });

  test("should preserve string edge cases", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "empty", value: "" },
      { op: "set", key: "space", value: " " },
      { op: "set", key: "spaces", value: "   " },
      { op: "set", key: "newline", value: "\n" },
      { op: "set", key: "tab", value: "\t" },
      { op: "set", key: "mixed", value: "\n\t\r" },
      { op: "set", key: "quote", value: '"' },
      { op: "set", key: "backslash", value: "\\" },
      { op: "set", key: "json", value: '{"key":"value"}' },
    ];

    const packer = new CoMapPackImplementation<string, string>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    // Verify exact string preservation
    for (let i = 0; i < changes.length; i++) {
      expect((unpacked[i] as MapOpPayloadSet<string, string>).value).toBe(
        (changes[i] as MapOpPayloadSet<string, string>).value,
      );
    }
  });

  test("should preserve complex nested objects", () => {
    type ComplexType = {
      id: number;
      name: string;
      nested: {
        deep: {
          value: boolean;
          array: (number | null)[];
        };
      };
      nullField: null;
      boolField: boolean;
    };

    const complexPacker = new CoMapPackImplementation<string, ComplexType>();
    const changes: MapOpPayload<string, ComplexType>[] = [
      {
        op: "set",
        key: "complex",
        value: {
          id: 123,
          name: "test",
          nested: {
            deep: {
              value: true,
              array: [1, null, 3],
            },
          },
          nullField: null,
          boolField: false,
        },
      },
    ];

    const packed = complexPacker.packChanges(changes);
    const unpacked = complexPacker.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const unpackedValue = (unpacked[0] as MapOpPayloadSet<string, ComplexType>)
      .value;
    expect(unpackedValue.id).toBe(123);
    expect(unpackedValue.name).toBe("test");
    expect(unpackedValue.nested.deep.value).toBe(true);
    expect(unpackedValue.nested.deep.array).toEqual([1, null, 3]);
    expect(unpackedValue.nullField).toBe(null);
    expect(unpackedValue.boolField).toBe(false);
  });

  test("should preserve arrays with mixed types", () => {
    type MixedArray = (string | number | boolean | null)[];
    const arrayPacker = new CoMapPackImplementation<string, MixedArray>();
    const changes: MapOpPayload<string, MixedArray>[] = [
      {
        op: "set",
        key: "mixed",
        value: ["string", 42, true, null, false, 0, ""],
      },
    ];

    const packed = arrayPacker.packChanges(changes);
    const unpacked = arrayPacker.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const arr = (unpacked[0] as MapOpPayloadSet<string, MixedArray>).value;
    expect(arr[0]).toBe("string");
    expect(arr[1]).toBe(42);
    expect(arr[2]).toBe(true);
    expect(arr[3]).toBe(null);
    expect(arr[4]).toBe(false);
    expect(arr[5]).toBe(0);
    expect(arr[6]).toBe("");
  });

  test("should preserve operation order in complex sequences", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "a", value: "1" },
      { op: "set", key: "b", value: "2" },
      { op: "del", key: "c" },
      { op: "set", key: "d", value: "3" },
      { op: "del", key: "e" },
      { op: "del", key: "f" },
      { op: "set", key: "g", value: "4" },
    ];

    const packer = new CoMapPackImplementation<string, string>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect(unpacked.length).toBe(changes.length);

    // Verify exact order and types
    for (let i = 0; i < changes.length; i++) {
      expect(unpacked[i]?.op).toBe(changes[i]?.op);
      expect(unpacked[i]?.key).toBe(changes[i]?.key);
      if (changes[i]?.op === "set") {
        expect((unpacked[i] as MapOpPayloadSet<string, string>).value).toBe(
          (changes[i] as MapOpPayloadSet<string, string>).value,
        );
      }
    }
  });

  test("should handle Unicode edge cases", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "emoji", value: "👨‍👩‍👧‍👦" }, // family emoji with ZWJ
      { op: "set", key: "flag", value: "🇺🇸" }, // flag (2 regional indicators)
      { op: "set", key: "combining", value: "é" }, // e + combining acute
      { op: "set", key: "rtl", value: "مرحبا" }, // Arabic RTL text
      { op: "set", key: "chinese", value: "你好世界" },
      { op: "set", key: "mixed", value: "Hello 👋 World 🌍" },
    ];

    const packer = new CoMapPackImplementation<string, string>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    // Verify exact character preservation
    for (let i = 0; i < changes.length; i++) {
      const originalValue = (changes[i] as MapOpPayloadSet<string, string>)
        .value;
      const unpackedValue = (unpacked[i] as MapOpPayloadSet<string, string>)
        .value;
      expect(unpackedValue).toBe(originalValue);
      expect(unpackedValue.length).toBe(originalValue.length);
    }
  });

  test("should handle keys with various special characters", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "dot.notation.key", value: "v1" },
      { op: "set", key: "bracket[0]", value: "v2" },
      { op: "set", key: "space in key", value: "v3" },
      { op: "set", key: "slash/key", value: "v4" },
      { op: "set", key: "backslash\\key", value: "v5" },
      { op: "set", key: "colon:key", value: "v6" },
      { op: "set", key: "question?key", value: "v7" },
      { op: "set", key: "ampersand&key", value: "v8" },
      { op: "set", key: "pipe|key", value: "v9" },
      { op: "del", key: "special!@#$%^&*()" },
    ];

    const packer = new CoMapPackImplementation<string, string>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    for (let i = 0; i < changes.length; i++) {
      expect(unpacked[i]?.key).toBe(changes[i]?.key);
    }
  });

  test("should preserve type identity through multiple cycles", () => {
    type TestValue = {
      str: string;
      num: number;
      bool: boolean;
      nul: null;
      arr: number[];
    };
    const packer = new CoMapPackImplementation<string, TestValue>();

    const original: MapOpPayload<string, TestValue>[] = [
      {
        op: "set",
        key: "data",
        value: {
          str: "test",
          num: 42,
          bool: true,
          nul: null,
          arr: [1, 2, 3],
        },
      },
      { op: "del", key: "old" },
    ];

    // Multiple pack/unpack cycles
    let current = original;
    for (let i = 0; i < 5; i++) {
      const packed = packer.packChanges(current);
      current = packer.unpackChanges(packed as any);
    }

    expect(current).toEqual(original);
    const val = (current[0] as MapOpPayloadSet<string, TestValue>).value;
    expect(typeof val.str).toBe("string");
    expect(typeof val.num).toBe("number");
    expect(typeof val.bool).toBe("boolean");
    expect(val.nul).toBe(null);
    expect(Array.isArray(val.arr)).toBe(true);
  });

  test("should handle alternating set/del operations", () => {
    const changes: MapOpPayload<string, number>[] = [];
    for (let i = 0; i < 50; i++) {
      if (i % 2 === 0) {
        changes.push({ op: "set", key: `key${i}`, value: i });
      } else {
        changes.push({ op: "del", key: `key${i}` });
      }
    }

    const packer = new CoMapPackImplementation<string, number>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect(unpacked.length).toBe(changes.length);

    for (let i = 0; i < changes.length; i++) {
      expect(unpacked[i]).toEqual(changes[i]);
    }
  });

  test("should preserve empty arrays and objects in values", () => {
    type ComplexValue = {
      emptyArray: any[];
      emptyObject: Record<string, never>;
      nestedEmpty: { arr: any[]; obj: Record<string, never> };
    };
    const packer = new CoMapPackImplementation<string, ComplexValue>();

    const changes: MapOpPayload<string, ComplexValue>[] = [
      {
        op: "set",
        key: "data",
        value: {
          emptyArray: [],
          emptyObject: {},
          nestedEmpty: { arr: [], obj: {} },
        },
      },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const val = (unpacked[0] as MapOpPayloadSet<string, ComplexValue>).value;
    expect(Array.isArray(val.emptyArray)).toBe(true);
    expect(val.emptyArray.length).toBe(0);
    expect(typeof val.emptyObject).toBe("object");
    expect(Object.keys(val.emptyObject).length).toBe(0);
    expect(Array.isArray(val.nestedEmpty.arr)).toBe(true);
    expect(val.nestedEmpty.arr.length).toBe(0);
    expect(Object.keys(val.nestedEmpty.obj).length).toBe(0);
  });

  test("should handle same key with different operations", () => {
    const changes: MapOpPayload<string, string>[] = [
      { op: "set", key: "sameKey", value: "value1" },
      { op: "del", key: "sameKey" },
      { op: "set", key: "sameKey", value: "value2" },
      { op: "set", key: "sameKey", value: "value3" },
      { op: "del", key: "sameKey" },
    ];

    const packer = new CoMapPackImplementation<string, string>();
    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    expect(unpacked.length).toBe(5);

    expect(unpacked[0]?.op).toBe("set");
    expect((unpacked[0] as MapOpPayloadSet<string, string>).value).toBe(
      "value1",
    );
    expect(unpacked[1]?.op).toBe("del");
    expect(unpacked[2]?.op).toBe("set");
    expect((unpacked[2] as MapOpPayloadSet<string, string>).value).toBe(
      "value2",
    );
    expect(unpacked[3]?.op).toBe("set");
    expect((unpacked[3] as MapOpPayloadSet<string, string>).value).toBe(
      "value3",
    );
    expect(unpacked[4]?.op).toBe("del");
  });

  test("should preserve object property order", () => {
    type OrderedObj = {
      z: number;
      a: number;
      m: number;
    };
    const packer = new CoMapPackImplementation<string, OrderedObj>();

    const changes: MapOpPayload<string, OrderedObj>[] = [
      { op: "set", key: "obj", value: { z: 1, a: 2, m: 3 } },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const val = (unpacked[0] as MapOpPayloadSet<string, OrderedObj>).value;
    const keys = Object.keys(val);
    expect(keys).toEqual(["z", "a", "m"]);
  });

  test("should handle deeply nested structures", () => {
    type DeepNested = {
      level1: {
        level2: {
          level3: {
            level4: {
              level5: {
                value: string;
              };
            };
          };
        };
      };
    };
    const packer = new CoMapPackImplementation<string, DeepNested>();

    const changes: MapOpPayload<string, DeepNested>[] = [
      {
        op: "set",
        key: "deep",
        value: {
          level1: {
            level2: {
              level3: {
                level4: {
                  level5: {
                    value: "deep value",
                  },
                },
              },
            },
          },
        },
      },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const val = (unpacked[0] as MapOpPayloadSet<string, DeepNested>).value;
    expect(val.level1.level2.level3.level4.level5.value).toBe("deep value");
  });

  test("should preserve arrays with null values", () => {
    type ArrayWithNull = (string | null)[];
    const packer = new CoMapPackImplementation<string, ArrayWithNull>();

    const changes: MapOpPayload<string, ArrayWithNull>[] = [
      {
        op: "set",
        key: "arrayWithNull",
        value: ["a", null, "c", null, "e"],
      },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    expect(unpacked).toEqual(changes);
    const arr = (unpacked[0] as MapOpPayloadSet<string, ArrayWithNull>).value;
    expect(arr[0]).toBe("a");
    expect(arr[1]).toBe(null);
    expect(arr[2]).toBe("c");
    expect(arr[3]).toBe(null);
    expect(arr[4]).toBe("e");
    expect(arr.length).toBe(5);
  });

  test("should preserve exact JSON serialization through roundtrip", () => {
    type JsonData = {
      string: string;
      number: number;
      boolean: boolean;
      null: null;
      array: JsonValue[];
      object: { nested: string };
    };
    const packer = new CoMapPackImplementation<string, JsonData>();

    const changes: MapOpPayload<string, JsonData>[] = [
      {
        op: "set",
        key: "json",
        value: {
          string: "value",
          number: 123,
          boolean: true,
          null: null,
          array: [1, "two", false, null],
          object: { nested: "data" },
        },
      },
    ];

    const packed = packer.packChanges(changes);
    const unpacked = packer.unpackChanges(packed as any);

    // Verify JSON serialization is identical
    expect(JSON.stringify(unpacked)).toBe(JSON.stringify(changes));
    expect(unpacked).toEqual(changes);
  });
});
