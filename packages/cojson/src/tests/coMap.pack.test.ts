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
