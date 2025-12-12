import { describe, expect, test, beforeAll } from "vitest";
import { stableStringify as stableStringifyTS } from "../jsonStringify.js";

// Import Rust implementations
let stableStringifyWasm: (value: string) => string | undefined;
let stableStringifyNapi: (value: string) => string | undefined;

// Try to import WASM
try {
  const wasmModule = await import("cojson-core-wasm");
  if (wasmModule.initialize) {
    await wasmModule.initialize();
  }
  stableStringifyWasm = wasmModule.stableStringify;
} catch (e) {
  console.warn("WASM module not available:", e);
  stableStringifyWasm = () => undefined;
}

// Try to import NAPI
try {
  const napiModule = await import("cojson-core-napi");
  stableStringifyNapi = napiModule.stableStringify;
} catch (e) {
  console.warn("NAPI module not available:", e);
  stableStringifyNapi = () => undefined;
}

describe("stableStringify comparison", () => {
  const testCases = [
    // Primitives
    { name: "null", value: null },
    { name: "true", value: true },
    { name: "false", value: false },
    { name: "zero", value: 0 },
    { name: "positive integer", value: 42 },
    { name: "negative integer", value: -42 },
    { name: "positive float", value: 3.14 },
    { name: "negative float", value: -3.14 },
    { name: "empty string", value: "" },
    { name: "simple string", value: "hello" },
    { name: "string with quotes", value: 'hello "world"' },
    { name: "string with special chars", value: "hello\nworld\t" },
    { name: "Infinity", value: Infinity },
    { name: "-Infinity", value: -Infinity },
    { name: "NaN", value: NaN },
    { name: "Very large number", value: 1.7976931348623157e308 },
    { name: "Very small number", value: 5e-324 },
    {
      name: "Very large number with exponent sign",
      value: 1.7976931348623157e308,
    },

    // Arrays
    { name: "empty array", value: [] },
    { name: "array of numbers", value: [1, 2, 3] },
    { name: "array of strings", value: ["a", "b", "c"] },
    { name: "array with null", value: [1, null, 3] },
    {
      name: "nested arrays",
      value: [
        [1, 2],
        [3, 4],
      ],
    },
    { name: "array with objects", value: [{ a: 1 }, { b: 2 }] },

    // Objects
    { name: "empty object", value: {} },
    { name: "simple object", value: { a: 1, b: 2 } },
    { name: "object with unsorted keys", value: { z: 3, a: 1, m: 2 } },
    {
      name: "object with string values",
      value: { name: "test", value: "data" },
    },
    { name: "object with null", value: { a: 1, b: null, c: 3 } },
    { name: "object with undefined", value: { a: 1, b: undefined, c: 3 } },
    { name: "nested objects", value: { outer: { inner: "value" } } },
    {
      name: "complex nested structure",
      value: {
        users: [
          { id: 1, name: "Alice" },
          { id: 2, name: "Bob" },
        ],
        metadata: {
          version: "1.0",
          count: 2,
        },
      },
    },

    // Special strings
    { name: "encrypted_U prefix", value: "encrypted_U12345" },
    { name: "binary_U prefix", value: "binary_U67890" },
    { name: "string starting with encrypted_U", value: "encrypted_User123" },
    { name: "string starting with binary_U", value: "binary_User456" },
  ];

  function runComparisonTest(
    testName: string,
    value: any,
    rustImpl: (value: string) => string | undefined,
    implName: string,
  ) {
    test(`${testName} [${implName}]`, () => {
      if (!rustImpl) {
        // Skip test if implementation is not available
        return;
      }

      // Get TypeScript result
      const tsResult = stableStringifyTS(value);

      // If TypeScript returns undefined, skip the test
      // (This happens when value is undefined, which can't be serialized to JSON)
      if (tsResult === undefined) {
        return;
      }

      // Convert value to JSON string for Rust (Rust expects a JSON string)
      const jsonInput = JSON.stringify(value);

      // Get Rust result
      const rustResult = rustImpl(jsonInput);

      // Compare results
      expect(rustResult).toBe(tsResult);

      // Verify that JSON.stringify on Rust result produces valid JSON
      // (This should just add quotes around the already-valid JSON string)
      const rustStringified = JSON.stringify(rustResult);
      expect(() => JSON.parse(rustStringified)).not.toThrow();
      expect(JSON.parse(rustStringified)).toBe(rustResult);
    });
  }

  testCases.forEach(({ name, value }) => {
    if (stableStringifyWasm) {
      runComparisonTest(name, value, stableStringifyWasm, "WASM");
    }

    if (stableStringifyNapi) {
      runComparisonTest(name, value, stableStringifyNapi, "NAPI");
    }
  });

  describe("edge cases", () => {
    const edgeCases = [
      {
        name: "object with all special values",
        value: {
          null: null,
          undefined: undefined,
          true: true,
          false: false,
          zero: 0,
          empty: "",
          array: [],
          object: {},
        },
      },
      {
        name: "deeply nested structure",
        value: {
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
      },
      {
        name: "array with mixed types",
        value: [1, "string", true, null, { key: "value" }, [1, 2, 3]],
      },
      {
        name: "object with numeric keys",
        value: { "1": "one", "2": "two", "10": "ten" },
      },
      {
        name: "object with special key names",
        value: {
          "key with spaces": "value",
          "key-with-dashes": "value",
          "key.with.dots": "value",
        },
      },

      // Unicode and multibyte characters
      { name: "string with emoji", value: "hello 😀 world 🌍" },
      { name: "string with accented characters", value: "café résumé naïve" },
      { name: "string with surrogate pairs", value: "\uD83D\uDE00" },
      { name: "string with non-BMP characters", value: "\u{1F600}" },
      {
        name: "object with unicode keys",
        value: { café: 1, résumé: 2, 测试: 3 },
      },

      // Complex escape sequences
      { name: "string with multiple backslashes", value: "path\\\\to\\\\file" },
      { name: "string with nested escapes", value: '\\"quoted\\"' },
      { name: "string with literal escape", value: "\\n" },
      {
        name: "string with all escape sequences",
        value: '\\"\\/\\b\\f\\n\\r\\t',
      },

      // Control characters and non-printable
      { name: "string with null byte", value: "\u0000" },
      { name: "string with control characters", value: "\u0001\u0002\u0003" },
      { name: "string with BOM", value: "\uFEFF" },
      {
        name: "object with control character keys",
        value: { "\u0001": "value", "\u0002": "value2" },
      },

      // Number edge cases
      { name: "negative zero", value: -0 },
      { name: "number with many zeros", value: 0.0000000000000000000000001 },
      {
        name: "high precision decimal",
        value: 3.141592653589793238462643383279,
      },
      { name: "scientific notation lowercase e", value: 1e-10 },
      { name: "scientific notation uppercase E", value: 1e10 },
      { name: "number beyond safe integer", value: 9007199254740992 },
      { name: "number at safe integer limit", value: 9007199254740991 },

      // Strings that look like JSON
      { name: "string containing JSON object", value: '{"fake": "json"}' },
      { name: "string with curly braces", value: "{key: value}" },
      { name: "string containing JSON array", value: "[1, 2, 3]" },
      {
        name: "string with nested JSON-like structure",
        value: '{"nested": {"deep": "value"}}',
      },

      // Keys with special characters
      {
        name: "object with unicode keys",
        value: { 测试: 1, тест: 2, café: 3 },
      },
      {
        name: "object with escaped keys",
        value: { "key\nwith\nnewlines": "value" },
      },
      {
        name: "object with empty key",
        value: { "": "empty key", normal: "value" },
      },
      {
        name: "object with long key",
        value: Object.fromEntries([["a".repeat(100), "value"]]),
      },
      {
        name: "object with special key characters",
        value: {
          "key with spaces": 1,
          "key-with-dashes": 2,
          "key.with.dots": 3,
        },
      },

      // Long strings
      { name: "very long string", value: "a".repeat(1000) },
      {
        name: "array with long strings",
        value: ["a".repeat(500), "b".repeat(500)],
      },
      {
        name: "object with many keys",
        value: Object.fromEntries(
          Array.from({ length: 100 }, (_, i) => [`key${i}`, i]),
        ),
      },

      // Edge cases with special prefixes
      { name: "encrypted_U with special chars", value: "encrypted_U\n123" },
      { name: "binary_U with escape", value: "binary_U\\n123" },
      {
        name: "string containing encrypted_U prefix",
        value: "prefix encrypted_U123",
      },
      { name: "encrypted_U with unicode", value: "encrypted_U😀123" },

      // Special array cases
      { name: "array with sparse elements", value: [1, , 3] },
      { name: "array with undefined elements", value: [1, undefined, 3] },
      {
        name: "very deep nested array",
        value: Array(20)
          .fill(null)
          .reduce((acc) => [acc], "deep"),
      },

      // Complex combinations
      {
        name: "object with unicode keys and escaped values",
        value: { 测试: "value\nwith\tescapes", café: "path\\\\to\\\\file" },
      },
      {
        name: "array with mixed unicode and escapes",
        value: ["normal", "😀", "\\n", "\u0001", "encrypted_U123"],
      },
      {
        name: "deep nested with unicode",
        value: { level1: { 测试: { level3: { café: "deep value 😀" } } } },
      },
      {
        name: "complex structure with all edge cases",
        value: {
          "unicode-key-测试": {
            "escaped-value": "path\\\\file\nwith\ttabs",
            numbers: [1e-10, -0, 9007199254740992],
            encrypted_U: "encrypted_U123",
            array: ["😀", "\u0001", "\\n"],
          },
        },
      },
    ];

    edgeCases.forEach(({ name, value }) => {
      if (stableStringifyWasm) {
        runComparisonTest(name, value, stableStringifyWasm, "WASM");
      }

      if (stableStringifyNapi) {
        runComparisonTest(name, value, stableStringifyNapi, "NAPI");
      }
    });
  });

  describe("deterministic ordering", () => {
    test("object keys are sorted alphabetically", () => {
      const value = { z: 3, a: 1, m: 2 };
      const tsResult = stableStringifyTS(value);

      if (stableStringifyWasm) {
        const rustResult = stableStringifyWasm(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);
        // Verify keys are sorted: a, m, z
        expect(tsResult).toMatch(/"a":1/);
        expect(tsResult).toMatch(/"m":2/);
        expect(tsResult).toMatch(/"z":3/);
      }

      if (stableStringifyNapi) {
        const rustResult = stableStringifyNapi(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);
      }
    });

    test("nested object keys are sorted", () => {
      const value = {
        c: { z: 1, a: 2 },
        a: { m: 3, b: 4 },
        b: { x: 5, y: 6 },
      };
      const tsResult = stableStringifyTS(value);

      if (stableStringifyWasm) {
        const rustResult = stableStringifyWasm(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);
      }

      if (stableStringifyNapi) {
        const rustResult = stableStringifyNapi(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);
      }
    });
  });

  describe("round-trip validation", () => {
    test("result can be parsed back to original value", () => {
      const value = { a: 1, b: "test", c: [1, 2, 3] };
      const tsResult = stableStringifyTS(value);

      if (stableStringifyWasm) {
        const rustResult = stableStringifyWasm(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);

        // Parse back and compare
        const parsed = JSON.parse(rustResult || "");
        expect(parsed).toEqual(value);
      }

      if (stableStringifyNapi) {
        const rustResult = stableStringifyNapi(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);

        // Parse back and compare
        const parsed = JSON.parse(rustResult || "");
        expect(parsed).toEqual(value);
      }
    });

    test("JSON.stringify on Rust result produces valid JSON string", () => {
      const value = { a: 1, b: "test", c: [1, 2, 3] };
      const tsResult = stableStringifyTS(value);

      if (stableStringifyWasm) {
        const rustResult = stableStringifyWasm(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);

        // Apply JSON.stringify to Rust result
        const rustStringified = JSON.stringify(rustResult);

        // Verify it's a valid JSON string (should be the string representation of the JSON)
        expect(typeof rustStringified).toBe("string");
        expect(() => JSON.parse(rustStringified)).not.toThrow();

        // Parse and verify it matches the original Rust result
        const parsed = JSON.parse(rustStringified);
        expect(parsed).toBe(rustResult);
      }

      if (stableStringifyNapi) {
        const rustResult = stableStringifyNapi(JSON.stringify(value));
        expect(rustResult).toBe(tsResult);

        // Apply JSON.stringify to Rust result
        const rustStringified = JSON.stringify(rustResult);

        // Verify it's a valid JSON string (should be the string representation of the JSON)
        expect(typeof rustStringified).toBe("string");
        expect(() => JSON.parse(rustStringified)).not.toThrow();

        // Parse and verify it matches the original Rust result
        const parsed = JSON.parse(rustStringified);
        expect(parsed).toBe(rustResult);
      }
    });
  });
});
