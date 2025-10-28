import { describe, expect, test } from "vitest";
import { getStorageOptions } from "../storageOptions.js";

describe("getStorageOptions", () => {
  test("should default to indexedDB only when no storage option is provided", () => {
    const result = getStorageOptions();
    expect(result).toEqual({
      useIndexedDB: true,
    });
  });

  test("should enable only indexedDB when 'indexedDB' is provided", () => {
    const result = getStorageOptions("indexedDB");
    expect(result).toEqual({
      useIndexedDB: true,
    });
  });

  test("should enable only indexedDB when array with only indexedDB is provided", () => {
    const result = getStorageOptions(["indexedDB"]);
    expect(result).toEqual({
      useIndexedDB: true,
    });
  });

  // Type checking tests
  test("should handle type checking for StorageConfig", () => {
    // These should compile without type errors
    getStorageOptions("indexedDB");
    getStorageOptions(["indexedDB"]);
    getStorageOptions(undefined);
  });
});
