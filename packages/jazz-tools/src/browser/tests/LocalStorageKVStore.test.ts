// @vitest-environment happy-dom

import { beforeEach, describe, expect, it } from "vitest";
import { LocalStorageKVStore } from "../auth/LocalStorageKVStore";

describe("LocalStorageKVStore", () => {
  let kvStore: LocalStorageKVStore;

  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
    kvStore = new LocalStorageKVStore();
  });

  describe("get", () => {
    it("should return null for non-existent key", async () => {
      const value = await kvStore.get("nonexistent");
      expect(value).toBeNull();
    });

    it("should return stored value for existing key", async () => {
      localStorage.setItem("testKey", "testValue");
      const value = await kvStore.get("testKey");
      expect(value).toBe("testValue");
    });
  });

  describe("set", () => {
    it("should store value in localStorage", async () => {
      await kvStore.set("testKey", "testValue");
      expect(localStorage.getItem("testKey")).toBe("testValue");
    });

    it("should overwrite existing value", async () => {
      localStorage.setItem("testKey", "oldValue");
      await kvStore.set("testKey", "newValue");
      expect(localStorage.getItem("testKey")).toBe("newValue");
    });
  });

  describe("delete", () => {
    it("should remove item from localStorage", async () => {
      localStorage.setItem("testKey", "testValue");
      await kvStore.delete("testKey");
      expect(localStorage.getItem("testKey")).toBeNull();
    });

    it("should not throw when deleting non-existent key", async () => {
      await expect(kvStore.delete("nonexistent")).resolves.not.toThrow();
    });
  });

  describe("clearAll", () => {
    it("should remove all items from localStorage", async () => {
      localStorage.setItem("key1", "value1");
      localStorage.setItem("key2", "value2");

      await kvStore.clearAll();

      expect(localStorage.length).toBe(0);
    });

    it("should work with empty localStorage", async () => {
      await expect(kvStore.clearAll()).resolves.not.toThrow();
    });
  });
});
