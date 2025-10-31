import { beforeEach, describe, expect, it, afterEach } from "vitest";
import { EncryptedLocalStorageKVStore } from "jazz-tools/browser";

describe("EncryptedLocalStorageKVStore", () => {
  let kvStore: EncryptedLocalStorageKVStore;

  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
  });

  afterEach(async () => {
    // Clean up encryption key and localStorage after each test
    if (kvStore) {
      await kvStore.clearAll();
      try {
        await kvStore.deleteEncryptionKey();
      } catch (e) {
        // Key might not exist, that's okay
      }
    }
  });

  describe("initialization", () => {
    it("should initialize successfully", async () => {
      kvStore = new EncryptedLocalStorageKVStore();
      // Wait for key initialization by performing an operation
      await kvStore.set("test", "value");
      expect(await kvStore.get("test")).toBe("value");
    });

    it("should generate a new encryption key on first use", async () => {
      kvStore = new EncryptedLocalStorageKVStore();
      await kvStore.set("key1", "value1");
      const retrieved = await kvStore.get("key1");
      expect(retrieved).toBe("value1");
    });

    it("should reuse existing encryption key across instances", async () => {
      const store1 = new EncryptedLocalStorageKVStore();
      await store1.set("key1", "value1");

      // Create a new instance which should use the same encryption key
      const store2 = new EncryptedLocalStorageKVStore();
      const retrieved = await store2.get("key1");
      expect(retrieved).toBe("value1");

      await store1.deleteEncryptionKey();
    });

    it("should use custom prefix when provided", async () => {
      kvStore = new EncryptedLocalStorageKVStore("custom-prefix:");
      await kvStore.set("key1", "value1");

      // Check that the key in localStorage has the custom prefix
      let foundPrefixedKey = false;
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key?.startsWith("custom-prefix:")) {
          foundPrefixedKey = true;
          break;
        }
      }
      expect(foundPrefixedKey).toBe(true);
    });
  });

  describe("get", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should return null for non-existent key", async () => {
      const value = await kvStore.get("nonexistent");
      expect(value).toBeNull();
    });

    it("should return decrypted value for existing key", async () => {
      await kvStore.set("testKey", "testValue");
      const value = await kvStore.get("testKey");
      expect(value).toBe("testValue");
    });

    it("should handle special characters in values", async () => {
      const specialValue = "Hello 🎉 Special: @#$%^&*()_+{}[]|\\:;\"'<>,.?/~`";
      await kvStore.set("specialKey", specialValue);
      const retrieved = await kvStore.get("specialKey");
      expect(retrieved).toBe(specialValue);
    });

    it("should handle unicode characters", async () => {
      const unicodeValue = "Hello 世界 🌍 Здравствуй مرحبا";
      await kvStore.set("unicodeKey", unicodeValue);
      const retrieved = await kvStore.get("unicodeKey");
      expect(retrieved).toBe(unicodeValue);
    });

    it("should handle empty string values", async () => {
      await kvStore.set("emptyKey", "");
      const retrieved = await kvStore.get("emptyKey");
      expect(retrieved).toBe("");
    });
  });

  describe("set", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should store encrypted value in localStorage", async () => {
      const plainValue = "testValue";
      await kvStore.set("testKey", plainValue);

      // The value in localStorage should be encrypted (not equal to plain text)
      const storedValue = localStorage.getItem("jazz-encrypted:testKey");
      expect(storedValue).not.toBeNull();
      expect(storedValue).not.toBe(plainValue);
    });

    it("should overwrite existing value", async () => {
      await kvStore.set("testKey", "oldValue");
      await kvStore.set("testKey", "newValue");
      const retrieved = await kvStore.get("testKey");
      expect(retrieved).toBe("newValue");
    });

    it("should create different ciphertexts for same value (due to random IV)", async () => {
      await kvStore.set("key1", "sameValue");
      await kvStore.set("key2", "sameValue");

      const encrypted1 = localStorage.getItem("jazz-encrypted:key1");
      const encrypted2 = localStorage.getItem("jazz-encrypted:key2");

      expect(encrypted1).not.toBeNull();
      expect(encrypted2).not.toBeNull();
      expect(encrypted1).not.toBe(encrypted2);
    });

    it("should handle multiple consecutive sets", async () => {
      for (let i = 0; i < 10; i++) {
        await kvStore.set(`key${i}`, `value${i}`);
      }

      for (let i = 0; i < 10; i++) {
        const value = await kvStore.get(`key${i}`);
        expect(value).toBe(`value${i}`);
      }
    });
  });

  describe("delete", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should remove item from storage", async () => {
      await kvStore.set("testKey", "testValue");
      await kvStore.delete("testKey");
      const value = await kvStore.get("testKey");
      expect(value).toBeNull();
    });

    it("should not throw when deleting non-existent key", async () => {
      await expect(kvStore.delete("nonexistent")).resolves.not.toThrow();
    });

    it("should remove encrypted data from localStorage", async () => {
      await kvStore.set("testKey", "testValue");
      await kvStore.delete("testKey");
      const storedValue = localStorage.getItem("jazz-encrypted:testKey");
      expect(storedValue).toBeNull();
    });
  });

  describe("clearAll", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should remove all encrypted items from storage", async () => {
      await kvStore.set("key1", "value1");
      await kvStore.set("key2", "value2");
      await kvStore.set("key3", "value3");

      await kvStore.clearAll();

      expect(await kvStore.get("key1")).toBeNull();
      expect(await kvStore.get("key2")).toBeNull();
      expect(await kvStore.get("key3")).toBeNull();
    });

    it("should only remove items with the correct prefix", async () => {
      await kvStore.set("key1", "value1");
      // Manually add an item with a different prefix
      localStorage.setItem("other-prefix:key2", "value2");

      await kvStore.clearAll();

      expect(await kvStore.get("key1")).toBeNull();
      expect(localStorage.getItem("other-prefix:key2")).toBe("value2");

      // Clean up
      localStorage.removeItem("other-prefix:key2");
    });

    it("should work with empty storage", async () => {
      await expect(kvStore.clearAll()).resolves.not.toThrow();
    });
  });

  describe("encryption security", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should store encrypted data that is not readable as plain text", async () => {
      const secretValue = "my-secret-password-123";
      await kvStore.set("secret", secretValue);

      const encryptedValue = localStorage.getItem("jazz-encrypted:secret");
      expect(encryptedValue).not.toBeNull();
      expect(encryptedValue).not.toContain(secretValue);
      expect(encryptedValue).not.toContain("password");
    });

    it("should produce different encrypted outputs for same value set twice", async () => {
      await kvStore.set("key1", "sameValue");
      const encrypted1 = localStorage.getItem("jazz-encrypted:key1");

      await kvStore.set("key1", "sameValue");
      const encrypted2 = localStorage.getItem("jazz-encrypted:key1");

      // Different IVs should produce different ciphertexts
      expect(encrypted1).not.toBe(encrypted2);
    });

    it("should fail to decrypt if encrypted data is tampered with", async () => {
      await kvStore.set("key1", "value1");

      // Tamper with the encrypted data
      const tamperedValue = "invalidbase64data!!!";
      localStorage.setItem("jazz-encrypted:key1", tamperedValue);

      const result = await kvStore.get("key1");
      expect(result).toBeNull(); // Should return null on decryption failure
    });
  });

  describe("deleteEncryptionKey", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should delete the encryption key from IndexedDB", async () => {
      await kvStore.set("key1", "value1");
      await kvStore.deleteEncryptionKey();

      // After deleting the key, we can't decrypt existing data
      // Create a new instance which will generate a new key
      const newStore = new EncryptedLocalStorageKVStore();
      const result = await newStore.get("key1");

      // The data is still in localStorage but can't be decrypted
      expect(result).toBeNull();

      await newStore.deleteEncryptionKey();
    });

    it("should allow setting new data after key deletion", async () => {
      await kvStore.set("key1", "value1");
      await kvStore.deleteEncryptionKey();

      // Create new instance with new key
      const newStore = new EncryptedLocalStorageKVStore();
      await newStore.set("key2", "value2");
      const retrieved = await newStore.get("key2");

      expect(retrieved).toBe("value2");

      await newStore.deleteEncryptionKey();
    });
  });

  describe("concurrent operations", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should handle multiple simultaneous set operations", async () => {
      await Promise.all([
        kvStore.set("key1", "value1"),
        kvStore.set("key1", "value2"),
        kvStore.set("key1", "value3"),
      ]);
      const value = await kvStore.get("key1");

      expect(value).toBe("value3");
    });
  });

  describe("plaintext migration", () => {
    beforeEach(() => {
      kvStore = new EncryptedLocalStorageKVStore();
    });

    it("should read plaintext value from non-prefixed key", async () => {
      // Manually set a plaintext value without prefix (simulating old storage)
      localStorage.setItem("oldKey", "plainTextValue");

      const retrieved = await kvStore.get("oldKey");
      expect(retrieved).toBe("plainTextValue");
    });

    it("should migrate plaintext value to encrypted storage", async () => {
      // Set plaintext value
      localStorage.setItem("migrateKey", "plaintextValue");

      // First get should trigger migration
      await kvStore.get("migrateKey");

      // Check that encrypted version exists with prefix
      const encryptedValue = localStorage.getItem("jazz-encrypted:migrateKey");
      expect(encryptedValue).not.toBeNull();
      expect(encryptedValue).not.toBe("plaintextValue");
    });

    it("should delete old plaintext value after migration", async () => {
      // Set plaintext value
      localStorage.setItem("deleteKey", "plaintextValue");

      // Trigger migration
      await kvStore.get("deleteKey");

      // Old plaintext value should be removed
      const oldValue = localStorage.getItem("deleteKey");
      expect(oldValue).toBeNull();
    });

    it("should use encrypted value on subsequent reads after migration", async () => {
      // Set plaintext value
      localStorage.setItem("subsequentKey", "plaintextValue");

      // First read triggers migration
      const firstRead = await kvStore.get("subsequentKey");
      expect(firstRead).toBe("plaintextValue");

      // Verify plaintext is gone
      expect(localStorage.getItem("subsequentKey")).toBeNull();

      // Second read should use encrypted version
      const secondRead = await kvStore.get("subsequentKey");
      expect(secondRead).toBe("plaintextValue");
    });

    it("should prefer encrypted value over plaintext if both exist", async () => {
      // Set plaintext value
      localStorage.setItem("bothKey", "plaintextValue");

      // Set encrypted value (should take precedence)
      await kvStore.set("bothKey", "encryptedValue");

      // Should get encrypted value, not plaintext
      const retrieved = await kvStore.get("bothKey");
      expect(retrieved).toBe("encryptedValue");

      // Plaintext should still exist (not migrated since encrypted took precedence)
      expect(localStorage.getItem("bothKey")).toBe("plaintextValue");
    });

    it("should handle migration of values with special characters", async () => {
      const specialValue = "Hello 🎉 Special: @#$%^&*()_+{}[]|\\:;\"'<>,.?/~`";
      localStorage.setItem("specialMigrateKey", specialValue);

      const retrieved = await kvStore.get("specialMigrateKey");
      expect(retrieved).toBe(specialValue);

      // Should be migrated
      expect(localStorage.getItem("specialMigrateKey")).toBeNull();
      expect(
        localStorage.getItem("jazz-encrypted:specialMigrateKey"),
      ).not.toBeNull();
    });

    it("should handle migration of unicode values", async () => {
      const unicodeValue = "Hello 世界 🌍 Здравствуй مرحبا";
      localStorage.setItem("unicodeMigrateKey", unicodeValue);

      const retrieved = await kvStore.get("unicodeMigrateKey");
      expect(retrieved).toBe(unicodeValue);

      // Should be migrated
      expect(localStorage.getItem("unicodeMigrateKey")).toBeNull();
      expect(
        localStorage.getItem("jazz-encrypted:unicodeMigrateKey"),
      ).not.toBeNull();
    });

    it("should handle migration of JSON string values", async () => {
      const jsonValue = JSON.stringify({ key: "value", nested: { data: 123 } });
      localStorage.setItem("jsonMigrateKey", jsonValue);

      const retrieved = await kvStore.get("jsonMigrateKey");
      expect(retrieved).toBe(jsonValue);
      expect(JSON.parse(retrieved!)).toEqual({
        key: "value",
        nested: { data: 123 },
      });

      // Should be migrated
      expect(localStorage.getItem("jsonMigrateKey")).toBeNull();
    });

    it("should handle migration with custom prefix", async () => {
      const customStore = new EncryptedLocalStorageKVStore("custom:");
      localStorage.setItem("customKey", "plaintextValue");

      const retrieved = await customStore.get("customKey");
      expect(retrieved).toBe("plaintextValue");

      // Should be migrated with custom prefix
      expect(localStorage.getItem("custom:customKey")).not.toBeNull();
      expect(localStorage.getItem("customKey")).toBeNull();

      await customStore.clearAll();
      await customStore.deleteEncryptionKey();
    });

    it("should handle multiple migrations in parallel", async () => {
      // Set multiple plaintext values
      localStorage.setItem("parallel1", "value1");
      localStorage.setItem("parallel2", "value2");
      localStorage.setItem("parallel3", "value3");

      // Trigger migrations in parallel
      const [val1, val2, val3] = await Promise.all([
        kvStore.get("parallel1"),
        kvStore.get("parallel2"),
        kvStore.get("parallel3"),
      ]);

      expect(val1).toBe("value1");
      expect(val2).toBe("value2");
      expect(val3).toBe("value3");

      // All should be migrated
      expect(localStorage.getItem("parallel1")).toBeNull();
      expect(localStorage.getItem("parallel2")).toBeNull();
      expect(localStorage.getItem("parallel3")).toBeNull();
      expect(localStorage.getItem("jazz-encrypted:parallel1")).not.toBeNull();
      expect(localStorage.getItem("jazz-encrypted:parallel2")).not.toBeNull();
      expect(localStorage.getItem("jazz-encrypted:parallel3")).not.toBeNull();
    });
  });
});
