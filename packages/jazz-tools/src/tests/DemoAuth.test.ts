import { AgentSecret } from "cojson";
import { beforeEach, describe, expect, test, vi } from "vitest";
import { AuthSecretStorage } from "../auth/AuthSecretStorage";
import { DemoAuth } from "../auth/DemoAuth";
import { InMemoryKVStore } from "../auth/InMemoryKVStore";
import { KvStoreContext } from "../auth/KvStoreContext";
import { Account } from "../exports";
import { ID } from "../internal";
import { createJazzTestAccount } from "../testing";

// Initialize KV store for tests
const kvStore = new InMemoryKVStore();
KvStoreContext.getInstance().initialize(kvStore);

describe("DemoAuth", () => {
  let mockAuthenticate: any;
  let authSecretStorage: AuthSecretStorage;
  let demoAuth: DemoAuth;

  beforeEach(async () => {
    // Reset mock authenticate
    mockAuthenticate = vi.fn();

    // Clear KV store
    kvStore.clearAll();

    // Create new AuthSecretStorage instance
    authSecretStorage = new AuthSecretStorage();

    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Create DemoAuth instance
    demoAuth = new DemoAuth(mockAuthenticate, authSecretStorage);
  });

  describe("logIn", () => {
    test("should successfully log in existing user", async () => {
      const testUser = "testUser";

      const credentials = {
        accountID: "new-account-id" as ID<Account>,
        accountSecret: "new-secret" as AgentSecret,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "anonymous",
      };

      await authSecretStorage.set(credentials);

      await demoAuth.signUp(testUser);

      await demoAuth.logIn(testUser);

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: credentials.accountID,
        accountSecret: credentials.accountSecret,
      });

      const storedData = await authSecretStorage.get();
      expect(storedData).toEqual({
        accountID: credentials.accountID,
        accountSecret: credentials.accountSecret,
        secretSeed: credentials.secretSeed,
        provider: "demo",
      });
    });

    test("should throw error for non-existent user", async () => {
      await expect(demoAuth.logIn("nonexistentUser")).rejects.toThrow(
        "User not found",
      );
    });
  });

  describe("signUp", () => {
    test("should successfully sign up new user", async () => {
      const testUser = "newUser";
      const credentials = {
        accountID: "new-account-id" as ID<Account>,
        accountSecret: "new-secret" as AgentSecret,
        secretSeed: new Uint8Array([1, 2, 3]),
      };

      await authSecretStorage.set({
        ...credentials,
        provider: "anonymous",
      });

      await demoAuth.signUp(testUser);

      const storedData = await authSecretStorage.get();
      expect(storedData).toEqual({
        ...credentials,
        provider: "demo",
      });
    });

    test("should throw error for existing username", async () => {
      const testUser = "existingUser";
      await kvStore.set("demo-auth-storage-version", "3");
      await kvStore.set(
        "demo-auth-users",
        JSON.stringify({
          [testUser]: {
            accountID: "existing-account-id" as ID<Account>,
            accountSecret: "existing-secret" as AgentSecret,
            secretSeed: [1, 2, 3],
          },
        }),
      );

      await expect(demoAuth.signUp(testUser)).rejects.toThrow(
        "User already registered",
      );
    });

    test("should throw error when no credentials found", async () => {
      await expect(demoAuth.signUp("newUser")).rejects.toThrow(
        "No credentials found",
      );
    });
  });

  describe("DemoAuth Storage Migration", () => {
    test("should migrate from version 1 to version 3", async () => {
      // Set up version 1 data
      const testUser = "test@example.com";
      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: [1, 2, 3],
      };

      // Store data in old format
      await kvStore.set("demo-auth-existing-users", testUser);
      await kvStore.set(
        `demo-auth-existing-users-${testUser}`,
        JSON.stringify(storageData),
      );

      // Trigger migration by getting existing users
      await demoAuth.getExistingUsers();

      // Verify migration
      const version = await kvStore.get("demo-auth-storage-version");
      expect(version).toBe("3");

      // Old key should be deleted
      const oldData = await kvStore.get(`demo-auth-existing-users-${testUser}`);
      expect(oldData).toBeNull();

      // New encoded key should exist
      const newData = await kvStore.get("demo-auth-users");
      expect(JSON.parse(newData!)).toEqual({
        [testUser]: storageData,
      });
    });

    test("should migrate from version 2 to version 3 (new storage format)", async () => {
      // Set up version 2 data
      const testUser = "test@example.com";
      const encodedUsername = btoa(testUser)
        .replace(/=/g, "-")
        .replace(/\+/g, "_")
        .replace(/\//g, ".");

      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: [1, 2, 3],
      };

      // Store data in version 2 format
      await kvStore.set("demo-auth-storage-version", "2");
      await kvStore.set("demo-auth-existing-users", testUser);
      await kvStore.set(
        `demo-auth-existing-users-${encodedUsername}`,
        JSON.stringify(storageData),
      );

      // Trigger migration by getting existing users
      await demoAuth.getExistingUsers();

      // Verify migration
      const version = await kvStore.get("demo-auth-storage-version");
      expect(version).toBe("3");

      // Old keys should be deleted
      const oldListData = await kvStore.get("demo-auth-existing-users");
      expect(oldListData).toBeNull();

      const oldUserData = await kvStore.get(
        `demo-auth-existing-users-${encodedUsername}`,
      );
      expect(oldUserData).toBeNull();

      // New storage format should be used
      const newData = await kvStore.get("demo-auth-users");
      expect(JSON.parse(newData!)).toEqual({
        [testUser]: storageData,
      });
    });

    test("should handle new users management after migration", async () => {
      // Add a test user
      const testUser = "newuser";
      await authSecretStorage.set({
        accountID: "test-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "anonymous",
      });

      // Sign up new user
      await demoAuth.signUp(testUser);

      // Verify user is stored in new format
      const usersData = await kvStore.get("demo-auth-users");
      const users = JSON.parse(usersData!);

      expect(Object.keys(users)).toContain(testUser);
      expect(users[testUser]).toEqual({
        accountID: "test-id",
        accountSecret: "test-secret",
        secretSeed: [1, 2, 3],
      });

      // Verify we can get existing users
      const existingUsers = await demoAuth.getExistingUsers();
      expect(existingUsers).toEqual([testUser]);

      // Verify we can log in with the stored user
      await demoAuth.logIn(testUser);
      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toEqual({
        accountID: "test-id",
        accountSecret: "test-secret",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "demo",
      });
    });

    test("should handle multiple users in new storage format", async () => {
      // Set up initial auth data
      await authSecretStorage.set({
        accountID: "test-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "anonymous",
      });

      // Add multiple users
      const users = ["user1", "user2", "user3"];
      for (const user of users) {
        await demoAuth.signUp(user);
      }

      // Verify all users are stored
      const existingUsers = await demoAuth.getExistingUsers();
      expect(existingUsers.sort()).toEqual(users.sort());

      // Verify we can't add duplicate users
      await expect(demoAuth.signUp("user1")).rejects.toThrow(
        "User already registered",
      );
    });
  });
});
