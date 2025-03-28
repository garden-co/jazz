// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { assert, beforeEach, describe, expect, it, vi } from "vitest";
import { PassphraseAuth } from "../auth/PassphraseAuth";
import {
  Account,
  AuthSecretStorage,
  ID,
  InMemoryKVStore,
  KvStoreContext,
} from "../exports";
import {
  TestJazzContextManager,
  createJazzTestAccount,
  setupJazzTestSync,
} from "../testing";
import { testWordlist } from "./fixtures";

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("PassphraseAuth", () => {
  let crypto: PureJSCrypto;
  let mockAuthenticate: any;
  let mockRegister: any;
  let authSecretStorage: AuthSecretStorage;
  let passphraseAuth: PassphraseAuth;
  let account: Account;

  beforeEach(async () => {
    // Reset storage
    KvStoreContext.getInstance().getStorage().clearAll();

    // Set up crypto and mocks
    crypto = await PureJSCrypto.create();
    mockAuthenticate = vi.fn();
    mockRegister = vi.fn();
    authSecretStorage = new AuthSecretStorage();

    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Create PassphraseAuth instance
    passphraseAuth = new PassphraseAuth(
      crypto,
      mockAuthenticate,
      mockRegister,
      authSecretStorage,
      testWordlist,
    );
  });

  describe("logIn", () => {
    it("should successfully log in with valid passphrase", async () => {
      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: new Uint8Array([
          173, 58, 235, 40, 67, 188, 236, 11, 107, 237, 97, 23, 182, 49, 188,
          63, 237, 52, 27, 84, 142, 66, 244, 149, 243, 114, 203, 164, 115, 239,
          175, 194,
        ]),
        provider: "anonymous",
      };

      await authSecretStorage.set(storageData);

      // Generate a valid passphrase
      const passphrase = await passphraseAuth.getCurrentAccountPassphrase();

      await passphraseAuth.logIn(passphrase);

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.any(String),
        accountSecret: expect.any(String),
      });

      const storedData = await authSecretStorage.get();
      expect(storedData).toEqual({
        accountID: expect.any(String),
        accountSecret: expect.any(String),
        secretSeed: storageData.secretSeed,
        provider: "passphrase",
      });
    });

    it("should throw error with invalid passphrase", async () => {
      await expect(passphraseAuth.logIn("invalid words here")).rejects.toThrow(
        "Invalid passphrase",
      );
    });
  });

  describe("signUp", () => {
    it("should successfully sign up new user", async () => {
      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: new Uint8Array([
          173, 58, 235, 40, 67, 188, 236, 11, 107, 237, 97, 23, 182, 49, 188,
          63, 237, 52, 27, 84, 142, 66, 244, 149, 243, 114, 203, 164, 115, 239,
          175, 194,
        ]),
        provider: "anonymous",
      };

      await authSecretStorage.set(storageData);

      const passphrase = await passphraseAuth.signUp();

      const storedData = await authSecretStorage.get();
      expect(storedData).toEqual({
        accountID: storageData.accountID,
        accountSecret: storageData.accountSecret,
        secretSeed: storageData.secretSeed,
        provider: "passphrase",
      });
      expect(passphrase).toMatchInlineSnapshot(
        `"pudding struggle skate manual solution aisle quick promote bless ranch humor lemon spy asset fall sign virus question syrup nuclear elbow water sample garden"`,
      );
    });

    it("should throw error when no credentials found", async () => {
      await expect(passphraseAuth.signUp()).rejects.toThrow(
        "No credentials found",
      );
    });

    it("should set account name when provided during signup", async () => {
      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: new Uint8Array([
          173, 58, 235, 40, 67, 188, 236, 11, 107, 237, 97, 23, 182, 49, 188,
          63, 237, 52, 27, 84, 142, 66, 244, 149, 243, 114, 203, 164, 115, 239,
          175, 194,
        ]),
        provider: "anonymous",
      };

      await authSecretStorage.set(storageData);

      const testName = "Test User";
      await passphraseAuth.signUp(testName);

      // Verify the account name was set
      const { profile } = await account.ensureLoaded({
        resolve: {
          profile: true,
        },
      });
      expect(profile.name).toBe(testName);

      // Verify storage was updated correctly
      const storedData = await authSecretStorage.get();
      expect(storedData).toEqual({
        accountID: storageData.accountID,
        accountSecret: storageData.accountSecret,
        secretSeed: storageData.secretSeed,
        provider: "passphrase",
      });
    });
  });

  describe("getCurrentAccountPassphrase", () => {
    it("should return current user passphrase when credentials exist", async () => {
      const storageData = {
        accountID: "test-account-id" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed: crypto.newRandomSecretSeed(),
        provider: "anonymous",
      };

      await authSecretStorage.set(storageData);

      // First sign up to create valid credentials
      const originalPassphrase = await passphraseAuth.signUp();

      // Then get the current passphrase
      const retrievedPassphrase =
        await passphraseAuth.getCurrentAccountPassphrase();
      expect(retrievedPassphrase).toBe(originalPassphrase);
    });

    it("should throw error when no credentials found", async () => {
      await expect(
        passphraseAuth.getCurrentAccountPassphrase(),
      ).rejects.toThrow("No credentials found");
    });
  });
});

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("PassphraseAuth with TestJazzContextManager", () => {
  let crypto: PureJSCrypto;
  let contextManager: TestJazzContextManager<any>;
  let authSecretStorage: AuthSecretStorage;
  let passphraseAuth: PassphraseAuth;

  beforeEach(async () => {
    // Reset storage
    KvStoreContext.getInstance().getStorage().clearAll();

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Set up crypto and context manager
    crypto = await PureJSCrypto.create();
    contextManager = TestJazzContextManager.fromAccountOrGuest(account);
    authSecretStorage = contextManager.getAuthSecretStorage();

    // Create initial context
    await contextManager.createContext({});

    // Create PassphraseAuth instance
    passphraseAuth = new PassphraseAuth(
      crypto,
      contextManager.authenticate,
      contextManager.register,
      authSecretStorage,
      testWordlist,
    );
  });

  describe("logIn", () => {
    it("should successfully log in with valid passphrase", async () => {
      // First sign up to create initial credentials
      const passphrase = await passphraseAuth.signUp();

      // Log out
      await contextManager.logOut();

      // Log back in with passphrase
      await passphraseAuth.logIn(passphrase);

      // Verify we're logged in
      const context = contextManager.getCurrentValue();

      assert(context && "me" in context);

      // Verify storage was updated
      const storedData = await authSecretStorage.get();
      expect(storedData?.provider).toBe("passphrase");
    });

    it("should throw error with invalid passphrase", async () => {
      await expect(passphraseAuth.logIn("invalid words here")).rejects.toThrow(
        "Invalid passphrase",
      );
    });
  });

  describe("signUp", () => {
    it("should successfully sign up new user", async () => {
      expect(authSecretStorage.isAuthenticated).toBe(false);

      const passphrase = await passphraseAuth.signUp();

      expect(authSecretStorage.isAuthenticated).toBe(true);

      // Verify passphrase format
      expect(passphrase.split(" ").length).toBeGreaterThan(0);

      // Verify storage was updated
      const storedData = await authSecretStorage.get();
      expect(storedData?.provider).toBe("passphrase");

      // Verify we can log in with the passphrase
      await contextManager.logOut();
      await passphraseAuth.logIn(passphrase);
      const context = contextManager.getCurrentValue();
      assert(context && "me" in context);
      expect(context.me).toBeDefined();
    });

    it("should throw error when no credentials found", async () => {
      await authSecretStorage.clear();
      await expect(passphraseAuth.signUp()).rejects.toThrow(
        "No credentials found",
      );
    });
  });

  describe("registerNewAccount", () => {
    it("should successfully register new account with passphrase", async () => {
      expect(authSecretStorage.isAuthenticated).toBe(false);

      const passphrase = passphraseAuth.generateRandomPassphrase();
      const accountId = await passphraseAuth.registerNewAccount(
        passphrase,
        "Test User",
      );

      // Verify account was created
      expect(accountId).toBeDefined();

      await contextManager
        .getCurrentValue()
        ?.node.syncManager.waitForAllCoValuesSync();

      // Verify we can log in with the passphrase
      await contextManager.logOut();
      await passphraseAuth.logIn(passphrase);

      const context = contextManager.getCurrentValue();

      assert(context && "me" in context);
      expect(context.me.id).toBe(accountId);
      expect(context.me.profile?.name).toBe("Test User");

      expect(authSecretStorage.isAuthenticated).toBe(true);

      const credentials = await authSecretStorage.get();
      assert(credentials);
      expect(credentials.accountID).toBe(accountId);
      expect(credentials.provider).toBe("passphrase");
    });

    it("should throw error with invalid passphrase during registration", async () => {
      await expect(
        passphraseAuth.registerNewAccount("invalid words", "Test User"),
      ).rejects.toThrow();
    });
  });

  describe("getCurrentAccountPassphrase", () => {
    it("should return current user passphrase when credentials exist", async () => {
      const originalPassphrase = await passphraseAuth.signUp();
      const retrievedPassphrase =
        await passphraseAuth.getCurrentAccountPassphrase();

      expect(retrievedPassphrase).toBe(originalPassphrase);
    });

    it("should throw error when no credentials found", async () => {
      await authSecretStorage.clear();
      await expect(
        passphraseAuth.getCurrentAccountPassphrase(),
      ).rejects.toThrow("No credentials found");
    });
  });
});
