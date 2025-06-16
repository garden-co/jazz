// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { Account, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { ID } from "jazz-tools";
import { AuthSecretStorage } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { BrowserPasskeyAuth } from "../auth/PasskeyAuth";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());
const authSecretStorage = new AuthSecretStorage();

beforeEach(async () => {
  await authSecretStorage.clear();

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("BrowserPasskeyAuth", () => {
  let mockNavigator: any;
  const mockCrypto = {
    randomBytes: (l: number) => crypto.getRandomValues(new Uint8Array(l)),
    newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
    agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
  } as any;
  const mockAuthenticate = vi.fn();

  beforeEach(() => {
    mockNavigator = {
      credentials: {
        create: vi.fn(),
        get: vi.fn(),
      },
    };
    global.navigator = mockNavigator;
    mockAuthenticate.mockReset();
  });

  describe("initialization", () => {
    it("should initialize with default hostname", () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );
      expect(auth.appName).toBe("Test App");
      expect(auth.appHostname).toBe(window.location.hostname);
    });

    it("should initialize with custom hostname", () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "custom.host",
      );
      expect(auth.appHostname).toBe("custom.host");
    });
  });

  describe("authentication flows", () => {
    it("should handle login flow", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.get.mockResolvedValue({
        response: {
          userHandle: new ArrayBuffer(32), // Mocked credential payload
        },
      });

      await auth.logIn();

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.any(String),
        accountSecret: "mock-secret",
      });

      expect(await authSecretStorage.get()).toEqual({
        accountID: expect.any(String),
        secretSeed: expect.any(Uint8Array),
        provider: "passkey",
        accountSecret: "mock-secret",
      });
    });

    it("should handle signup flow", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.create.mockResolvedValue({
        type: "public-key",
        id: new Uint8Array([1, 2, 3, 4]),
      });

      // Set up existing credentials in storage
      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      await auth.signUp("testuser");

      expect(mockNavigator.credentials.create).toHaveBeenCalledWith({
        publicKey: expect.objectContaining({
          rp: {
            name: "Test App",
            id: window.location.hostname,
          },
          user: expect.objectContaining({
            displayName: "testuser",
          }),
        }),
      });

      expect(await authSecretStorage.get()).toEqual({
        accountID: "test123",
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret",
        provider: "passkey",
      });
    });

    it("should handle passkey errors during login", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.get.mockRejectedValue(
        new Error("User rejected the passkey"),
      );

      await expect(auth.logIn()).rejects.toThrow("Passkey creation aborted");
    });

    it("should handle passkey errors during signup", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.create.mockRejectedValue(
        new Error("User rejected the passkey"),
      );

      // Set up existing credentials in storage
      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      await expect(auth.signUp("testuser")).rejects.toThrow(
        "Passkey creation aborted",
      );
    });

    it("should leave the account profile name unchanged if username is empty", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.create.mockResolvedValue({
        type: "public-key",
        id: new Uint8Array([1, 2, 3, 4]),
      });

      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      await auth.signUp("");

      const currentAccount = await Account.getMe().ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      // 'Test Account' is the name provided during account creation (see: `jazz-tools/src/testing.ts`)
      expect(currentAccount.profile.name).toEqual("Test Account");
    });

    it("should update the account profile name if username is provided", async () => {
      const auth = new BrowserPasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
      );

      mockNavigator.credentials.create.mockResolvedValue({
        type: "public-key",
        id: new Uint8Array([1, 2, 3, 4]),
      });

      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      await auth.signUp("testuser");

      const currentAccount = await Account.getMe().ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      expect(currentAccount.profile.name).toEqual("testuser");
    });
  });
});
