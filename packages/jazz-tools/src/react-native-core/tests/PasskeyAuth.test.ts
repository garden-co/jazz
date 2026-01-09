// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { Account, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { AuthSecretStorage } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  ReactNativePasskeyAuth,
  setPasskeyModule,
} from "../auth/PasskeyAuth.js";
import {
  base64UrlToUint8Array,
  uint8ArrayToBase64Url,
} from "../auth/passkey-utils.js";

// Create mock functions
const mockCreate = vi.fn();
const mockGet = vi.fn();
const mockIsSupported = vi.fn();

// Create mock passkey module
const mockPasskeyModule = {
  create: mockCreate,
  get: mockGet,
  isSupported: mockIsSupported,
};

KvStoreContext.getInstance().initialize(new InMemoryKVStore());
const authSecretStorage = new AuthSecretStorage();

beforeEach(async () => {
  await authSecretStorage.clear();
  vi.clearAllMocks();

  // Inject the mock module using dependency injection
  setPasskeyModule(mockPasskeyModule);

  await createJazzTestAccount({
    isCurrentActiveAccount: true,
  });
});

describe("ReactNativePasskeyAuth", () => {
  const mockCrypto = {
    randomBytes: (l: number) => crypto.getRandomValues(new Uint8Array(l)),
    newRandomSecretSeed: () => new Uint8Array(32).fill(1),
    agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
  } as any;
  const mockAuthenticate = vi.fn();

  describe("initialization", () => {
    it("should initialize with app name and rpId", () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );
      expect(auth.appName).toBe("Test App");
      expect(auth.rpId).toBe("example.com");
    });

    it("should have static id property", () => {
      expect(ReactNativePasskeyAuth.id).toBe("passkey");
    });
  });

  describe("logIn", () => {
    it("should call Passkey.get with correct parameters", async () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      // Create a mock credential payload (secretSeed + accountID)
      const mockPayload = new Uint8Array(56);
      mockPayload.fill(1, 0, 32); // secretSeed
      mockPayload.fill(2, 32, 56); // accountID hash

      mockGet.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          authenticatorData: "mock-auth-data",
          signature: "mock-signature",
          userHandle: uint8ArrayToBase64Url(mockPayload),
        },
      });

      await auth.logIn();

      expect(mockGet).toHaveBeenCalledWith({
        challenge: expect.any(String),
        rpId: "example.com",
        timeout: 60000,
        userVerification: "preferred",
      });

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.any(String),
        accountSecret: "mock-secret",
      });
    });

    it("should store credentials after successful login", async () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      const mockPayload = new Uint8Array(56);
      mockPayload.fill(1, 0, 32);
      mockPayload.fill(2, 32, 56);

      mockGet.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          authenticatorData: "mock-auth-data",
          signature: "mock-signature",
          userHandle: uint8ArrayToBase64Url(mockPayload),
        },
      });

      await auth.logIn();

      const stored = await authSecretStorage.get();
      expect(stored).toEqual({
        accountID: expect.any(String),
        secretSeed: expect.any(Uint8Array),
        accountSecret: "mock-secret",
        provider: "passkey",
      });
    });

    it("should throw error when passkey authentication fails", async () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      mockGet.mockRejectedValue(new Error("User cancelled"));

      await expect(auth.logIn()).rejects.toThrow(
        "Passkey authentication aborted",
      );
    });

    it("should return early when passkey.get returns null", async () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      mockGet.mockResolvedValue(null);

      await auth.logIn();

      expect(mockAuthenticate).not.toHaveBeenCalled();
    });

    it("should throw error when userHandle is null", async () => {
      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      mockGet.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          authenticatorData: "mock-auth-data",
          signature: "mock-signature",
          userHandle: null,
        },
      });

      await expect(auth.logIn()).rejects.toThrow(
        "Passkey credential is missing userHandle",
      );
    });
  });

  describe("signUp", () => {
    it("should call Passkey.create with correct parameters", async () => {
      // Use the real account from createJazzTestAccount
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      // Set up credentials with the real account ID
      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          attestationObject: "mock-attestation",
        },
      });

      await auth.signUp("testuser");

      expect(mockCreate).toHaveBeenCalledWith({
        challenge: expect.any(String),
        rp: {
          id: "example.com",
          name: "Test App",
        },
        user: {
          id: expect.any(String),
          name: expect.stringContaining("testuser"),
          displayName: "testuser",
        },
        pubKeyCredParams: [
          { alg: -7, type: "public-key" },
          { alg: -257, type: "public-key" },
        ],
        authenticatorSelection: {
          residentKey: "required",
          userVerification: "preferred",
        },
        timeout: 60000,
        attestation: "none",
      });
    });

    it("should update provider to passkey after signup", async () => {
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          attestationObject: "mock-attestation",
        },
      });

      await auth.signUp("testuser");

      const stored = await authSecretStorage.get();
      expect(stored?.provider).toBe("passkey");
    });

    it("should throw error when no credentials exist", async () => {
      await authSecretStorage.clear();

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await expect(auth.signUp("testuser")).rejects.toThrow(
        "Not enough credentials to register the account with passkey",
      );
    });

    it("should throw error when passkey creation fails", async () => {
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockRejectedValue(new Error("User cancelled"));

      await expect(auth.signUp("testuser")).rejects.toThrow(
        "Passkey creation aborted",
      );
    });

    it("should leave profile name unchanged if username is empty", async () => {
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          attestationObject: "mock-attestation",
        },
      });

      await auth.signUp("");

      const currentAccount = await Account.getMe().$jazz.ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      // 'Test Account' is the name provided during account creation
      expect(currentAccount.profile.name).toEqual("Test Account");
    });

    it("should update profile name if username is provided", async () => {
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          attestationObject: "mock-attestation",
        },
      });

      await auth.signUp("testuser");

      const currentAccount = await Account.getMe().$jazz.ensureLoaded({
        resolve: {
          profile: true,
        },
      });

      expect(currentAccount.profile.name).toEqual("testuser");
    });
  });

  describe("credential encoding", () => {
    it("should encode user.id as base64url in create request", async () => {
      const me = await Account.getMe().$jazz.ensureLoaded({ resolve: true });

      const auth = new ReactNativePasskeyAuth(
        mockCrypto,
        mockAuthenticate,
        authSecretStorage,
        "Test App",
        "example.com",
      );

      await authSecretStorage.set({
        accountID: me.$jazz.id,
        secretSeed: new Uint8Array(32).fill(1),
        accountSecret: "mock-secret" as AgentSecret,
        provider: "anonymous",
      });

      mockCreate.mockResolvedValue({
        id: "credential-id",
        rawId: "raw-credential-id",
        type: "public-key",
        response: {
          clientDataJSON: "mock-client-data",
          attestationObject: "mock-attestation",
        },
      });

      await auth.signUp("testuser");

      const createCall = mockCreate.mock.calls[0]![0];
      const userId = createCall.user.id;

      // Should be a valid base64url string (no +, /, or =)
      expect(userId).not.toContain("+");
      expect(userId).not.toContain("/");
      expect(userId).not.toContain("=");

      // Should decode to expected length (secretSeedLength 32 + shortHashLength 19 = 51 bytes)
      const decoded = base64UrlToUint8Array(userId);
      expect(decoded.length).toBe(51);
    });
  });
});
