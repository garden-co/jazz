// @vitest-environment happy-dom

import { AgentSecret, CryptoProvider, bytesToBase64url } from "cojson";
import {
  Account,
  AuthSecretStorage,
  ID,
  InMemoryKVStore,
  KvStoreContext,
} from "jazz-tools";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  SecretURLAuth,
  createAuthURL,
  parseAuthURL,
} from "../auth/SecretURLAuth";
import { TestJSCrypto } from "../testing";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("SecretURLAuth", () => {
  let crypto: CryptoProvider;
  let mockAuthenticate: any;
  let authSecretStorage: AuthSecretStorage;
  let secretURLAuth: SecretURLAuth;

  beforeEach(async () => {
    KvStoreContext.getInstance().getStorage().clearAll();
    crypto = await TestJSCrypto.create();
    mockAuthenticate = vi.fn();
    authSecretStorage = new AuthSecretStorage();

    secretURLAuth = new SecretURLAuth(
      crypto as CryptoProvider,
      mockAuthenticate,
      authSecretStorage,
    );
  });

  describe("logIn", () => {
    test("success with valid secret URL", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      const validURL = createAuthURL(bytesToBase64url(secretSeed));

      await secretURLAuth.logIn(validURL);

      expect(mockAuthenticate).toHaveBeenCalled();
    });

    test("fail with expired URL", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      const expiredURL = createAuthURL(
        bytesToBase64url(secretSeed),
        Date.now() - 1000,
      );

      await expect(secretURLAuth.logIn(expiredURL)).rejects.toThrow("expired");
    });

    test("fail with tampered URL", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      const validURL = createAuthURL(bytesToBase64url(secretSeed));
      const tamperedURL = validURL + "tampered";

      await expect(secretURLAuth.logIn(tamperedURL)).rejects.toThrow("Invalid");
    });
  });

  describe("createPairingURL", () => {
    test("generate valid URL with expiration", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      await authSecretStorage.set({
        accountID: "test-account" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed,
        provider: "anonymous",
      });

      const url = await secretURLAuth.createPairingURL();
      const parsed = parseAuthURL(url);

      expect(parsed?.expiresAt).toBeGreaterThan(Date.now());
      expect(parsed?.secret).toBe(bytesToBase64url(secretSeed));
    });

    test("fail when no credentials", async () => {
      await expect(secretURLAuth.createPairingURL()).rejects.toThrow(
        "No existing authentication found",
      );
    });
  });
});
