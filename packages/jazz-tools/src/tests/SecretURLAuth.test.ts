// @vitest-environment happy-dom

import { AgentSecret, CryptoProvider, bytesToBase64url } from "cojson";
import { beforeEach, describe, expect, test, vi } from "vitest";
import { AuthSecretStorage } from "../auth/AuthSecretStorage.js";
import { SecretURLAuth } from "../auth/SecretURLAuth.js";
import { Account } from "../coValues/account.js";
import {
  AuthenticateAccountFunction,
  InMemoryKVStore,
  KvStoreContext,
} from "../exports.js";
import { ID } from "../internal.js";
import { TestJSCrypto, createJazzTestAccount } from "../testing.js";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("SecretURLAuth", () => {
  let crypto: CryptoProvider;
  let mockAuthenticate: AuthenticateAccountFunction;
  let authSecretStorage: AuthSecretStorage;
  let secretURLAuth: SecretURLAuth;
  let account: Account;

  beforeEach(async () => {
    KvStoreContext.getInstance().getStorage().clearAll();
    crypto = await TestJSCrypto.create();
    mockAuthenticate = vi.fn();
    authSecretStorage = new AuthSecretStorage();

    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    secretURLAuth = new SecretURLAuth(
      crypto as CryptoProvider,
      mockAuthenticate,
      authSecretStorage,
      window.location.origin,
    );
  });

  describe("createAuthTransferURL", () => {
    test("creates a URL", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      await authSecretStorage.set({
        accountID: "test-account" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed,
        provider: "anonymous",
      });

      const { url, transfer } = await secretURLAuth.createAuthTransferURL();

      expect(url).toBeDefined();
      expect(transfer.status).toBe("init");
      expect(transfer.acceptedBy).toBeUndefined();
      expect(transfer.secret).toBe(bytesToBase64url(secretSeed));
    });

    // TODO: More tests
  });

  describe("logIn", () => {
    test("does something", async () => {
      const secretSeed = crypto.newRandomSecretSeed();
      await authSecretStorage.set({
        accountID: "test-account" as ID<Account>,
        accountSecret: "test-secret" as AgentSecret,
        secretSeed,
        provider: "anonymous",
      });

      const { transfer } = await secretURLAuth.createAuthTransferURL();

      expect(transfer.status).toBe("init");

      const result = await secretURLAuth.logIn(transfer.id);

      expect(result.status).toBe("authorized");
      expect(mockAuthenticate).toHaveBeenCalled();
    });

    // TODO: More tests
  });
});
