// @vitest-environment happy-dom

import { bytesToBase64url } from "cojson";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MagicLinkAuth } from "../auth/MagicLinkAuth";
import {
  Account,
  AuthSecretStorage,
  InMemoryKVStore,
  KvStoreContext,
} from "../exports";
import { createJazzTestAccount, setupJazzTestSync } from "../testing";

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

beforeEach(async () => {
  await setupJazzTestSync();
});

describe("MagicLinkAuth", () => {
  let crypto: PureJSCrypto;
  let mockAuthenticate: any;
  let authSecretStorage: AuthSecretStorage;
  let magicLinkAuth: MagicLinkAuth;
  let account: Account;

  beforeEach(async () => {
    // Reset storage
    KvStoreContext.getInstance().getStorage().clearAll();

    // Set up crypto and mocks
    crypto = await PureJSCrypto.create();
    mockAuthenticate = vi.fn();
    authSecretStorage = new AuthSecretStorage();

    account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });

    // Create MagicLinkAuth instance
    magicLinkAuth = new MagicLinkAuth(
      crypto,
      mockAuthenticate,
      authSecretStorage,
      "http://localhost:3000",
    );
  });

  describe("createTransferAsProvider", () => {
    it("creates a transfer", async () => {
      const transfer = await magicLinkAuth.createTransferAsProvider();

      // The transfer should be loaded as the logged-in account
      expect((transfer._loadedAs as Account).id).toBe(account.id);

      expect(transfer.status).toBe("pending");
    });
  });

  describe("createTransferAsConsumer", () => {
    it("creates a transfer", async () => {
      const transfer = await magicLinkAuth.createTransferAsConsumer();

      // The transfer should NOT be loaded as the logged-in account
      expect((transfer._loadedAs as Account).id).not.toBe(account.id);

      expect(transfer.status).toBe("pending");
    });
  });

  describe("createConfirmationCode", () => {
    it("creates a code", async () => {
      const mockRandomBytes = vi.fn();
      mockRandomBytes.mockReturnValue(new Uint8Array([1]));
      crypto.randomBytes = mockRandomBytes;

      const code = await magicLinkAuth.createConfirmationCode();

      expect(code).toBe("111111");
    });

    it("creates a code using custom code function", async () => {
      magicLinkAuth = new MagicLinkAuth(
        crypto,
        mockAuthenticate,
        authSecretStorage,
        "http://localhost:3000",
        { confirmationCodeFn: () => Promise.resolve("123456") },
      );

      const code = await magicLinkAuth.createConfirmationCode();
      expect(code).toBe("123456");
    });
  });

  describe("logInViaTransfer", () => {
    it("logs in via transfer", async () => {
      const transfer = await magicLinkAuth.createTransferAsProvider();

      transfer.secret = bytesToBase64url(
        new Uint8Array([
          173, 58, 235, 40, 67, 188, 236, 11, 107, 237, 97, 23, 182, 49, 188,
          63, 237, 52, 27, 84, 142, 66, 244, 149, 243, 114, 203, 164, 115, 239,
          175, 194,
        ]),
      );

      await magicLinkAuth.logInViaTransfer(transfer);

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.stringMatching(/^co_[^/]+$/),
        accountSecret: expect.stringMatching(
          /^sealerSecret_[^/]+\/signerSecret_[^/]+$/,
        ),
      });
    });
  });
});

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());
