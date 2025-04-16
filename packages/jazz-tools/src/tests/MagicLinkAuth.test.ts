// @vitest-environment happy-dom

import { AgentSecret, bytesToBase64url } from "cojson";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  MagicLinkAuth,
  MagicLinkAuthCreateAsConsumer,
  MagicLinkAuthCreateAsProvider,
  MagicLinkAuthHandleAsConsumer,
  MagicLinkAuthHandleAsProvider,
} from "../auth/MagicLinkAuth";
import {
  Account,
  AuthSecretStorage,
  ID,
  InMemoryKVStore,
  KvStoreContext,
} from "../exports";
import { createJazzTestAccount } from "../testing";
import { waitFor } from "./utils";

// Initialize KV store for tests
KvStoreContext.getInstance().initialize(new InMemoryKVStore());

describe("MagicLinkAuth", () => {
  let crypto: PureJSCrypto;
  let mockAuthenticate: any;
  let authSecretStorage: AuthSecretStorage;
  let magicLinkAuth: MagicLinkAuth;
  let account: Account;

  beforeEach(async () => {
    vi.resetAllMocks();

    // Reset storage
    KvStoreContext.getInstance().getStorage().clearAll();

    // Set up crypto and mocks
    crypto = await PureJSCrypto.create();
    mockAuthenticate = vi.fn();
    authSecretStorage = new AuthSecretStorage();
    const secretSeed = crypto.newRandomSecretSeed();
    await authSecretStorage.set({
      accountID: "test-account" as ID<Account>,
      accountSecret: "test-secret" as AgentSecret,
      secretSeed,
      provider: "anonymous",
    });

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
      const transfer = await magicLinkAuth.createTransfer();

      expect(transfer.status).toBe("pending");
    });
  });

  describe("createTransferAsConsumer", () => {
    it("creates a transfer", async () => {
      const transfer = await magicLinkAuth.createTransfer();

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
      const transfer = await magicLinkAuth.createTransfer();

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

  describe("MagicLinkAuthCreateAsConsumer", () => {
    it("should initialize", () => {
      const createAsConsumer = new MagicLinkAuthCreateAsConsumer(magicLinkAuth);

      expect(createAsConsumer.authState.status).toEqual("idle");
      expect(createAsConsumer.authState.sendConfirmationCode).toBeUndefined();
    });

    it("should cancel flow", async () => {
      const createAsConsumer = new MagicLinkAuthCreateAsConsumer(magicLinkAuth);
      await createAsConsumer.createLink();

      createAsConsumer.cancelFlow();

      await waitFor(() => {
        expect(createAsConsumer.authState.status).toEqual("cancelled");
      });
    });
  });

  describe("MagicLinkAuthHandleAsProvider", () => {
    it("should initialize", () => {
      const handleAsProvider = new MagicLinkAuthHandleAsProvider(magicLinkAuth);

      expect(handleAsProvider.authState.status).toEqual("idle");
      expect(handleAsProvider.authState.confirmationCode).toBeUndefined();
    });

    it("should cancel flow", async () => {
      // Create the link as consumer
      const createAsConsumer = new MagicLinkAuthCreateAsConsumer(magicLinkAuth);
      const link = await createAsConsumer.createLink();

      // Handle the flow as provider
      const handleAsProvider = new MagicLinkAuthHandleAsProvider(magicLinkAuth);
      handleAsProvider.handleFlow(link);

      setTimeout(() => {
        handleAsProvider.cancelFlow();
      }, 50);

      await waitFor(() => {
        expect(handleAsProvider.authState.status).toEqual("cancelled");
      });
    });

    it("should handle the flow", async () => {
      // Create the link as consumer
      const createAsConsumer = new MagicLinkAuthCreateAsConsumer(magicLinkAuth);
      const link = await createAsConsumer.createLink();
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/magic-link-handler-provider\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsConsumer.authState.status).toEqual("waitingForProvider");

      // Handle the flow as provider
      const handleAsProvider = new MagicLinkAuthHandleAsProvider(magicLinkAuth);
      handleAsProvider.handleFlow(link);
      await waitFor(() => {
        expect(createAsConsumer.authState.status).toEqual(
          "confirmationCodeRequired",
        );
        expect(createAsConsumer.authState.sendConfirmationCode).toBeDefined();

        expect(handleAsProvider.authState.status).toEqual(
          "confirmationCodeGenerated",
        );
        expect(handleAsProvider.authState.confirmationCode).toBeDefined();
      });

      // Enter the confirmation code
      createAsConsumer.authState.sendConfirmationCode?.(
        handleAsProvider.authState.confirmationCode!,
      );
      // Check authorized
      await waitFor(() => {
        expect(createAsConsumer.authState.status).toEqual("authorized");
      });
      expect(handleAsProvider.authState.status).toEqual("authorized");

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.stringMatching(/^co_[^/]+$/),
        accountSecret: expect.stringMatching(
          /^sealerSecret_[^/]+\/signerSecret_[^/]+$/,
        ),
      });
    });
  });

  describe("MagicLinkAuthCreateAsProvider", () => {
    it("should initialize", () => {
      const createAsProvider = new MagicLinkAuthCreateAsProvider(magicLinkAuth);

      expect(createAsProvider.authState.status).toEqual("idle");
      expect(createAsProvider.authState.confirmationCode).toBeUndefined();
    });

    it("should cancel flow", async () => {
      const createAsProvider = new MagicLinkAuthCreateAsProvider(magicLinkAuth);
      await createAsProvider.createLink();

      setTimeout(() => {
        createAsProvider.cancelFlow();
      }, 50);

      await waitFor(() => {
        expect(createAsProvider.authState.status).toEqual("cancelled");
      });
    });
  });

  describe("MagicLinkAuthHandleAsConsumer", () => {
    it("should initialize", () => {
      const handleAsConsumer = new MagicLinkAuthHandleAsConsumer(magicLinkAuth);

      expect(handleAsConsumer.authState.status).toEqual("idle");
      expect(handleAsConsumer.authState.sendConfirmationCode).toBeNull();
    });

    it("should cancel flow", async () => {
      // Create the link as provider
      const createAsProvider = new MagicLinkAuthCreateAsProvider(magicLinkAuth);
      const link = await createAsProvider.createLink();

      // Handle the flow as consumer
      const handleAsConsumer = new MagicLinkAuthHandleAsConsumer(magicLinkAuth);
      handleAsConsumer.handleFlow(link);

      // Cancel the flow
      setTimeout(() => {
        handleAsConsumer.cancelFlow();
      }, 50);

      await waitFor(() => {
        expect(handleAsConsumer.authState.status).toEqual("cancelled");
      });
    });

    it("should handle the flow", async () => {
      // Create the link as provider
      const createAsProvider = new MagicLinkAuthCreateAsProvider(magicLinkAuth);
      const link = await createAsProvider.createLink();
      expect(link).toMatch(
        /^http:\/\/localhost:3000\/magic-link-handler-consumer\/co_[^/]+\/inviteSecret_[^/]+$/,
      );
      expect(createAsProvider.authState.status).toEqual("waitingForConsumer");

      // Handle the flow as consumer
      const handleAsConsumer = new MagicLinkAuthHandleAsConsumer(magicLinkAuth);
      handleAsConsumer.handleFlow(link);
      await waitFor(() => {
        expect(createAsProvider.authState.status).toEqual(
          "confirmationCodeGenerated",
        );
        expect(createAsProvider.authState.confirmationCode).toBeDefined();

        expect(handleAsConsumer.authState.status).toEqual(
          "confirmationCodeRequired",
        );
        expect(handleAsConsumer.authState.sendConfirmationCode).toBeDefined();
      });

      // Enter the confirmation code
      handleAsConsumer.authState.sendConfirmationCode?.(
        createAsProvider.authState.confirmationCode!,
      );

      // Check authorized
      await waitFor(() => {
        expect(handleAsConsumer.authState.status).toEqual("authorized");
      });
      expect(createAsProvider.authState.status).toEqual("authorized");

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: expect.stringMatching(/^co_[^/]+$/),
        accountSecret: expect.stringMatching(
          /^sealerSecret_[^/]+\/signerSecret_[^/]+$/,
        ),
      });
    });
  });
});
