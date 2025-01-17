// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { Account } from "jazz-tools";
import { ID } from "jazz-tools";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { BrowserAnonymousAuth } from "../auth/AnonymousAuth";
import { AuthSecretStorage } from "../auth/AuthSecretStorage";

beforeEach(() => {
  AuthSecretStorage.clear();
});

describe("BrowserAnonymousAuth", () => {
  function createDriver() {
    return {
      onSignedIn: vi.fn(),
      onError: vi.fn(),
    } satisfies BrowserAnonymousAuth.Driver;
  }

  describe("initialization", () => {
    it("should initialize with default username", () => {
      const driver = createDriver();
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);
      expect(auth.defaultUserName).toBe("Anonymous User");
    });
  });

  describe("authentication flows", () => {
    it("should handle new user signup", async () => {
      const driver = createDriver();
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      await result.saveCredentials?.({
        accountID: "test123" as ID<Account>,
        secret: "mock-secret" as AgentSecret,
      });

      result.onSuccess();

      expect(result.type).toBe("new");
      expect(result.creationProps).toEqual({
        name: "Anonymous User",
        anonymous: true,
      });
      expect(driver.onSignedIn).toHaveBeenCalled();
      expect(AuthSecretStorage.get()).toEqual({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        isAnonymous: true,
        accountSecret: "mock-secret" as AgentSecret,
      });
    });

    it("should handle existing user login", async () => {
      const driver = createDriver();
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        isAnonymous: true,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      result.onSuccess();

      expect(result.type).toBe("existing");
      expect(result.credentials).toEqual({
        accountID: "test123",
        secret: "mock-secret",
      });
      expect(driver.onSignedIn).toHaveBeenCalled();
    });

    it("should handle errors during login", async () => {
      const driver = createDriver();
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        isAnonymous: true,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      const testError = new Error("Test error");
      result.onError(testError);

      expect(driver.onError).toHaveBeenCalledWith(testError);
    });

    it("should clear storage on logout", async () => {
      const driver = createDriver();
      const auth = new BrowserAnonymousAuth("Anonymous User", driver);

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "mock-secret" as AgentSecret,
        isAnonymous: true,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);
      result.logOut();

      expect(AuthSecretStorage.get()).toBeNull();
    });
  });
});
