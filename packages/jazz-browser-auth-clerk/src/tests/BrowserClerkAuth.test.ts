// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { AuthSecretStorage } from "jazz-browser";
import { Account } from "jazz-tools";
import { ID } from "jazz-tools";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { BrowserClerkAuth } from "../index";

beforeEach(() => {
  AuthSecretStorage.clear();
});

describe("BrowserClerkAuth", () => {
  function createDriver() {
    return {
      onError: vi.fn(),
    } satisfies BrowserClerkAuth.Driver;
  }

  function createMockClerkClient(user?: any) {
    return {
      user,
      signOut: vi.fn(),
    };
  }

  describe("initialization", () => {
    it("should handle existing non-anonymous user from storage", async () => {
      const driver = createDriver();
      const mockClerkClient = createMockClerkClient();
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1]),
        accountSecret: "fromAuthStorage" as AgentSecret,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([2]),
        agentSecretFromSecretSeed: () => "xxxxx" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      expect(result.type).toBe("existing");

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      expect(result.credentials).toEqual({
        accountID: "test123",
        secret: "fromAuthStorage",
      });
    });

    it("should handle existing clerk user with credentials", async () => {
      const driver = createDriver();
      const mockUser = {
        id: "clerk123",
        fullName: "Test User",
        unsafeMetadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "clerkSecret",
          jazzAccountSeed: [1, 2, 3],
        },
        update: vi.fn(),
      };
      const mockClerkClient = createMockClerkClient(mockUser);
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([2]),
        agentSecretFromSecretSeed: () => "xxxxx" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      expect(result.type).toBe("existing");

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      expect(result.credentials).toEqual({
        accountID: "test123",
        secret: "clerkSecret",
      });
    });

    it("should handle anonymous user upgrade", async () => {
      const driver = createDriver();
      const mockUser = {
        id: "clerk123",
        fullName: "Test User",
        unsafeMetadata: {},
        update: vi.fn(),
      };
      const mockClerkClient = createMockClerkClient(mockUser);
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      // Set up anonymous user in storage
      AuthSecretStorage.set({
        accountID: "anon123" as ID<Account>,
        secretSeed: new Uint8Array([1]),
        accountSecret: "anonSecret" as AgentSecret,
        isAnonymous: true,
      });

      const result = await auth.start({} as any);

      expect(result.type).toBe("existing");

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      expect(result.username).toBe("Test User");
      expect(result.credentials).toEqual({
        accountID: "anon123",
        secret: "anonSecret",
      });

      // Test saving credentials updates both storage and clerk metadata
      await result.saveCredentials?.({
        accountID: "anon123" as ID<Account>,
        secret: "newSecret" as AgentSecret,
      });

      expect(mockUser.update).toHaveBeenCalledWith({
        unsafeMetadata: {
          jazzAccountID: "anon123",
          jazzAccountSecret: "newSecret",
          jazzAccountSeed: expect.any(Array),
        },
      });
    });

    it("should handle new user creation", async () => {
      const driver = createDriver();
      const mockUser = {
        id: "clerk123",
        fullName: "Test User",
        unsafeMetadata: {},
        update: vi.fn(),
      };
      const mockClerkClient = createMockClerkClient(mockUser);
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "newSecret" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      expect(result.type).toBe("new");

      if (result.type !== "new") {
        throw new Error("Expected new user login");
      }

      expect(result.creationProps).toEqual({
        name: "Test User",
      });
      expect(result.initialSecret).toBe("newSecret");

      await result.saveCredentials({
        accountID: "new123" as ID<Account>,
        secret: "newSecret" as AgentSecret,
      });

      expect(mockUser.update).toHaveBeenCalledWith({
        unsafeMetadata: {
          jazzAccountID: "new123",
          jazzAccountSecret: "newSecret",
          jazzAccountSeed: [1, 2, 3],
        },
      });
    });

    it("should throw error when not signed in", async () => {
      const driver = createDriver();
      const mockClerkClient = createMockClerkClient(undefined);
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      await expect(auth.start({} as any)).rejects.toThrow("Not signed in");
    });

    it("should throw error when clerk user has ID but no secret", async () => {
      const driver = createDriver();
      const mockUser = {
        id: "clerk123",
        fullName: "Test User",
        unsafeMetadata: {
          jazzAccountID: "test123",
        },
        update: vi.fn(),
      };
      const mockClerkClient = createMockClerkClient(mockUser);
      const auth = new BrowserClerkAuth(driver, mockClerkClient);

      await expect(auth.start({} as any)).rejects.toThrow(
        "No secret for existing user",
      );
    });
  });
});
