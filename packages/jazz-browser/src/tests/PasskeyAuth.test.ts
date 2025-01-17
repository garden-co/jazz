// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { Account } from "jazz-tools";
import { ID } from "jazz-tools";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthSecretStorage } from "../auth/AuthSecretStorage";
import { BrowserPasskeyAuth } from "../auth/PasskeyAuth";
import { waitFor } from "./utils";

beforeEach(() => {
  AuthSecretStorage.clear();
});

describe("BrowserPasskeyAuth", () => {
  let mockNavigator: any;

  function createDriver() {
    return {
      onReady: vi.fn().mockImplementation((api) => {
        api.signUp("testuser");
      }),
      onSignedIn: vi.fn(),
      onError: vi.fn(),
    } satisfies BrowserPasskeyAuth.Driver;
  }

  beforeEach(() => {
    mockNavigator = {
      credentials: {
        create: vi.fn(),
        get: vi.fn(),
      },
    };
    global.navigator = mockNavigator;
  });

  describe("initialization", () => {
    it("should initialize with default hostname", () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");
      expect(auth.appName).toBe("Test App");
      expect(auth.appHostname).toBe(window.location.hostname);
    });

    it("should initialize with custom hostname", () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App", "custom.host");
      expect(auth.appHostname).toBe("custom.host");
    });

    it("should handle existing user login", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockRejectedValue(new Error("test"));

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

      result.onSuccess();

      expect(result.type).toBe("existing");

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      expect(result.credentials).toEqual({
        accountID: "test123",
        secret: "fromAuthStorage",
      });

      expect(driver.onSignedIn).toHaveBeenCalled();
      expect(driver.onReady).not.toHaveBeenCalled();
    });

    it("should not automatically login when the existing user is anonymous", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.logIn();
      });

      mockNavigator.credentials.get.mockResolvedValue({
        response: {
          userHandle: new ArrayBuffer(32), // Mocked credential payload
        },
      });

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1]),
        accountSecret: "fromAuthStorage" as AgentSecret,
        isAnonymous: true,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1]),
        agentSecretFromSecretSeed: () => "fromLogIn" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      expect(result.credentials).toEqual({
        accountID: "co_z",
        secret: "fromLogIn",
      });
    });

    it("should upgrade anonymous account to passkey", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.signUp("testuser");
      });

      mockNavigator.credentials.create.mockResolvedValue({
        type: "public-key",
        id: new Uint8Array([1, 2, 3, 4]),
      });

      // Set up existing user in storage
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1]),
        accountSecret: "fromAuthStorage" as AgentSecret,
        isAnonymous: true,
      });

      const mockCrypto = {
        newRandomSecretSeed: () => new Uint8Array([1]),
        agentSecretFromSecretSeed: () => "fromLogIn" as AgentSecret,
      };

      const result = await auth.start(mockCrypto as any);

      if (result.type !== "existing") {
        throw new Error("Expected existing user login");
      }

      await result.saveCredentials?.({
        accountID: "test123" as ID<Account>,
        secret: "fromLogIn" as AgentSecret,
      });

      expect(mockNavigator.credentials.create).toHaveBeenCalled();
      expect(result.credentials).toEqual({
        accountID: "test123" as ID<Account>,
        secret: "fromAuthStorage" as AgentSecret,
      });
    });
  });

  describe("authentication flows", () => {
    it("should handle new user signup", async () => {
      const driver = createDriver();

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.signUp("testuser");
      });

      const auth = new BrowserPasskeyAuth(driver, "Test App");

      mockNavigator.credentials.create.mockResolvedValue({
        type: "public-key",
        id: new Uint8Array([1, 2, 3, 4]),
      });

      const result = await auth.start({
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      } as any);

      await result.saveCredentials?.({
        accountID: "test123" as ID<Account>,
        secret: "mock-secret" as AgentSecret,
      });

      result.onSuccess();

      expect(result.type).toBe("new");
      expect(driver.onSignedIn).toHaveBeenCalled();
      expect(AuthSecretStorage.get()).toEqual({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        isAnonymous: false,
        accountSecret: "mock-secret" as AgentSecret,
      });
    });

    it("should handle existing user login", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.logIn();
      });

      mockNavigator.credentials.get.mockResolvedValue({
        response: {
          userHandle: new ArrayBuffer(32), // Mocked credential payload
        },
      });

      const result = await auth.start({
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      } as any);

      result.saveCredentials?.({
        accountID: "test123" as ID<Account>,
        secret: "mock-secret" as AgentSecret,
      });

      result.onSuccess();

      expect(result.type).toBe("existing");
      expect(driver.onSignedIn).toHaveBeenCalled();
      expect(AuthSecretStorage.get()).toEqual({
        accountID: "test123" as ID<Account>,
        secretSeed: expect.any(Uint8Array),
        isAnonymous: false,
        accountSecret: "mock-secret" as AgentSecret,
      });
    });

    it("should handle passkey errors during login (invalid credentials)", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.logIn();
      });

      mockNavigator.credentials.get.mockResolvedValue(null);

      auth.start({
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      } as any);

      await waitFor(() => {
        expect(driver.onError).toHaveBeenCalledWith(
          "Error while accessing the passkey credentials",
        );
      });
    });

    it("should handle passkey errors during login (rejected by user)", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.logIn();
      });

      mockNavigator.credentials.get.mockRejectedValue(
        new Error("User rejected the passkey"),
      );

      auth.start({
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      } as any);

      await waitFor(() => {
        expect(driver.onError).toHaveBeenCalledWith(
          "Error while accessing the passkey credentials",
        );
      });
    });

    it("should handle passkey during signup", async () => {
      const driver = createDriver();
      const auth = new BrowserPasskeyAuth(driver, "Test App");

      driver.onReady = vi.fn().mockImplementation((api) => {
        api.signUp("testuser");
      });

      mockNavigator.credentials.create.mockRejectedValue(
        new Error("User rejected the passkey"),
      );

      const result = await auth.start({
        newRandomSecretSeed: () => new Uint8Array([1, 2, 3]),
        agentSecretFromSecretSeed: () => "mock-secret" as AgentSecret,
      } as any);

      const saveResult = result.saveCredentials?.({
        accountID: "test123" as ID<Account>,
        secret: "mock-secret" as AgentSecret,
      });

      await expect(saveResult).rejects.toThrow("User rejected the passkey");
    });
  });
});
