// @vitest-environment happy-dom

import { Account } from "jazz-tools";
import { ID } from "jazz-tools";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthSecretStorage } from "../auth/AuthSecretStorage";

describe("AuthSecretStorage", () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
  });

  describe("migrate", () => {
    it("should migrate demo auth secret", () => {
      const demoSecret = JSON.stringify({
        accountID: "demo123",
        accountSecret: "secret123",
      });
      localStorage.setItem("demo-auth-logged-in-secret", demoSecret);

      AuthSecretStorage.migrate();

      expect(localStorage.getItem("jazz-logged-in-secret")).toBe(demoSecret);
      expect(localStorage.getItem("demo-auth-logged-in-secret")).toBeNull();
    });

    it("should migrate clerk auth secret", () => {
      const clerkSecret = JSON.stringify({
        accountID: "clerk123",
        accountSecret: "secret123",
      });
      localStorage.setItem("jazz-clerk-auth", clerkSecret);

      AuthSecretStorage.migrate();

      expect(localStorage.getItem("jazz-logged-in-secret")).toBe(clerkSecret);
      expect(localStorage.getItem("jazz-clerk-auth")).toBeNull();
    });
  });

  describe("get", () => {
    it("should return null when no data exists", () => {
      expect(AuthSecretStorage.get()).toBeNull();
    });

    it("should return credentials with secretSeed", () => {
      const credentials = {
        accountID: "test123",
        secretSeed: [1, 2, 3],
        accountSecret: "secret123",
        isAnonymous: true,
      };
      localStorage.setItem(
        "jazz-logged-in-secret",
        JSON.stringify(credentials),
      );

      const result = AuthSecretStorage.get();

      expect(result).toEqual({
        accountID: "test123",
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "secret123",
        isAnonymous: true,
      });
    });

    it("should return non-anonymous credentials without secretSeed", () => {
      const credentials = {
        accountID: "test123",
        accountSecret: "secret123",
      };
      localStorage.setItem(
        "jazz-logged-in-secret",
        JSON.stringify(credentials),
      );

      const result = AuthSecretStorage.get();

      expect(result).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        isAnonymous: false,
      });
    });

    it("should throw error for invalid data", () => {
      localStorage.setItem(
        "jazz-logged-in-secret",
        JSON.stringify({ invalid: "data" }),
      );

      expect(() => AuthSecretStorage.get()).toThrow(
        "Invalid auth secret storage data",
      );
    });
  });

  describe("set", () => {
    it("should set credentials with secretSeed", () => {
      const payload = {
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        isAnonymous: true,
      };

      AuthSecretStorage.set(payload);

      const stored = JSON.parse(localStorage.getItem("jazz-logged-in-secret")!);
      expect(stored).toEqual({
        accountID: "test123",
        secretSeed: [1, 2, 3],
        accountSecret: "secret123",
        isAnonymous: true,
      });
    });

    it("should set credentials without secretSeed", () => {
      const payload = {
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
      };

      AuthSecretStorage.set(payload);

      const stored = JSON.parse(localStorage.getItem("jazz-logged-in-secret")!);
      expect(stored).toEqual(payload);
    });

    it("should emit update event when setting credentials", () => {
      const handler = vi.fn();
      AuthSecretStorage.onUpdate(handler);

      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
      });

      expect(handler).toHaveBeenCalled();
    });
  });

  describe("isAnonymous", () => {
    it("should return false when no data exists", () => {
      expect(AuthSecretStorage.isAnonymous()).toBe(false);
    });

    it("should return true for anonymous credentials", () => {
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        isAnonymous: true,
      });
      expect(AuthSecretStorage.isAnonymous()).toBe(true);
    });

    it("should not set isAnonymous to true when secretSeed is missing", () => {
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        isAnonymous: false,
      });
      expect(AuthSecretStorage.isAnonymous()).toBe(false);
    });

    it("should return false for non-anonymous credentials", () => {
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        isAnonymous: false,
      });
      expect(AuthSecretStorage.isAnonymous()).toBe(false);
    });
  });

  describe("onUpdate", () => {
    it("should add and remove event listener", () => {
      const handler = vi.fn();

      const removeListener = AuthSecretStorage.onUpdate(handler);

      AuthSecretStorage.emitUpdate();
      expect(handler).toHaveBeenCalledTimes(1);

      handler.mockClear();

      removeListener();
      AuthSecretStorage.emitUpdate();
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe("clear", () => {
    it("should remove stored credentials", () => {
      AuthSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        isAnonymous: false,
      });

      AuthSecretStorage.clear();

      expect(AuthSecretStorage.get()).toBeNull();
    });

    it("should emit update event when clearing", () => {
      const handler = vi.fn();
      AuthSecretStorage.onUpdate(handler);

      AuthSecretStorage.clear();

      expect(handler).toHaveBeenCalled();
    });
  });
});
