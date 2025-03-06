// @vitest-environment happy-dom

import { beforeEach, describe, expect, it, vi } from "vitest";
import { AuthSecretStorage } from "../auth/AuthSecretStorage";
import { InMemoryKVStore } from "../auth/InMemoryKVStore.js";
import KvStoreContext from "../auth/KvStoreContext";
import { Account, ID } from "../exports";

const kvStore = new InMemoryKVStore();
KvStoreContext.getInstance().initialize(kvStore);

let authSecretStorage = new AuthSecretStorage();

describe("AuthSecretStorage", () => {
  describe("migrate", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    it("should migrate demo auth secret", async () => {
      const demoSecret = JSON.stringify({
        accountID: "demo123",
        accountSecret: "secret123",
      });
      await kvStore.set("demo-auth-logged-in-secret", demoSecret);

      await authSecretStorage.migrate();

      expect(await kvStore.get("jazz-logged-in-secret")).toBe(
        JSON.stringify({
          accountID: "demo123",
          accountSecret: "secret123",
          provider: "demo",
        }),
      );
      expect(await kvStore.get("demo-auth-logged-in-secret")).toBeNull();
    });

    it("should migrate clerk auth secret", async () => {
      const clerkSecret = JSON.stringify({
        accountID: "clerk123",
        secret: "secret123",
      });
      await kvStore.set("jazz-clerk-auth", clerkSecret);

      await authSecretStorage.migrate();

      expect(await kvStore.get("jazz-logged-in-secret")).toBe(
        JSON.stringify({
          accountID: "clerk123",
          accountSecret: "secret123",
          provider: "clerk",
        }),
      );
      expect(await kvStore.get("jazz-clerk-auth")).toBeNull();
    });

    it("should migrate auth wrong secret key to accountSecret", async () => {
      const clerkSecret = JSON.stringify({
        accountID: "clerk123",
        secret: "secret123",
        provider: "clerk",
      });
      await kvStore.set("jazz-logged-in-secret", clerkSecret);

      await authSecretStorage.migrate();

      expect(await kvStore.get("jazz-logged-in-secret")).toBe(
        JSON.stringify({
          accountID: "clerk123",
          accountSecret: "secret123",
          provider: "clerk",
        }),
      );
      expect(await kvStore.get("jazz-clerk-auth")).toBeNull();
    });
  });

  describe("get", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    it("should return null when no data exists", async () => {
      expect(await authSecretStorage.get()).toBeNull();
    });

    it("should return credentials with secretSeed", async () => {
      const credentials = {
        accountID: "test123",
        secretSeed: [1, 2, 3],
        accountSecret: "secret123",
        provider: "anonymous",
      };
      await kvStore.set("jazz-logged-in-secret", JSON.stringify(credentials));

      const result = await authSecretStorage.get();

      expect(result).toEqual({
        accountID: "test123",
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "secret123",
        provider: "anonymous",
      });
    });

    it("should return non-anonymous credentials without secretSeed", async () => {
      const credentials = {
        accountID: "test123",
        accountSecret: "secret123",
        provider: "passphrase",
      };
      await kvStore.set("jazz-logged-in-secret", JSON.stringify(credentials));

      const result = await authSecretStorage.get();

      expect(result).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        provider: "passphrase",
      });
    });

    it("should throw error for invalid data", async () => {
      await kvStore.set(
        "jazz-logged-in-secret",
        JSON.stringify({ invalid: "data" }),
      );

      await expect(authSecretStorage.get()).rejects.toThrow(
        "Invalid auth secret storage data",
      );
    });
  });

  describe("set", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    it("should set credentials with secretSeed", async () => {
      const payload = {
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        provider: "passphrase",
      };

      await authSecretStorage.set(payload);

      const stored = JSON.parse((await kvStore.get("jazz-logged-in-secret"))!);
      expect(stored).toEqual({
        accountID: "test123",
        secretSeed: [1, 2, 3],
        accountSecret: "secret123",
        provider: "passphrase",
      });
    });

    it("should set credentials without secretSeed", async () => {
      const payload = {
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        provider: "passphrase",
      };

      await authSecretStorage.set(payload);

      const stored = JSON.parse((await kvStore.get("jazz-logged-in-secret"))!);
      expect(stored).toEqual(payload);
    });
  });

  describe("onUpdate", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    it("should add and remove event listener", () => {
      const handler = vi.fn();

      const removeListener = authSecretStorage.onUpdate(handler);

      authSecretStorage.emitUpdate({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "demo",
      });
      expect(handler).toHaveBeenCalledTimes(1);

      handler.mockClear();

      removeListener();
      authSecretStorage.emitUpdate({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "anonymous",
      });
      expect(handler).not.toHaveBeenCalled();
    });

    it("should emit events only when the isAuthenticated state changes", () => {
      const handler = vi.fn();

      authSecretStorage.onUpdate(handler);

      authSecretStorage.emitUpdate({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "demo",
      });
      expect(handler).toHaveBeenCalledTimes(1);

      handler.mockClear();

      authSecretStorage.emitUpdate({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "demo",
      });
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe("clear", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    it("should remove stored credentials", async () => {
      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        accountSecret:
          "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
        provider: "passphrase",
      });

      await authSecretStorage.clear();

      expect(await authSecretStorage.get()).toBeNull();
    });
  });

  describe("notify", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    describe("set", () => {
      it("should emit update event when setting credentials", async () => {
        const handler = vi.fn();
        authSecretStorage.onUpdate(handler);

        await authSecretStorage.set({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          provider: "passphrase",
        });

        expect(handler).toHaveBeenCalled();
      });
    });

    describe("isAuthenticated", () => {
      it("should return false when no data exists", async () => {
        expect(authSecretStorage.isAuthenticated).toBe(false);
      });

      it("should return false for anonymous credentials", async () => {
        await authSecretStorage.set({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
          provider: "anonymous",
        });
        expect(authSecretStorage.isAuthenticated).toBe(false);
      });

      it("should return true for non-anonymous credentials", async () => {
        await authSecretStorage.set({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
          provider: "demo",
        });
        expect(authSecretStorage.isAuthenticated).toBe(true);
      });

      it("should return true when the provider is missing", async () => {
        await authSecretStorage.set({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
        } as any);
        expect(authSecretStorage.isAuthenticated).toBe(true);
      });
    });

    describe("clear", () => {
      it("should emit update event when clearing", async () => {
        await authSecretStorage.set({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          provider: "passphrase",
        });

        const handler = vi.fn();
        authSecretStorage.onUpdate(handler);

        await authSecretStorage.clear();

        expect(handler).toHaveBeenCalled();
      });
    });
  });

  describe("without notify", () => {
    beforeEach(() => {
      kvStore.clearAll();
      authSecretStorage = new AuthSecretStorage();
    });

    describe("set", () => {
      it("should not emit update event when setting credentials", async () => {
        const handler = vi.fn();
        authSecretStorage.onUpdate(handler);

        await authSecretStorage.setWithoutNotify({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          provider: "passphrase",
        });

        expect(handler).not.toHaveBeenCalled();
      });
    });

    describe("isAuthenticated", () => {
      it("should return false when no data exists", async () => {
        expect(authSecretStorage.isAuthenticated).toBe(false);
      });

      it("should return false for anonymous credentials", async () => {
        await authSecretStorage.setWithoutNotify({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
          provider: "anonymous",
        });
        expect(authSecretStorage.isAuthenticated).toBe(false);
      });

      it("should return true for non-anonymous credentials", async () => {
        await authSecretStorage.setWithoutNotify({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
          provider: "demo",
        });
        expect(authSecretStorage.isAuthenticated).toBe(false);
        authSecretStorage.emitUpdate({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
          provider: "demo",
        });
        expect(authSecretStorage.isAuthenticated).toBe(true);
      });

      it("should return true when the provider is missing", async () => {
        await authSecretStorage.setWithoutNotify({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
        } as any);
        expect(authSecretStorage.isAuthenticated).toBe(false);
        authSecretStorage.emitUpdate({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          secretSeed: new Uint8Array([1, 2, 3]),
        } as any);
        expect(authSecretStorage.isAuthenticated).toBe(true);
      });
    });

    describe("clear", () => {
      it("should not emit update event when clearing", async () => {
        await authSecretStorage.setWithoutNotify({
          accountID: "test123" as ID<Account>,
          accountSecret:
            "secret123" as `sealerSecret_z${string}/signerSecret_z${string}`,
          provider: "passphrase",
        });

        const handler = vi.fn();
        authSecretStorage.onUpdate(handler);

        await authSecretStorage.clearWithoutNotify();

        expect(handler).not.toHaveBeenCalled();
      });
    });
  });
});
