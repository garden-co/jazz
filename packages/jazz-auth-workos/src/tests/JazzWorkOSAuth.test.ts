// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { AuthSecretStorage } from "jazz-tools";
import { Account, ID, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { JazzWorkOSAuth } from "../index";
import type { MinimalWorkOSClient } from "../types";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());
const authSecretStorage = new AuthSecretStorage();

describe("JazzWorkOSAuth", () => {
  const mockAuthenticate = vi.fn();
  let auth: JazzWorkOSAuth;

  beforeEach(async () => {
    await authSecretStorage.clear();
    mockAuthenticate.mockReset();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
  });

  describe("onWorkOSUserChange", () => {
    it("should do nothing if no workos user", async () => {
      const mockWorkOS = {
        user: null,
      } as MinimalWorkOSClient;

      await auth.onWorkOSUserChange(mockWorkOS);
      expect(mockAuthenticate).not.toHaveBeenCalled();
    });

    it("should throw if not authenticated locally", async () => {
      const mockWorkOS = {
        user: {
          metadata: {},
        },
        signOut: vi.fn(),
      } as unknown as MinimalWorkOSClient;

      await expect(auth.onWorkOSUserChange(mockWorkOS)).rejects.toThrow();
      expect(mockAuthenticate).not.toHaveBeenCalled();
    });

    it("should call signIn for new users", async () => {
      // Set up local auth
      await authSecretStorage.set({
        accountID: "test123" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "secret123" as AgentSecret,
        provider: "anonymous",
      });

      const mockWorkOS = {
        user: {
          firstName: "Guido",
          metadata: {},
          update: vi.fn(),
        },
        signOut: vi.fn(),
      } as unknown as MinimalWorkOSClient;

      await auth.onWorkOSUserChange(mockWorkOS);

      expect(mockWorkOS.user?.update).toHaveBeenCalledWith({
        metadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
      });
      expect(await authSecretStorage.get()).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "workos",
      });

      const me = await Account.getMe().ensureLoaded({
        resolve: {
          profile: true,
        },
      });
      expect(me.profile.name).toBe("Guido");
    });

    it("should call logIn for existing users", async () => {
      // Set up local auth
      await authSecretStorage.set({
        accountID: "xxxx" as ID<Account>,
        secretSeed: new Uint8Array([2, 2, 2]),
        accountSecret: "xxxx" as AgentSecret,
        provider: "anonymous",
      });

      const mockWorkOS = {
        user: {
          fullName: "Guido",
          metadata: {
            jazzAccountID: "test123",
            jazzAccountSecret: "secret123",
            jazzAccountSeed: [1, 2, 3],
          },
        },
        signOut: vi.fn(),
      } as unknown as MinimalWorkOSClient;

      await auth.onWorkOSUserChange(mockWorkOS);

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "workos",
      });
    });
  });

  describe("registerListener", () => {
    function setupmockWorkOS(user: MinimalWorkOSClient["user"]) {
      const listners = new Set<
        (workosClient: Pick<MinimalWorkOSClient, "user">) => void
      >();

      return {
        client: {
          user,
          addListener: vi.fn((callback) => {
            listners.add(callback);
            return () => {
              listners.delete(callback);
            };
          }),
        } as unknown as MinimalWorkOSClient,
        triggerUserChange: (user: unknown) => {
          for (const listener of listners) {
            listener({ user } as Pick<MinimalWorkOSClient, "user">);
          }
        },
      };
    }

    
    it("should call onWorkOSUserChange on the first trigger", async () => {
      const { client, triggerUserChange } = setupmockWorkOS(null);

      const auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
      const onWorkOSUserChangeSpy = vi.spyOn(auth, "onWorkOSUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      expect(onWorkOSUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should call onWorkOSUserChange when user changes", async () => {
      const { client, triggerUserChange } = setupmockWorkOS(null);

      const auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
      const onWorkOSUserChangeSpy = vi.spyOn(auth, "onWorkOSUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      triggerUserChange({
        metadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
      });

      expect(onWorkOSUserChangeSpy).toHaveBeenCalledTimes(2);
    });

    it("should call onWorkOSUserChange when user passes from null to non-null", async () => {
      const { client, triggerUserChange } = setupmockWorkOS(null);

      const auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
      const onWorkOSUserChangeSpy = vi.spyOn(auth, "onWorkOSUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      expect(onWorkOSUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should not call onWorkOSUserChange when user is the same", async () => {
      const { client, triggerUserChange } = setupmockWorkOS(null);

      const auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
      const onWorkOSUserChangeSpy = vi.spyOn(auth, "onWorkOSUserChange");

      auth.registerListener(client);

      triggerUserChange({
        metadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
      });

      triggerUserChange({
        metadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
      });

      expect(onWorkOSUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should not call onWorkOSUserChange when user switches from undefined to null", async () => {
      const { client, triggerUserChange } = setupmockWorkOS(null);

      const auth = new JazzWorkOSAuth(mockAuthenticate, authSecretStorage);
      const onWorkOSUserChangeSpy = vi.spyOn(auth, "onWorkOSUserChange");

      auth.registerListener(client);

      triggerUserChange(null);
      triggerUserChange(undefined);
      triggerUserChange(null);

      expect(onWorkOSUserChangeSpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("initializeAuth", () => {
    it("should load auth data when credentials exist", async () => {
      const mockWorkOS = {
        user: {
          metadata: {
            jazzAccountID: "test123",
            jazzAccountSecret: "secret123",
            jazzAccountSeed: [1, 2, 3],
          },
        },
      } as unknown as MinimalWorkOSClient;

      await JazzWorkOSAuth.initializeAuth(mockWorkOS);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "workos",
      });
    });

    it("should do nothing when no credentials exist", async () => {
      const mockWorkOS = {
        user: {
          metadata: {},
        },
      } as unknown as MinimalWorkOSClient;

      await JazzWorkOSAuth.initializeAuth(mockWorkOS);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toBeNull();
    });

    it("should do nothing when no user exists", async () => {
      const mockWorkOS = {
        user: null,
      } as unknown as MinimalWorkOSClient;

      await JazzWorkOSAuth.initializeAuth(mockWorkOS);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toBeNull();
    });
  });
});
