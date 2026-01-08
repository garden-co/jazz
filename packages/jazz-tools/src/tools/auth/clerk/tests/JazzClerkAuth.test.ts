// @vitest-environment happy-dom

import { AgentSecret } from "cojson";
import { AuthSecretStorage } from "jazz-tools";
import { Account, ID, InMemoryKVStore, KvStoreContext } from "jazz-tools";
import { createJazzTestAccount } from "jazz-tools/testing";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { JazzClerkAuth } from "../index";
import type { ClerkEventSchema, ClerkUser, MinimalClerkClient } from "../types";

KvStoreContext.getInstance().initialize(new InMemoryKVStore());
const authSecretStorage = new AuthSecretStorage();

describe("JazzClerkAuth", () => {
  const mockAuthenticate = vi.fn();
  const mockLogOut = vi.fn();
  let auth: JazzClerkAuth;

  beforeEach(async () => {
    await authSecretStorage.clear();
    mockAuthenticate.mockReset();
    await createJazzTestAccount({
      isCurrentActiveAccount: true,
    });
    auth = new JazzClerkAuth(mockAuthenticate, mockLogOut, authSecretStorage);
  });

  describe("onClerkUserChange", () => {
    it("should do nothing if no clerk user", async () => {
      await auth.onClerkUserChange(null);
      expect(mockAuthenticate).not.toHaveBeenCalled();
    });

    it("should throw if not authenticated locally", async () => {
      const user = {
        unsafeMetadata: {},
      } as ClerkUser;

      await expect(auth.onClerkUserChange(user)).rejects.toThrow();
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

      const mockClerk = {
        user: {
          fullName: "Guido",
          unsafeMetadata: {},
          update: vi.fn(),
        } as ClerkUser,
      };

      await auth.onClerkUserChange(mockClerk.user);

      expect(mockClerk.user?.update).toHaveBeenCalledWith({
        unsafeMetadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
      });
      expect(await authSecretStorage.get()).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "clerk",
      });

      const me = await Account.getMe().$jazz.ensureLoaded({
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

      const mockClerk = {
        user: {
          fullName: "Guido",
          unsafeMetadata: {
            jazzAccountID: "test123",
            jazzAccountSecret: "secret123",
            jazzAccountSeed: [1, 2, 3],
          },
          update: vi.fn(),
        } as ClerkUser,
      };

      await auth.onClerkUserChange(mockClerk.user);

      expect(mockAuthenticate).toHaveBeenCalledWith({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "clerk",
      });
    });

    it("should preserve credentials when logging in", async () => {
      // Set up local auth with different credentials
      await authSecretStorage.set({
        accountID: "local-account" as ID<Account>,
        secretSeed: new Uint8Array([9, 9, 9]),
        accountSecret: "local-secret" as AgentSecret,
        provider: "anonymous",
      });

      const mockClerk = {
        user: {
          fullName: "Guido",
          unsafeMetadata: {
            jazzAccountID: "clerk-account-123",
            jazzAccountSecret: "clerk-secret-456",
            jazzAccountSeed: [4, 5, 6],
          },
          update: vi.fn(),
        } as ClerkUser,
      };

      await auth.onClerkUserChange(mockClerk.user);

      // Verify credentials from Clerk are preserved in storage
      const storedCredentials = await authSecretStorage.get();
      expect(storedCredentials).toEqual({
        accountID: "clerk-account-123",
        accountSecret: "clerk-secret-456",
        secretSeed: new Uint8Array([4, 5, 6]),
        provider: "clerk",
      });
    });

    it("should call LogOut", async () => {
      // Set up local auth
      await authSecretStorage.set({
        accountID: "xxxx" as ID<Account>,
        secretSeed: new Uint8Array([2, 2, 2]),
        accountSecret: "xxxx" as AgentSecret,
        provider: "anonymous",
      });

      const mockClerk = {
        user: {
          fullName: "Guido",
          unsafeMetadata: {
            jazzAccountID: "test123",
            jazzAccountSecret: "secret123",
            jazzAccountSeed: [1, 2, 3],
          },
          update: vi.fn(),
        } as ClerkUser,
      };

      await auth.onClerkUserChange(mockClerk.user);
      await auth.onClerkUserChange(null);

      expect(authSecretStorage.isAuthenticated).toBe(false);
      expect(mockLogOut).toHaveBeenCalled();
    });
  });

  describe("registerListener", () => {
    function setupMockClerk(user: ClerkUser | null) {
      const listners = new Set<(clerkClient: ClerkEventSchema) => void>();

      return {
        client: {
          user,
          addListener: vi.fn((callback) => {
            listners.add(callback);
            return () => {
              listners.delete(callback);
            };
          }),
        } as unknown as MinimalClerkClient,
        triggerUserChange: (user: ClerkUser | null | undefined) => {
          for (const listener of listners) {
            listener({ user });
          }
        },
      };
    }

    it("should call onClerkUserChange on the first trigger", async () => {
      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );
      const onClerkUserChangeSpy = vi.spyOn(auth, "onClerkUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      expect(onClerkUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should call onClerkUserChange when user changes", async () => {
      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );
      const onClerkUserChangeSpy = vi.spyOn(auth, "onClerkUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      triggerUserChange({
        unsafeMetadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
        update: vi.fn(),
      });

      expect(onClerkUserChangeSpy).toHaveBeenCalledTimes(2);
    });

    it("should call onClerkUserChange when user passes from null to non-null", async () => {
      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );
      const onClerkUserChangeSpy = vi.spyOn(auth, "onClerkUserChange");

      auth.registerListener(client);

      triggerUserChange(null);

      expect(onClerkUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should not call onClerkUserChange when user is the same", async () => {
      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );
      const onClerkUserChangeSpy = vi.spyOn(auth, "onClerkUserChange");

      auth.registerListener(client);

      triggerUserChange({
        unsafeMetadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
        update: vi.fn(),
      });

      triggerUserChange({
        unsafeMetadata: {
          jazzAccountID: "test123",
          jazzAccountSecret: "secret123",
          jazzAccountSeed: [1, 2, 3],
        },
        update: vi.fn(),
      });

      expect(onClerkUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should not call onClerkUserChange when user switches from undefined to null", async () => {
      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );
      const onClerkUserChangeSpy = vi.spyOn(auth, "onClerkUserChange");

      auth.registerListener(client);

      triggerUserChange(null);
      triggerUserChange(undefined);
      triggerUserChange(null);

      expect(onClerkUserChangeSpy).toHaveBeenCalledTimes(1);
    });

    it("should complete signup flow when new Clerk user is detected", async () => {
      // 1. Setup local credentials (simulating anonymous user)
      await authSecretStorage.set({
        accountID: "test-account-id" as ID<Account>,
        secretSeed: new Uint8Array([1, 2, 3]),
        accountSecret: "test-secret" as AgentSecret,
        provider: "anonymous",
      });

      const { client, triggerUserChange } = setupMockClerk(null);

      const auth = new JazzClerkAuth(
        mockAuthenticate,
        mockLogOut,
        authSecretStorage,
      );

      // 2. Register listener with null user (no one logged in yet)
      auth.registerListener(client);

      // Initial trigger with no user
      triggerUserChange(null);

      // 3. Trigger event with new Clerk user (no Jazz credentials yet)
      const mockUserUpdate = vi.fn((data) => {
        triggerUserChange({
          ...data,
          update: mockUserUpdate,
        });
      });

      const signInSpy = vi.spyOn(auth, "signIn");
      const logInSpy = vi.spyOn(auth, "logIn");

      const newClerkUser = {
        fullName: "Test User",
        firstName: "Test",
        lastName: "User",
        username: "testuser",
        id: "clerk-user-123",
        primaryEmailAddress: { emailAddress: "test@example.com" },
        unsafeMetadata: {}, // No Jazz credentials yet
        update: mockUserUpdate,
      };

      triggerUserChange(newClerkUser);

      // Wait for async operations to complete
      await vi.waitFor(() => {
        expect(mockUserUpdate).toHaveBeenCalled();
      });

      // 4. Verify credentials synced to Clerk
      expect(mockUserUpdate).toHaveBeenCalledWith({
        unsafeMetadata: {
          jazzAccountID: "test-account-id",
          jazzAccountSecret: "test-secret",
          jazzAccountSeed: [1, 2, 3],
        },
      });

      // Verify profile name was updated from Clerk username
      const me = await Account.getMe().$jazz.ensureLoaded({
        resolve: { profile: true },
      });
      expect(me.profile.name).toBe("Test User");

      // Verify authSecretStorage is updated with provider "clerk"
      const storedCredentials = await authSecretStorage.get();
      expect(storedCredentials).toEqual({
        accountID: "test-account-id",
        accountSecret: "test-secret",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "clerk",
      });

      expect(signInSpy).toHaveBeenCalled();
      expect(logInSpy).not.toHaveBeenCalled();
    });
  });

  describe("initializeAuth", () => {
    it("should load auth data when credentials exist", async () => {
      const mockClerk = {
        user: {
          unsafeMetadata: {
            jazzAccountID: "test123",
            jazzAccountSecret: "secret123",
            jazzAccountSeed: [1, 2, 3],
          },
        },
      } as unknown as MinimalClerkClient;

      await JazzClerkAuth.initializeAuth(mockClerk);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toEqual({
        accountID: "test123",
        accountSecret: "secret123",
        secretSeed: new Uint8Array([1, 2, 3]),
        provider: "clerk",
      });
    });

    it("should do nothing when no credentials exist", async () => {
      const mockClerk = {
        user: {
          unsafeMetadata: {},
        },
      } as unknown as MinimalClerkClient;

      await JazzClerkAuth.initializeAuth(mockClerk);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toBeNull();
    });

    it("should do nothing when no user exists", async () => {
      const mockClerk = {
        user: null,
      } as unknown as MinimalClerkClient;

      await JazzClerkAuth.initializeAuth(mockClerk);

      const storedAuth = await authSecretStorage.get();
      expect(storedAuth).toBeNull();
    });
  });
});
