import { createAuthClient } from "better-auth/client";
import type { Account, AuthSecretStorage } from "jazz-tools";
import {
  TestJazzContextManager,
  createJazzTestAccount,
  setActiveAccount,
  setupJazzTestSync,
} from "jazz-tools/testing";
import { assert, beforeEach, describe, expect, it, vi } from "vitest";
import { jazzPluginClient } from "../client.js";
import { emailOTPClient, genericOAuthClient } from "better-auth/client/plugins";

describe("Better-Auth client plugin", () => {
  let account: Account;
  let jazzContextManager: TestJazzContextManager<Account>;
  let authSecretStorage: AuthSecretStorage;
  let authClient: ReturnType<
    typeof createAuthClient<{
      plugins: ReturnType<
        | typeof jazzPluginClient
        | typeof emailOTPClient
        | typeof genericOAuthClient
      >[];
    }>
  >;
  let customFetchImpl = vi.fn();

  beforeEach(async () => {
    account = await setupJazzTestSync();
    setActiveAccount(account);

    jazzContextManager = TestJazzContextManager.fromAccountOrGuest(account);
    authSecretStorage = jazzContextManager.getAuthSecretStorage();

    // start a new context
    await jazzContextManager.createContext({});

    authClient = createAuthClient({
      baseURL: "http://localhost:3000",
      plugins: [jazzPluginClient(), emailOTPClient(), genericOAuthClient()],
      fetchOptions: {
        customFetchImpl,
      },
    });

    const context = jazzContextManager.getCurrentValue();
    assert(context, "Jazz context is not available");
    authClient.jazz.setJazzContext(context);
    authClient.jazz.setAuthSecretStorage(authSecretStorage);

    customFetchImpl.mockReset();
  });

  it("should send Jazz credentials over signup", async () => {
    const credentials = await authSecretStorage.get();
    expect(authSecretStorage.isAuthenticated).toBe(false);
    assert(credentials, "Jazz credentials are not available");

    customFetchImpl.mockResolvedValue(
      new Response(
        JSON.stringify({
          token: "6diDScDDcLJLl3sxAEestZz63mrw9Azy",
          user: {
            id: "S6SDKApdnh746gUnP3zujzsEY53tjuTm",
            email: "test@jazz.dev",
            name: "Matteo",
            image: null,
            emailVerified: false,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          jazzAuth: {
            accountID: credentials.accountID,
            secretSeed: credentials.secretSeed,
            accountSecret: credentials.accountSecret,
          },
        }),
      ),
    );

    // Sign up
    await authClient.signUp.email({
      email: "test@jazz.dev",
      password: "12345678",
      name: "Matteo",
    });

    expect(customFetchImpl).toHaveBeenCalledTimes(1);
    expect(customFetchImpl.mock.calls[0]![0].toString()).toBe(
      "http://localhost:3000/api/auth/sign-up/email",
    );

    // Verify the credentials have been injected in the request body
    expect(
      customFetchImpl.mock.calls[0]![1].headers.get("x-jazz-auth")!,
    ).toEqual(
      JSON.stringify({
        accountID: credentials!.accountID,
        secretSeed: credentials!.secretSeed,
        accountSecret: credentials!.accountSecret,
      }),
    );

    expect(authSecretStorage.isAuthenticated).toBe(true);

    // Verify the profile name has been updated
    const context = jazzContextManager.getCurrentValue();
    assert(context && "me" in context);
    expect(context.me.$jazz.id).toBe(credentials!.accountID);
  });

  it("should become logged in Jazz credentials after sign-in", async () => {
    const credentials = await jazzContextManager.getAuthSecretStorage().get();

    // Log out from initial context
    await jazzContextManager.logOut();
    expect(authSecretStorage.isAuthenticated).toBe(false);

    customFetchImpl.mockResolvedValue(
      new Response(
        JSON.stringify({
          user: {
            id: "123",
            email: "test@jazz.dev",
            name: "Matteo",
          },
          jazzAuth: {
            accountID: credentials!.accountID,
            secretSeed: credentials!.secretSeed,
            accountSecret: credentials!.accountSecret,
            provider: "better-auth",
          },
        }),
      ),
    );

    // Retrieve the BetterAuth session and trigger the authentication
    await authClient.signIn.email({
      email: "test@jazz.dev",
      password: "12345678",
    });

    expect(customFetchImpl).toHaveBeenCalledTimes(1);
    expect(customFetchImpl.mock.calls[0]![0].toString()).toBe(
      "http://localhost:3000/api/auth/sign-in/email",
    );

    expect(authSecretStorage.isAuthenticated).toBe(true);

    const newContext = jazzContextManager.getCurrentValue()!;
    expect("me" in newContext).toBe(true);
    expect(await authSecretStorage.get()).toMatchObject({
      accountID: credentials!.accountID,
      provider: "better-auth",
    });
  });

  it("should logout from Jazz after BetterAuth sign-out", async () => {
    const credentials = await authSecretStorage.get();
    expect(authSecretStorage.isAuthenticated).toBe(false);
    customFetchImpl.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          token: "6diDScDDcLJLl3sxAEestZz63mrw9Azy",
          user: {
            id: "S6SDKApdnh746gUnP3zujzsEY53tjuTm",
            email: "test@jazz.dev",
            name: "Matteo",
            image: null,
            emailVerified: false,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          jazzAuth: {
            accountID: credentials!.accountID,
            secretSeed: credentials!.secretSeed,
            accountSecret: credentials!.accountSecret,
            provider: "better-auth",
          },
        }),
      ),
    );

    // 1. Sign up
    await authClient.signUp.email({
      email: "test@jazz.dev",
      password: "12345678",
      name: "Matteo",
    });

    expect(authSecretStorage.isAuthenticated).toBe(true);

    // 2. Sign out
    customFetchImpl.mockResolvedValueOnce(
      new Response(JSON.stringify({ success: true })),
    );

    await authClient.signOut();

    expect(authSecretStorage.isAuthenticated).toBe(false);

    const anonymousCredentials = await authSecretStorage.get();
    expect(anonymousCredentials).not.toMatchObject(credentials!);
  });

  it("should logout from Jazz after BetterAuth user deletion", async () => {
    const credentials = await authSecretStorage.get();
    expect(authSecretStorage.isAuthenticated).toBe(false);
    customFetchImpl.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          token: "6diDScDDcLJLl3sxAEestZz63mrw9Azy",
          user: {
            id: "S6SDKApdnh746gUnP3zujzsEY53tjuTm",
            email: "test@jazz.dev",
            name: "Matteo",
            image: null,
            emailVerified: false,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          jazzAuth: {
            accountID: credentials!.accountID,
            secretSeed: credentials!.secretSeed,
            accountSecret: credentials!.accountSecret,
            provider: "better-auth",
          },
        }),
      ),
    );

    // 1. Sign up
    await authClient.signUp.email({
      email: "test@jazz.dev",
      password: "12345678",
      name: "Matteo",
    });

    expect(authSecretStorage.isAuthenticated).toBe(true);

    // 2. Delete user
    customFetchImpl.mockResolvedValueOnce(
      new Response(JSON.stringify({ success: true })),
    );

    await authClient.deleteUser();

    expect(authSecretStorage.isAuthenticated).toBe(false);

    const anonymousCredentials = await authSecretStorage.get();
    expect(anonymousCredentials).not.toMatchObject(credentials!);
  });

  it("should send Jazz credentials using social login", async () => {
    const credentials = await authSecretStorage.get();
    assert(credentials, "Jazz credentials are not available");

    customFetchImpl.mockResolvedValue(new Response(JSON.stringify({})));

    // Sign up
    await authClient.signIn.social({
      provider: "github",
    });

    expect(customFetchImpl).toHaveBeenCalledTimes(1);
    expect(customFetchImpl.mock.calls[0]![0].toString()).toBe(
      "http://localhost:3000/api/auth/sign-in/social",
    );

    // Verify the credentials have been injected in the request body
    expect(
      customFetchImpl.mock.calls[0]![1].headers.get("x-jazz-auth")!,
    ).toEqual(
      JSON.stringify({
        accountID: credentials!.accountID,
        secretSeed: credentials!.secretSeed,
        accountSecret: credentials!.accountSecret,
      }),
    );
  });

  it("should send Jazz credentials using oauth generic plugin", async () => {
    const credentials = await authSecretStorage.get();
    assert(credentials, "Jazz credentials are not available");

    customFetchImpl.mockResolvedValue(new Response(JSON.stringify({})));

    // Sign up
    await authClient.signIn.oauth2({
      providerId: "github",
    });

    expect(customFetchImpl).toHaveBeenCalledTimes(1);
    expect(customFetchImpl.mock.calls[0]![0].toString()).toBe(
      "http://localhost:3000/api/auth/sign-in/oauth2",
    );

    // Verify the credentials have been injected in the request body
    expect(
      customFetchImpl.mock.calls[0]![1].headers.get("x-jazz-auth")!,
    ).toEqual(
      JSON.stringify({
        accountID: credentials!.accountID,
        secretSeed: credentials!.secretSeed,
        accountSecret: credentials!.accountSecret,
      }),
    );
  });

  it("should send Jazz credentials using email OTP", async () => {
    const credentials = await authSecretStorage.get();
    assert(credentials, "Jazz credentials are not available");

    customFetchImpl.mockResolvedValue(new Response(JSON.stringify({})));

    // Sign up
    await authClient.emailOtp.sendVerificationOtp({
      email: "test@jazz.dev",
      type: "sign-in",
    });

    expect(customFetchImpl).toHaveBeenCalledTimes(1);
    expect(customFetchImpl.mock.calls[0]![0].toString()).toBe(
      "http://localhost:3000/api/auth/email-otp/send-verification-otp",
    );

    // Verify the credentials have been injected in the request body
    expect(
      customFetchImpl.mock.calls[0]![1].headers.get("x-jazz-auth")!,
    ).toEqual(
      JSON.stringify({
        accountID: credentials!.accountID,
        secretSeed: credentials!.secretSeed,
        accountSecret: credentials!.accountSecret,
      }),
    );
  });

  it("should log out from Jazz when the session is null", async () => {
    const credentials = await authSecretStorage.get();
    assert(credentials, "Jazz credentials are not available");

    await jazzContextManager.authenticate({
      ...credentials,
      provider: "better-auth",
    });
    await authSecretStorage.set({
      ...credentials,
      provider: "better-auth",
    });

    expect(authSecretStorage.isAuthenticated).toBe(true);

    customFetchImpl.mockResolvedValue(new Response(JSON.stringify(null)));

    await authClient.getSession();

    expect(authSecretStorage.isAuthenticated).toBe(false);
  });

  describe("Race condition handling", () => {
    it("should handle multiple concurrent get-session calls without errors", async () => {
      const credentials = await authSecretStorage.get();
      assert(credentials, "Jazz credentials are not available");

      // Mock multiple get-session responses with fresh Response objects
      customFetchImpl.mockImplementation(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "YW5kcmVpYnVkb2k",
                email: "test@jazz.dev",
                name: "andreibudoi",
                accountID: credentials.accountID,
              },
              jazzAuth: {
                accountID: credentials.accountID,
                secretSeed: credentials.secretSeed,
                accountSecret: credentials.accountSecret,
                provider: "better-auth",
              },
            }),
          ),
        ),
      );

      // Simulate multiple concurrent get-session calls (like during OAuth)
      const promises = [];
      for (let i = 0; i < 5; i++) {
        promises.push(authClient.$fetch("/get-session", { method: "GET" }));
      }

      // Should complete without errors due to race condition handling
      await expect(Promise.all(promises)).resolves.toBeDefined();

      // Should have been called multiple times by better-auth
      expect(customFetchImpl).toHaveBeenCalledTimes(5);

      // User should be authenticated
      expect(authSecretStorage.isAuthenticated).toBe(true);
    });

    it("should handle credentials mismatch scenario without errors", async () => {
      const originalCredentials = await authSecretStorage.get();
      assert(originalCredentials, "Jazz credentials are not available");

      // Create a test account for the mismatch scenario
      const testAccount = await createJazzTestAccount();

      // Mock get-session response with different account using fresh Response objects
      customFetchImpl.mockImplementation(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "RGlmZmVyZW50IFVzZXI",
                email: "different@jazz.dev",
                name: "Different User",
                accountID: testAccount.$jazz.id,
              },
              jazzAuth: {
                accountID: testAccount.$jazz.id,
                secretSeed: new Uint8Array([4, 5, 6]),
                accountSecret:
                  testAccount.$jazz.localNode.getCurrentAgent().agentSecret,
                provider: "better-auth",
              },
            }),
          ),
        ),
      );

      // Simulate multiple concurrent get-session calls with mismatched credentials
      const promises = [];
      for (let i = 0; i < 3; i++) {
        promises.push(authClient.$fetch("/get-session", { method: "GET" }));
      }

      // Should complete without errors despite credential mismatch
      await expect(Promise.all(promises)).resolves.toBeDefined();

      // Should have been called multiple times by better-auth
      expect(customFetchImpl).toHaveBeenCalledTimes(3);

      // Should be authenticated with the new account
      expect(authSecretStorage.isAuthenticated).toBe(true);
      const currentCredentials = await authSecretStorage.get();
      expect(currentCredentials?.accountID).toBe(testAccount.$jazz.id);
    });

    it("should allow authentication after sign out without being blocked", async () => {
      const credentials = await authSecretStorage.get();
      assert(credentials, "Jazz credentials are not available");

      const getSessionResponseData = {
        user: {
          id: "123",
          accountID: credentials.accountID,
        },
        jazzAuth: {
          accountID: credentials.accountID,
          secretSeed: credentials.secretSeed,
          accountSecret: credentials.accountSecret,
          provider: "better-auth",
        },
      };

      // First authenticate
      customFetchImpl.mockResolvedValueOnce(
        new Response(JSON.stringify(getSessionResponseData)),
      );

      await authClient.$fetch("/get-session", { method: "GET" });
      expect(authSecretStorage.isAuthenticated).toBe(true);

      // Then sign out
      customFetchImpl.mockResolvedValueOnce(
        new Response(JSON.stringify({ success: true })),
      );

      await authClient.signOut();
      expect(authSecretStorage.isAuthenticated).toBe(false);

      // Authenticating again should work without being blocked
      customFetchImpl.mockResolvedValueOnce(
        new Response(JSON.stringify(getSessionResponseData)),
      );

      // Should complete without hanging or errors
      await expect(
        authClient.$fetch("/get-session", { method: "GET" }),
      ).resolves.toBeDefined();

      expect(authSecretStorage.isAuthenticated).toBe(true);
    });

    it("should fail fast when trying to authenticate different accounts concurrently", async () => {
      const originalCredentials = await authSecretStorage.get();
      assert(originalCredentials, "Jazz credentials are not available");

      const testAccount1 = await createJazzTestAccount();
      const testAccount2 = await createJazzTestAccount();
      const testAccount3 = await createJazzTestAccount();

      const accounts = [testAccount1, testAccount2, testAccount3];
      let callCount = 0;

      customFetchImpl.mockImplementation(() => {
        const accountIndex = callCount % 3;
        const account = accounts[accountIndex]!;
        callCount++;

        return Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: `user-${accountIndex + 1}`,
                email: `user${accountIndex + 1}@jazz.dev`,
                name: `User ${accountIndex + 1}`,
                accountID: account.$jazz.id,
              },
              jazzAuth: {
                accountID: account.$jazz.id,
                secretSeed: new Uint8Array([
                  accountIndex + 1,
                  accountIndex + 2,
                  accountIndex + 3,
                ]),
                accountSecret:
                  account.$jazz.localNode.getCurrentAgent().agentSecret,
                provider: "better-auth",
              },
            }),
          ),
        );
      });

      const promises = [];
      for (let i = 0; i < 3; i++) {
        promises.push(authClient.$fetch("/get-session", { method: "GET" }));
      }

      await expect(Promise.all(promises)).rejects.toThrow();

      expect(customFetchImpl).toHaveBeenCalledTimes(3);
    });

    it("should deduplicate auth requests for the same account", async () => {
      const credentials = await authSecretStorage.get();
      assert(credentials, "Jazz credentials are not available");

      customFetchImpl.mockImplementation(() =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "test-user",
                email: "test@jazz.dev",
                name: "Test User",
                accountID: credentials.accountID,
              },
              jazzAuth: {
                accountID: credentials.accountID,
                secretSeed: credentials.secretSeed,
                accountSecret: credentials.accountSecret,
                provider: "better-auth",
              },
            }),
          ),
        ),
      );

      const promises = [];
      for (let i = 0; i < 3; i++) {
        promises.push(authClient.$fetch("/get-session", { method: "GET" }));
      }

      await expect(Promise.all(promises)).resolves.toBeDefined();

      expect(customFetchImpl).toHaveBeenCalledTimes(3);

      expect(authSecretStorage.isAuthenticated).toBe(true);
      const finalCredentials = await authSecretStorage.get();
      expect(finalCredentials?.accountID).toBe(credentials.accountID);
    });
  });
});
