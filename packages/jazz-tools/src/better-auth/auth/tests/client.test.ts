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

describe("auth client", () => {
  let account: Account;
  let jazzContextManager: TestJazzContextManager<Account>;
  let authSecretStorage: AuthSecretStorage;
  let authClient: ReturnType<
    typeof createAuthClient<{
      plugins: ReturnType<typeof jazzPluginClient>[];
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
      plugins: [jazzPluginClient()],
      fetchOptions: {
        customFetchImpl,
      },
    });

    authClient.jazz.setJazzContext(jazzContextManager.getCurrentValue()!);
    authClient.jazz.setAuthSecretStorage(authSecretStorage);

    customFetchImpl.mockReset();
  });

  it("should send Jazz credentials over signup", async () => {
    const credentials = await authSecretStorage.get();
    expect(authSecretStorage.isAuthenticated).toBe(false);

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
            accountID: credentials!.accountID,
            secretSeed: credentials!.secretSeed,
            accountSecret: credentials!.accountSecret,
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
    expect(context.me.id).toBe(credentials!.accountID);
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

  it.todo("should logout from Better Auth after Jazz's log-out");
});
