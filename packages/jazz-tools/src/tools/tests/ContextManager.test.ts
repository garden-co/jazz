import { StorageAPI } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  Account,
  AccountClass,
  AuthSecretStorage,
  Group,
  InMemoryKVStore,
  JazzAuthContext,
  KvStoreContext,
  co,
  z,
} from "../exports";
import {
  JazzContextManager,
  JazzContextManagerAuthProps,
  JazzContextManagerBaseProps,
} from "../implementation/ContextManager";
import {
  createJazzContext,
  randomSessionProvider,
} from "../implementation/createContext";
import {
  CoValueFromRaw,
  InstanceOfSchema,
  Loaded,
  coValueClassFromCoValueClassOrSchema,
} from "../internal";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing";
import { createAsyncStorage, getDbPath } from "./testStorage";

const Crypto = await WasmCrypto.create();

class TestJazzContextManager<Acc extends Account> extends JazzContextManager<
  Acc,
  JazzContextManagerBaseProps<Acc> & {
    defaultProfileName?: string;
    AccountSchema?: AccountClass<Acc>;
    storage?: string;
  }
> {
  async getNewContext(
    props: JazzContextManagerBaseProps<Acc> & {
      defaultProfileName?: string;
      AccountSchema?: AccountClass<Acc> & CoValueFromRaw<Acc>;
      storage?: string;
    },
    authProps?: JazzContextManagerAuthProps,
  ) {
    const context = await createJazzContext({
      credentials: authProps?.credentials,
      defaultProfileName: props.defaultProfileName,
      newAccountProps: authProps?.newAccountProps,
      peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
      crypto: Crypto,
      sessionProvider: randomSessionProvider,
      authSecretStorage: this.getAuthSecretStorage(),
      AccountSchema: props.AccountSchema,
      storage: await createAsyncStorage({ filename: props.storage }),
    });

    return {
      me: context.account,
      node: context.node,
      done: () => {
        context.done();
      },
      logOut: async () => {
        await context.logOut();
      },
      addConnectionListener: () => () => {},
      connected: () => false,
    };
  }
}

describe("ContextManager", () => {
  let manager: TestJazzContextManager<Account>;
  let authSecretStorage: AuthSecretStorage;

  function getCurrentValue() {
    return manager.getCurrentValue() as JazzAuthContext<Account>;
  }

  beforeEach(async () => {
    KvStoreContext.getInstance().initialize(new InMemoryKVStore());
    authSecretStorage = new AuthSecretStorage();
    await authSecretStorage.clear();
    await setupJazzTestSync();

    manager = new TestJazzContextManager<Account>();
  });

  test("creates new context when initialized", async () => {
    await manager.createContext({});

    const context = getCurrentValue();

    expect(context.me.profile?.name).toBe("Anonymous user");
    expect(context.node).toBeDefined();
    expect(manager.getCurrentValue()).toBeDefined();
  });

  test("creates new context when initialized with default profile name", async () => {
    await manager.createContext({
      defaultProfileName: "Test User",
    });

    const context = getCurrentValue();

    expect(context.me.profile?.name).toBe("Test User");
    expect(context.node).toBeDefined();
    expect(manager.getCurrentValue()).toBeDefined();
  });

  test("handles authentication with credentials", async () => {
    const account = await createJazzTestAccount();

    // First create an initial context to get credentials
    await manager.createContext({});

    const credentials = {
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    };

    // Authenticate with those credentials
    await manager.authenticate(credentials);

    expect(getCurrentValue().me.$jazz.id).toBe(credentials.accountID);
  });

  test("handles race conditions on the context creation", async () => {
    const account = await createJazzTestAccount();

    manager.createContext({});

    const credentials = {
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    };

    // Authenticate without waiting for the previous context to be created
    await manager.authenticate(credentials);

    expect(getCurrentValue().me.$jazz.id).toBe(credentials.accountID);
  });

  test("calls onLogOut callback when logging out", async () => {
    const onLogOut = vi.fn();
    await manager.createContext({ onLogOut });

    await manager.logOut();

    expect(onLogOut).toHaveBeenCalled();
  });

  test("calls logoutReplacement callback instead of the Jazz logout when logging out", async () => {
    const logOutReplacement = vi.fn();
    await manager.createContext({ logOutReplacement });

    const context = manager.getCurrentValue();

    await manager.logOut();

    expect(logOutReplacement).toHaveBeenCalled();
    expect(manager.getCurrentValue()).toBe(context);
  });

  test("notifies listeners of context changes", async () => {
    const listener = vi.fn();
    manager.subscribe(listener);

    await manager.createContext({});

    expect(listener).toHaveBeenCalled();
  });

  test("cleans up context when done", async () => {
    await manager.createContext({});

    const context = manager.getCurrentValue();
    expect(context).toBeDefined();

    manager.done();

    // Should still have context, just cleaned up
    expect(manager.getCurrentValue()).toBe(context);
  });

  test("calls onAnonymousAccountDiscarded when authenticating from anonymous user", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    const account = await createJazzTestAccount();

    // Create initial anonymous context
    await manager.createContext({ onAnonymousAccountDiscarded });
    const anonymousAccount = getCurrentValue().me;

    // Authenticate with credentials
    await manager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Verify callback was called with the anonymous account
    expect(onAnonymousAccountDiscarded).toHaveBeenCalledWith(anonymousAccount);
  });

  test("does not call onAnonymousAccountDiscarded when authenticating from authenticated user", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    const account = await createJazzTestAccount();

    await manager.getAuthSecretStorage().set({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Create initial authenticated context
    await manager.createContext({ onAnonymousAccountDiscarded });

    // Authenticate with same credentials
    await manager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Verify callback was not called
    expect(onAnonymousAccountDiscarded).not.toHaveBeenCalled();
  });

  test("onAnonymousAccountDiscarded should not block the authentication when storage is active", async () => {
    const dbFilename = getDbPath();

    const AccountRoot = co.map({
      value: z.string(),
      get transferredRoot(): co.Optional<typeof AccountRoot> {
        return co.optional(AccountRoot);
      },
    });

    let lastRootId: string | undefined;

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration(async (account) => {
        account.$jazz.set(
          "root",
          AccountRoot.create(
            {
              value: "Hello",
            },
            Group.create(this).makePublic(),
          ),
        );
      });

    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    await customManager.createContext({
      AccountSchema: coValueClassFromCoValueClassOrSchema(CustomAccount),
      storage: dbFilename,
      onAnonymousAccountDiscarded: async (anonymousAccount) => {
        const anonymousAccountWithRoot =
          await anonymousAccount.$jazz.ensureLoaded({
            resolve: { root: true },
          });

        const me = await CustomAccount.getMe().$jazz.ensureLoaded({
          resolve: { root: true },
        });

        me.root.$jazz.set("transferredRoot", anonymousAccountWithRoot.root);
      },
    });

    const prevContextNode = customManager.getCurrentValue()!.node;

    expect(prevContextNode.storage).toBeDefined();

    const account = (
      customManager.getCurrentValue() as JazzAuthContext<
        InstanceOfSchema<typeof CustomAccount>
      >
    ).me;

    await customManager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // The storage should be closed and set to undefined
    expect(prevContextNode.storage).toBeUndefined();

    const me = await CustomAccount.getMe().$jazz.ensureLoaded({
      resolve: { root: { transferredRoot: true } },
    });

    expect(me.root.transferredRoot?.value).toBe("Hello");
  });

  test("the migration should be applied correctly on existing accounts", async () => {
    const AccountRoot = co.map({
      value: z.string(),
    });

    let lastRootId: string | undefined;

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration(async (account: Account) => {
        account.$jazz.set("root", {
          value: "Hello",
        });
        lastRootId = account.root!.$jazz.id;
      });

    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      AccountSchema: coValueClassFromCoValueClassOrSchema(CustomAccount),
    });

    const account = (
      customManager.getCurrentValue() as JazzAuthContext<
        InstanceOfSchema<typeof CustomAccount>
      >
    ).me;

    await customManager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().$jazz.ensureLoaded({
      resolve: { root: true },
    });

    expect(me.root.$jazz.id).toBe(lastRootId);
  });

  test("the migration should be applied correctly on existing accounts (2)", async () => {
    const AccountRoot = co.map({
      value: z.number(),
    });

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration(async (account) => {
        if (!account.$jazz.has("root")) {
          account.$jazz.set(
            "root",
            AccountRoot.create({
              value: 1,
            }),
          );
        } else {
          const { root } = await account.$jazz.ensureLoaded({
            resolve: { root: true },
          });

          root.$jazz.set("value", 2);
        }
      });
    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      AccountSchema: coValueClassFromCoValueClassOrSchema(CustomAccount),
    });

    const account = (
      customManager.getCurrentValue() as JazzAuthContext<
        InstanceOfSchema<typeof CustomAccount>
      >
    ).me;

    await customManager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().$jazz.ensureLoaded({
      resolve: { root: true },
    });

    expect(me.root.value).toBe(2);
  });

  test("onAnonymousAccountDiscarded should work on transfering data between accounts", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      get transferredRoot(): co.Optional<typeof AccountRoot> {
        return co.optional(AccountRoot);
      },
    });

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration(async (account) => {
        if (!account.$jazz.has("root")) {
          account.$jazz.set(
            "root",
            AccountRoot.create({
              value: "Hello",
            }),
          );
        }
      });

    const onAnonymousAccountDiscarded = async (
      anonymousAccount: Loaded<typeof CustomAccount, { root: true }>,
    ) => {
      const anonymousAccountWithRoot =
        await anonymousAccount.$jazz.ensureLoaded({
          resolve: {
            root: true,
          },
        });

      const meWithRoot = await CustomAccount.getMe().$jazz.ensureLoaded({
        resolve: {
          root: true,
        },
      });

      const rootToTransfer = anonymousAccountWithRoot.root;

      await rootToTransfer.$jazz.owner.addMember(meWithRoot, "admin");

      meWithRoot.root.$jazz.set("transferredRoot", rootToTransfer);
    };

    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      onAnonymousAccountDiscarded,
      AccountSchema: coValueClassFromCoValueClassOrSchema(CustomAccount),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      AccountSchema: CustomAccount,
    });

    await customManager.authenticate({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().$jazz.ensureLoaded({
      resolve: {
        root: true,
      },
    });

    expect(me.root.transferredRoot?.value).toBe("Hello");
  });

  test("handles registration of new account", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    await manager.createContext({ onAnonymousAccountDiscarded });

    const secret = Crypto.newRandomAgentSecret();
    const accountId = await manager.register(secret, { name: "Test User" });

    expect(accountId).toBeDefined();
    const context = getCurrentValue();
    expect(context.me.profile?.name).toBe("Test User");
    expect(context.me.$jazz.id).toBe(accountId);
  });

  test("calls onAnonymousAccountDiscarded when registering from anonymous user", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    await manager.createContext({ onAnonymousAccountDiscarded });
    const anonymousAccount = getCurrentValue().me;

    const secret = Crypto.newRandomAgentSecret();
    await manager.register(secret, { name: "Test User" });

    expect(onAnonymousAccountDiscarded).toHaveBeenCalledWith(anonymousAccount);
  });

  test("does not call onAnonymousAccountDiscarded when registering from authenticated user", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    const account = await createJazzTestAccount();

    await manager.getAuthSecretStorage().set({
      accountID: account.$jazz.id,
      accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
      provider: "test",
    });

    await manager.createContext({ onAnonymousAccountDiscarded });

    const secret = Crypto.newRandomAgentSecret();
    await manager.register(secret, { name: "New User" });

    expect(onAnonymousAccountDiscarded).not.toHaveBeenCalled();
  });

  test("throws error when registering without props", async () => {
    const secret = Crypto.newRandomAgentSecret();
    await expect(
      manager.register(secret, { name: "Test User" }),
    ).rejects.toThrow("Props required");
  });

  describe("configurable storage key", () => {
    test("uses the configured storage key", async () => {
      const KEY = "test-auth-secret";
      const manager = new TestJazzContextManager<Account>({
        authSecretStorageKey: KEY,
      });
      expect(manager.getAuthSecretStorage().getStorageKey()).toBe(KEY);
    });
  });

  describe("Race condition handling", () => {
    test("prevents concurrent authentication attempts", async () => {
      const account = await createJazzTestAccount();
      const onAnonymousAccountDiscarded = vi.fn();

      // Create initial anonymous context
      await manager.createContext({ onAnonymousAccountDiscarded });

      const credentials = {
        accountID: account.$jazz.id,
        accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      // Start multiple concurrent authentication attempts
      const promises = [];
      for (let i = 0; i < 5; i++) {
        promises.push(manager.authenticate(credentials));
      }

      await Promise.all(promises);

      // onAnonymousAccountDiscarded should only be called once despite multiple authenticate calls
      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(1);
    });

    test("prevents concurrent registration attempts", async () => {
      const onAnonymousAccountDiscarded = vi.fn();
      await manager.createContext({ onAnonymousAccountDiscarded });

      const secret = Crypto.newRandomAgentSecret();

      // Start multiple concurrent registration attempts
      const promises = [];
      for (let i = 0; i < 3; i++) {
        promises.push(manager.register(secret, { name: "Test User" }));
      }

      await Promise.allSettled(promises);

      // onAnonymousAccountDiscarded should only be called once
      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(1);
    });

    test("allows authentication after logout", async () => {
      const account = await createJazzTestAccount();
      const onAnonymousAccountDiscarded = vi.fn();

      // Create initial context and authenticate
      await manager.createContext({ onAnonymousAccountDiscarded });

      await manager.authenticate({
        accountID: account.$jazz.id,
        accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      });

      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(1);

      await manager.logOut();

      // Should be able to authenticate again
      const account2 = await createJazzTestAccount();

      await manager.authenticate({
        accountID: account2.$jazz.id,
        accountSecret: account2.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      });

      // Should be called again since we logged out and reset the migration state
      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(2);
    });

    test("handles authentication in progress correctly", async () => {
      const account = await createJazzTestAccount();
      const onAnonymousAccountDiscarded = vi.fn();

      await manager.createContext({ onAnonymousAccountDiscarded });

      const credentials = {
        accountID: account.$jazz.id,
        accountSecret: account.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      // Start first authentication
      const firstAuth = manager.authenticate(credentials);

      // Try to authenticate while first is in progress
      await manager.authenticate(credentials);

      // Wait for first to complete
      await firstAuth;

      // Should only have been called once
      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(1);
    });

    test("prevents duplicate onAnonymousAccountDiscarded calls during complex migration", async () => {
      const AccountRoot = co.map({
        value: z.string(),
        get transferredRoot(): co.Optional<typeof AccountRoot> {
          return co.optional(AccountRoot);
        },
      });

      const CustomAccount = co
        .account({
          root: AccountRoot,
          profile: co.profile(),
        })
        .withMigration(async (account) => {
          account.$jazz.set(
            "root",
            AccountRoot.create(
              { value: "Hello" },
              Group.create(this).makePublic(),
            ),
          );
        });

      const customManager = new TestJazzContextManager<
        InstanceOfSchema<typeof CustomAccount>
      >();

      const onAnonymousAccountDiscarded = vi
        .fn()
        .mockImplementation(async (anonymousAccount) => {
          // Simulate complex migration work
          await new Promise((resolve) => setTimeout(resolve, 10));

          const anonymousAccountWithRoot =
            await anonymousAccount.$jazz.ensureLoaded({
              resolve: { root: true },
            });

          const me = await CustomAccount.getMe().$jazz.ensureLoaded({
            resolve: { root: true },
          });

          me.root.$jazz.set("transferredRoot", anonymousAccountWithRoot.root);
        });

      await customManager.createContext({
        AccountSchema: coValueClassFromCoValueClassOrSchema(CustomAccount),
        onAnonymousAccountDiscarded,
      });

      const account = (
        customManager.getCurrentValue() as JazzAuthContext<
          InstanceOfSchema<typeof CustomAccount>
        >
      ).me;

      // Start multiple concurrent authentication attempts
      const promises = [];
      for (let i = 0; i < 3; i++) {
        promises.push(
          customManager.authenticate({
            accountID: account.$jazz.id,
            accountSecret:
              account.$jazz.localNode.getCurrentAgent().agentSecret,
            provider: "test",
          }),
        );
      }

      await Promise.all(promises);

      // Migration should only happen once
      expect(onAnonymousAccountDiscarded).toHaveBeenCalledTimes(1);

      const me = await CustomAccount.getMe().$jazz.ensureLoaded({
        resolve: { root: { transferredRoot: true } },
      });

      expect(me.root.transferredRoot?.value).toBe("Hello");
    });

    test("fails fast when trying to authenticate different accounts concurrently", async () => {
      const account1 = await createJazzTestAccount();
      const account2 = await createJazzTestAccount();

      await manager.createContext({});

      const credentials1 = {
        accountID: account1.$jazz.id,
        accountSecret: account1.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      const credentials2 = {
        accountID: account2.$jazz.id,
        accountSecret: account2.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      const auth1Promise = manager.authenticate(credentials1);

      // Try to authenticate account2 while account1 is in progress
      await expect(manager.authenticate(credentials2)).rejects.toThrow();

      // First authentication should still complete successfully
      await expect(auth1Promise).resolves.toBeUndefined();

      // After first auth completes, second account should be able to authenticate
      await expect(manager.authenticate(credentials2)).resolves.toBeUndefined();
    });

    test("throws error when authenticating with different credentials simultaneously", async () => {
      const account1 = await createJazzTestAccount();
      const account2 = await createJazzTestAccount();

      await manager.createContext({});

      const credentials1 = {
        accountID: account1.$jazz.id,
        accountSecret: account1.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      const credentials2 = {
        accountID: account2.$jazz.id,
        accountSecret: account2.$jazz.localNode.getCurrentAgent().agentSecret,
        provider: "test",
      };

      const results = await Promise.allSettled([
        manager.authenticate(credentials1),
        manager.authenticate(credentials2),
      ]);

      // One should succeed and one should fail
      const successCount = results.filter(
        (r) => r.status === "fulfilled",
      ).length;
      const failureCount = results.filter(
        (r) => r.status === "rejected",
      ).length;

      expect(successCount).toBe(1);
      expect(failureCount).toBe(1);

      // Verify the successful authentication resulted in a valid context
      const currentAccount = getCurrentValue().me;
      expect([credentials1.accountID, credentials2.accountID]).toContain(
        currentAccount.$jazz.id,
      );
    });
  });
});
