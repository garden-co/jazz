import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { beforeEach, describe, expect, test, vi } from "vitest";
import {
  Account,
  AccountClass,
  AuthSecretStorage,
  CoMap,
  Group,
  InMemoryKVStore,
  JazzAuthContext,
  KvStoreContext,
  co,
  coField,
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
  zodSchemaToCoSchema,
} from "../internal";
import {
  createJazzTestAccount,
  getPeerConnectedToTestSyncServer,
  setupJazzTestSync,
} from "../testing";

const Crypto = await WasmCrypto.create();

class TestJazzContextManager<Acc extends Account> extends JazzContextManager<
  Acc,
  JazzContextManagerBaseProps<Acc> & {
    defaultProfileName?: string;
    AccountSchema?: AccountClass<Acc>;
  }
> {
  async getNewContext(
    props: JazzContextManagerBaseProps<Acc> & {
      defaultProfileName?: string;
      AccountSchema?: AccountClass<Acc> & CoValueFromRaw<Acc>;
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
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    };

    // Authenticate with those credentials
    await manager.authenticate(credentials);

    expect(getCurrentValue().me.id).toBe(credentials.accountID);
  });

  test("handles race conditions on the context creation", async () => {
    const account = await createJazzTestAccount();

    manager.createContext({});

    const credentials = {
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    };

    // Authenticate without waiting for the previous context to be created
    await manager.authenticate(credentials);

    expect(getCurrentValue().me.id).toBe(credentials.accountID);
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
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Verify callback was called with the anonymous account
    expect(onAnonymousAccountDiscarded).toHaveBeenCalledWith(anonymousAccount);
  });

  test("does not call onAnonymousAccountDiscarded when authenticating from authenticated user", async () => {
    const onAnonymousAccountDiscarded = vi.fn();
    const account = await createJazzTestAccount();

    await manager.getAuthSecretStorage().set({
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Create initial authenticated context
    await manager.createContext({ onAnonymousAccountDiscarded });

    // Authenticate with same credentials
    await manager.authenticate({
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    // Verify callback was not called
    expect(onAnonymousAccountDiscarded).not.toHaveBeenCalled();
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
      .withMigration(async (account) => {
        account.root = AccountRoot.create({
          value: "Hello",
        });
        lastRootId = account.root.id;
      });

    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
    });

    const account = (
      customManager.getCurrentValue() as JazzAuthContext<
        InstanceOfSchema<typeof CustomAccount>
      >
    ).me;

    console.log("before", account._refs.root?.id);

    await customManager.authenticate({
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().ensureLoaded({
      resolve: { root: true },
    });

    console.log("after", me._refs.root?.id);

    expect(me.root.id).toBe(lastRootId);
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
        if (account.root === undefined) {
          account.root = AccountRoot.create({
            value: 1,
          });
        } else {
          const { root } = await account.ensureLoaded({
            resolve: { root: true },
          });

          root.value = 2;
        }
      });
    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
    });

    const account = (
      customManager.getCurrentValue() as JazzAuthContext<
        InstanceOfSchema<typeof CustomAccount>
      >
    ).me;

    await customManager.authenticate({
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().ensureLoaded({
      resolve: { root: true },
    });

    expect(me.root.value).toBe(2);
  });

  test("onAnonymousAccountDiscarded should work on transfering data between accounts", async () => {
    const AccountRoot = co.map({
      value: z.string(),
      get transferredRoot(): z.ZodOptional<typeof AccountRoot> {
        return z.optional(AccountRoot);
      },
    });

    const CustomAccount = co
      .account({
        root: AccountRoot,
        profile: co.profile(),
      })
      .withMigration(async (account) => {
        if (account.root === undefined) {
          account.root = AccountRoot.create({
            value: "Hello",
          });
        }
      });

    const onAnonymousAccountDiscarded = async (
      anonymousAccount: Loaded<typeof CustomAccount, { root: true }>,
    ) => {
      const anonymousAccountWithRoot = await anonymousAccount.ensureLoaded({
        resolve: {
          root: true,
        },
      });

      const meWithRoot = await CustomAccount.getMe().ensureLoaded({
        resolve: {
          root: true,
        },
      });

      const rootToTransfer = anonymousAccountWithRoot.root;

      await rootToTransfer._owner.castAs(Group).addMember(meWithRoot, "admin");

      meWithRoot.root.transferredRoot = rootToTransfer;
    };

    const customManager = new TestJazzContextManager<
      InstanceOfSchema<typeof CustomAccount>
    >();

    // Create initial anonymous context
    await customManager.createContext({
      onAnonymousAccountDiscarded,
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
    });

    const account = await createJazzTestAccount({
      isCurrentActiveAccount: true,
      AccountSchema: zodSchemaToCoSchema(CustomAccount),
    });

    await customManager.authenticate({
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
      provider: "test",
    });

    const me = await CustomAccount.getMe().ensureLoaded({
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
    expect(context.me.id).toBe(accountId);
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
      accountID: account.id,
      accountSecret: account._raw.core.node.getCurrentAgent().agentSecret,
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
});
