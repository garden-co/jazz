import { LocalNode } from "cojson";
import { cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";
import {
  Account,
  RegisteredSchemas,
  type AnonymousJazzAgent,
  AuthCredentials,
  CoValueFromRaw,
  CoreAccountSchema,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextManagerAuthProps,
  JazzContextManagerBaseProps,
  activeAccountContext,
  co,
  createAnonymousJazzContext,
  createJazzContext,
  randomSessionProvider,
  Loaded,
  PlatformSpecificContext,
  asConstructable,
  AccountSchema,
} from "./internal.js";

export { assertLoaded } from "./lib/utils.js";

const syncServer: { current: LocalNode | null; asyncPeers: boolean } = {
  current: null,
  asyncPeers: false,
};

export class TestJSCrypto extends PureJSCrypto {
  static async create() {
    if ("navigator" in globalThis && navigator.userAgent?.includes("jsdom")) {
      // Mocking crypto seal & encrypt to make it work with JSDom. Getting "Error: Uint8Array expected" there
      const crypto = new PureJSCrypto();

      crypto.seal = (options) =>
        `sealed_U${cojsonInternals.stableStringify(options.message)}` as any;
      crypto.unseal = (sealed) =>
        JSON.parse(sealed.substring("sealed_U".length));
      crypto.encrypt = (message) =>
        `encrypted_U${cojsonInternals.stableStringify(message)}` as any;
      crypto.decryptRaw = (encrypted) =>
        encrypted.substring("encrypted_U".length) as any;

      return crypto;
    }

    // For non-jsdom environments, we use the real crypto
    return new PureJSCrypto();
  }
}

export function getPeerConnectedToTestSyncServer() {
  if (!syncServer.current) {
    throw new Error("Sync server not initialized");
  }

  const [aPeer, bPeer] = cojsonInternals.connectedPeers(
    Math.random().toString(),
    Math.random().toString(),
    {
      peer1role: "client",
      peer2role: "server",
    },
  );

  if (syncServer.asyncPeers) {
    const push = aPeer.outgoing.push;

    aPeer.outgoing.push = (message) => {
      setTimeout(() => {
        push.call(aPeer.outgoing, message);
      });
    };

    bPeer.outgoing.push = (message) => {
      setTimeout(() => {
        push.call(bPeer.outgoing, message);
      });
    };
  }

  syncServer.current.syncManager.addPeer(aPeer);

  return bPeer;
}

const SecretSeedMap = new Map<string, Uint8Array>();
let isMigrationActive = false;

export async function createJazzTestAccount<
  S extends CoreAccountSchema = CoreAccountSchema,
>(options?: {
  isCurrentActiveAccount?: boolean;
  AccountSchema?: S;
  creationProps?: Record<string, unknown>;
}): Promise<InstanceOfSchema<S>> {
  const AccountSchemaToUse = options?.AccountSchema
    ? asConstructable(options.AccountSchema)
    : co.account();
  const peers = [];
  if (syncServer.current) {
    peers.push(getPeerConnectedToTestSyncServer());
  }

  const crypto = await TestJSCrypto.create();
  const secretSeed = crypto.newRandomSecretSeed();

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps: {
      name: "Test Account",
      ...options?.creationProps,
    },
    initialAgentSecret: crypto.agentSecretFromSecretSeed(secretSeed),
    crypto,
    peers: peers,
    migration: async (rawAccount, _node, creationProps) => {
      if (isMigrationActive) {
        throw new Error(
          "It is not possible to create multiple accounts in parallel inside the test environment.",
        );
      }

      isMigrationActive = true;

      const account = AccountSchemaToUse.fromRaw(rawAccount);

      // We need to set the account as current because the migration
      // will probably rely on the global me
      const prevActiveAccount = activeAccountContext.maybeGet();
      activeAccountContext.set(account);

      await account.applyMigration?.(creationProps);

      if (!options?.isCurrentActiveAccount) {
        activeAccountContext.set(prevActiveAccount);
      }

      isMigrationActive = false;
    },
  });

  const account = co.account().fromNode(node);
  SecretSeedMap.set(account.$jazz.id, secretSeed);

  if (options?.isCurrentActiveAccount) {
    activeAccountContext.set(account);
  }

  return account as InstanceOfSchema<S>;
}

export function setActiveAccount(account: Account) {
  activeAccountContext.set(account);
}

/**
 * Run a callback without an active account.
 *
 * Takes care of restoring the active account after the callback is run.
 *
 * If the callback returns a promise, waits for it before restoring the active account.
 *
 * @param callback - The callback to run.
 * @returns The result of the callback.
 */
export function runWithoutActiveAccount<Result>(
  callback: () => Result,
): Result {
  const me = Account.getMe();
  activeAccountContext.set(null);
  const result = callback();

  if (result instanceof Promise) {
    return result.finally(() => {
      activeAccountContext.set(me);
      return result;
    }) as Result;
  }

  activeAccountContext.set(me);
  return result;
}

export async function createJazzTestGuest() {
  const ctx = await createAnonymousJazzContext({
    crypto: await PureJSCrypto.create(),
    peers: [],
  });

  return {
    guest: ctx.agent,
  };
}

export class MockConnectionStatus {
  static connected: boolean = true;
  static connectionListeners = new Set<(isConnected: boolean) => void>();
  static setIsConnected(isConnected: boolean) {
    MockConnectionStatus.connected = isConnected;
    for (const listener of MockConnectionStatus.connectionListeners) {
      listener(isConnected);
    }
  }
  static addConnectionListener(listener: (isConnected: boolean) => void) {
    MockConnectionStatus.connectionListeners.add(listener);
    return () => {
      MockConnectionStatus.connectionListeners.delete(listener);
    };
  }
}

export type TestJazzContextManagerProps<Acc extends CoreAccountSchema> =
  JazzContextManagerBaseProps<Acc> & {
    defaultProfileName?: string;
    AccountSchema?: Acc;
    isAuthenticated?: boolean;
  };

export class TestJazzContextManager<
  Acc extends CoreAccountSchema,
> extends JazzContextManager<Acc, TestJazzContextManagerProps<Acc>> {
  static fromAccountOrGuest<Acc extends CoreAccountSchema>(
    account?: Loaded<Acc> | { guest: AnonymousJazzAgent },
    props?: TestJazzContextManagerProps<Acc>,
  ) {
    if (account && "guest" in account) {
      return this.fromGuest<Acc>(
        account as { guest: AnonymousJazzAgent },
        props,
      );
    }

    return this.fromAccount<Acc>(
      account ?? (co.account().getMe() as Loaded<Acc>), // TODO: we can't really know the Account schema here?
      props,
    );
  }

  static fromAccount<Acc extends CoreAccountSchema>(
    account: Loaded<Acc>,
    props?: TestJazzContextManagerProps<Acc>,
  ) {
    const context = new TestJazzContextManager<Acc>();

    const provider = props?.isAuthenticated ? "testProvider" : "anonymous";
    const storage = context.getAuthSecretStorage();
    const node = account.$jazz.localNode;

    const credentials = {
      accountID: account.$jazz.id,
      accountSecret: node.getCurrentAgent().agentSecret,
      secretSeed: SecretSeedMap.get(account.$jazz.id),
      provider,
    } satisfies AuthCredentials;

    storage.set(credentials);

    context.updateContext(
      {
        AccountSchema: account.$jazz.sourceSchema as Acc,
        ...props,
      },
      {
        me: account,
        node,
        done: () => {
          node.gracefulShutdown();
        },
        logOut: async () => {
          await storage.clear();
          node.gracefulShutdown();
        },
        addConnectionListener: (listener) => {
          return MockConnectionStatus.addConnectionListener(listener);
        },
        connected: () => MockConnectionStatus.connected,
      },
      {
        credentials,
      },
    );

    return context;
  }

  static fromGuest<Acc extends CoreAccountSchema>(
    { guest }: { guest: AnonymousJazzAgent },
    props: TestJazzContextManagerProps<Acc> = {},
  ) {
    const context = new TestJazzContextManager<Acc>();
    const node = guest.node;

    context.updateContext(props, {
      guest,
      node,
      done: () => {
        node.gracefulShutdown();
      },
      logOut: async () => {
        node.gracefulShutdown();
      },
      addConnectionListener: (listener) => {
        return MockConnectionStatus.addConnectionListener(listener);
      },
      connected: () => MockConnectionStatus.connected,
    });

    return context;
  }

  async getNewContext(
    props: TestJazzContextManagerProps<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ): Promise<PlatformSpecificContext<Acc>> {
    if (!syncServer.current) {
      throw new Error(
        "You need to setup a test sync server with setupJazzTestSync to use the Auth functions",
      );
    }

    const context = await createJazzContext({
      credentials: authProps?.credentials,
      defaultProfileName: props.defaultProfileName,
      newAccountProps: authProps?.newAccountProps,
      peers: [getPeerConnectedToTestSyncServer()],
      crypto: await TestJSCrypto.create(),
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
      logOut: () => {
        return context.logOut();
      },
      addConnectionListener: (listener: (isConnected: boolean) => void) => {
        return MockConnectionStatus.addConnectionListener(listener);
      },
      connected: () => MockConnectionStatus.connected,
    };
  }
}

export async function linkAccounts(
  a: Account,
  b: Account,
  aRole: "server" | "client" = "server",
  bRole: "server" | "client" = "server",
) {
  const [aPeer, bPeer] = cojsonInternals.connectedPeers(
    b.$jazz.id,
    a.$jazz.id,
    {
      peer1role: aRole,
      peer2role: bRole,
    },
  );

  a.$jazz.localNode.syncManager.addPeer(aPeer);
  b.$jazz.localNode.syncManager.addPeer(bPeer);

  await a.$jazz.waitForAllCoValuesSync();
  await b.$jazz.waitForAllCoValuesSync();
}

export async function setupJazzTestSync({
  asyncPeers = false,
}: {
  asyncPeers?: boolean;
} = {}) {
  if (syncServer.current) {
    syncServer.current.gracefulShutdown();
  }

  const account = await asConstructable(RegisteredSchemas["Account"]).create({
    creationProps: {
      name: "Test Account",
    },
    crypto: await TestJSCrypto.create(),
  });

  syncServer.current = account.$jazz.localNode;
  syncServer.asyncPeers = asyncPeers;

  return account;
}

export function disableJazzTestSync() {
  if (syncServer.current) {
    syncServer.current.gracefulShutdown();
  }
  syncServer.current = null;
}
