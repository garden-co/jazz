import { AgentSecret, CryptoProvider, LocalNode, Peer } from "cojson";
import { cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto";
import {
  Account,
  AccountClass,
  AuthCredentials,
  JazzContextManagerAuthProps,
} from "./exports.js";
import {
  JazzContextManager,
  JazzContextManagerBaseProps,
} from "./implementation/ContextManager.js";
import { activeAccountContext } from "./implementation/activeAccountContext.js";
import {
  type AnonymousJazzAgent,
  type CoValueClass,
  createAnonymousJazzContext,
  createJazzContext,
  randomSessionProvider,
} from "./internal.js";

const syncServer: { current: LocalNode | null } = { current: null };

type TestAccountSchema<Acc extends Account> = CoValueClass<Acc> & {
  fromNode: (typeof Account)["fromNode"];
  create: (options: {
    creationProps: { name: string };
    initialAgentSecret?: AgentSecret;
    peersToLoadFrom?: Peer[];
    crypto: CryptoProvider;
  }) => Promise<Acc>;
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
      peer1role: "server",
      peer2role: "server",
    },
  );
  syncServer.current.syncManager.addPeer(aPeer);

  return bPeer;
}

const SecretSeedMap = new Map<string, Uint8Array>();
let isMigrationActive = false;

export async function createJazzTestAccount<Acc extends Account>(options?: {
  isCurrentActiveAccount?: boolean;
  AccountSchema?: CoValueClass<Acc>;
  creationProps?: Record<string, unknown>;
}): Promise<Acc> {
  const AccountSchema = (options?.AccountSchema ??
    Account) as unknown as TestAccountSchema<Acc>;
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
    peersToLoadFrom: peers,
    migration: async (rawAccount, _node, creationProps) => {
      if (isMigrationActive) {
        throw new Error(
          "It is not possible to create multiple accounts in parallel inside the test environment.",
        );
      }

      isMigrationActive = true;

      const account = new AccountSchema({
        fromRaw: rawAccount,
      });

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

  const account = AccountSchema.fromNode(node);
  SecretSeedMap.set(account.id, secretSeed);

  if (options?.isCurrentActiveAccount) {
    activeAccountContext.set(account);
  }

  return account;
}

export function setActiveAccount(account: Account) {
  activeAccountContext.set(account);
}

export async function createJazzTestGuest() {
  const ctx = await createAnonymousJazzContext({
    crypto: await PureJSCrypto.create(),
    peersToLoadFrom: [],
  });

  return {
    guest: ctx.agent,
  };
}

export type TestJazzContextManagerProps<Acc extends Account> =
  JazzContextManagerBaseProps<Acc> & {
    defaultProfileName?: string;
    AccountSchema?: AccountClass<Acc>;
    isAuthenticated?: boolean;
  };

export class TestJazzContextManager<
  Acc extends Account,
> extends JazzContextManager<Acc, TestJazzContextManagerProps<Acc>> {
  static fromAccountOrGuest<Acc extends Account>(
    account?: Acc | { guest: AnonymousJazzAgent },
    props?: TestJazzContextManagerProps<Acc>,
  ) {
    if (account && "guest" in account) {
      return this.fromGuest<Acc>(account, props);
    }

    return this.fromAccount<Acc>(account ?? (Account.getMe() as Acc), props);
  }

  static fromAccount<Acc extends Account>(
    account: Acc,
    props?: TestJazzContextManagerProps<Acc>,
  ) {
    const context = new TestJazzContextManager<Acc>();

    const provider = props?.isAuthenticated ? "testProvider" : "anonymous";
    const storage = context.getAuthSecretStorage();
    const node = account._raw.core.node;

    const credentials = {
      accountID: account.id,
      accountSecret: node.account.agentSecret,
      secretSeed: SecretSeedMap.get(account.id),
      provider,
    } satisfies AuthCredentials;

    storage.set(credentials);

    context.updateContext(
      {
        AccountSchema: account.constructor as AccountClass<Acc>,
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
      },
      {
        credentials,
      },
    );

    return context;
  }

  static fromGuest<Acc extends Account>(
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
    });

    return context;
  }

  async getNewContext(
    props: TestJazzContextManagerProps<Acc>,
    authProps?: JazzContextManagerAuthProps,
  ) {
    if (!syncServer.current) {
      throw new Error(
        "You need to setup a test sync server with setupJazzTestSync to use the Auth functions",
      );
    }

    const context = await createJazzContext<Acc>({
      credentials: authProps?.credentials,
      defaultProfileName: props.defaultProfileName,
      newAccountProps: authProps?.newAccountProps,
      peersToLoadFrom: [getPeerConnectedToTestSyncServer()],
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
    };
  }
}

export async function linkAccounts(
  a: Account,
  b: Account,
  aRole: "server" | "client" = "server",
  bRole: "server" | "client" = "server",
) {
  const [aPeer, bPeer] = cojsonInternals.connectedPeers(b.id, a.id, {
    peer1role: aRole,
    peer2role: bRole,
  });

  a._raw.core.node.syncManager.addPeer(aPeer);
  b._raw.core.node.syncManager.addPeer(bPeer);

  await a.waitForAllCoValuesSync();
  await b.waitForAllCoValuesSync();
}

export async function setupJazzTestSync() {
  if (syncServer.current) {
    syncServer.current.gracefulShutdown();
  }

  const account = await Account.create({
    creationProps: {
      name: "Test Account",
    },
    crypto: await TestJSCrypto.create(),
  });

  syncServer.current = account._raw.core.node;

  return account;
}
