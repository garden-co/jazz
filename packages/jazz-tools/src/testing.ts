import { AgentSecret, CryptoProvider, LocalNode, Peer } from "cojson";
import { cojsonInternals } from "cojson";
import { PureJSCrypto } from "cojson/crypto";
import { Account, type AccountClass } from "./exports.js";
import { activeAccountContext } from "./implementation/activeAccountContext.js";
import {
  type AnonymousJazzAgent,
  type CoValueClass,
  createAnonymousJazzContext,
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

class TestJSCrypto extends PureJSCrypto {
  static async create() {
    if ("navigator" in globalThis && navigator.userAgent.includes("jsdom")) {
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

export async function createJazzTestAccount<Acc extends Account>(options?: {
  isCurrentActiveAccount?: boolean;
  AccountSchema?: CoValueClass<Acc>;
  creationProps?: Record<string, unknown>;
}): Promise<Acc> {
  const AccountSchema = (options?.AccountSchema ??
    Account) as unknown as TestAccountSchema<Acc>;
  const peers = [];
  if (syncServer.current) {
    const [aPeer, bPeer] = cojsonInternals.connectedPeers(
      Math.random().toString(),
      Math.random().toString(),
      {
        peer1role: "server",
        peer2role: "server",
      },
    );
    syncServer.current.syncManager.addPeer(aPeer);
    peers.push(bPeer);
  }

  const { node } = await LocalNode.withNewlyCreatedAccount({
    creationProps: {
      name: "Test Account",
      ...options?.creationProps,
    },
    crypto: await TestJSCrypto.create(),
    peersToLoadFrom: peers,
    migration: async (rawAccount, _node, creationProps) => {
      const account = new AccountSchema({
        fromRaw: rawAccount,
      });

      if (options?.isCurrentActiveAccount) {
        activeAccountContext.set(account);
      }

      await account.applyMigration?.(creationProps);
    },
  });

  const account = AccountSchema.fromNode(node);

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

export function getJazzContextShape<Acc extends Account>(
  account: Acc | { guest: AnonymousJazzAgent },
) {
  if ("guest" in account) {
    return {
      guest: account.guest,
      AccountSchema: Account,
      logOut: () => account.guest.node.gracefulShutdown(),
      done: () => account.guest.node.gracefulShutdown(),
    };
  }

  return {
    me: account,
    AccountSchema: account.constructor as AccountClass<Acc>,
    logOut: () => account._raw.core.node.gracefulShutdown(),
    done: () => account._raw.core.node.gracefulShutdown(),
  };
}

export function linkAccounts(
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

export { CoRichTextDebug } from "./coValues/coRichText.js";
