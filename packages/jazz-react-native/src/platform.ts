import { type SQLiteAdapter, SQLiteStorage } from "cojson-storage-rn-sqlite";
import {
  Account,
  AgentID,
  AnonymousJazzAgent,
  AuthMethod,
  CoValue,
  CoValueClass,
  CryptoProvider,
  ID,
  SessionID,
  createInviteLink as baseCreateInviteLink,
  createJazzContext,
} from "jazz-tools";

import { RawAccountID } from "cojson";

export { RNDemoAuth } from "./auth/DemoAuthMethod.js";

import { PureJSCrypto } from "cojson/native";
import { createWebSocketPeerWithReconnection } from "./createWebSocketPeerWithReconnection.js";
import type { RNQuickCrypto } from "./crypto/RNQuickCrypto.js";
import { ExpoSecureStoreAdapter } from "./storage/expo-secure-store-adapter.js";
import { KvStoreContext } from "./storage/kv-store-context.js";

/** @category Context Creation */
export type ReactNativeContext<Acc extends Account> = {
  me: Acc;
  logOut: () => void;
  // TODO: Symbol.dispose?
  done: () => void;
};

export type ReactNativeGuestContext = {
  guest: AnonymousJazzAgent;
  logOut: () => void;
  done: () => void;
};

export type ReactNativeContextOptions<Acc extends Account> = {
  auth: AuthMethod;
  AccountSchema: CoValueClass<Acc> & {
    fromNode: (typeof Account)["fromNode"];
  };
} & BaseReactNativeContextOptions;

export type BaseReactNativeContextOptions = {
  peer: `wss://${string}` | `ws://${string}`;
  reconnectionTimeout?: number;
  storage?: SQLiteAdapter | "disabled";
  CryptoProvider?: typeof PureJSCrypto | typeof RNQuickCrypto;
};

/** @category Context Creation */
export async function createJazzRNContext<Acc extends Account>(
  options: ReactNativeContextOptions<Acc>,
): Promise<ReactNativeContext<Acc>>;
export async function createJazzRNContext(
  options: BaseReactNativeContextOptions,
): Promise<ReactNativeGuestContext>;
export async function createJazzRNContext<Acc extends Account>(
  options: ReactNativeContextOptions<Acc> | BaseReactNativeContextOptions,
): Promise<ReactNativeContext<Acc> | ReactNativeGuestContext>;
export async function createJazzRNContext<Acc extends Account>(
  options: ReactNativeContextOptions<Acc> | BaseReactNativeContextOptions,
): Promise<ReactNativeContext<Acc> | ReactNativeGuestContext> {
  const websocketPeer = createWebSocketPeerWithReconnection(
    options.peer,
    options.reconnectionTimeout,
    (peer) => {
      node.syncManager.addPeer(peer);
    },
  );

  const CryptoProvider = options.CryptoProvider || PureJSCrypto;

  const peersToLoadFrom = [websocketPeer.peer];

  if (options.storage && options.storage !== "disabled") {
    // const adapter = new options.storage("jazz-storage");
    const storage = await SQLiteStorage.asPeer({
      adapter: options.storage,
    });
    console.log("[createJazzRNContext] storage initialized as peer", storage);
    peersToLoadFrom.push(storage);
  }

  const context =
    "auth" in options
      ? await createJazzContext({
          AccountSchema: options.AccountSchema,
          auth: options.auth,
          crypto: await CryptoProvider.create(),
          peersToLoadFrom,
          sessionProvider: provideLockSession,
        })
      : await createJazzContext({
          crypto: await CryptoProvider.create(),
          peersToLoadFrom,
        });

  const node =
    "account" in context ? context.account._raw.core.node : context.agent.node;

  return "account" in context
    ? {
        me: context.account,
        done: () => {
          websocketPeer.done();
          context.done();
        },
        logOut: () => {
          context.logOut();
        },
      }
    : {
        guest: context.agent,
        done: () => {
          websocketPeer.done();
          context.done();
        },
        logOut: () => {
          context.logOut();
        },
      };
}

/** @category Auth Providers */
export type SessionProvider = (
  accountID: ID<Account> | AgentID,
) => Promise<SessionID>;

export async function provideLockSession(
  accountID: ID<Account> | AgentID,
  crypto: CryptoProvider,
) {
  const sessionDone = () => {};

  const kvStore = KvStoreContext.getInstance().getStorage();

  const sessionID =
    ((await kvStore.get(accountID)) as SessionID) ||
    crypto.newRandomSessionID(accountID as RawAccountID | AgentID);
  await kvStore.set(accountID, sessionID);

  return Promise.resolve({
    sessionID,
    sessionDone,
  });
}

/** @category Invite Links */
export function createInviteLink<C extends CoValue>(
  value: C,
  role: "reader" | "writer" | "admin",
  { baseURL, valueHint }: { baseURL?: string; valueHint?: string } = {},
): string {
  return baseCreateInviteLink(value, role, baseURL ?? "", valueHint);
}

export function setupKvStore(kvStore = new ExpoSecureStoreAdapter()) {
  KvStoreContext.getInstance().initialize(kvStore);

  return kvStore;
}
