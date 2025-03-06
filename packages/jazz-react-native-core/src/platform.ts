import NetInfo from "@react-native-community/netinfo";
import { LocalNode, Peer, RawAccountID } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto"; // Importing from dist to not rely on the exports field
import {
  Account,
  AgentID,
  AuthCredentials,
  AuthSecretStorage,
  CoValue,
  CoValueClass,
  CryptoProvider,
  ID,
  NewAccountProps,
  SessionID,
  SyncConfig,
  createInviteLink as baseCreateInviteLink,
  createAnonymousJazzContext,
  createJazzContext,
} from "jazz-tools";
import { KvStore, KvStoreContext } from "./storage/kv-store-context.js";
import type { SQLiteAdapter } from "./storage/sqlite-adapter.js";
import { SQLiteReactNative } from "./storage/sqlite-react-native.js";

import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import type { RNQuickCrypto } from "./crypto/RNQuickCrypto.js";

export type BaseReactNativeContextOptions = {
  sync: SyncConfig;
  reconnectionTimeout?: number;
  storage?: SQLiteAdapter | "disabled";
  CryptoProvider?: typeof PureJSCrypto | typeof RNQuickCrypto;
  authSecretStorage: AuthSecretStorage;
};

class ReactNativeWebSocketPeerWithReconnection extends WebSocketPeerWithReconnection {
  onNetworkChange(callback: (connected: boolean) => void): () => void {
    return NetInfo.addEventListener((state) =>
      callback(state.isConnected ?? false),
    );
  }
}

async function setupPeers(options: BaseReactNativeContextOptions) {
  const CryptoProvider = options.CryptoProvider || PureJSCrypto;
  const crypto = await CryptoProvider.create();
  let node: LocalNode | undefined = undefined;

  const peersToLoadFrom: Peer[] = [];

  if (options.storage && options.storage !== "disabled") {
    const storage = await SQLiteReactNative.asPeer({
      adapter: options.storage,
    });
    peersToLoadFrom.push(storage);
  }

  if (options.sync.when === "never") {
    return {
      toggleNetwork: () => {},
      peersToLoadFrom,
      setNode: () => {},
      crypto,
    };
  }

  const wsPeer = new ReactNativeWebSocketPeerWithReconnection({
    peer: options.sync.peer,
    reconnectionTimeout: options.reconnectionTimeout,
    addPeer: (peer) => {
      if (node) {
        node.syncManager.addPeer(peer);
      } else {
        peersToLoadFrom.push(peer);
      }
    },
    removePeer: (peer) => {
      peersToLoadFrom.splice(peersToLoadFrom.indexOf(peer), 1);
    },
  });

  function toggleNetwork(enabled: boolean) {
    if (enabled) {
      wsPeer.enable();
    } else {
      wsPeer.disable();
    }
  }

  function setNode(value: LocalNode) {
    node = value;
  }

  if (options.sync.when === "always" || !options.sync.when) {
    toggleNetwork(true);
  }

  return {
    toggleNetwork,
    peersToLoadFrom,
    setNode,
    crypto,
  };
}

export async function createJazzReactNativeGuestContext(
  options: BaseReactNativeContextOptions,
) {
  const { toggleNetwork, peersToLoadFrom, setNode, crypto } =
    await setupPeers(options);

  const context = await createAnonymousJazzContext({
    crypto,
    peersToLoadFrom,
  });

  setNode(context.agent.node);

  options.authSecretStorage.emitUpdate(null);

  return {
    guest: context.agent,
    node: context.agent.node,
    done: () => {
      // TODO: Sync all the covalues before closing the connection & context
      toggleNetwork(false);
      context.done();
    },
    logOut: () => {
      return context.logOut();
    },
  };
}

export type ReactNativeContextOptions<Acc extends Account> = {
  credentials?: AuthCredentials;
  AccountSchema?: CoValueClass<Acc> & {
    fromNode: (typeof Account)["fromNode"];
  };
  newAccountProps?: NewAccountProps;
  defaultProfileName?: string;
} & BaseReactNativeContextOptions;

export async function createJazzReactNativeContext<Acc extends Account>(
  options: ReactNativeContextOptions<Acc>,
) {
  const { toggleNetwork, peersToLoadFrom, setNode, crypto } =
    await setupPeers(options);

  let unsubscribeAuthUpdate = () => {};

  if (options.sync.when === "signedUp") {
    const authSecretStorage = options.authSecretStorage;
    const credentials = options.credentials ?? (await authSecretStorage.get());

    // To update the internal state with the current credentials
    authSecretStorage.emitUpdate(credentials);

    function handleAuthUpdate(isAuthenticated: boolean) {
      if (isAuthenticated) {
        toggleNetwork(true);
      } else {
        toggleNetwork(false);
      }
    }

    unsubscribeAuthUpdate = authSecretStorage.onUpdate(handleAuthUpdate);
    handleAuthUpdate(authSecretStorage.isAuthenticated);
  }

  const context = await createJazzContext({
    credentials: options.credentials,
    newAccountProps: options.newAccountProps,
    peersToLoadFrom,
    crypto,
    defaultProfileName: options.defaultProfileName,
    AccountSchema: options.AccountSchema,
    sessionProvider: provideLockSession,
    authSecretStorage: options.authSecretStorage,
  });

  setNode(context.node);

  return {
    me: context.account,
    node: context.node,
    authSecretStorage: context.authSecretStorage,
    done: () => {
      // TODO: Sync all the covalues before closing the connection & context
      toggleNetwork(false);
      unsubscribeAuthUpdate();
      context.done();
    },
    logOut: () => {
      unsubscribeAuthUpdate();
      return context.logOut();
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

export function setupKvStore(kvStore: KvStore) {
  KvStoreContext.getInstance().initialize(kvStore);
  return kvStore;
}
