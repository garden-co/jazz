import NetInfo from "@react-native-community/netinfo";
import { LocalNode, Peer, RawAccountID, getSqliteStorageAsync } from "cojson";
import { PureJSCrypto } from "cojson/dist/crypto/PureJSCrypto"; // Importing from dist to not rely on the exports field
import {
  Account,
  AccountClass,
  AgentID,
  AnyAccountSchema,
  AuthCredentials,
  AuthSecretStorage,
  CoValue,
  CoValueFromRaw,
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

import { SQLiteDatabaseDriverAsync } from "cojson";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import type { RNQuickCrypto } from "jazz-tools/react-native-core/crypto";

export type BaseReactNativeContextOptions = {
  sync: SyncConfig;
  reconnectionTimeout?: number;
  storage?: SQLiteDatabaseDriverAsync | "disabled";
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

  const peers: Peer[] = [];

  const storage =
    options.storage && options.storage !== "disabled"
      ? await getSqliteStorageAsync(options.storage)
      : undefined;

  if (options.sync.when === "never") {
    return {
      toggleNetwork: () => {},
      addConnectionListener: () => () => {},
      connected: () => false,
      peers,
      setNode: () => {},
      crypto,
      storage,
    };
  }

  const wsPeer = new ReactNativeWebSocketPeerWithReconnection({
    peer: options.sync.peer,
    reconnectionTimeout: options.reconnectionTimeout,
    addPeer: (peer) => {
      if (node) {
        node.syncManager.addPeer(peer);
      } else {
        peers.push(peer);
      }
    },
    removePeer: (peer) => {
      peers.splice(peers.indexOf(peer), 1);
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
    addConnectionListener(listener: (connected: boolean) => void) {
      wsPeer.subscribe(listener);

      return () => {
        wsPeer.unsubscribe(listener);
      };
    },
    connected: () => wsPeer.connected,
    peers,
    setNode,
    crypto,
    storage,
  };
}

export async function createJazzReactNativeGuestContext(
  options: BaseReactNativeContextOptions,
) {
  const {
    toggleNetwork,
    peers,
    setNode,
    crypto,
    storage,
    addConnectionListener,
    connected,
  } = await setupPeers(options);

  const context = createAnonymousJazzContext({
    crypto,
    peers,
    storage,
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
    addConnectionListener,
    connected,
  };
}

export type ReactNativeContextOptions<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  credentials?: AuthCredentials;
  AccountSchema?: S;
  newAccountProps?: NewAccountProps;
  defaultProfileName?: string;
} & BaseReactNativeContextOptions;

export async function createJazzReactNativeContext<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(options: ReactNativeContextOptions<S>) {
  const {
    toggleNetwork,
    peers,
    setNode,
    crypto,
    storage,
    addConnectionListener,
    connected,
  } = await setupPeers(options);

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
    peers,
    crypto,
    defaultProfileName: options.defaultProfileName,
    AccountSchema: options.AccountSchema,
    sessionProvider: provideLockSession,
    authSecretStorage: options.authSecretStorage,
    storage,
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
    addConnectionListener,
    connected,
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

export function setupKvStore(
  kvStore: KvStore | undefined,
): KvStore | undefined {
  if (!kvStore) {
    return undefined;
  }
  KvStoreContext.getInstance().initialize(kvStore);
  return kvStore;
}
