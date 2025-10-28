import { LocalNode, Peer, RawAccountID } from "cojson";
import { getIndexedDBStorage } from "cojson-storage-indexeddb";
import { WebSocketPeerWithReconnection } from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
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
  InviteSecret,
  NewAccountProps,
  SessionID,
  SyncConfig,
  cojsonInternals,
  createAnonymousJazzContext,
} from "jazz-tools";
import { createJazzContext } from "jazz-tools";
import { StorageConfig, getStorageOptions } from "./storageOptions.js";
import { setupInspector } from "./utils/export-account-inspector.js";

setupInspector();

export type BaseBrowserContextOptions = {
  sync: SyncConfig;
  reconnectionTimeout?: number;
  storage?: StorageConfig;
  crypto?: CryptoProvider;
  authSecretStorage: AuthSecretStorage;
};

class BrowserWebSocketPeerWithReconnection extends WebSocketPeerWithReconnection {
  onNetworkChange(callback: (connected: boolean) => void): () => void {
    const handler = () => callback(navigator.onLine);
    window.addEventListener("online", handler);
    window.addEventListener("offline", handler);

    return () => {
      window.removeEventListener("online", handler);
      window.removeEventListener("offline", handler);
    };
  }
}

async function setupPeers(options: BaseBrowserContextOptions) {
  const crypto = options.crypto || (await WasmCrypto.create());
  let node: LocalNode | undefined = undefined;

  const { useIndexedDB } = getStorageOptions(options.storage);

  const peers: Peer[] = [];

  const storage = useIndexedDB ? await getIndexedDBStorage() : undefined;

  if (options.sync.when === "never") {
    return {
      addConnectionListener: () => () => {},
      connected: () => false,
      toggleNetwork: () => {},
      peers,
      storage,
      setNode: () => {},
      crypto,
    };
  }

  const wsPeer = new BrowserWebSocketPeerWithReconnection({
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
    connected() {
      return wsPeer.connected;
    },
    peers,
    storage,
    setNode,
    crypto,
  };
}

export async function createJazzBrowserGuestContext(
  options: BaseBrowserContextOptions,
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

  const context = await createAnonymousJazzContext({
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

export type BrowserContextOptions<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  credentials?: AuthCredentials;
  AccountSchema?: S;
  newAccountProps?: NewAccountProps;
  defaultProfileName?: string;
} & BaseBrowserContextOptions;

export async function createJazzBrowserContext<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(options: BrowserContextOptions<S>) {
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

    function handleAuthUpdate(isAuthenticated: boolean) {
      if (isAuthenticated) {
        toggleNetwork(true);
      } else {
        toggleNetwork(false);
      }
    }

    unsubscribeAuthUpdate = authSecretStorage.onUpdate(handleAuthUpdate);
    handleAuthUpdate(authSecretStorage.getIsAuthenticated(credentials));
  }

  const context = await createJazzContext({
    credentials: options.credentials,
    newAccountProps: options.newAccountProps,
    peers,
    storage,
    crypto,
    defaultProfileName: options.defaultProfileName,
    AccountSchema: options.AccountSchema,
    sessionProvider: provideBrowserLockSession,
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
    addConnectionListener,
    connected,
  };
}

/** @category Auth Providers */
export type SessionProvider = (
  accountID: ID<Account> | AgentID,
) => Promise<SessionID>;

export function provideBrowserLockSession(
  accountID: ID<Account> | AgentID,
  crypto: CryptoProvider,
) {
  if (typeof navigator === "undefined" || !navigator.locks?.request) {
    // Fallback to random session ID for each tab session
    return Promise.resolve({
      sessionID: crypto.newRandomSessionID(accountID as RawAccountID | AgentID),
      sessionDone: () => {},
    });
  }

  let sessionDone!: () => void;
  const donePromise = new Promise<void>((resolve) => {
    sessionDone = resolve;
  });

  let resolveSession: (sessionID: SessionID) => void;
  const sessionPromise = new Promise<SessionID>((resolve) => {
    resolveSession = resolve;
  });

  void (async function () {
    for (let idx = 0; idx < 100; idx++) {
      // To work better around StrictMode
      for (let retry = 0; retry < 2; retry++) {
        // console.debug("Trying to get lock", accountID + "_" + idx);
        const sessionFinishedOrNoLock = await navigator.locks.request(
          accountID + "_" + idx,
          { ifAvailable: true },
          async (lock) => {
            if (!lock) return "noLock";

            const sessionID =
              localStorage.getItem(accountID + "_" + idx) ||
              crypto.newRandomSessionID(accountID as RawAccountID | AgentID);
            localStorage.setItem(accountID + "_" + idx, sessionID);

            resolveSession(sessionID as SessionID);

            await donePromise;
            console.log("Done with lock", accountID + "_" + idx, sessionID);
            return "sessionFinished";
          },
        );

        if (sessionFinishedOrNoLock === "sessionFinished") {
          return;
        }
      }
    }
    throw new Error("Couldn't get lock on session after 100x2 tries");
  })();

  return sessionPromise.then((sessionID) => ({
    sessionID,
    sessionDone,
  }));
}

/** @category Invite Links */
export function createInviteLink<C extends CoValue>(
  value: C,
  role: "reader" | "writer" | "admin" | "writeOnly",
  // default to same address as window.location, but without hash
  {
    baseURL = window.location.href.replace(/#.*$/, ""),
    valueHint,
  }: { baseURL?: string; valueHint?: string } = {},
): string {
  const coValueCore = value.$jazz.raw.core;
  let currentCoValue = coValueCore;

  while (currentCoValue.verified.header.ruleset.type === "ownedByGroup") {
    currentCoValue = currentCoValue.getGroup().core;
  }

  const { ruleset, meta } = currentCoValue.verified.header;

  if (ruleset.type !== "group" || meta?.type === "account") {
    throw new Error("Can't create invite link for object without group");
  }

  const group = cojsonInternals.expectGroup(currentCoValue.getCurrentContent());
  const inviteSecret = group.createInvite(role);

  return `${baseURL}#/invite/${valueHint ? valueHint + "/" : ""}${
    value.$jazz.id
  }/${inviteSecret}`;
}

/** @category Invite Links */
export function parseInviteLink<C extends CoValue>(
  inviteURL: string,
):
  | {
      valueID: ID<C>;
      valueHint?: string;
      inviteSecret: InviteSecret;
    }
  | undefined {
  const url = new URL(inviteURL);
  const parts = url.hash.split("/");

  let valueHint: string | undefined;
  let valueID: ID<C> | undefined;
  let inviteSecret: InviteSecret | undefined;

  if (parts[0] === "#" && parts[1] === "invite") {
    if (parts.length === 5) {
      valueHint = parts[2];
      valueID = parts[3] as ID<C>;
      inviteSecret = parts[4] as InviteSecret;
    } else if (parts.length === 4) {
      valueID = parts[2] as ID<C>;
      inviteSecret = parts[3] as InviteSecret;
    }

    if (!valueID || !inviteSecret) {
      return undefined;
    }
    return { valueID, inviteSecret, valueHint };
  }
}
