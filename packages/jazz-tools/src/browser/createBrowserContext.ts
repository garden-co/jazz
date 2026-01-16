import { LocalNode, Peer } from "cojson";
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
import { setupInspector } from "./utils/export-account-inspector.js";
import { getBrowserLockSessionProvider } from "./provideBrowserLockSession/index.js";

setupInspector();

export type BaseBrowserContextOptions = {
  sync: SyncConfig;
  reconnectionTimeout?: number;
  storage?: "indexedDB";
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

  const peers: Peer[] = [];

  const storage = await getIndexedDBStorage();

  if (options.sync.when === "never") {
    return {
      addConnectionListener: () => () => {},
      connected: () => false,
      toggleNetwork: () => {},
      peers,
      syncWhen: options.sync.when,
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
    syncWhen: options.sync.when,
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
    syncWhen,
    setNode,
    crypto,
    storage,
    addConnectionListener,
    connected,
  } = await setupPeers(options);

  const context = await createAnonymousJazzContext({
    crypto,
    peers,
    syncWhen,
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
    syncWhen,
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
    syncWhen,
    storage,
    crypto,
    defaultProfileName: options.defaultProfileName,
    AccountSchema: options.AccountSchema,
    sessionProvider: getBrowserLockSessionProvider(),
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
