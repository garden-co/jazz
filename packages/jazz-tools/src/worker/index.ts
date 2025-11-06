import {
  AgentSecret,
  CryptoProvider,
  LocalNode,
  Peer,
  StorageAPI,
} from "cojson";
import {
  type AnyWebSocketConstructor,
  WebSocketPeerWithReconnection,
} from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  Inbox,
  InstanceOfSchema,
  Loaded,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
} from "jazz-tools";

type WorkerOptions<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  accountID?: string;
  accountSecret?: string;
  syncServer?: string;
  WebSocket?: AnyWebSocketConstructor;
  AccountSchema?: S;
  crypto?: CryptoProvider;
  /**
   * If true, the inbox will not be loaded.
   */
  skipInboxLoad?: boolean;
  /**
   * If false, the worker will not set in the global account context
   */
  asActiveAccount?: boolean;
  storage?: StorageAPI;
};

/** @category Context Creation */
export async function startWorker<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(options: WorkerOptions<S>) {
  const {
    accountID = process.env.JAZZ_WORKER_ACCOUNT,
    accountSecret = process.env.JAZZ_WORKER_SECRET,
    syncServer = "wss://cloud.jazz.tools",
    AccountSchema = Account as unknown as S,
    skipInboxLoad = false,
    asActiveAccount = true,
  } = options;

  let node: LocalNode | undefined = undefined;

  const peers: Peer[] = [];

  const wsPeer = new WebSocketPeerWithReconnection({
    peer: syncServer,
    reconnectionTimeout: 100,
    addPeer: (peer) => {
      if (node) {
        node.syncManager.addPeer(peer);
      } else {
        peers.push(peer);
      }
    },
    removePeer: () => {},
    WebSocketConstructor: options.WebSocket,
  });

  wsPeer.enable();

  if (!accountID) {
    throw new Error("No accountID provided");
  }
  if (!accountSecret) {
    throw new Error("No accountSecret provided");
  }
  if (!accountID.startsWith("co_")) {
    throw new Error("Invalid accountID");
  }
  if (!accountSecret?.startsWith("sealerSecret_")) {
    throw new Error("Invalid accountSecret");
  }

  const context = await createJazzContextFromExistingCredentials({
    credentials: {
      accountID: accountID,
      secret: accountSecret as AgentSecret,
    },
    AccountSchema,
    sessionProvider: randomSessionProvider,
    peers,
    crypto: options.crypto ?? (await WasmCrypto.create()),
    asActiveAccount,
    storage: options.storage,
  });

  const account = context.account as InstanceOfSchema<S>;
  node = account.$jazz.localNode;

  if (!account.$jazz.refs.profile?.id) {
    throw new Error("Account has no profile");
  }

  const inbox = skipInboxLoad ? undefined : await Inbox.load(account);

  async function done() {
    await context.account.$jazz.waitForAllCoValuesSync();

    wsPeer.disable();
    context.done();
  }

  const inboxPublicApi = inbox
    ? {
        subscribe: inbox.subscribe.bind(inbox) as Inbox["subscribe"],
      }
    : {
        subscribe: () => {},
      };

  return {
    /**
     * The worker account instance.
     */
    worker: context.account as Loaded<S>,
    experimental: {
      /**
       * API to subscribe to the inbox messages.
       *
       * More info on the Inbox API: https://jazz.tools/docs/react/server-side/inbox
       */
      inbox: inboxPublicApi,
    },
    /**
     * Wait for the connection to the sync server to be established.
     *
     * If already connected, it will resolve immediately.
     */
    waitForConnection() {
      return wsPeer.waitUntilConnected();
    },
    subscribeToConnectionChange(listener: (connected: boolean) => void) {
      wsPeer.subscribe(listener);

      return () => {
        wsPeer.unsubscribe(listener);
      };
    },
    /**
     * Waits for all CoValues to sync and then shuts down the worker.
     *
     * To only wait for sync use worker.$jazz.waitForAllCoValuesSync()
     *
     * @deprecated Use shutdownWorker
     */
    done,
    /**
     * Waits for all CoValues to sync and then shuts down the worker.
     *
     * To only wait for sync use worker.$jazz.waitForAllCoValuesSync()
     */
    shutdownWorker() {
      return done();
    },
  };
}
