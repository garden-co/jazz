import { AgentSecret, CryptoProvider, LocalNode, Peer } from "cojson";
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

  const peersToLoadFrom: Peer[] = [];

  const wsPeer = new WebSocketPeerWithReconnection({
    peer: syncServer,
    reconnectionTimeout: 100,
    addPeer: (peer) => {
      if (node) {
        node.syncManager.addPeer(peer);
      } else {
        peersToLoadFrom.push(peer);
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
    // TODO: locked sessions similar to browser
    sessionProvider: randomSessionProvider,
    peersToLoadFrom,
    crypto: options.crypto ?? (await WasmCrypto.create()),
    asActiveAccount,
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
    worker: context.account as Loaded<S>,
    experimental: {
      inbox: inboxPublicApi,
    },
    waitForConnection() {
      return wsPeer.waitUntilConnected();
    },
    subscribeToConnectionChange(listener: (connected: boolean) => void) {
      wsPeer.subscribe(listener);

      return () => {
        wsPeer.unsubscribe(listener);
      };
    },
    done,
  };
}
