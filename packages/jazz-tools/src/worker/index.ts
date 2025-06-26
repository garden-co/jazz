import {
  AgentSecret,
  CojsonInternalTypes,
  CryptoProvider,
  JsonValue,
  LocalNode,
  Peer,
  cojsonInternals,
} from "cojson";
import {
  type AnyWebSocketConstructor,
  WebSocketPeerWithReconnection,
} from "cojson-transport-ws";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { CoValueKnownState, NewContentMessage } from "cojson/dist/sync.js";
import {
  Account,
  AccountClass,
  AccountSchema,
  AnyAccountSchema,
  CoValue,
  CoValueFromRaw,
  CoValueOrZodSchema,
  Inbox,
  InstanceOfSchema,
  Loaded,
  ResolveQuery,
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
  });

  const account = context.account as InstanceOfSchema<S>;
  node = account._raw.core.node;

  if (!account._refs.profile?.id) {
    throw new Error("Account has no profile");
  }

  const inbox = await Inbox.load(account);

  async function done() {
    await context.account.waitForAllCoValuesSync();

    wsPeer.disable();
    context.done();
  }

  const inboxPublicApi = {
    subscribe: inbox.subscribe.bind(inbox) as Inbox["subscribe"],
  };

  return {
    worker: context.account as InstanceOfSchema<S>,
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

export function experimental_sendCoValueRequest<
  V extends Record<string, CoValue>,
  P extends JsonValue,
  R extends JsonValue,
>(
  values: V,
  params: P,
  url: URL | string,
  options?: {
    assumeUnknown?: boolean;
  },
): Promise<{ responseBody: R | undefined; response: Response }> {
  const valuesWithAccount = { ...values, madeBy: context.account };

  const knownStates = Object.fromEntries(
    Object.values(valuesWithAccount).map((v) => [
      v.id,
      v._raw.core.knownState(),
    ]),
  );

  const signature: CojsonInternalTypes.Signature = {} as any;
  const signerID: CojsonInternalTypes.SignerID = {} as any;

  const requestBody = {
    signed: {
      params,
      values: Object.fromEntries(
        Object.entries(valuesWithAccount).map(([name, v]) => [name, v.id]),
      ),
    },
    signerID,
    signature,
    knownStates,
  };

  let request = new Request(url, {
    method: "POST",
    body: JSON.stringify(requestBody),
  });

  return Promise.race([
    (async () => {
      while (true) {
        let response = await fetch(request);
        if (response.status === 200) {
          return {
            responseBody: (await response.json()) as R,
            response,
          };
        } else if (response.status === 100) {
          // "continue"
          const serverKnownStates = (await response.json()) as {
            knownStates: Record<string, CoValueKnownState>;
          };

          const contentPieces = Object.fromEntries(
            Object.values(values)
              .map((v) => [
                v.id,
                v._raw.core.verified.newContentSince(
                  serverKnownStates.knownStates[v.id],
                ),
              ])
              .filter(([_, content]) => content !== undefined),
          );

          const newRequestBody = {
            params,
            contentPieces,
          };

          const newRequest = new Request(url, {
            method: "POST",
            body: JSON.stringify(newRequestBody),
          });

          request = newRequest;
        } else {
          console.error("Unexpected response status", response.status);
          return {
            responseBody: undefined,
            response,
          };
        }
      }
    })(),
    new Promise<{ responseBody: R | undefined; response: Response }>(
      (_resolve, reject) =>
        setTimeout(() => reject(new Error("Request timed out")), 1000),
    ),
  ]);
}

export async function experimental_handleCoValueRequest<
  V extends Record<string, CoValue>,
  P extends JsonValue,
  R extends JsonValue,
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(
  request: Request,
  callback: (values: V, payload: R) => Promise<void>,
  worker: S | WorkerOptions<S>,
) {
  const body = (await request.json()) as {
    values: V;
    params: P;
  } & (
    | {
        contentPieces: Record<string, NewContentMessage>;
      }
    | {
        knownStates: Record<string, CoValueKnownState>;
      }
  );

  // make sure that for all values ("relevant values") we know more and
  // are fully synced with the sync server before proceeding

  if ("knownStates" in body) {
  } else {
  }
}
