import {
  AgentSecret,
  CojsonInternalTypes,
  CryptoProvider,
  JsonValue,
  LocalNode,
  Peer,
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
  AnyAccountSchema,
  CoValueFromRaw,
  CoValueOrZodSchema,
  Inbox,
  InstanceOfSchema,
  Loaded,
  ResolveQuery,
  createJazzContextFromExistingCredentials,
  randomSessionProvider,
  z,
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

export type ValueSchemas = Record<string, CoValueOrZodSchema>;

export type ValueResolves<Vs extends ValueSchemas> = {
  [K in keyof Vs]?: ResolveQuery<Vs[K]>;
};

export type ValuesFor<Vs extends ValueSchemas, Vr extends ValueResolves<Vs>> = {
  [K in keyof Vs]: Vr[K] extends ResolveQuery<Vs[K]>
    ? Loaded<Vs[K], Vr[K]>
    : Loaded<Vs[K]>;
};

export type CoValueRequest<Vs extends ValueSchemas, P extends z.z.ZodType> = {
  payload: {
    values: Record<keyof Vs, string>;
    params: z.infer<P>;
  };
  madeBy: string;
  signerID: CojsonInternalTypes.SignerID;
  signature: CojsonInternalTypes.Signature;
  knownStates?: Record<string, CoValueKnownState>;
  contentPieces?: Record<string, CojsonInternalTypes.NewContentMessage>;
};

export function experimental_defineRequest<
  Vs extends ValueSchemas,
  Vr extends ValueResolves<Vs>,
  P extends z.z.ZodType,
  R extends z.z.ZodType,
>(
  valueSchemas: Vs,
  defOptions: {
    resolve: Vr;
    paramsSchema: P;
    responseSchema: R;
    url: string;
  },
) {
  const makeRequest = (
    values: ValuesFor<Vs, {}>,
    params: z.infer<P>,
    _options?: { assumeUnknown?: boolean },
  ): CoValueRequest<Vs, P> => {
    const payload = {
      values: Object.fromEntries(
        Object.entries(values).map(([k, v]) => [k, v.id]),
      ) as Record<keyof Vs, string>,
      params: defOptions.paramsSchema.parse(params),
    };

    // TODO: either send known states for values (as deep as Vr, but best effort), or content if assumeUnknown

    const me = Account.getMe();
    const signerID = me._raw.core.node.getCurrentAgent().currentSignerID();
    const signature = me._raw.core.node.crypto.sign(
      me._raw.core.node.getCurrentAgent().currentSignerSecret(),
      payload as JsonValue,
    );

    return {
      payload,
      madeBy: me.id,
      signerID,
      signature,
    };
  };

  const processRequest = async (
    request: JsonValue,
    as: Account,
    process: (
      values: ValuesFor<Vs, Vr>,
      params: z.infer<P>,
      madeBy: Account,
    ) => Promise<z.infer<R>>,
  ) => {
    const requestParsed = z
      .object({
        payload: z.object({
          values: z.z.record(z.string(), z.string()),
          params: defOptions.paramsSchema,
        }),
        madeBy: z.string(),
        signerID: z.string(),
        signature: z.string(),
      })
      .parse(request);

    const signerID = requestParsed.signerID as CojsonInternalTypes.SignerID;
    const signature = requestParsed.signature as CojsonInternalTypes.Signature;

    if (
      !as._raw.core.node.crypto.verify(
        signature,
        requestParsed.payload as JsonValue,
        signerID,
      )
    ) {
      throw new Error("Invalid signature");
    }

    const madeByLoaded = await Account.load(requestParsed.madeBy);

    if (!madeByLoaded) {
      throw new Error("Made by account not found");
    }

    if (!madeByLoaded._raw.currentAgentID().includes(signerID)) {
      throw new Error("Not a valid signer ID for madeBy");
    }

    const values = Object.fromEntries(
      await Promise.all(
        Object.entries(requestParsed.payload.values).map(async ([k, id]) => [
          k,
          (await (valueSchemas[k] as any).load(id, {
            resolve: defOptions.resolve[k],
            loadAs: as,
          })) ||
            (() => {
              throw new Error(`Value ${k} in request not found/accessible`);
            })(),
        ]),
      ),
    ) as ValuesFor<Vs, Vr>;

    const responsePayload = await process(
      values,
      (requestParsed.payload as { params: z.infer<P> }).params, // TODO: weird
      madeByLoaded,
    );

    return {
      type: "success",
      payload: responsePayload,
    };
  };

  const handle = async (
    request: Request,
    as: Account,
    callback: (
      values: ValuesFor<Vs, Vr>,
      params: z.infer<P>,
      madeBy: Account,
    ) => Promise<z.infer<R>>,
  ): Promise<Response> => {
    const responsePayload = await processRequest(
      await request.json(),
      as,
      callback,
    );

    const _ = z
      .object({
        type: z.literal("success"),
        payload: defOptions.responseSchema,
      })
      .parse(responsePayload);

    return new Response(JSON.stringify(responsePayload), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });
  };

  const handleResponse = (
    response:
      | { type: "success"; payload: z.infer<R> }
      | { type: "continue"; knownStates: Record<string, CoValueKnownState> },
  ):
    | { type: "newRequest"; payload: CoValueRequest<Vs, P> }
    | { type: "success"; payload: z.infer<R> }
    | { type: "error"; error: string } => {
    if (response.type === "success") {
      return { type: "success", payload: response.payload };
    } else if (response.type === "continue") {
      // TODO: send content pieces according to response.knownStates
      throw new Error("Continue response not yet implemented");
    } else {
      throw new Error("Unknown response type");
    }
  };

  const send = async (
    values: ValuesFor<Vs, {}>,
    params: z.infer<P>,
    options?: { assumeUnknown?: boolean },
  ) => {
    const request = makeRequest(values, params, options);

    const response = await fetch(defOptions.url, {
      method: "POST",
      body: JSON.stringify(request),
    });

    const responseBody = await response.json();
    const responseParsed = z
      .object({
        type: z.literal("success"),
        payload: defOptions.responseSchema,
      })
      .parse(responseBody);
    const responseResult = handleResponse(
      responseParsed as {
        type: "success";
        payload: z.infer<R>;
      },
    ); // TODO: weird

    if (responseResult.type === "success") {
      return responseResult.payload;
    } else if (responseResult.type === "newRequest") {
      throw new Error("New request response not yet implemented");
    } else if (responseResult.type === "error") {
      throw new Error(responseResult.error);
    } else {
      throw new Error("Unknown response type");
    }
  };

  return {
    send,
    handle,
    internal: {
      makeRequest,
      processRequest,
      handleResponse,
    },
  };
}
