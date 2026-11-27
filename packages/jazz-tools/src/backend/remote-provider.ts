import { backendNative } from "./backend-native.js";
import { cloudflareWebSocketUpgrade } from "./cloudflare-websocket.js";
import { vercelWebSocketUpgrade } from "./vercel-host.js";
import type {
  JazzCore,
  JazzCoreOpenContext,
  JazzProvider,
  JazzCoreTransport,
  BackendAccess,
  CoreSocket,
} from "./core-provider.js";
import {
  networkTransport,
  refuseRedirect,
  type CoreTransport,
  type CoreWebSocket,
} from "./core-transport.js";
import type { JazzCoreSocketEvent } from "jazz-napi";
import { logStoreOpened } from "./store-log.js";

const CREDENTIAL_HEADER = "x-jazz-backend-credential";
const ACCESS_HEADER = "x-jazz-backend-access";
class RemoteProvider implements JazzProvider {
  getWebSocketUpgrade(handler: "fetch" | "next") {
    return backendNative().host === "workerd"
      ? cloudflareWebSocketUpgrade
      : vercelWebSocketUpgrade(handler);
  }
  readonly kind = "remote";
  constructor(
    readonly url: string,
    readonly appId: string,
    readonly backendCredential: BackendCredential,
    readonly secret?: string,
    readonly transport?: CoreTransport,
  ) {}
  async open(context: JazzCoreOpenContext) {
    const core = openRemoteCore(this.appId, this, context);
    logStoreOpened(`jazz: store remote at ${this.url} (app ${this.appId})`);
    return core;
  }
}

/**
 * The credential this backend presents to its core, fresh for each request
 * and socket: the app's secret, or a token that proves which workload this
 * is (a sync server started with `--trusted-workload-*` accepts its
 * platform's OIDC token, such as a Vercel function's).
 */
export type BackendCredential = () => string | Promise<string>;

/**
 * A core running elsewhere (Jazz Cloud, or a sync server you run), serving
 * the app `appId`, reached from this backend with the app's one secret (its
 * admin secret), or with a `backendCredential` of your own. Browsers still
 * talk only to your backend: it decides who each user is, and the core
 * trusts it to say so. The secret never leaves your server.
 */
export function remote(
  options: { transport?: CoreTransport } & (
    | { url: string; appId: string; secret: string }
    | { url: string; appId: string; backendCredential: BackendCredential }
  ),
): RemoteProvider {
  const appId = options.appId;
  if (typeof appId !== "string" || !appId.trim()) {
    throw new Error("remote(): appId must name the app the core serves (it is empty)");
  }
  let backendCredential: BackendCredential;
  let secret: string | undefined;
  if ("backendCredential" in options) {
    backendCredential = options.backendCredential;
  } else {
    secret = options.secret;
    if (secret === undefined && "adminSecret" in options) {
      throw new Error(
        "remote(): `adminSecret` and `backendSecret` were replaced by one `secret`, the app's admin secret",
      );
    }
    if (typeof secret !== "string" || !secret.trim()) {
      throw new Error("remote(): secret must be the app's admin secret (it is empty)");
    }
    const given = secret;
    backendCredential = () => given;
  }
  return new RemoteProvider(
    options.url.replace(/\/+$/, ""),
    appId,
    backendCredential,
    secret,
    options.transport,
  );
}
/**
 * A core reached over the network. Every request and socket carries the
 * backend credential and the access it states; a core that trusts a workload
 * admits nothing else. The credential determines which access the request receives.
 */
function openRemoteCore(
  appId: string,
  target: {
    url: string;
    backendCredential: BackendCredential;
    secret?: string | undefined;
    transport?: CoreTransport | undefined;
  },
  context: JazzCoreOpenContext,
): JazzCore {
  const transport = target.transport ?? networkTransport();
  const credentialHeaders = async (access: BackendAccess): Promise<string[][]> => {
    const credential = await target.backendCredential();
    if (typeof credential !== "string" || !credential) {
      throw new Error("backendCredential() returned no credential");
    }
    return access === "session"
      ? [[CREDENTIAL_HEADER, credential]]
      : [
          [CREDENTIAL_HEADER, credential],
          [ACCESS_HEADER, access],
        ];
  };
  const withoutStated = (headers: string[][]) =>
    headers.filter(([name]) => {
      const lower = name!.toLowerCase();
      return lower !== CREDENTIAL_HEADER && lower !== ACCESS_HEADER;
    });

  const remote: JazzCoreTransport = {
    appId,
    async handle(method, path, headers, body, access = "session") {
      const requestUrl = `${target.url}${path}`;
      const response = await transport
        .fetch(requestUrl, {
          method,
          // The request carries this backend's credential: never to another
          // URL. ("manual", not "error", which workerd doesn't implement.)
          redirect: "manual",
          headers: [...withoutStated(headers), ...(await credentialHeaders(access))].map(
            ([name, value]) => [name!, value!] as [string, string],
          ),
          ...(method === "GET" || method === "HEAD" ? {} : { body: body as BodyInit }),
        })
        .catch((error) => {
          throw unreachableCore(target.url, error);
        });
      await refuseRedirect(target.url, requestUrl, response);
      const replyHeaders: string[][] = [];
      response.headers.forEach((value, name) => {
        // fetch already decoded the body.
        if (name !== "content-encoding" && name !== "content-length") {
          replyHeaders.push([name, value]);
        }
      });
      return {
        status: response.status,
        headers: replyHeaders,
        body: new Uint8Array(await response.arrayBuffer()),
      };
    },
    openSocket: (path, _headers, onEvent, access) =>
      connect(target.url.replace(/^http/, "ws") + path, onEvent, access),
    async close() {},
  };
  return {
    ...context.connect(remote),
    ...(target.secret === undefined
      ? {}
      : { appSecret: target.secret, tokenSecret: target.secret }),
  };
  // Browsers' own headers stay here: the core sees only this backend.
  function connect(
    url: string,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access: BackendAccess,
  ): CoreSocket {
    let ws: CoreWebSocket | undefined;
    const queued: Uint8Array[] = [];
    let ended = false;
    let markOpened!: () => void;
    const opened = new Promise<void>((resolve) => {
      markOpened = resolve;
    });
    const finish = (code: number, reason: string) => {
      if (ended) return;
      ended = true;
      onEvent(null, { kind: "close", code, reason });
    };
    credentialHeaders(access)
      .then((headers) => {
        if (ended) return;
        ws = transport.openWebSocket(url, Object.fromEntries(headers));
        ws.binaryType = "arraybuffer";
        ws.addEventListener("open", () => {
          for (const data of queued.splice(0)) ws!.send(data as Uint8Array<ArrayBuffer>);
          if (!ended) markOpened();
        });
        ws.addEventListener("message", (event) => {
          const data =
            typeof event.data === "string"
              ? new TextEncoder().encode(event.data)
              : new Uint8Array(event.data as ArrayBuffer);
          onEvent(null, { kind: "message", data });
        });
        ws.addEventListener("close", (event) => finish(event.code, event.reason));
        // A socket that never connects (the core is down or restarting) can
        // report only the error, and stay CONNECTING: without this, the link
        // waits on it forever instead of retrying.
        ws.addEventListener("error", () => {
          finish(1006, "connection to the core failed");
          try {
            ws?.close();
          } catch {
            // Already closing.
          }
        });
      })
      // A failing credential hook, or a socket that can't even be created.
      .catch((error: unknown) =>
        finish(1011, error instanceof Error ? error.message : "no backend credential"),
      );
    return {
      send(data) {
        if (ended) return false;
        if (ws?.readyState === 1) ws.send(data as Uint8Array<ArrayBuffer>);
        else queued.push(data);
        return true;
      },
      close() {
        ended = true;
        ws?.close();
      },
      opened,
    };
  }
}

/**
 * A remote core that could not be reached, named, instead of the bare
 * `fetch failed` the request saw. The next use of this Jazz tries again.
 */
export function unreachableCore(url: string, error: unknown): unknown {
  const cause = (error as { cause?: { code?: unknown; message?: unknown } } | null)?.cause;
  if (!(error instanceof TypeError) || !cause) return error;
  const why = typeof cause.code === "string" ? cause.code : String(cause.message ?? error.message);
  return new Error(
    `createJazz: could not reach the Jazz core at ${url} (${why}); this Jazz tries again on its next use`,
    { cause: error },
  );
}
