import type {
  JazzCore,
  JazzCoreSocket as CoreSocket,
  JazzCoreAccess as BackendAccess,
  JazzCoreEvent as JazzCoreSocketEvent,
  JazzCoreProvider,
} from "./core-provider.js";
import { authenticatedSocket, linkChallengeSigner } from "./authentication.js";

const CREDENTIAL_HEADER = "x-jazz-backend-credential";
const ACCESS_HEADER = "x-jazz-backend-access";
const STAND_IN_SECRET = "stated-by-backend-credential";
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
export function remoteConfig(
  options:
    | { url: string; appId: string; secret: string }
    | { url: string; appId: string; backendCredential: BackendCredential },
): { url: string; appId: string; backendCredential: BackendCredential } {
  const appId = options.appId;
  if (typeof appId !== "string" || !appId.trim()) {
    throw new Error("remote(): appId must name the app the core serves (it is empty)");
  }
  let backendCredential: BackendCredential;
  if ("backendCredential" in options) {
    backendCredential = options.backendCredential;
  } else {
    const secret = options.secret;
    if (secret === undefined && "adminSecret" in options) {
      throw new Error(
        "remote(): `adminSecret` and `backendSecret` were replaced by one `secret`, the app's admin secret",
      );
    }
    if (typeof secret !== "string" || !secret.trim()) {
      throw new Error("remote(): secret must be the app's admin secret (it is empty)");
    }
    backendCredential = () => secret;
  }
  return { url: options.url.replace(/\/+$/, ""), appId, backendCredential };
}

/** Connect to an existing core; closing this provider does not stop that server. */
export function remote(options: Parameters<typeof remoteConfig>[0]): JazzCoreProvider {
  const config = remoteConfig(options);
  return { open: async () => openRemoteCore(config.appId, config) };
}
/**
 * A core reached over the network. Every request and socket carries the
 * backend credential and the access it states; a core that trusts a workload
 * admits nothing else, and either way the secrets a request offers itself
 * never count next to it. `secrets` fill the secret slots (stand-ins unless
 * the caller holds the real ones).
 */
export function openRemoteCore(
  appId: string,
  target: {
    url: string;
    backendCredential: BackendCredential;
    secrets?: { adminSecret: string; backendSecret: string };
    /** Host-specific authenticated upgrade. The socket is returned open. */
    openWebSocket?: (url: string, headers: string[][], signal: AbortSignal) => Promise<WebSocket>;
  },
): JazzCore {
  const socketUrl = `${target.url.replace(/^http/, "ws")}/apps/${encodeURIComponent(appId)}/ws`;
  if (!globalThis.WebSocket && !target.openWebSocket) {
    throw new Error("remote() needs a global WebSocket (Node 22 or newer)");
  }
  // Node's WebSocket (undici) takes upgrade headers as a second argument.
  const NativeWebSocket = globalThis.WebSocket as unknown as new (
    url: string,
    options?: { headers?: Record<string, string> },
  ) => WebSocket;
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

  const core: JazzCore = {
    appId,
    signLinkChallenge: linkChallengeSigner(appId, target.secrets?.adminSecret ?? STAND_IN_SECRET),
    async handle(method, path, headers, body, access = "session") {
      const response = await fetch(`${target.url}${path}`, {
        method,
        headers: [...withoutStated(headers), ...(await credentialHeaders(access))].map(
          ([name, value]) => [name!, value!] as [string, string],
        ),
        ...(method === "GET" || method === "HEAD" ? {} : { body: body as BodyInit }),
      });
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
    openSocket: (_headers, onEvent, access = "session") =>
      authenticatedSocket(
        connect(socketUrl, onEvent, access),
        target.secrets?.backendSecret ?? STAND_IN_SECRET,
      ),
    async close() {},
  };
  // Browsers' own headers stay here: the core sees only this backend.
  function connect(
    url: string,
    onEvent: (error: Error | null, event: JazzCoreSocketEvent) => void,
    access: BackendAccess,
  ): CoreSocket {
    let ws: WebSocket | undefined;
    const queued: Uint8Array[] = [];
    let ended = false;
    let queuedBytes = 0;
    const abort = new AbortController();
    const finish = (code: number, reason: string) => {
      if (ended) return;
      ended = true;
      queued.length = 0;
      queuedBytes = 0;
      abort.abort();
      try {
        if (ws && ws.readyState < 2) ws.close();
      } catch {
        /* Already closing. */
      }
      onEvent(null, { kind: "close", code, reason });
    };
    credentialHeaders(access)
      .then(async (headers) => {
        if (ended) return;
        ws = target.openWebSocket
          ? await target.openWebSocket(url, headers, abort.signal)
          : new NativeWebSocket(url, { headers: Object.fromEntries(headers) });
        if (ended) {
          ws.close();
          return;
        }
        ws.binaryType = "arraybuffer";
        const flush = () => {
          queuedBytes = 0;
          for (const data of queued.splice(0)) ws!.send(data as Uint8Array<ArrayBuffer>);
        };
        ws.addEventListener("open", flush);
        ws.addEventListener("message", (event) => {
          const data =
            typeof event.data === "string"
              ? new TextEncoder().encode(event.data)
              : new Uint8Array(event.data as ArrayBuffer);
          onEvent(null, { kind: "message", data });
        });
        ws.addEventListener("close", (event) => finish(event.code, event.reason));
        if (ws.readyState === 1) flush();
        // A failed opening connection must end so the link can retry.
        ws.addEventListener("error", () => {
          finish(1006, "connection to the core failed");
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
        else {
          if (data.byteLength > 8 * 1024 * 1024 - queuedBytes) {
            finish(1009, "core opening queue exceeded 8 MiB");
            return false;
          }
          queuedBytes += data.byteLength;
          queued.push(data.slice());
        }
        return true;
      },
      close() {
        ended = true;
        abort.abort();
        queued.length = 0;
        queuedBytes = 0;
        ws?.close();
      },
    };
  }
  return core;
}
