import {
  createJazzService,
  type CreateJazzOptions,
  type JazzServiceApi,
  type WsLikeSocket,
} from "../create-jazz-shared.js";
import { openRemoteCore, remoteConfig } from "../core-providers/remote.js";
import type { JazzCoreProvider } from "../core-providers/core-provider.js";

export { schema } from "../../schema-namespace.js";
export { isRequestAuthenticationError, RequestAuthenticationError } from "../request-auth.js";
export { isNoJazzAccountError, NoJazzAccountError } from "../no-account-error.js";
export { acceptLocalFirstAuth } from "../create-jazz-shared.js";
export { fromJWT } from "../from-jwt.js";
export { trustWorkload } from "../backend-clients.js";
export type { CreateJazzOptions } from "../create-jazz-shared.js";
export type { JazzCoreProvider } from "../core-providers/core-provider.js";

export type WorkerJazz = Omit<JazzServiceApi, "ready" | "handle"> & {
  ready(): Promise<WorkerJazz>;
  /** Owns an upgraded socket until disconnect. Do not close Jazz after a 101 response. */
  handle(
    request: Request,
    context?: { waitUntil(work: Promise<unknown>): void },
  ): Promise<Response>;
};

/** Open within a Worker invocation; close after awaited application work.
 * Configuration may be shared, but the returned client must not cross requests.
 * This entry point requires Workers' Node compatibility (crypto and Buffer).
 */
export function createJazz(options: CreateJazzOptions): WorkerJazz {
  const jazz = createJazzService(options);
  const result: WorkerJazz = {
    get appId() {
      return jazz.appId;
    },
    get basePath() {
      return jazz.basePath;
    },
    get db() {
      return jazz.db;
    },
    async ready() {
      await jazz.ready();
      return result;
    },
    async handle(request, context) {
      await jazz.ready();
      const path = new URL(request.url).pathname;
      const root = `${jazz.basePath}/apps/${encodeURIComponent(jazz.appId)}`;
      if (path === `${root}/stream` || path.startsWith(`${root}/stream/`)) {
        return new Response(
          "HTTP stream sessions across Worker invocations are not supported; use WebSockets",
          { status: 501 },
        );
      }
      if (path !== `${root}/ws`) return jazz.handle(request);
      if (
        request.method !== "GET" ||
        request.headers.get("upgrade")?.toLowerCase() !== "websocket"
      ) {
        const response = await jazz.handle(request);
        // No cross-request HTTP stream fallback in this host.
        response.headers.delete("x-jazz-stream");
        return response;
      }
      if (!context)
        throw new Error("Worker WebSocket hosting requires the fetch execution context");
      const { WebSocketPair } = globalThis as unknown as {
        WebSocketPair: new () => { 0: WebSocket; 1: WebSocket & { accept(): void } };
      };
      const refusal = await jazz.checkSocket(request);
      if (refusal) return refusal;
      const pair = new WebSocketPair();
      const server = pair[1];
      server.binaryType = "arraybuffer";
      server.accept();
      const socket: WsLikeSocket = {
        send: (data) => server.send(data as Uint8Array<ArrayBuffer>),
        close: (code, reason) => server.close(code, reason),
        on(event, listener) {
          if (event === "message")
            server.addEventListener("message", (message) => listener(message.data));
          else server.addEventListener(event, listener as EventListener);
        },
      };
      const cleanup = () => context.waitUntil(jazz.close());
      server.addEventListener("close", cleanup, { once: true });
      server.addEventListener("error", cleanup, { once: true });
      jazz.acceptSocket(request, socket);
      return new Response(null, { status: 101, webSocket: pair[0] } as ResponseInit);
    },
    acceptSocket: (request, socket) => jazz.acceptSocket(request, socket),
    forReqUser: (request) => jazz.forReqUser(request),
    withAttribution: (request) => jazz.withAttribution(request),
    accountFor: (request) => jazz.accountFor(request),
    createAccountFor: (user) => jazz.createAccountFor(user),
    linkIdentity: (link) => jazz.linkIdentity(link),
    close: () => jazz.close(),
  };
  return result;
}

/** Connect to an already deployed core; deployment credentials stay outside this Worker. */
export function remote(options: Parameters<typeof remoteConfig>[0]): JazzCoreProvider {
  const config = remoteConfig(options);
  return {
    open: async () =>
      openRemoteCore(config.appId, {
        ...config,
        async openWebSocket(url, headers, signal) {
          const response = await fetch(url.replace(/^ws/, "http"), {
            headers: [
              ...headers.map(([name, value]) => [name!, value!] as [string, string]),
              ["Upgrade", "websocket"],
            ],
            signal,
          });
          const socket = (response as Response & { webSocket?: WebSocket & { accept(): void } })
            .webSocket;
          if (response.status !== 101 || !socket) {
            await response.body?.cancel();
            throw new Error(`Jazz core WebSocket upgrade failed (${response.status})`);
          }
          socket.binaryType = "arraybuffer";
          socket.accept();
          return socket;
        },
      }),
  };
}
