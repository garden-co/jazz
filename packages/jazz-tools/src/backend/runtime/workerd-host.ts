import {
  createJazzService,
  validateJazzOptions,
  type JazzServiceApi,
  type CreateJazzOptions,
  type WsLikeSocket,
} from "../create-jazz-shared.js";
export type Jazz = Omit<JazzServiceApi, "ready" | "handle"> & {
  ready(): Promise<Jazz>;
  /** Owns an upgraded socket until disconnect. Do not close Jazz after a 101 response. */
  handle(
    request: Request,
    context?: { waitUntil(work: Promise<unknown>): void },
  ): Promise<Response>;
};

export function createJazz(options: CreateJazzOptions): Jazz {
  validateJazzOptions(options);
  const jazz = createJazzService(options);
  const handleRequest = jazz.handle;
  const handle: Jazz["handle"] = async (request, context) => {
    await jazz.ready();
    const path = new URL(request.url).pathname;
    const root = `${jazz.basePath}/apps/${encodeURIComponent(jazz.appId)}`;
    if (path === `${root}/stream` || path.startsWith(`${root}/stream/`)) {
      return new Response(
        "HTTP stream sessions across Worker invocations are not supported; use WebSockets",
        { status: 501 },
      );
    }
    if (path !== `${root}/ws`) return handleRequest(request);
    if (request.method !== "GET" || request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      const response = await handleRequest(request);
      // No cross-request HTTP stream fallback in this host.
      response.headers.delete("x-jazz-stream");
      return response;
    }
    if (!context) throw new Error("Worker WebSocket hosting requires the fetch execution context");
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
  };
  return Object.assign(jazz, { handle }) as Jazz;
}
