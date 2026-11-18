/**
 * Jazz in a Cloudflare Worker: `createJazz` runs in a Durable Object (the
 * app's backend object), because a Worker can't use a socket one request
 * opened from another request, and a Jazz keeps its socket to the core open
 * across requests. The Worker's `fetch` forwards to that object; the object
 * serves Jazz's routes under `basePath` and hands everything else to the
 * app's own `fetch` (a Hono app, say), where `jazz.db` and `forReqUser` work.
 *
 * ```ts
 * import { createJazz, durableObjects, jazzBackend, routeToJazzBackend } from "jazz-tools/backend";
 * export { JazzCore } from "jazz-tools/durable-object";
 *
 * export const JazzBackend = jazzBackend((env: Env) => {
 *   const jazz = createJazz({
 *     app, permissions, authenticate,
 *     store: durableObjects({ binding: env.JAZZ, secret: env.JAZZ_CORE_SECRET }),
 *   });
 *   return { jazz, fetch: honoApp(jazz).fetch };
 * });
 * export default { fetch: routeToJazzBackend };
 * ```
 *
 * with the bindings `JAZZ` (class `JazzCore`), `JAZZ_BACKEND` (class
 * `JazzBackend`) and the secret `JAZZ_CORE_SECRET`.
 */
import type { WorkerdWebSocket } from "./core-transport.js";
import type { Jazz, WsLikeSocket } from "./create-jazz.js";

/** What the app's backend object runs: its Jazz, and its own routes. */
export interface JazzBackendApp {
  readonly jazz: Jazz;
  /** The app's routes: everything outside `jazz.basePath`. */
  fetch?(request: Request): Response | Promise<Response>;
}

/** A Durable Object namespace binding, as `routeToJazzBackend` uses it. */
interface BackendNamespace {
  idFromName(name: string): unknown;
  get(id: unknown): { fetch(request: Request): Promise<Response> };
}

/** The name of the app's one backend object. */
const BACKEND_OBJECT_NAME = "app";

/**
 * The app's backend object class: export it from the Worker (as
 * `JazzBackend`, bound as `JAZZ_BACKEND`). `make` runs once per object
 * instance, with the Worker's `env`.
 */
export function jazzBackend<Env>(
  make: (env: Env) => JazzBackendApp,
): new (ctx: unknown, env: Env) => { fetch(request: Request): Promise<Response> } {
  return class JazzBackend {
    #app: JazzBackendApp | undefined;
    constructor(
      readonly ctx: unknown,
      readonly env: Env,
    ) {}

    async fetch(request: Request): Promise<Response> {
      const app = (this.#app ??= make(this.env));
      if (underBasePath(new URL(request.url).pathname, app.jazz.basePath)) {
        return serveJazz(app.jazz, request);
      }
      if (app.fetch) return app.fetch(request);
      return new Response("Not found", { status: 404 });
    }
  };
}

/**
 * The Worker's `fetch`: every request goes to the app's backend object
 * (`env.JAZZ_BACKEND`).
 */
export function routeToJazzBackend(
  request: Request,
  env: { JAZZ_BACKEND: BackendNamespace },
): Promise<Response> {
  const namespace = env.JAZZ_BACKEND;
  if (!namespace || typeof namespace.idFromName !== "function") {
    throw new Error(
      "routeToJazzBackend: the Worker has no JAZZ_BACKEND Durable Object binding (class JazzBackend)",
    );
  }
  return namespace.get(namespace.idFromName(BACKEND_OBJECT_NAME)).fetch(request);
}

/**
 * Answer a request under `jazz.basePath` inside a Durable Object: sync
 * sockets through a `WebSocketPair`, everything else through `jazz.handle`.
 */
export async function serveJazz(jazz: Jazz, request: Request): Promise<Response> {
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return jazz.handle(request);
  const Pair = (globalThis as { WebSocketPair?: new () => Record<0 | 1, WorkerdWebSocket> })
    .WebSocketPair;
  if (!Pair) throw new Error("serveJazz: no WebSocketPair here; it runs in a Cloudflare Worker");
  const pair = new Pair();
  const client = pair[0];
  const server = pair[1];
  // Binary messages as ArrayBuffers, not workerd's default Blobs.
  server.binaryType = "arraybuffer";
  server.accept();
  jazz.acceptSocket(request, wsLike(server));
  return new Response(null, { status: 101, webSocket: client } as ResponseInit);
}

function underBasePath(pathname: string, basePath: string): boolean {
  return pathname === basePath || pathname.startsWith(`${basePath}/`);
}

/** workerd's server socket in the `ws` package's shape, which `acceptSocket` takes. */
function wsLike(ws: WorkerdWebSocket): WsLikeSocket {
  const socket = {
    send: (data: Uint8Array) => ws.send(data),
    close: (code?: number, reason?: string) => {
      try {
        ws.close(code, reason);
      } catch {
        // Already closed.
      }
    },
    on(event: "message" | "close" | "error", listener: (...args: never[]) => void) {
      const call = listener as (...args: unknown[]) => void;
      if (event === "message") {
        ws.addEventListener("message", ({ data }) => call(data, typeof data !== "string"));
      } else if (event === "close") {
        ws.addEventListener("close", () => call());
      } else {
        ws.addEventListener("error", (error) => call(error));
      }
      return socket;
    },
  };
  return socket as WsLikeSocket;
}
