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
import type { Jazz } from "./create-jazz.js";

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
 * Serve Jazz through its configured provider inside a Durable Object.
 */
export async function serveJazz(jazz: Jazz, request: Request): Promise<Response> {
  return jazz.handle(request);
}

function underBasePath(pathname: string, basePath: string): boolean {
  return pathname === basePath || pathname.startsWith(`${basePath}/`);
}
