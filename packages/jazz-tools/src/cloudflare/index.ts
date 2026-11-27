/**
 * Your app on Cloudflare: your backend and its Jazz core in one Worker.
 *
 * ```ts
 * import { createJazz, durableObjects } from "jazz-tools/backend";
 * import { jazzWorker } from "jazz-tools/cloudflare";
 *
 * const worker = jazzWorker((env: Env) => {
 *   const jazz = createJazz({ app, permissions, authenticate, store: durableObjects({ binding: env.JAZZ }) });
 *   return { jazz, fetch: routes(jazz).fetch };
 * });
 * export const { JazzCore, JazzBackend } = worker;
 * export default worker;
 * ```
 *
 * with the Durable Object bindings `JAZZ` (class `JazzCore`) and
 * `JAZZ_BACKEND` (class `JazzBackend`), both SQLite-backed
 * (`new_sqlite_classes`), and optionally an R2 bucket bound as `JAZZ_BLOBS`
 * for large values. There is no secret to set: `durableObjects()` gets the
 * core's from the core, over its binding. Only workerd loads this module
 * (the `workerd` export condition).
 *
 * @experimental
 */
import { JazzCore } from "jazz-wasm/durable-object";
import type { CoreBinding } from "../backend/core-transport.js";
// Through the workerd entry, which installs jazz-wasm as the backend engine.
import { jazzBackend, routeToJazzBackend, type JazzBackendApp } from "../backend/workerd.js";

export type { JazzBackendApp };

/** The bindings a `jazzWorker` Worker has: the core's, and the backend object's. */
export interface JazzWorkerEnv {
  /** The `JazzCore` namespace: pass it to `durableObjects({ binding: env.JAZZ })`. */
  readonly JAZZ: CoreBinding;
  readonly JAZZ_BACKEND: Parameters<typeof routeToJazzBackend>[1]["JAZZ_BACKEND"];
}

export interface JazzWorker<Env> {
  /** The Worker's `fetch`: every request goes to the app's backend object. */
  fetch(request: Request, env: Env): Promise<Response>;
  /** The core's Durable Object class: export it as `JazzCore`. */
  readonly JazzCore: typeof JazzCore;
  /** The backend's Durable Object class: export it as `JazzBackend`. */
  readonly JazzBackend: ReturnType<typeof jazzBackend<Env>>;
}

/**
 * The app's Worker. `make` runs once per backend object instance, with the
 * Worker's `env`, and returns the app's Jazz (`createJazz`, usually with
 * `durableObjects({ binding: env.JAZZ })`) and its own routes (`fetch`, for
 * every request outside the Jazz's `/jazz`). Export the result as the
 * Worker's default, and its `JazzCore` and `JazzBackend` under those names:
 * a renamed class is a new, empty app.
 */
export function jazzWorker<Env extends JazzWorkerEnv>(
  make: (env: Env) => JazzBackendApp,
): JazzWorker<Env> {
  return {
    fetch: (request, env) => routeToJazzBackend(request, env),
    JazzCore,
    JazzBackend: jazzBackend(make),
  };
}
