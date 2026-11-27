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
 * }, { blobs: "sqlite" });
 * export const { JazzCore, JazzBackend } = worker;
 * export default worker;
 * ```
 *
 * with the Durable Object bindings `JAZZ` (class `JazzCore`) and
 * `JAZZ_BACKEND` (class `JazzBackend`), both SQLite-backed
 * (`new_sqlite_classes`), and an explicit blob destination
 * for large values. There is no secret to set: `durableObjects()` gets the
 * core's from the core, over its binding. Only workerd loads this module
 * (the `workerd` export condition).
 *
 * @experimental
 */
import { createJazzCore, type JazzCoreEnv } from "jazz-wasm/durable-object";

// Through the workerd entry, which installs jazz-wasm as the backend engine.
import { jazzBackend, routeToJazzBackend, type JazzBackendApp } from "../backend/workerd.js";

export type { JazzBackendApp };

/** The bindings a `jazzWorker` Worker has: the core's, and the backend object's. */
export interface JazzWorkerEnv extends JazzCoreEnv {
  readonly JAZZ_BACKEND: Parameters<typeof routeToJazzBackend>[1]["JAZZ_BACKEND"];
}

export interface JazzWorker<Env extends JazzWorkerEnv> {
  /** The Worker's `fetch`: every request goes to the app's backend object. */
  fetch(request: Request, env: Env): Promise<Response>;
  /** The core's Durable Object class: export it as `JazzCore`. */
  readonly JazzCore: ReturnType<typeof createJazzCore<Env>>;
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
  options: Parameters<typeof createJazzCore<Env>>[0],
): JazzWorker<Env> {
  return {
    fetch: (request, env) => routeToJazzBackend(request, env),
    JazzCore: createJazzCore<Env>(options),
    JazzBackend: jazzBackend(make),
  };
}
