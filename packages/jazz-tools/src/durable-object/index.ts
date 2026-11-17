/**
 * A Jazz core in a Cloudflare Durable Object: the Worker side of the
 * `durableObjects()` store. Deploy a Worker whose module exports `JazzCore`
 * and forwards every request with `routeJazzRequest`:
 *
 * ```ts
 * import { JazzCore, routeJazzRequest } from "jazz-tools/durable-object";
 * export { JazzCore };
 * export default { fetch: routeJazzRequest };
 * ```
 *
 * with the bindings `JAZZ` (a Durable Object namespace of class `JazzCore`,
 * SQLite-backed: `new_sqlite_classes`), `JAZZ_CORE_SECRET` (a secret of at
 * least 32 characters, stable across deployments) and `JAZZ_APP_ID`. Each
 * app is one object. Only workerd loads this module (the `workerd` export
 * condition).
 */
export { JazzCore, routeJazzRequest, type JazzCoreEnv } from "jazz-wasm/durable-object";
