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
 * with two bindings: `JAZZ`, a Durable Object namespace of class `JazzCore`
 * (SQLite-backed: `new_sqlite_classes`), and `JAZZ_CORE_SECRET`, a secret of
 * at least 32 characters that stays the same across deployments. Each Worker
 * holds one app in one object, which keeps the app's id itself. Only workerd
 * loads this module (the `workerd` export condition).
 *
 * @experimental
 */
export { JazzCore, routeJazzRequest, type JazzCoreEnv } from "jazz-wasm/durable-object";
