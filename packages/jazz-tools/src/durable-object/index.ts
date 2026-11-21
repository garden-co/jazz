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
 * holds one app in one object, which keeps the app's id itself.
 *
 * Bind an R2 bucket as `JAZZ_BLOBS` too (recommended): large values' bytes
 * then live in the bucket, under `jazz/<app id>/` (or `JAZZ_BLOBS_PREFIX`),
 * instead of the object's SQLite. A store that started without it moves its
 * large values there once it is bound. Only workerd
 * loads this module (the `workerd` export condition).
 *
 * @experimental
 */
export { JazzCore, routeJazzRequest, type JazzCoreEnv } from "jazz-wasm/durable-object";
