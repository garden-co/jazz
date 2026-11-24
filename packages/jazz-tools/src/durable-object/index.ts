/**
 * A Jazz core in a Cloudflare Durable Object: the Worker side of the
 * `durableObjects()` store. Deploy a Worker whose module exports `JazzCore`
 * and forwards every request with `routeJazzRequest`:
 *
 * ```ts
 * import { createJazzCore, routeJazzRequest } from "jazz-tools/durable-object";
 * export const JazzCore = createJazzCore({ blobs: "sqlite" });
 * export default { fetch: routeJazzRequest };
 * ```
 *
 * with two bindings: `JAZZ`, a Durable Object namespace of class `JazzCore`
 * (SQLite-backed: `new_sqlite_classes`), and `JAZZ_CORE_SECRET`, a secret of
 * at least 32 characters that stays the same across deployments. Each Worker
 * holds one app in one object, which keeps the app's id itself.
 *
 * Select R2 with `blobs: (env) => r2(env.JAZZ_BLOBS!)` to move large
 * values to the bucket under `jazz/<app id>/` or `JAZZ_BLOBS_PREFIX`.
 * Select an existing external store with `blobs: () => s3({ bucket, region })`
 * or `blobs: () => vercelBlob()`. Configure credentials on this Worker.
 * Only workerd loads this module (the `workerd` export condition).
 *
 * @experimental
 */
export { createJazzCore, r2, routeJazzRequest, type JazzCoreEnv } from "jazz-wasm/durable-object";
export { s3, vercelBlob } from "../backend/blobs.js";
