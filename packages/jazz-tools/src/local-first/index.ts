/**
 * `jazz-tools/local-first`: the local-first browser client, with its WASM
 * engine, worker and local storage.
 *
 * Browser apps start on the thin client and load this entry when they
 * upgrade, from app code so the bundler gives it its own chunk:
 *
 * ```tsx
 * <JazzProvider client={() => import("jazz-tools/local-first")}>
 * ```
 *
 * or on demand with `useDb().upgradeToLocalFirst(() => import("jazz-tools/local-first"))`.
 */
export { createDb as createClient } from "../runtime/default-create-db.js";
export type { LocalFirstLoader, LocalFirstModule } from "../runtime/local-first-upgrade.js";
/** @internal How a `clearOnSignOut` session wipes a signed-out account's store (#5250). */
export { deleteLocalFirstStore } from "../runtime/delete-local-first-store.js";
