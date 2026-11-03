import type { AccountDbConfig } from "../accounts/context.js";
import type { Db } from "./db.js";

/**
 * What `import("jazz-tools/local-first")` resolves to: the local-first
 * client, with its WASM engine, worker and local storage.
 */
export interface LocalFirstModule {
  createDb(config: AccountDbConfig): Promise<Db>;
}

/**
 * Loads the local-first client, for example
 * `() => import("jazz-tools/local-first")`. Keeping the `import()` in app
 * code lets the bundler put the local-first client and its WASM in their own
 * chunk, which loads only when the app upgrades.
 */
export type LocalFirstLoader = () => Promise<LocalFirstModule>;

type Upgrader = (loader: LocalFirstLoader) => Promise<void>;

const upgraders = new WeakMap<Db, Upgrader>();
const thinDbs = new WeakSet<Db>();

/** @internal Mark a `Db` as thin, opened by `jazz-tools/thin`. */
export function markThinDb(db: Db): void {
  thinDbs.add(db);
}

/** @internal Whether `db` is a thin `Db`. */
export function isThinDb(db: Db): boolean {
  return thinDbs.has(db);
}

/** @internal A provider's session upgrades its thin `Db` through this. */
export function setLocalFirstUpgrader(db: Db, upgrade: Upgrader): void {
  upgraders.set(db, upgrade);
}

/** @internal Implementation of `Db.upgradeToLocalFirst`. */
export async function upgradeDbToLocalFirst(db: Db, loader: LocalFirstLoader): Promise<void> {
  const upgrade = upgraders.get(db);
  if (upgrade) return await upgrade(loader);
  if (isThinDb(db)) {
    throw new Error(
      "This thin Db was not opened by a Jazz provider. Use upgradeToLocalFirst from jazz-tools/thin, which returns the local-first Db.",
    );
  }
  // Already local-first: nothing to do.
}
