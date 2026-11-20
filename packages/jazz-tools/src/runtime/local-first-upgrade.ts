import type { AccountDbConfig } from "../accounts/context.js";
import type { Db } from "./db.js";
import { EncryptionNeedsLocalFirstError } from "./encryption-needs-local-first.js";

/**
 * What `import("jazz-tools/local-first")` resolves to: the local-first
 * client, with its WASM engine, worker and local storage.
 */
export interface LocalFirstModule {
  createClient(config: AccountDbConfig): Promise<Db>;
  /**
   * @internal Delete a signed-out account's browser store without opening
   * it (`clearOnSignOut`, #5250). `jazz-tools/local-first` provides it; a
   * loader that gives only `createClient` gets Jazz's own, loaded on demand.
   */
  deleteLocalFirstStore?(
    config: Omit<AccountDbConfig, "account"> & {
      accountId: string;
      accountRegistryAuthority: string;
    },
  ): Promise<"deleted" | "held">;
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
const loaders = new WeakMap<Db, LocalFirstLoader>();
const thinDbs = new WeakSet<Db>();

/** @internal Mark a `Db` as thin, opened by `jazz-tools/thin`. */
export function markThinDb(db: Db): void {
  thinDbs.add(db);
}

/** @internal Whether `db` is a thin `Db`. */
export function isThinDb(db: Db): boolean {
  // A thin Db that upgraded is a handle on its local-first successor.
  return thinDbs.has(typeof db?.current === "function" ? db.current() : db);
}

/**
 * @internal A provider's session upgrades its thin `Db` through this, with
 * `loader` when the app gave the session one (`client`).
 */
export function setLocalFirstUpgrader(db: Db, upgrade: Upgrader, loader?: LocalFirstLoader): void {
  upgraders.set(db, upgrade);
  if (loader) loaders.set(db, loader);
}

/**
 * @internal The loader a session's thin `Db` upgrades with: the session's
 * `client`, or the one the app passed to `db.upgradeToLocalFirst`. Without
 * one, an operation that touches an encrypted column can't wait for the
 * local-first client.
 */
export function localFirstLoaderOf(db: Db): LocalFirstLoader | undefined {
  return upgraders.has(db) ? loaders.get(db) : undefined;
}

let refusalLogged = false;

/**
 * @internal What a thin `Db`'s refusal of an operation on an encrypted
 * column tells the app. Without a loader it is logged, once per page. In a
 * transaction with a loader, the local-first client starts loading and the
 * refusal says to begin the transaction again (a callback transaction
 * reruns by itself once it has loaded).
 */
export function reviewThinRefusal(
  db: Db,
  refusal: EncryptionNeedsLocalFirstError,
  inTransaction: boolean,
): EncryptionNeedsLocalFirstError {
  if (!upgraders.has(db)) return refusal;
  const loader = loaders.get(db);
  if (!loader) {
    if (!refusalLogged) {
      refusalLogged = true;
      console.error(refusal);
    }
    return refusal;
  }
  if (!inTransaction) return refusal;
  // Not from inside the request being refused: the upgrade starts at once,
  // after it.
  void Promise.resolve()
    .then(() => upgradeDbToLocalFirst(db, loader))
    .catch(() => {});
  const { operation, table, column } = refusal;
  return new EncryptionNeedsLocalFirstError({
    reason: "transaction",
    operation,
    table,
    ...(column === undefined ? {} : { column }),
  });
}

/** @internal Whether a session upgrades `db` (`setLocalFirstUpgrader`). */
export function hasLocalFirstUpgrader(db: Db): boolean {
  return upgraders.has(db);
}

/** @internal Implementation of `Db.upgradeToLocalFirst`. */
export async function upgradeDbToLocalFirst(db: Db, loader: LocalFirstLoader): Promise<void> {
  // `const { upgradeToLocalFirst } = db` loses its Db: say so, instead of
  // resolving as if there were nothing to upgrade.
  if (typeof db !== "object" || db === null) {
    throw new TypeError(
      "upgradeToLocalFirst was called without its Db: call it as db.upgradeToLocalFirst(loader), not destructured from the Db",
    );
  }
  const upgrade = upgraders.get(db);
  if (upgrade) {
    // Operations that touch an encrypted column wait for the upgrade with it.
    if (!loaders.has(db)) loaders.set(db, loader);
    return await upgrade(loader);
  }
  if (isThinDb(db)) {
    throw new Error(
      "This thin Db was not opened by a Jazz provider. Use upgradeToLocalFirst from jazz-tools/thin, which returns the local-first Db.",
    );
  }
  // Already local-first: nothing to do.
}
