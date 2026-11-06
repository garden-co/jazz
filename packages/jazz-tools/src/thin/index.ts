/**
 * `jazz-tools/thin`: a browser client with no local engine (SPEC 8.14).
 *
 * The server evaluates every query under the signed-in session and sends
 * finished results; writes run on the server. The `Db` it returns has the
 * same query and write API as the local-first client's `Db`, without
 * WASM, workers or local storage.
 *
 * Transactions, branch and snapshot views and large-value streaming run on
 * the server too. Not supported yet: offline use and attributed writes.
 * Upgrade to the local-first client for offline use.
 */
import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";
import { createAccountManagerWithMinter } from "../accounts/account-manager-core.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { Db } from "../runtime/db.js";
import { isThinDb, markThinDb } from "../runtime/local-first-upgrade.js";
import { LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";
import { ThinRuntimeSource } from "./thin-runtime-source.js";
import { ThinRuntime, type ThinRuntimeOptions } from "./thin-runtime.js";

export { ThinRuntimeSource } from "./thin-runtime-source.js";
export {
  ThinConnectionLostError,
  ThinTransactionLostError,
  ThinRemoteError,
  type ThinRuntimeOptions,
} from "./thin-runtime.js";
export type {
  AccountHandle,
  AccountIdentity,
  AccountManager,
  AccountSnapshot,
} from "../accounts/state.js";
export { AccountAuthError, type JWTAuth } from "../accounts/enrollment.js";
export type { AccountStore } from "../accounts/persistence.js";
export type { Db } from "../runtime/db.js";
export type { LocalFirstLoader, LocalFirstModule } from "../runtime/local-first-upgrade.js";

export type ThinDbConfig = AccountDbConfig & { thin?: ThinRuntimeOptions };

/** Open a thin `Db` for a signed-in account. `serverUrl` is required. */
export async function createClient(config: ThinDbConfig): Promise<Db> {
  const { thin, ...dbConfig } = config;
  const db = await createAccountDbWithRuntimeSource(
    { ...dbConfig, driver: { type: "memory" } },
    new ThinRuntimeSource(thin),
  );
  markThinDb(db);
  return db;
}

/**
 * Move an app from a thin `Db` to a local-first one, once it has loaded the
 * full client. `open` creates the local-first `Db` for the same account, for
 * example after `await import("jazz-tools")`. The queries the app is
 * subscribed to are read into the new `Db` from the server first, so its live
 * queries answer from local data straight away. Queries subscribed during
 * that read are read too, for up to 3 rounds; one subscribed later opens on
 * the new `Db` with what it has locally. Then the thin `Db` waits for
 * its writes and shuts down. The app swaps the returned `Db` in where it held
 * the thin one.
 *
 * This is for a thin `Db` the app opened itself with {@link createClient}.
 * Under a Jazz provider or `createJazzClient`, call the `Db` method
 * `db.upgradeToLocalFirst(loader)` instead: it takes the loader, and the
 * session upgrades in place, so the app keeps the same `Db`.
 */
export async function upgradeToLocalFirst(thin: Db, open: () => Promise<Db>): Promise<Db> {
  thinRuntimeOf(thin);
  const next = await open();
  try {
    await prefetchThinQueries(thin, next);
    await thin.shutdown({ waitForSync: true });
  } catch (error) {
    // Until handoff succeeds, this operation owns the replacement.
    await next.shutdown().catch(() => {});
    throw error;
  }
  return next;
}

const PREFETCH_ROUNDS = 3;

/** @internal Read every query `thin` is subscribed to into `next` from the server. */
export async function prefetchThinQueries(thin: Db, next: Db): Promise<void> {
  const runtime = thinRuntimeOf(thin);
  if (!runtime) return;
  // Queries the app subscribes to while one round reads are read in the
  // next, so each live query opens on the local-first Db with its rows.
  // A few rounds: a component that keeps subscribing with new queries
  // must not hold the upgrade.
  const read = new Set<string>();
  for (let round = 0; round < PREFETCH_ROUNDS; round++) {
    const pending = runtime.liveQueryJsons().filter((query) => !read.has(query));
    if (pending.length === 0) return;
    await next.prefetchRuntimeQueries(runtime.schema, pending);
    for (const query of pending) read.add(query);
  }
}

function thinRuntimeOf(thin: Db): ThinRuntime | null {
  const runtime = thin.currentRuntime();
  if (!isThinDb(thin) || (runtime !== null && !(runtime instanceof ThinRuntime))) {
    throw new Error("upgradeToLocalFirst takes a Db created by jazz-tools/thin");
  }
  return runtime;
}

/**
 * The account manager of `jazz-tools`, signing local-first identities in
 * JavaScript instead of WASM.
 */
export async function createAccountManager(config: AccountManagerConfig) {
  return createAccountManagerWithMinter(config, mintLocalFirstTokenInJs);
}

/** @internal Sign a local-first token in JS, byte-identical to the WASM runtime. */
export function mintLocalFirstTokenInJs(secret: string, audience: string): string {
  return mintSelfSignedToken(LOCAL_FIRST_ISSUER, {
    secret: authSecretSeedForMinting(secret),
    audience,
    ttlSeconds: 3600,
    nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
  });
}
