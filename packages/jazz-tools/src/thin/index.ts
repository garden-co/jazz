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
import { createAccountDbWithRuntimeSource, type AccountDbConfig } from "../accounts/context.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { Db } from "../runtime/db.js";
import { isThinDb, markThinDb } from "../runtime/local-first-upgrade.js";
import { LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";
import { ThinRuntimeSource } from "./thin-runtime-source.js";
import { ThinRuntime, type ThinRuntimeOptions, type WrittenRow } from "./thin-runtime.js";
import { translateQuery } from "../runtime/query-adapter.js";
import type { WasmSchema } from "../drivers/types.js";

export { ThinRuntimeSource } from "./thin-runtime-source.js";
export {
  ThinConnectionLostError,
  ThinRequestNotSentError,
  ThinTransactionLostError,
  ThinRemoteError,
  type ThinRuntimeOptions,
} from "./thin-runtime.js";
// Accounts are the session's (`createJazzClient`): only its error and the
// storage a host may supply are public, as on the package root.
export * from "../accounts/exports.js";
export type { Db } from "../runtime/db.js";
export type { LocalFirstLoader, LocalFirstModule } from "../runtime/local-first-upgrade.js";

export type ThinDbConfig = AccountDbConfig & { thin?: ThinRuntimeOptions };

/**
 * @internal The session opens its thin `Db` with this (`createJazzClient`);
 * apps can't call it, since it takes the session's own account handle.
 * `serverUrl` is required.
 */
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
 * the new `Db` with what it has locally. The thin `Db` then refuses new work
 * and closes once its writes are answered, in the background: the returned
 * `Db` doesn't wait for them, so a server that is down doesn't hold the
 * upgrade (those writes stay in the thin client's memory until it is back,
 * and are lost if the page closes first). Shutting the returned `Db` down
 * (a sign-out) stops that wait and closes the thin runtime, dropping what it
 * still holds. A read the app made on the thin `Db` that never reached the
 * server ends with `ThinRequestNotSentError`: read again on the returned
 * `Db`. The app swaps the returned `Db` in where it held the thin one.
 *
 * This is for a thin `Db` the app opened itself with {@link createClient}.
 * Under a Jazz provider or `createJazzClient`, call the `Db` method
 * `db.upgradeToLocalFirst(loader)` instead: it takes the loader, and the
 * session upgrades in place, so the app keeps the same `Db`.
 */
export async function upgradeToLocalFirst(thin: Db, open: () => Promise<Db>): Promise<Db> {
  const runtime = thinRuntimeOf(thin);
  const next = await open();
  try {
    await prefetchThinQueries(thin, next);
    await handOffThinWrites(thin, next);
  } catch (error) {
    await next.shutdown();
    throw error;
  }
  // The thin Db's last writes upload in the background, then it closes: with
  // the server down, waiting for them would hold the upgrade until it is
  // back. Shutting `next` down ends the wait, and the thin runtime closes.
  runtime?.endUnsentReads();
  next.onShutdown(() => thin.abortGracefulShutdown());
  void thin
    .shutdown({ waitForSync: true })
    .catch(() => thin.shutdown())
    .catch(() => {});
  return next;
}

const PREFETCH_ROUNDS = 3;

/**
 * @internal Read every query `thin` is subscribed to into `next` from the
 * server, until the thin link reports offline or a read fails. The
 * prefetch only spares the app's live queries a moment without rows: with
 * no server to read from (a first visit offline, an outage) it must not
 * hold the upgrade, or a guest's writes would stay in the thin client's
 * memory, never stored, until the server is back.
 */
export async function prefetchThinQueries(thin: Db, next: Db): Promise<void> {
  const runtime = thinRuntimeOf(thin);
  if (!runtime) return;
  const ended = { done: false };
  // Still connecting: wait to learn whether the server answers (an outage
  // reports as offline after a few seconds).
  let stop!: () => void;
  const offline = new Promise<void>((resolve) => {
    stop = thin.onSyncStatusChange(({ link }) => {
      if (link.state === "offline") resolve();
    });
  });
  try {
    if (thin.getSyncStatus().link.state === "offline") return;
    // A read that fails (the server went away) just ends the prefetch.
    await Promise.race([prefetchRounds(runtime, next, ended).catch(() => {}), offline]);
  } finally {
    ended.done = true;
    stop();
  }
}

/**
 * The prefetch's rounds, until `ended`. A read already sent when the
 * prefetch ends may still land in `next` later: rows from the server, as its
 * own sync would bring them, so that is harmless.
 */
async function prefetchRounds(
  runtime: ThinRuntime,
  next: Db,
  ended: { done: boolean },
): Promise<void> {
  // Queries the app subscribes to while one round reads are read in the
  // next, so each live query opens on the local-first Db with its rows.
  // A few rounds: a component that keeps subscribing with new queries
  // must not hold the upgrade.
  const read = new Set<string>();
  for (let round = 0; round < PREFETCH_ROUNDS && !ended.done; round++) {
    const pending = runtime.liveQueryJsons().filter((query) => !read.has(query));
    if (pending.length === 0) return;
    await next.prefetchRuntimeQueries(runtime.schema, pending);
    for (const query of pending) read.add(query);
  }
}

/** How long one hand-off read may wait for the server. */
const HAND_OFF_READ_MS = 5_000;

/**
 * @internal The rows `thin` wrote reach `next` too, so a local-first read
 * there shows them: those the server already confirmed now, later ones as
 * the server confirms them. Each read is best-effort, like the prefetch: it
 * ends when either link reports offline or after {@link HAND_OFF_READ_MS},
 * so neither the upgrade nor a write's wait hangs on a server that doesn't
 * answer; the rows then arrive with `next`'s own sync. It never throws.
 *
 * Not handed off, also left to sync: a delete (a read of a deleted row
 * brings nothing back, and only a local store that already held the row,
 * another tab's, can show it), a write that ended with its fate unknown (a
 * `ThinConnectionLostError`: it may have committed, but the server never
 * confirmed it, so a local-first read shows it only once `next` syncs it),
 * and writes older than the last 1,024 the thin runtime keeps.
 */
export async function handOffThinWrites(thin: Db, next: Db): Promise<void> {
  const runtime = thinRuntimeOf(thin);
  if (!runtime) return;
  const read = (rows: readonly WrittenRow[]) =>
    readWhileOnline([thin, next], () =>
      next.prefetchRuntimeQueries(runtime.schema, rowQueries(runtime.schema, rows)),
    );
  const committed = runtime.handOffWrittenRows(read);
  // A failed or superseded upgrade shuts `next` down: nothing to hand to.
  next.onShutdown(() => runtime.stopHandingOffWrittenRows(read));
  if (committed.length > 0) await read(committed);
}

/** Run `read` until it settles, one of `dbs` reports offline, or the time is up. */
async function readWhileOnline(dbs: readonly Db[], read: () => Promise<void>): Promise<void> {
  if (dbs.some((db) => db.getSyncStatus().link.state === "offline")) return;
  const stops: (() => void)[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;
  const ended = new Promise<void>((resolve) => {
    for (const db of dbs)
      stops.push(
        db.onSyncStatusChange(({ link }) => {
          if (link.state === "offline") resolve();
        }),
      );
    timer = setTimeout(resolve, HAND_OFF_READ_MS);
  });
  try {
    await Promise.race([read().catch(() => {}), ended]);
  } finally {
    clearTimeout(timer);
    for (const stop of stops) stop();
  }
}

/** One runtime query per table, for the rows of it in `rows`. */
function rowQueries(schema: WasmSchema, rows: readonly WrittenRow[]): string[] {
  const ids = new Map<string, Set<string>>();
  for (const { table, id } of rows) {
    if (!schema[table]) continue;
    let set = ids.get(table);
    if (!set) ids.set(table, (set = new Set()));
    set.add(id);
  }
  return [...ids].map(([table, set]) =>
    translateQuery(
      JSON.stringify({ table, conditions: [{ column: "id", op: "in", value: [...set] }] }),
      schema,
    ),
  );
}

function thinRuntimeOf(thin: Db): ThinRuntime | null {
  const runtime = thin.currentRuntime();
  if (!isThinDb(thin) || (runtime !== null && !(runtime instanceof ThinRuntime))) {
    throw new Error("upgradeToLocalFirst takes a Db created by jazz-tools/thin");
  }
  return runtime;
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
