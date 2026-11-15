import {
  accountForgotten,
  ensureAutomaticLocalFirst,
  forgetDeviceAccount,
  keepDeviceKeyOnSignOut,
  keptDeviceKey,
} from "../accounts/persistence.js";
import { NoKeptDeviceAccountError } from "./no-kept-device-account-error.js";
import { deviceAccountStoreMissing } from "../accounts/process-store.js";
import { admitBackendAccount, type AccountHandle, type AccountManager } from "../accounts/state.js";
import type { JWTAuth, BackendAuth } from "../accounts/enrollment.js";
import {
  GracefulShutdownSyncError,
  SharedClientShutdownError,
} from "../runtime/graceful-shutdown-error.js";
import { createListeners, toError } from "../runtime/util.js";
import type { Db } from "../runtime/db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import { parseAuthSecret } from "../runtime/auth-secret-codec.js";
import type { DbReplacedReason } from "../runtime/db-replaced-error.js";
import { JazzSessionOperationError } from "./session-operation-error.js";

export interface SessionClient {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
}
/** A client whose database can wipe its local storage (`clearOnSignOut`). */
type WipeableClient = {
  db?: {
    markReplaced?(reason: DbReplacedReason): void;
    waitForSync?(options?: { timeoutMs?: number }): Promise<void>;
    deleteClientStorage?(): Promise<void>;
    getSyncStatus?(): { pendingUploads: number };
    isUsable?(): boolean;
    abortGracefulShutdown?(): void;
    getConfig?(): { driver?: { type?: string } };
    onSyncStatusChange?(listener: (status: { link: { state: string } }) => void): () => void;
  };
};

/**
 * Whether `client` still works after a teardown step failed: its Db says
 * so, or (a client without one) the failure is one that stops before
 * teardown, its sync wait or a shared client still in use elsewhere.
 */
function stillUsable(client: unknown, cause: unknown): boolean {
  const usable = (client as WipeableClient | undefined)?.db?.isUsable?.();
  if (usable !== undefined) return usable;
  return cause instanceof GracefulShutdownSyncError || cause instanceof SharedClientShutdownError;
}
export type SessionCoreOperation =
  | "createLocalFirst"
  | "restoreLocalFirst"
  | "reopenLocalFirst"
  | "loginWithBackend"
  | "linkWithBackend"
  | "logout"
  | "retry";
export interface SessionCoreSnapshot<Client> {
  /**
   * `signing-out`: signed out on this device (no account, no client), while
   * the signed-out user's client still uploads its last writes: the sign-out
   * couldn't reach the server (offline). It moves on to `signed-out` once
   * they have uploaded (or the server refused them); `error` says why it
   * waits. Nothing signs in meanwhile.
   */
  readonly status: "ready" | "signed-out" | "transitioning" | "signing-out" | "error" | "closed";
  readonly account?: AccountHandle;
  readonly client?: Client;
  readonly pending?: SessionCoreOperation;
  readonly error?: Error;
  /** Whether recovery repeats an uncommitted action or only reopens the selected account. */
  readonly recovery?: "action" | "session";
}
/** Say what a sign-out that drops unconfirmed writes dropped (`reason`: who asked). */
function warnDiscarded(writes: number, reason: string) {
  if (writes > 0)
    console.warn(
      `Jazz signed out and wiped ${writes} write(s) the server hadn't confirmed, as ${reason} asks. They are gone from this device; any the server did receive stay there.`,
    );
}

export interface SessionCoreActions {
  createLocalFirst(): Promise<void>;
  restoreLocalFirst(secret: string): Promise<void>;
  /**
   * Sign in again as the device account this device keeps that signed in
   * most recently (a sign-out keeps its key; `clearOnSignOut` doesn't).
   * Rejects with {@link NoKeptDeviceAccountError} when it keeps none.
   */
  reopenLocalFirst(): Promise<void>;
  /** That kept device account's key, as this session last saw the device's store; `undefined` without one. */
  keptLocalFirstKey(): string | undefined;
  /**
   * Sign in as whoever the app's own backend says is signed in. For apps that
   * mount Jazz with `createJazz`: the backend's `authenticate` decides, from
   * the app's own session cookie. The client holds no Jazz credential.
   */
  loginWithBackend(): Promise<void>;
  /**
   * Keep a local-first guest's account and data when they sign up: after your
   * own sign-up, the backend links this guest to the user it now names, and
   * the session continues as that user.
   */
  linkWithBackend(): Promise<void>;
  /**
   * Sign out. With `discardUnsynced`, drop the signed-out user's writes the
   * server hasn't confirmed and their local data instead of uploading them
   * first (also from `signing-out`).
   */
  logout(options?: { discardUnsynced?: boolean }): Promise<void>;
  retry(): Promise<void>;
  close(): Promise<void>;
}
export interface SessionCore<Client> extends SessionCoreActions {
  getSnapshot(): SessionCoreSnapshot<Client>;
  subscribe(listener: () => void): () => void;
}
export interface SessionCoreConsumer<Client> {
  acknowledge(snapshot: SessionCoreSnapshot<Client>): void;
  release(): void;
}
/**
 * @internal How long a transition waits for a view to acknowledge it before
 * moving on without that view (a view suspended behind React Suspense, or a
 * custom view that never acknowledges, would otherwise hold the session in
 * `transitioning` forever).
 */
export const SESSION_VIEW_ACKNOWLEDGE_TIMEOUT_MS = 5_000;
type Lease = {
  readonly name: string;
  waiting?: () => void;
  /** It missed a deadline: later transitions don't wait for it until it acknowledges again. */
  stale?: boolean;
};
const consumers = new WeakMap<object, (name: string) => SessionCoreConsumer<unknown>>();
type ClientReplacer = (
  open: (current: unknown, account: AccountHandle) => Promise<unknown>,
  adopt?: (current: unknown, next: unknown) => void,
) => Promise<void>;
const replacers = new WeakMap<object, ClientReplacer>();
const switchers = new WeakMap<object, (from: AccountHandle) => Promise<boolean>>();
const signOutCounts = new WeakMap<object, () => number>();

/**
 * @internal How many sign-outs (`logout`) the session has started. Work that
 * began before a sign-out (a backend check) compares it to drop its late
 * result instead of signing someone in again after the sign-out.
 */
export function jazzSessionSignOuts(session: SessionCore<unknown>): number {
  return signOutCounts.get(session)?.() ?? 0;
}

/**
 * The first wait before a `signing-out` session tries its upload again
 * (it doubles, up to {@link SIGN_OUT_RETRY_MAX_MS}); it also tries again
 * whenever the client reconnects.
 */
export const SIGN_OUT_RETRY_MS = 2_000;
/**
 * Failed deletes after which passes stop trying a pending discard on their
 * own (it stays pending and visible; `retry()` tries again). Failures count
 * at most once per {@link PENDING_WIPE_FAILURE_SPACING_MS}, so a passing
 * failure can't use them up in seconds: giving up takes at least ~10 minutes.
 */
const MAX_PENDING_WIPE_FAILURES = 5;
/** @internal */
export const PENDING_WIPE_FAILURE_SPACING_MS = 150_000;

/** A sign-in as a device account a discard on this device forgot while it opened. */
export class DiscardedDeviceAccountError extends Error {
  override readonly name = "DiscardedDeviceAccountError";
  constructor() {
    super(
      "Jazz couldn't sign in: this device account was discarded on this device (signOut({ discardUnsynced })) while it opened, so its data and key are gone here. Restore it with its key (restoreDeviceAccount) to use it again.",
    );
  }
}
const SIGN_OUT_RETRY_MAX_MS = 60_000;
const upgradeListeners = new WeakMap<object, Set<() => void>>();

/**
 * @internal Call `listener` when the session's client is upgraded in place
 * (see `replaceJazzSessionClient`'s `adopt`), which publishes no snapshot.
 */
export function onJazzSessionClientUpgraded<Client>(
  session: SessionCore<Client>,
  listener: () => void,
): () => void {
  let listeners = upgradeListeners.get(session);
  if (!listeners) upgradeListeners.set(session, (listeners = new Set()));
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/**
 * @internal Sign the current account out and the user the backend names now
 * in, as one transition: the session stays `transitioning` from the first
 * account to the next and never publishes `signed-out` in between. The first
 * account is torn down as on `logout` (its credentials revoked and, with
 * `clearOnSignOut`, its storage wiped). If the sign-in fails, the session
 * ends signed out with that error: the first account is gone either way.
 *
 * It waits for an operation still finishing, and resolves false without
 * switching when the session no longer holds `from` by then.
 */
export function switchJazzSessionUser<Client>(
  session: SessionCore<Client>,
  from: AccountHandle,
): Promise<boolean> {
  const switchUser = switchers.get(session);
  if (!switchUser) return Promise.reject(new Error("Unknown Jazz session"));
  return switchUser(from);
}

/**
 * @internal Swap the signed-in account's client for another one for the same
 * account, as the thin-to-local-first upgrade does. `open` gets the current
 * client; once it resolves the session publishes the new client, then shuts
 * the old one down after its writes have synced. If the session changes
 * account, logs out or closes meanwhile, the new client is shut down and this
 * rejects.
 */
export function replaceJazzSessionClient<Client>(
  session: SessionCore<Client>,
  open: (current: Client, account: AccountHandle) => Promise<Client>,
  /**
   * Hand the current client's work to `next` in place, instead of publishing
   * `next`: the session keeps its client, so the app keeps its handles,
   * queries and component state.
   */
  adopt?: (current: Client, next: Client) => void,
): Promise<void> {
  const replace = replacers.get(session);
  if (!replace) return Promise.reject(new Error("Unknown Jazz session"));
  return replace(
    open as (current: unknown, account: AccountHandle) => Promise<unknown>,
    adopt as ((current: unknown, next: unknown) => void) | undefined,
  );
}
/** @internal Framework commit acknowledgement; never required by imperative callers. */
export function attachSessionCoreConsumer<Client>(
  session: SessionCore<Client>,
  /** Names the view in the warning when it misses a transition's deadline. */
  name = "a Jazz session view",
): SessionCoreConsumer<Client> {
  const attach = consumers.get(session);
  if (!attach) throw new Error("Unknown Jazz session");
  return attach(name) as SessionCoreConsumer<Client>;
}

/** @internal Hosts prepare accounts and supply their client adapter once. */
export async function createJazzSessionOwner<
  Client extends SessionClient,
  Extra extends object = {},
>(options: {
  accounts: AccountManager<JWTAuth>;
  openClient(account: AccountHandle): Promise<Client>;
  initial?: "local-first" | "signed-out";
  /**
   * @internal Open as the backend this core admits with `auth`: only the
   * Node host passes it, for the session `createJazz` opens on its own core
   * and for a backend client its main backend trusts as a machine.
   */
  backend?: BackendAuth;
  /**
   * Wipe the signed-out account's local storage, unsynced writes included,
   * instead of keeping it for that user's next sign-in (shared devices).
   */
  clearOnSignOut?: boolean;
  /**
   * @internal With `clearOnSignOut`, how the host closes a signed-out
   * account's local-first client and wipes its storage, in place of
   * deleting it through the client: the browser closes this tab's client
   * first and, as the last tab out, wipes (see `replicaLifecycle`). False
   * when another tab still has the account open: that tab decides.
   */
  wipeSignedOut?(client: Client, account: AccountHandle): Promise<boolean | void>;
  /**
   * @internal With `clearOnSignOut`, whether another tab still has
   * `account`'s local store open, asked when this tab had no local store to
   * wipe (a thin client). Then, as when its wipe is held off, the
   * device account's key is kept for the last tab out to decide.
   */
  storeHeldElsewhere?(account: AccountHandle): Promise<boolean>;
  /**
   * @internal For `signOut({ discardUnsynced })`: delete `account`'s local
   * store by id, after its client shut down (the browser: under the store's
   * lock, `"held"` while another tab has it open). Without it the session
   * deletes through the client (`db.deleteClientStorage()`) before closing it.
   */
  deleteSignedOutStore?(
    account: string,
    /**
     * Finishing a pending discard: checked again, and run, inside the
     * store's exclusive lock. `wanted()` false (someone signed in as the
     * account since) cancels the wipe; `forget()` (the device key) runs
     * once the store is deleted, before another tab can open it again.
     */
    finishing?: { wanted(): boolean; forget(): Promise<void> },
  ): Promise<"deleted" | "held" | "cancelled">;
  /**
   * @internal Where a discard another tab held off is kept, so a reload or
   * another page finishes it (the browser: localStorage). Signing in as that
   * account clears its entry.
   */
  pendingDiscards?: {
    record(account: string): void;
    clear(account: string): void;
    list(): string[];
  };
  /** Add host methods (e.g. `reauthenticate`) to the session before it is sealed. */
  decorate?(session: SessionCore<Client>): Extra;
}): Promise<SessionCore<Client> & Extra> {
  const { accounts, openClient } = options;
  let selected = accounts.getLoggedIn();
  if (!selected && options.initial === "local-first") {
    const missing = deviceAccountStoreMissing(accounts, 'initial: "device"');
    if (missing) throw missing;
    selected = await ensureAutomaticLocalFirst(accounts);
  }
  if (options.backend)
    selected = await admitBackendAccount(accounts as AccountManager<unknown>, options.backend);
  await accounts.settleSelection();
  // Signed in as an account a discard left pending: it's in use again
  // (cleared before its client takes the store, so a finishing wipe sees it).
  if (selected) options.pendingDiscards?.clear(selected.id);
  let client = selected ? await openClient(selected) : undefined;
  let snapshot: SessionCoreSnapshot<Client> = Object.freeze({
    status: client ? "ready" : "signed-out",
    account: selected,
    client,
  });
  const listeners = createListeners("Jazz session observer failed");
  const leases = new Set<Lease>();
  const versions = new WeakMap<object, number>();
  let version = 0;
  versions.set(snapshot, version);
  let generation = 0;
  let busy: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  let loggingOut: Promise<void> | undefined;
  let signOuts = 0;
  // This session's own discard another tab held off, shown as the
  // signed-out error until it is finished or someone signs in. Finishing
  // it is kept in `pendingDiscards`, so a reload or another page does it
  // if this one doesn't.
  let pendingWipe: AccountHandle | undefined;
  const wipeHeld = () =>
    new Error(
      "Jazz signed out, but another tab still has this account open, so its data stays on this device (that tab may still upload its unsynced writes). Close that tab, then call retry() to finish wiping it (discardUnsynced).",
    );
  let finishing: Promise<void> | undefined;
  const wipeFailures = new Map<string, { count: number; at: number }>();
  // Discards whose wipe failed (an explicit try, or every automatic one):
  // their entry stays, and they are the signed-out error until a wipe
  // succeeds or someone signs in as that account. Passes on their own stop
  // trying them; `retry()` tries again.
  const stuckWipes = new Map<string, Error>();
  const wipeStuck = (cause: unknown) =>
    new Error(
      "Jazz signed out, but couldn't wipe the signed-out account's data from this device (discardUnsynced): it is still here. Call retry() to try again.",
      { cause },
    );
  /** What a signed-out session shows of its discards: held off, or failing. */
  const discardError = (): Error | undefined =>
    pendingWipe ? wipeHeld() : stuckWipes.values().next().value;
  /** Show `discardError()` (or its absence) while signed out here. */
  const showDiscardState = () => {
    if (selected || closed || snapshot.status !== "signed-out") return;
    const error = discardError();
    if (error?.message === snapshot.error?.message && !!error === !!snapshot.error) return;
    publish({ status: "signed-out", ...(error ? { error, recovery: "action" as const } : {}) });
  };
  /**
   * Finish the discards other tabs held off (this session's and earlier
   * pages'), each once no tab holds its store: wipe it and forget its key.
   * Runs on start, once each operation settles, and on `retry()`. Not while
   * an operation runs, nor for the account signed in here (it's in use).
   */
  const finishPendingWipes = (explicit = false): Promise<void> => {
    const deleteStore = options.deleteSignedOutStore;
    if (!deleteStore) return Promise.resolve();
    if (finishing) return explicit ? finishing.then(() => finishPendingWipes(true)) : finishing;
    let run: Promise<void> | undefined = undefined;
    run = (async () => {
      // Started first, so a pass that ends at once still clears `finishing`.
      await Promise.resolve();
      try {
        const pending = new Set(options.pendingDiscards?.list() ?? []);
        if (pendingWipe) pending.add(pendingWipe.id);
        for (const account of stuckWipes.keys()) pending.add(account);
        // Still pending, as of now: read again for each account (another
        // tab may have signed in as it since this pass began).
        const wanted = (account: string) =>
          !closed &&
          selected?.id !== account &&
          (options.pendingDiscards
            ? options.pendingDiscards.list().includes(account)
            : pendingWipe?.id === account);
        for (const account of pending) {
          if (closed || busy || loggingOut || draining) return;
          if (selected?.id === account) {
            options.pendingDiscards?.clear(account);
            stuckWipes.delete(account);
            continue;
          }
          // Gave up on its own: only `retry()` tries it again.
          if (!explicit && stuckWipes.has(account)) continue;
          const outcome = await deleteStore(account, {
            wanted: () => wanted(account),
            forget: () =>
              forgetDeviceAccount(accounts, account).catch((error: unknown) =>
                console.warn("Jazz couldn't forget the signed-out device account's key.", error),
              ),
          }).catch((error: unknown) => {
            // A delete that keeps failing stops being tried on its own after
            // a few passes (an explicit retry gives up at once), but stays
            // pending, and visible: the signed-out error says so until a
            // wipe succeeds. Never dropped while its data may be here.
            const now = Date.now();
            const last = wipeFailures.get(account);
            // Counted at most once per spacing: quick passes don't add up.
            const counts = !last || now - last.at >= PENDING_WIPE_FAILURE_SPACING_MS;
            const failed = (last?.count ?? 0) + (counts ? 1 : 0);
            if (counts) wipeFailures.set(account, { count: failed, at: now });
            if (explicit || failed >= MAX_PENDING_WIPE_FAILURES) {
              console.error(
                `Jazz couldn't wipe a discarded account's data (${failed} failed attempt(s)); it stays on this device until retry() wipes it.`,
                error,
              );
              stuckWipes.set(account, wipeStuck(error));
              if (pendingWipe?.id === account) pendingWipe = undefined;
              showDiscardState();
            } else console.warn("Jazz couldn't finish a discarded account's wipe yet.", error);
            return "failed" as const;
          });
          if (outcome === "held" || outcome === "failed") continue;
          wipeFailures.delete(account);
          stuckWipes.delete(account);
          if (pendingWipe?.id === account) pendingWipe = undefined;
          // Gone (or cancelled by a sign-in as it): nothing pending.
          if (outcome === "deleted") options.pendingDiscards?.clear(account);
          showDiscardState();
        }
      } finally {
        if (finishing === run) finishing = undefined;
      }
    })();
    finishing = run;
    return finishing;
  };
  /** `retry()` with a discard held off or failing: try to finish it now. */
  const finishWipe = async () => {
    await finishPendingWipes(true);
    const error = discardError();
    if (!error) return;
    if (!selected && !closed) publish({ status: "signed-out", error, recovery: "action" });
    throw error;
  };
  // A sign-out that couldn't reach the server (`signing-out`): the
  // signed-out user's client, still open so it can upload its last writes.
  let draining:
    | {
        readonly client: Client;
        /** The signed-out account: its credential stays valid for the upload. */
        readonly account: AccountHandle | undefined;
        /** Revokes that credential once the client is shut down. */
        release?: () => void;
        error: Error;
        /**
         * Why it signed out, when the app didn't ask (a refused upgrade:
         * `DiscardedDeviceAccountError`): shown while it waits, and on the
         * `signed-out` it ends in.
         */
        notice?: Error;
        attempt?: Promise<void>;
        timer?: ReturnType<typeof setTimeout>;
        waitMs: number;
        stopStatus?: () => void;
      }
    | undefined;
  let closed = false;
  const publish = (next: SessionCoreSnapshot<Client>) => {
    snapshot = Object.freeze({ ...next });
    versions.set(snapshot, ++version);
    listeners.notify();
  };
  const detach = (next: SessionCoreSnapshot<Client>) => {
    const late = new Set<Lease>();
    const waits = [...leases]
      .filter((lease) => !lease.stale)
      .map(
        (lease) =>
          new Promise<void>((resolve) => {
            late.add(lease);
            // A later transition (logout/close) also releases an earlier waiter.
            const previous = lease.waiting;
            lease.waiting = () => {
              previous?.();
              late.delete(lease);
              resolve();
            };
          }),
      );
    publish(next);
    if (waits.length === 0) return { done: Promise.resolve() };
    // Bounded: a view that never acknowledges (suspended, or gone without
    // releasing) must not hold the session in `transitioning`. The session
    // moves on without it, says which view it was, and stops waiting for it
    // until it acknowledges again.
    let timer: ReturnType<typeof setTimeout> | undefined;
    const deadline = new Promise<void>((resolve) => {
      timer = setTimeout(() => {
        for (const lease of late) {
          if (!leases.has(lease)) continue;
          lease.stale = true;
          console.warn(
            `Jazz moved on without ${lease.name}: it didn't acknowledge the session's change to "${next.status}" within ${SESSION_VIEW_ACKNOWLEDGE_TIMEOUT_MS}ms (is it suspended?). Its queries on the previous client end when that client shuts down. Jazz doesn't wait for this view again until it renders the session again.`,
          );
        }
        resolve();
      }, SESSION_VIEW_ACKNOWLEDGE_TIMEOUT_MS);
    });
    return {
      done: Promise.race([Promise.all(waits), deadline]).then(() => clearTimeout(timer)),
    };
  };
  const superseded = () => new JazzSessionOperationError("superseded");
  const perform = async (
    operation: SessionCoreOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    token: number,
    detached?: Promise<void>,
    // The current account is signed out first (a switch to another user).
    signsOut = operation === "logout",
    // `logout({ discardUnsynced })`: drop what the server hasn't confirmed.
    discard = false,
  ) => {
    if (token !== generation) throw superseded();
    // A switch has signed the previous account out: a failed sign-in can't reopen it.
    const previous = signsOut ? undefined : selected;
    // A clearOnSignOut wipe that may not have finished, reported with the
    // signed-out state: on a shared device the app must know.
    let unfinishedWipe: Error | undefined;
    let wiped = false;
    // Another tab still has the signed-out account's store open.
    let heldElsewhere = false;
    const previousSelected = signsOut ? selected : undefined;
    let thinSignOut = false;
    // A sign-out whose local part is done, uploading the user's last writes.
    let uploadingSignOut = false;
    await (detached ??
      detach({ status: "transitioning", account: selected, pending: operation }).done);
    if (token !== generation) throw superseded();
    try {
      if (client) {
        // A thin client keeps nothing in the browser, so there is nothing to wipe.
        const thin = isThinDb((client as WipeableClient).db as Db);
        thinSignOut = thin;
        if (signsOut && discard) {
          // The app gives the unsynced writes up: no wait for the server.
          const outcome = await discardLocal(client, selected);
          wiped = !outcome.kept;
          // Its data is still here (held off, or the wipe failed): keep the key too.
          heldElsewhere = outcome.kept;
          unfinishedWipe = outcome.unfinished;
          if (outcome.held) pendingWipe = selected;
        } else if (
          signsOut &&
          options.clearOnSignOut &&
          !thin &&
          options.wipeSignedOut &&
          selected
        ) {
          // False: another tab still has the account open, and decides.
          try {
            wiped = (await options.wipeSignedOut(client, selected)) !== false;
          } catch (error) {
            // Whether another tab still has the store is unknown: keep the key.
            heldElsewhere = true;
            throw error;
          }
          heldElsewhere = !wiped;
        } else if (signsOut && options.clearOnSignOut && !thin) {
          const db = (client as WipeableClient).db;
          // Upload what the server still accepts before wiping the rest.
          await db?.waitForSync?.({ timeoutMs: 5_000 }).catch(() => {});
          // Say what the wipe took, as a sign-out past refused writes does.
          const writes = db?.getSyncStatus?.().pendingUploads ?? 0;
          await db?.deleteClientStorage?.();
          wiped = true;
          warnDiscarded(writes, "clearOnSignOut");
          await client.shutdown();
        } else {
          // A client the server refuses (this user's sign-in ended) can't
          // upload, and the session is moving on from it either way.
          // What's left of a sign-out is the server's part: the upload.
          uploadingSignOut = operation === "logout";
          await stopPastRefusal(
            client,
            signsOut ? "Jazz signed out" : "Jazz closed the previous client",
          );
        }
        // Code that kept this client's Db learns it is gone, instead of
        // its queries going quiet.
        (client as WipeableClient).db?.markReplaced?.("signed-out");
        client = undefined;
      }
    } catch (cause) {
      if (token !== generation) throw cause;
      if (
        uploadingSignOut &&
        cause instanceof GracefulShutdownSyncError &&
        stillUsable(client, cause)
      ) {
        // Offline: the sign-out's local part is done (the server neither
        // refused the user nor took their last writes). Sign out now, and
        // keep the client open, out of sight, to upload them once the
        // server is back (`signing-out`).
        (client as WipeableClient).db?.markReplaced?.("signed-out");
        draining = {
          client: client!,
          account: selected,
          error: toError(cause),
          waitMs: SIGN_OUT_RETRY_MS,
        };
        client = undefined;
      } else {
        if (stillUsable(client, cause)) {
          // The step failed before the client's teardown began (offline, a
          // storage delete the worker couldn't start): it keeps working, and
          // the app may try the operation again.
          publish({
            status: "ready",
            account: selected,
            client,
            error: toError(cause),
            recovery: "action",
          });
          throw cause;
        }
        if (operation !== "logout") {
          // Its teardown began, so it can't be offered again. Unknown teardown
          // failures retain the client privately as a shutdown barrier: never
          // publish a potentially stopped runtime or enroll over it.
          publish({
            status: "error",
            account: selected,
            error: toError(cause),
            recovery: "action",
          });
          throw cause;
        }
        // A sign-out whose teardown began and failed still signs out: the
        // user asked to leave, and did. The failure is logged, and published
        // as the session's error only when a clearOnSignOut wipe may not have
        // finished. Code that kept the half-closed client's Db learns it is gone.
        console.error("Jazz signed out, but closing the signed-out client failed.", cause);
        if (options.clearOnSignOut && !wiped && !isThinDb((client as WipeableClient).db as Db))
          unfinishedWipe = new Error(
            "Jazz signed out, but couldn't finish wiping this device's data (clearOnSignOut). Some of the signed-out user's data may still be on this device.",
            { cause },
          );
        (client as WipeableClient).db?.markReplaced?.("signed-out");
        client = undefined;
      }
    }
    if (token !== generation) throw superseded();
    if (signsOut && options.clearOnSignOut && previousSelected) {
      if (!heldElsewhere && thinSignOut && options.storeHeldElsewhere)
        heldElsewhere = await options.storeHeldElsewhere(previousSelected).catch(() => true);
      // The last tab out decides: this tab's sign-out keeps the key.
      if (heldElsewhere) keepDeviceKeyOnSignOut(accounts);
    }
    if (token !== generation) throw superseded();
    try {
      selected = await mutate();
      // Signing in as an account a discard left pending cancels it: cleared
      // before its client takes the store, so a finishing wipe (which checks
      // inside the store's lock) sees it.
      if (selected) {
        options.pendingDiscards?.clear(selected.id);
        stuckWipes.delete(selected.id);
      }
    } catch (cause) {
      if (token !== generation) throw cause;
      selected = previous;
      try {
        client = previous ? await openClient(previous) : undefined;
      } catch (startup) {
        if (token === generation)
          publish({
            status: "error",
            account: selected,
            error: toError(startup),
            recovery: "action",
          });
        throw cause;
      }
      if (token === generation)
        publish({
          status: client ? "ready" : "signed-out",
          account: selected,
          client,
          error: toError(cause),
          recovery: "action",
        });
      throw cause;
    }
    if (token !== generation) throw superseded();
    try {
      // The registry operation has already succeeded. A local persistence
      // failure must not restore a now-invalid previous handle or repeat linking.
      await accounts.settleSelection(operation === "retry");
      if (token !== generation) throw superseded();
      client = selected ? await openClient(selected) : undefined;
      // A discard in another tab may have wiped and forgotten this device
      // account while its client waited for the store: the store it opened
      // is a fresh, empty one, and a reload couldn't reopen it. Say so
      // instead of signing in.
      if (
        client &&
        selected?.identity.issuer === "urn:jazz:local-first" &&
        options.pendingDiscards &&
        (await accountForgotten(accounts, selected.id))
      ) {
        const fresh = client;
        client = undefined;
        await fresh.shutdown().catch(() => {});
        accounts.logout();
        selected = undefined;
        await accounts.settleSelection().catch(() => {});
        const error = new DiscardedDeviceAccountError();
        if (token === generation) publish({ status: "signed-out", error, recovery: "action" });
        throw error;
      }
    } catch (cause) {
      if (cause instanceof DiscardedDeviceAccountError) throw cause;
      if (token === generation)
        publish({ status: "error", account: selected, error: toError(cause), recovery: "session" });
      // A sign-out that left its upload pending still uploads it.
      if (draining && !client) watchDrain(draining);
      throw cause;
    }
    if (token !== generation) throw superseded();
    // Discarded: nothing on this device reopens that account, as with
    // clearOnSignOut (unless another tab still has it open: it decides).
    if (discard && previousSelected && !heldElsewhere)
      await forgetDeviceAccount(accounts, previousSelected.id).catch((error: unknown) =>
        console.warn("Jazz couldn't forget the signed-out device account's key.", error),
      );
    if (token !== generation) throw superseded();
    if (draining && !client) {
      publish({
        status: "signing-out",
        error: draining.notice ?? draining.error,
        recovery: "action",
      });
      watchDrain(draining);
      return;
    }
    // Someone signed in: this session's held-off discard is no longer its
    // error (another account's sign-out mustn't bring it back). Finishing it
    // is left to `pendingDiscards`; signing in as that account cancels it.
    if (selected) pendingWipe = undefined;
    const discarding = client ? undefined : discardError();
    publish({
      status: client ? "ready" : "signed-out",
      account: selected,
      client,
      ...(unfinishedWipe
        ? { error: unfinishedWipe, recovery: "session" as const }
        : discarding
          ? { error: discarding, recovery: "action" as const }
          : {}),
    });
  };
  type Drain = NonNullable<typeof draining>;
  /** The `signed-out` a sign-out's upload ends in: with its notice, if any. */
  const drainEnded = (drain: Drain): SessionCoreSnapshot<Client> => ({
    status: "signed-out",
    ...(drain.notice ? { error: drain.notice, recovery: "action" as const } : {}),
  });
  /**
   * A sign-out that left its upload pending but couldn't save the signed-out
   * selection shows `error`: save it again first, so a reload can't reopen
   * the account, and show `signing-out` again once it landed.
   */
  const resumeDrain = async () => {
    const drain = draining;
    if (!drain || snapshot.status !== "error") return;
    await accounts.settleSelection(true);
    if (draining === drain && !closed)
      publish({ status: "signing-out", error: drain.notice ?? drain.error, recovery: "action" });
  };
  /**
   * `signOut({ discardUnsynced })`'s teardown of `client`: drop its writes
   * the server hasn't confirmed and `account`'s local data, and shut it
   * down without waiting for sync. A thin or memory client keeps nothing
   * on the device: it just closes. `attempt`: an upload being given up
   * (`signing-out`). Never throws: a wipe that couldn't finish is returned
   * (`unfinished`), and the sign-out goes on; `held`: another tab still has
   * the store open, so it stays (that tab decides).
   */
  const discardLocal = async (
    client: Client,
    account: AccountHandle | undefined,
    attempt?: Promise<void>,
  ): Promise<{ unfinished?: Error; held: boolean; kept: boolean }> => {
    const db = (client as WipeableClient).db;
    const writes = db?.getSyncStatus?.().pendingUploads ?? 0;
    const keepsNothing = isThinDb(db as Db) || db?.getConfig?.().driver?.type === "memory";
    let failure: unknown;
    let held = false;
    // The data stays for now: held off, or the host's wipe failed (pending).
    let kept = false;
    // Through the client, while it is still open: before an upload being
    // given up can finish and tear it down.
    if (!keepsNothing && !options.deleteSignedOutStore)
      await db?.deleteClientStorage?.().catch((cause: unknown) => (failure = cause));
    db?.abortGracefulShutdown?.();
    await attempt?.catch(() => {});
    try {
      await client.shutdown();
    } catch (cause) {
      console.error("Jazz signed out, but closing the signed-out client failed.", cause);
    }
    // By account, once nothing of this page holds the store: whether or not
    // an upload got as far as closing the client, and a thin tab's store
    // from an earlier session too.
    if (options.deleteSignedOutStore && account)
      try {
        held = (await options.deleteSignedOutStore(account.id)) !== "deleted";
        kept = held;
        // Kept, so a reload or another page finishes it.
        if (held) options.pendingDiscards?.record(account.id);
        else options.pendingDiscards?.clear(account.id);
      } catch (cause) {
        // Kept too, and shown until a wipe succeeds: `retry()` tries again.
        options.pendingDiscards?.record(account.id);
        stuckWipes.set(account.id, wipeStuck(cause));
        console.error("Jazz couldn't wipe the signed-out account's data from this device.", cause);
        kept = true;
      }
    if (!kept) warnDiscarded(writes, "signOut({ discardUnsynced: true })");
    return {
      held,
      kept,
      ...(failure === undefined
        ? {}
        : {
            unfinished: new Error(
              "Jazz signed out, but couldn't finish wiping the signed-out user's data from this device (discardUnsynced). Some of it may still be on this device.",
              { cause: failure },
            ),
          }),
    };
  };
  /**
   * `logout({ discardUnsynced })` while `signing-out`: stop the upload, wipe
   * the signed-out user's local data with their unsynced writes (a thin
   * client keeps none), and finish the sign-out now. Nothing signs in until
   * it has.
   */
  const discardDrain = (drain: Drain, unsaved?: unknown): Promise<void> => {
    const task = (async () => {
      endDrain(drain, false);
      let outcome: Awaited<ReturnType<typeof discardLocal>>;
      try {
        outcome = await discardLocal(drain.client, drain.account, drain.attempt);
      } finally {
        drain.release?.();
      }
      if (outcome.held) pendingWipe = drain.account;
      else if (drain.account && !outcome.kept)
        await forgetDeviceAccount(accounts, drain.account.id).catch((error: unknown) =>
          console.warn("Jazz couldn't forget the signed-out device account's key.", error),
        );
      if (closed) return;
      // The signed-out selection still didn't save: as any sign-out whose
      // save failed, `error`, and `retry()` saves it again.
      if (unsaved !== undefined) {
        publish({ status: "error", error: toError(unsaved), recovery: "session" });
        throw unsaved;
      }
      const discarding = discardError();
      publish({
        status: "signed-out",
        ...(outcome.unfinished
          ? { error: outcome.unfinished, recovery: "session" as const }
          : discarding
            ? { error: discarding, recovery: "action" as const }
            : {}),
      });
    })();
    loggingOut = task;
    void task
      .finally(() => {
        if (loggingOut === task) loggingOut = undefined;
      })
      .catch(() => {});
    return task;
  };
  /** Try the next upload on a backoff, and as soon as the client reconnects. */
  const watchDrain = (drain: Drain) => {
    drain.stopStatus ??= (drain.client as WipeableClient).db?.onSyncStatusChange?.((status) => {
      if (status.link.state === "connected") void drainNow().catch(() => {});
    });
    if (drain.timer) clearTimeout(drain.timer);
    const wait = drain.waitMs;
    drain.waitMs = Math.min(wait * 2, SIGN_OUT_RETRY_MAX_MS);
    drain.timer = setTimeout(() => {
      drain.timer = undefined;
      void drainNow().catch(() => {});
    }, wait);
  };
  /** Stop retrying; `release` revokes its credential (once its client is down). */
  const endDrain = (drain: Drain, release = true) => {
    if (drain.timer) clearTimeout(drain.timer);
    drain.stopStatus?.();
    if (draining === drain) draining = undefined;
    if (release) drain.release?.();
  };
  /**
   * A `signing-out` session's upload, tried now (one at a time). It ends
   * the sign-out once the writes uploaded, or when the server refused them
   * (as `stopPastRefusal` decides), and rejects while the server stays out
   * of reach.
   */
  const drainNow = (): Promise<void> => {
    const drain = draining;
    if (!drain || closed) return Promise.resolve();
    if (drain.attempt) return drain.attempt;
    const attempt = stopPastRefusal(drain.client, "Jazz signed out").then(
      () => {
        if (draining !== drain) return;
        endDrain(drain);
        publish(drainEnded(drain));
      },
      (cause: unknown) => {
        drain.attempt = undefined;
        if (draining !== drain || closed) throw cause;
        // Still out of reach. The drain failed before teardown began, so
        // the client is open (it reads as unusable only because the session
        // marked it replaced): wait for the next try.
        if (cause instanceof GracefulShutdownSyncError) {
          watchDrain(drain);
          throw cause;
        }
        // Its teardown began and failed: signed out all the same, as a
        // sign-out whose teardown fails is.
        console.error("Jazz signed out, but closing the signed-out client failed.", cause);
        endDrain(drain);
        publish(drainEnded(drain));
      },
    );
    drain.attempt = attempt;
    return attempt;
  };
  const run = (
    operation: SessionCoreOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    signsOut?: boolean,
  ): Promise<void> => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (loggingOut || draining) return Promise.reject(new JazzSessionOperationError("signing-out"));
    if (busy) return Promise.reject(new JazzSessionOperationError("pending"));
    const token = ++generation;
    // Publish "transitioning" as the operation becomes pending, so no one
    // sees a settled session that would refuse their operation as busy. It
    // is busy before listeners hear of it, so one that starts an operation
    // of its own is refused rather than racing this one.
    let detached!: { done: Promise<void> };
    const task = Promise.resolve().then(() =>
      perform(operation, mutate, token, detached.done, signsOut),
    );
    busy = task;
    detached = detach({ status: "transitioning", account: selected, pending: operation });
    void task
      .finally(() => {
        if (busy === task) busy = undefined;
        void finishPendingWipes();
      })
      .catch(() => {});
    return task;
  };
  const session: SessionCore<Client> = {
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    createLocalFirst: () => {
      const missing = deviceAccountStoreMissing(accounts, "createDeviceAccount()");
      if (missing) return Promise.reject(missing);
      return run("createLocalFirst", () => accounts.createLocalFirst());
    },
    restoreLocalFirst: (secret) => {
      const missing = deviceAccountStoreMissing(accounts, "restoreDeviceAccount(key)");
      if (missing) return Promise.reject(missing);
      // A malformed secret is the caller's mistake: refuse it before the
      // current client is shut down, so the session goes on as it was.
      try {
        parseAuthSecret(secret);
      } catch (error) {
        return Promise.reject(error);
      }
      return run("restoreLocalFirst", () => accounts.restoreLocalFirst(secret));
    },
    reopenLocalFirst: async () => {
      const missing = deviceAccountStoreMissing(accounts, "reopenDeviceAccount()");
      if (missing) throw missing;
      // Looked up first: with none kept, the session goes on as it was.
      const key = await keptDeviceKey(accounts).latest();
      if (!key) throw new NoKeptDeviceAccountError("reopenDeviceAccount()");
      return run("reopenLocalFirst", () => accounts.restoreLocalFirst(key));
    },
    keptLocalFirstKey: () => keptDeviceKey(accounts).known(),
    loginWithBackend: () => run("loginWithBackend", () => accounts.loginWithBackend()),
    linkWithBackend: () => run("linkWithBackend", () => accounts.linkWithBackend()),
    // Signing out: save a selection that failed to land, then try the upload now.
    // Signed out with a held-off discard: wipe what it left, now.
    retry: () =>
      draining
        ? resumeDrain().then(drainNow)
        : (pendingWipe || stuckWipes.size > 0) && !selected && snapshot.status === "signed-out"
          ? finishWipe()
          : run("retry", () => selected),
    logout: (logoutOptions = {}) => {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      const discard = logoutOptions.discardUnsynced === true;
      // Discarding during a sign-out: once it has signed out here, drop
      // what it still waits to upload.
      if (loggingOut)
        return discard
          ? loggingOut.catch(() => {}).then(() => session.logout(logoutOptions))
          : loggingOut;
      // Signed out here already; its upload goes on (`signing-out`), unless
      // the app gives it up.
      if (draining) {
        const drain = draining;
        if (!discard) return resumeDrain();
        // The escape hatch runs even while the selection won't save.
        return resumeDrain().then(
          () => discardDrain(drain),
          (unsaved: unknown) => discardDrain(drain, unsaved),
        );
      }
      const token = ++generation;
      ++signOuts;
      const pending = busy;
      const task = Promise.resolve().then(async () => {
        await barrier.done;
        await pending?.catch(() => {});
        if (token !== generation) throw superseded();
        await perform(
          "logout",
          () => {
            // A client left uploading keeps its credential until it's done.
            const drain = draining;
            const release = accounts.logout(drain?.account ? { keep: drain.account } : {});
            if (drain) drain.release = release;
            return undefined;
          },
          token,
          undefined,
          undefined,
          discard,
        );
      });
      loggingOut = task;
      const barrier = detach({ status: "transitioning", account: selected, pending: "logout" });
      void task
        .finally(() => {
          if (loggingOut === task) loggingOut = undefined;
          void finishPendingWipes();
        })
        .catch(() => {});
      return task;
    },
    close: () => {
      if (closing) return closing;
      closed = true;
      ++generation;
      const pending = busy;
      const logout = loggingOut;
      closing = Promise.resolve().then(async () => {
        await barrier.done;
        await pending?.catch(() => {});
        await logout?.catch(() => {});
        const old = client;
        client = undefined;
        await old?.shutdown();
        // Closed while signing out: stop the upload. A persistent client
        // keeps the writes on this device, under that account.
        const drain = draining;
        if (drain) {
          endDrain(drain, false);
          const db = (drain.client as WipeableClient).db;
          db?.abortGracefulShutdown?.();
          await drain.attempt?.catch(() => {});
          const writes = db?.getSyncStatus?.().pendingUploads ?? 0;
          if (writes > 0)
            console.warn(
              `Jazz closed while signing out, with ${writes} write(s) of the signed-out user not uploaded yet. A persistent client keeps them on this device and uploads them when that account signs in again; one that keeps nothing loses them.`,
            );
          try {
            await drain.client.shutdown();
          } finally {
            drain.release?.();
          }
        }
      });
      const barrier = detach({ status: "closed" });
      return closing;
    },
  };
  signOutCounts.set(session, () => signOuts);
  switchers.set(session, async (from) => {
    // An operation that published its outcome may still be finishing.
    while (busy || loggingOut) {
      await busy?.catch(() => {});
      await loggingOut?.catch(() => {});
    }
    if (closed || selected !== from) return false;
    await run(
      "loginWithBackend",
      () => {
        accounts.logout();
        return accounts.loginWithBackend();
      },
      true,
    );
    return true;
  });
  replacers.set(session, async (open, adopt) => {
    if (closed) throw new Error("Jazz session is closed");
    await busy?.catch(() => {});
    await loggingOut?.catch(() => {});
    const token = generation;
    const current = client;
    const account = selected;
    if (closed || !current || !account || snapshot.status !== "ready")
      throw new Error("The Jazz session has no signed-in client to replace");
    const next = (await open(current, account)) as Client;
    if (token !== generation || client !== current || closed) {
      await next.shutdown().catch(() => {});
      throw superseded();
    }
    // The upgrade's client is the first to take the store (a thin sign-in
    // takes no hold), so another tab's discard may have wiped and forgotten
    // this device account before it: `next` opened an empty store. As a
    // sign-in would, refuse it, and sign out.
    if (
      account.identity.issuer === "urn:jazz:local-first" &&
      options.pendingDiscards &&
      (await accountForgotten(accounts, account.id))
    ) {
      const refused = new DiscardedDeviceAccountError();
      await next.shutdown().catch(() => {});
      // What `next` opened is no one's now (the writes the thin client
      // handed it are still in the thin client, which uploads them as it
      // signs out): wipe it by account, as the discard did, or leave it
      // pending if another tab holds it.
      if (options.deleteSignedOutStore) {
        const wiped = await options.deleteSignedOutStore(account.id).catch((error: unknown) => {
          console.warn("Jazz couldn't wipe the refused local-first store yet.", error);
          return "held" as const;
        });
        if (wiped !== "deleted") options.pendingDiscards.record(account.id);
      }
      if (token === generation && client === current && !closed) {
        await session.logout().catch(() => {});
        // Narrowed above, but the logout moved it on.
        const now = snapshot as SessionCoreSnapshot<Client>;
        if (!selected && !closed) {
          if (now.status === "signed-out")
            publish({ status: "signed-out", error: refused, recovery: "action" });
          // Offline: the thin client still uploads. Say why while it waits,
          // and on the signed-out it ends in.
          else if (now.status === "signing-out" && draining) {
            draining.notice = refused;
            publish({ status: "signing-out", error: refused, recovery: "action" });
          }
        }
      }
      throw refused;
    }
    if (token !== generation || client !== current || closed) {
      await next.shutdown().catch(() => {});
      throw superseded();
    }
    if (adopt) {
      adopt(current, next);
      // Same client, upgraded in place: the core publishes nothing (there is
      // nothing for a provider to redo), but the public session re-reads
      // its snapshot's `localFirst`.
      for (const listener of [...(upgradeListeners.get(session) ?? [])]) listener();
      return;
    }
    client = next;
    publish({ status: "ready", account: selected, client: next });
    // Code that kept the old Db learns it is gone, instead of its queries
    // going quiet. Framework providers follow the session, not the old Db.
    (current as WipeableClient).db?.markReplaced?.("replaced");
    // Children re-render onto the new client first; the old one finishes
    // uploading its writes before it stops. The swap is done either way: a
    // slow or failing shutdown of the old client neither holds nor fails it.
    // One the server refuses can't upload them, and is closed anyway.
    void stopPastRefusal(current, "Jazz replaced this client").catch((error: unknown) => {
      console.warn("Jazz could not shut the replaced client down cleanly.", error);
    });
  });
  consumers.set(session, (name) => {
    const lease: Lease = { name };
    leases.add(lease);
    return {
      acknowledge: (observed) => {
        // It rendered the session again: later transitions wait for it.
        lease.stale = false;
        if (!observed.client && (versions.get(observed) ?? -1) >= version) {
          lease.waiting?.();
          lease.waiting = undefined;
        }
      },
      release: () => {
        leases.delete(lease);
        lease.waiting?.();
        lease.waiting = undefined;
      },
    };
  });
  if (options.decorate) Object.assign(session, options.decorate(session));
  // Discards an earlier page left for another tab to let go of.
  void finishPendingWipes();
  return Object.freeze(session) as SessionCore<Client> & Extra;
}

type SigningOutClient = {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
  db?: {
    getAuthState?(): { error?: unknown };
    getSyncStatus?(): { pendingUploads: number };
    getConfig?(): { driver?: { type?: string } };
  };
};

/**
 * Stop `client` as the session moves on from it (a sign-out, a switch, a
 * new sign-in, a replacement): upload what the server still accepts, then
 * stop it. When the server refuses this user's sign-in (their session at
 * the backend ended, or `jazz.revokeUser`), no retry uploads the rest once
 * the session has moved on, and failing would leave the session stuck on a
 * client it can't close, through reloads too: it stops the client anyway
 * and says what became of those writes (`lead` names what happened). A persistent client keeps them
 * on this device, under that account, and uploads them when it next opens;
 * one that keeps nothing (in memory, or thin) loses them. Any refusal counts,
 * an `expired` credential too: the user is leaving, and no one renews it once
 * they have. Any other failure (an outage) may pass, so it still fails the
 * sign-out for the app to retry.
 */
async function stopPastRefusal(client: SigningOutClient, lead: string) {
  try {
    await client.shutdown({ waitForSync: true });
  } catch (cause) {
    const db = client.db;
    if (!(cause instanceof GracefulShutdownSyncError) || !db?.getAuthState?.().error) throw cause;
    const writes = db.getSyncStatus?.().pendingUploads ?? 0;
    const kept = !isThinDb(db as Db) && db.getConfig?.().driver?.type !== "memory";
    // Warn only about writes left behind: with none, nothing was lost.
    if (writes > 0)
      console.warn(
        kept
          ? `${lead} with ${writes} write(s) the server refused, as it no longer accepts this user's sign-in. They stay on this device and upload when this account signs in again.`
          : `${lead} and dropped ${writes} write(s) the server refused, as it no longer accepts this user's sign-in. This client keeps nothing on the device.`,
        cause.cause,
      );
    await client.shutdown();
  }
}
