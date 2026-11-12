import { ensureAutomaticLocalFirst } from "../accounts/persistence.js";
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
  | "loginWithBackend"
  | "linkWithBackend"
  | "logout"
  | "retry";
export interface SessionCoreSnapshot<Client> {
  readonly status: "ready" | "signed-out" | "transitioning" | "error" | "closed";
  readonly account?: AccountHandle;
  readonly client?: Client;
  readonly pending?: SessionCoreOperation;
  readonly error?: Error;
  /** Whether recovery repeats an uncommitted action or only reopens the selected account. */
  readonly recovery?: "action" | "session";
}
export interface SessionCoreActions {
  createLocalFirst(): Promise<void>;
  restoreLocalFirst(secret: string): Promise<void>;
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
  logout(): Promise<void>;
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
const consumers = new WeakMap<object, () => SessionCoreConsumer<unknown>>();
type ClientReplacer = (
  open: (current: unknown, account: AccountHandle) => Promise<unknown>,
  adopt?: (current: unknown, next: unknown) => void,
) => Promise<void>;
const replacers = new WeakMap<object, ClientReplacer>();
const switchers = new WeakMap<object, (from: AccountHandle) => Promise<boolean>>();

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
): SessionCoreConsumer<Client> {
  const attach = consumers.get(session);
  if (!attach) throw new Error("Unknown Jazz session");
  return attach() as SessionCoreConsumer<Client>;
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
  /** Add host methods (e.g. `reauthenticate`) to the session before it is sealed. */
  decorate?(session: SessionCore<Client>): Extra;
}): Promise<SessionCore<Client> & Extra> {
  const { accounts, openClient } = options;
  let selected = accounts.getLoggedIn();
  if (!selected && options.initial === "local-first")
    selected = await ensureAutomaticLocalFirst(accounts);
  if (options.backend)
    selected = await admitBackendAccount(accounts as AccountManager<unknown>, options.backend);
  await accounts.settleSelection();
  let client = selected ? await openClient(selected) : undefined;
  let snapshot: SessionCoreSnapshot<Client> = Object.freeze({
    status: client ? "ready" : "signed-out",
    account: selected,
    client,
  });
  const listeners = createListeners("Jazz session observer failed");
  const leases = new Set<{ waiting?: () => void }>();
  const versions = new WeakMap<object, number>();
  let version = 0;
  versions.set(snapshot, version);
  let generation = 0;
  let busy: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  let loggingOut: Promise<void> | undefined;
  let closed = false;
  const publish = (next: SessionCoreSnapshot<Client>) => {
    snapshot = Object.freeze({ ...next });
    versions.set(snapshot, ++version);
    listeners.notify();
  };
  const detach = (next: SessionCoreSnapshot<Client>) => {
    const waits = [...leases].map(
      (lease) =>
        new Promise<void>((resolve) => {
          // A later transition (logout/close) also releases an earlier waiter.
          const previous = lease.waiting;
          lease.waiting = () => {
            previous?.();
            resolve();
          };
        }),
    );
    publish(next);
    return { done: Promise.all(waits).then(() => {}) };
  };
  const superseded = () => new Error("Jazz session operation was superseded");
  const perform = async (
    operation: SessionCoreOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    token: number,
    detached?: Promise<void>,
    // The current account is signed out first (a switch to another user).
    signsOut = operation === "logout",
  ) => {
    if (token !== generation) throw superseded();
    // A switch has signed the previous account out: a failed sign-in can't reopen it.
    const previous = signsOut ? undefined : selected;
    // A clearOnSignOut wipe that may not have finished, reported with the
    // signed-out state: on a shared device the app must know.
    let unfinishedWipe: Error | undefined;
    let wiped = false;
    await (detached ??
      detach({ status: "transitioning", account: selected, pending: operation }).done);
    if (token !== generation) throw superseded();
    try {
      if (client) {
        // A thin client keeps nothing in the browser, so there is nothing to wipe.
        const thin = isThinDb((client as WipeableClient).db as Db);
        if (signsOut && options.clearOnSignOut && !thin) {
          const db = (client as WipeableClient).db;
          // Upload what the server still accepts before wiping the rest.
          await db?.waitForSync?.({ timeoutMs: 5_000 }).catch(() => {});
          // Say what the wipe took, as a sign-out past refused writes does.
          const writes = db?.getSyncStatus?.().pendingUploads ?? 0;
          await db?.deleteClientStorage?.();
          wiped = true;
          if (writes > 0)
            console.warn(
              `Jazz signed out and wiped ${writes} write(s) the server hadn't confirmed, as clearOnSignOut asks. They are gone from this device; any the server did receive stay there.`,
            );
          await client.shutdown();
        } else if (signsOut) {
          await signOutOf(client);
        } else {
          await client.shutdown({ waitForSync: true });
        }
        // Code that kept this client's Db learns it is gone, instead of
        // its queries going quiet.
        (client as WipeableClient).db?.markReplaced?.("signed-out");
        client = undefined;
      }
    } catch (cause) {
      if (token !== generation) throw cause;
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
        publish({ status: "error", account: selected, error: toError(cause), recovery: "action" });
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
    if (token !== generation) throw superseded();
    try {
      selected = await mutate();
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
    } catch (cause) {
      if (token === generation)
        publish({ status: "error", account: selected, error: toError(cause), recovery: "session" });
      throw cause;
    }
    if (token !== generation) throw superseded();
    publish({
      status: client ? "ready" : "signed-out",
      account: selected,
      client,
      ...(unfinishedWipe ? { error: unfinishedWipe, recovery: "session" as const } : {}),
    });
  };
  const run = (
    operation: SessionCoreOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    signsOut?: boolean,
  ): Promise<void> => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (busy || loggingOut)
      return Promise.reject(new Error("A Jazz session operation is already pending"));
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
      })
      .catch(() => {});
    return task;
  };
  const session: SessionCore<Client> = {
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    createLocalFirst: () => run("createLocalFirst", () => accounts.createLocalFirst()),
    restoreLocalFirst: (secret) => {
      // A malformed secret is the caller's mistake: refuse it before the
      // current client is shut down, so the session goes on as it was.
      try {
        parseAuthSecret(secret);
      } catch (error) {
        return Promise.reject(error);
      }
      return run("restoreLocalFirst", () => accounts.restoreLocalFirst(secret));
    },
    loginWithBackend: () => run("loginWithBackend", () => accounts.loginWithBackend()),
    linkWithBackend: () => run("linkWithBackend", () => accounts.linkWithBackend()),
    retry: () => run("retry", () => selected),
    logout: () => {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      if (loggingOut) return loggingOut;
      const token = ++generation;
      const pending = busy;
      const task = Promise.resolve().then(async () => {
        await barrier.done;
        await pending?.catch(() => {});
        if (token !== generation) throw superseded();
        await perform(
          "logout",
          () => {
            accounts.logout();
            return undefined;
          },
          token,
        );
      });
      loggingOut = task;
      const barrier = detach({ status: "transitioning", account: selected, pending: "logout" });
      void task
        .finally(() => {
          if (loggingOut === task) loggingOut = undefined;
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
      });
      const barrier = detach({ status: "closed" });
      return closing;
    },
  };
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
    if (adopt) {
      adopt(current, next);
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
    void current.shutdown({ waitForSync: true }).catch((error: unknown) => {
      console.warn("Jazz could not shut the replaced client down cleanly.", error);
    });
  });
  consumers.set(session, () => {
    const lease: { waiting?: () => void } = {};
    leases.add(lease);
    return {
      acknowledge: (observed) => {
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
 * Sign out of `client`: upload what the server still accepts, then stop it.
 * When the server refuses this user's sign-in (their session at the backend
 * ended), no retry uploads the rest while they are signed out, and refusing
 * the sign-out would leave them no way out, through reloads too: it signs
 * out and says what became of those writes. A persistent client keeps them
 * on this device, under that account, and uploads them when it next opens;
 * one that keeps nothing (in memory, or thin) loses them. Any refusal counts,
 * an `expired` credential too: the user is leaving, and no one renews it once
 * they have. Any other failure (an outage) may pass, so it still fails the
 * sign-out for the app to retry.
 */
async function signOutOf(client: SigningOutClient) {
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
          ? `Jazz signed out with ${writes} write(s) the server refused, as it no longer accepts this user's sign-in. They stay on this device and upload when this account signs in again.`
          : `Jazz signed out and dropped ${writes} write(s) the server refused, as it no longer accepts this user's sign-in. This client keeps nothing on the device.`,
        cause.cause,
      );
    await client.shutdown();
  }
}
