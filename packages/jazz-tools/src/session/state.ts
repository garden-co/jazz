import { ensureAutomaticLocalFirst } from "../accounts/persistence.js";
import type { AccountHandle, AccountManager } from "../accounts/state.js";
import type { JWTAuth, BackendAuth } from "../accounts/enrollment.js";
import {
  GracefulShutdownSyncError,
  SharedClientShutdownError,
} from "../runtime/graceful-shutdown-error.js";
import { createListeners, toError } from "../runtime/util.js";
import type { Db } from "../runtime/db.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";

export interface SessionClient {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
}
/** A client whose database can wipe its local storage (`clearOnSignOut`). */
type WipeableClient = {
  db?: {
    waitForSync?(options?: { timeoutMs?: number }): Promise<void>;
    deleteClientStorage?(): Promise<void>;
  };
};
export type JazzSessionOperation =
  | "createLocalFirst"
  | "restoreLocalFirst"
  | "becomeBackend"
  | "loginWithBackend"
  | "linkWithBackend"
  | "logout"
  | "retry";
export interface JazzSessionSnapshot<Client> {
  readonly status: "ready" | "signed-out" | "transitioning" | "error" | "closed";
  readonly account?: AccountHandle;
  readonly client?: Client;
  readonly pending?: JazzSessionOperation;
  readonly error?: Error;
  /** Whether recovery repeats an uncommitted action or only reopens the selected account. */
  readonly recovery?: "action" | "session";
}
export interface JazzSessionActions {
  createLocalFirst(): Promise<void>;
  restoreLocalFirst(secret: string): Promise<void>;
  becomeBackend(auth: BackendAuth): Promise<void>;
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
export interface JazzSession<Client> extends JazzSessionActions {
  getSnapshot(): JazzSessionSnapshot<Client>;
  subscribe(listener: () => void): () => void;
}
export interface JazzSessionConsumer<Client> {
  acknowledge(snapshot: JazzSessionSnapshot<Client>): void;
  release(): void;
}
const consumers = new WeakMap<object, () => JazzSessionConsumer<unknown>>();
type ClientReplacer = (
  open: (current: unknown, account: AccountHandle) => Promise<unknown>,
) => Promise<void>;
const replacers = new WeakMap<object, ClientReplacer>();

/**
 * @internal Swap the signed-in account's client for another one for the same
 * account, as the thin-to-local-first upgrade does. `open` gets the current
 * client; once it resolves the session publishes the new client, then shuts
 * the old one down after its writes have synced. If the session changes
 * account, logs out or closes meanwhile, the new client is shut down and this
 * rejects.
 */
export function replaceJazzSessionClient<Client>(
  session: JazzSession<Client>,
  open: (current: Client, account: AccountHandle) => Promise<Client>,
): Promise<void> {
  const replace = replacers.get(session);
  if (!replace) return Promise.reject(new Error("Unknown Jazz session"));
  return replace(open as (current: unknown, account: AccountHandle) => Promise<unknown>);
}
/** @internal Framework commit acknowledgement; never required by imperative callers. */
export function attachJazzSessionConsumer<Client>(
  session: JazzSession<Client>,
): JazzSessionConsumer<Client> {
  const attach = consumers.get(session);
  if (!attach) throw new Error("Unknown Jazz session");
  return attach() as JazzSessionConsumer<Client>;
}

/** @internal Hosts prepare accounts and supply their client adapter once. */
export async function createJazzSessionOwner<
  Client extends SessionClient,
  Extra extends object = {},
>(options: {
  accounts: AccountManager<JWTAuth>;
  openClient(account: AccountHandle): Promise<Client>;
  initial?: "local-first" | "signed-out" | BackendAuth;
  /**
   * Wipe the signed-out account's local storage, unsynced writes included,
   * instead of keeping it for that user's next sign-in (shared devices).
   */
  clearOnSignOut?: boolean;
  /** Add host methods (e.g. `reauthenticate`) to the session before it is sealed. */
  decorate?(session: JazzSession<Client>): Extra;
}): Promise<JazzSession<Client> & Extra> {
  const { accounts, openClient } = options;
  let selected = accounts.getLoggedIn();
  if (!selected && options.initial === "local-first")
    selected = await ensureAutomaticLocalFirst(accounts);
  if (typeof options.initial === "object") selected = await accounts.becomeBackend(options.initial);
  await accounts.settleSelection();
  let client = selected ? await openClient(selected) : undefined;
  let snapshot: JazzSessionSnapshot<Client> = Object.freeze({
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
  const publish = (next: JazzSessionSnapshot<Client>) => {
    snapshot = Object.freeze({ ...next });
    versions.set(snapshot, ++version);
    listeners.notify();
  };
  const detach = (next: JazzSessionSnapshot<Client>) => {
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
    operation: JazzSessionOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
    token: number,
    detached?: Promise<void>,
  ) => {
    if (token !== generation) throw superseded();
    const previous = selected;
    await (detached ??
      detach({ status: "transitioning", account: selected, pending: operation }).done);
    if (token !== generation) throw superseded();
    try {
      if (client) {
        // A thin client keeps nothing in the browser, so there is nothing to wipe.
        const thin = isThinDb((client as WipeableClient).db as Db);
        if (operation === "logout" && options.clearOnSignOut && !thin) {
          const db = (client as WipeableClient).db;
          // Upload what the server still accepts before wiping the rest.
          await db?.waitForSync?.({ timeoutMs: 5_000 }).catch(() => {});
          await db?.deleteClientStorage?.();
          await client.shutdown();
        } else {
          await client.shutdown({ waitForSync: true });
        }
        client = undefined;
      }
    } catch (cause) {
      if (token === generation) {
        const usable =
          cause instanceof GracefulShutdownSyncError || cause instanceof SharedClientShutdownError;
        // Unknown teardown failures retain the client privately as a shutdown
        // barrier. Never publish a potentially stopped runtime or enroll over it.
        publish({
          status: usable ? "ready" : "error",
          account: selected,
          client: usable ? client : undefined,
          error: toError(cause),
          recovery: "action",
        });
      }
      throw cause;
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
    publish({ status: client ? "ready" : "signed-out", account: selected, client });
  };
  const run = (
    operation: JazzSessionOperation,
    mutate: () => AccountHandle | undefined | Promise<AccountHandle | undefined>,
  ): Promise<void> => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (busy || loggingOut)
      return Promise.reject(new Error("A Jazz session operation is already pending"));
    const token = ++generation;
    // Publish "transitioning" as the operation becomes pending, so no one
    // sees a settled session that would refuse their operation as busy.
    const detached = detach({ status: "transitioning", account: selected, pending: operation });
    const task = Promise.resolve().then(() => perform(operation, mutate, token, detached.done));
    busy = task;
    void task
      .finally(() => {
        if (busy === task) busy = undefined;
      })
      .catch(() => {});
    return task;
  };
  const session: JazzSession<Client> = {
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    createLocalFirst: () => run("createLocalFirst", () => accounts.createLocalFirst()),
    restoreLocalFirst: (secret) =>
      run("restoreLocalFirst", () => accounts.restoreLocalFirst(secret)),
    becomeBackend: (auth) => run("becomeBackend", () => accounts.becomeBackend(auth)),
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
  replacers.set(session, async (open) => {
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
    client = next;
    publish({ status: "ready", account: selected, client: next });
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
  return Object.freeze(session) as JazzSession<Client> & Extra;
}
