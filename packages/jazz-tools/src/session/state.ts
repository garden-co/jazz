import { ensureAutomaticLocalFirst } from "../accounts/persistence.js";
import type { AccountHandle, AccountManager } from "../accounts/state.js";
import { isProvisionalAccount, type JWTAuth, type BackendAuth } from "../accounts/enrollment.js";
import { hasAuthProviderConnection } from "./auth-provider.js";
import { watchNeedsLogin } from "./needs-login-warning.js";
import {
  GracefulShutdownSyncError,
  SharedClientShutdownError,
} from "../runtime/graceful-shutdown-error.js";
import { createListeners, toError } from "../runtime/util.js";

/** Re-admit the selected account in place, or decline for an ordinary transition. */
interface InPlaceRevalidation {
  check(isCurrent: () => boolean): Promise<AccountHandle | undefined>;
  retry(): Promise<void>;
}

export interface SessionClient {
  shutdown(options?: { waitForSync?: boolean }): Promise<void>;
}
export type JazzSessionOperation =
  | "createLocalFirst"
  | "restoreLocalFirst"
  | "becomeBackend"
  | "loginWithBackend"
  | "registerJWT"
  | "loginJWT"
  | "loginOrRegisterJWT"
  | "linkJWT"
  | "logout"
  | "retry";
/**
 * Whether the open account still has to log in before it syncs.
 *
 * True while the selected account is a retained external-JWT account that a
 * reload reopened before any provider credential: it reads and writes local
 * data, but nothing leaves the device until `loginJWT` or
 * `loginOrRegisterJWT` (or managed auth's revalidation) re-admits it. Its
 * identity is already the signed-in identity, so check this flag rather than
 * the identity to decide whether to log in again.
 */
export interface NeedsLoginState {
  readonly needsLogin: boolean;
}
/** @internal The one place that decides {@link NeedsLoginState.needsLogin}. */
export function accountNeedsLogin(account: AccountHandle | undefined): boolean {
  return account !== undefined && isProvisionalAccount(account);
}
export interface JazzSessionSnapshot<Client> extends Partial<NeedsLoginState> {
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
  registerJWT(auth: JWTAuth): Promise<void>;
  loginJWT(auth: JWTAuth): Promise<void>;
  loginOrRegisterJWT(auth: JWTAuth): Promise<void>;
  linkJWT(auth: JWTAuth): Promise<void>;
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
/** @internal Framework commit acknowledgement; never required by imperative callers. */
export function attachJazzSessionConsumer<Client>(
  session: JazzSession<Client>,
): JazzSessionConsumer<Client> {
  const attach = consumers.get(session);
  if (!attach) throw new Error("Unknown Jazz session");
  return attach() as JazzSessionConsumer<Client>;
}

/** @internal Hosts prepare accounts and supply their client adapter once. */
export async function createJazzSessionOwner<Client extends SessionClient>(options: {
  accounts: AccountManager<JWTAuth>;
  openClient(account: AccountHandle): Promise<Client>;
  initial?: "local-first" | BackendAuth;
}): Promise<JazzSession<Client>> {
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
    needsLogin: accountNeedsLogin(selected),
  });
  const listeners = createListeners("Jazz session observer failed");
  const leases = new Set<{ waiting?: () => void }>();
  // Development builds only: warn an app that drives logins itself when it
  // keeps writing to an account that still needs one.
  const needsLoginWarning = watchNeedsLogin({
    current: () => snapshot,
    isManaged: () => hasAuthProviderConnection(session),
    isBusy: () => busy !== undefined || loggingOut !== undefined,
  });
  const versions = new WeakMap<object, number>();
  let version = 0;
  versions.set(snapshot, version);
  let generation = 0;
  let busy: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  let loggingOut: Promise<void> | undefined;
  // A same-identity login whose in-place revalidation failed. retry() runs it
  // again: reopening the still-unconfirmed account would hide the failure.
  let failedRevalidation: (() => Promise<void>) | undefined;
  let closed = false;
  const publish = (next: Omit<JazzSessionSnapshot<Client>, "needsLogin">) => {
    snapshot = Object.freeze({ ...next, needsLogin: accountNeedsLogin(next.account) });
    versions.set(snapshot, ++version);
    needsLoginWarning.update();
    listeners.notify();
  };
  const detach = (next: Omit<JazzSessionSnapshot<Client>, "needsLogin">) => {
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
    revalidate?: InPlaceRevalidation,
  ) => {
    if (token !== generation) throw superseded();
    if (revalidate && client && selected) {
      // Logging in again as the selected identity re-admits it in place: the
      // open client and its subscriptions stay. A changed provider identity
      // or registry assignment falls through to the ordinary transition.
      const current = client;
      const account = selected;
      let same: AccountHandle | undefined;
      try {
        same = await revalidate.check(() => token === generation);
      } catch (cause) {
        if (token === generation) failedRevalidation = revalidate.retry;
        if (token === generation)
          publish({
            status: "ready",
            account,
            client: current,
            error: toError(cause),
            recovery: "action",
          });
        throw cause;
      }
      if (token !== generation) throw superseded();
      if (same === account && selected === account && client === current) {
        // Clear an earlier failure and the pending login; an unchanged ready
        // snapshot stays as is.
        if (
          snapshot.error ||
          snapshot.status !== "ready" ||
          snapshot.client !== current ||
          snapshot.needsLogin !== accountNeedsLogin(account)
        )
          publish({ status: "ready", account, client: current });
        return;
      }
    }
    const previous = selected;
    await detach({ status: "transitioning", account: selected, pending: operation }).done;
    if (token !== generation) throw superseded();
    try {
      if (client) {
        // A retained account that never received a credential cannot reach
        // the core; its unsynchronized writes stay durable in its own local
        // store and sync the next time that identity signs in.
        await client.shutdown(
          selected && isProvisionalAccount(selected) ? {} : { waitForSync: true },
        );
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
    revalidate?: InPlaceRevalidation,
  ): Promise<void> => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (busy || loggingOut)
      return Promise.reject(new Error("A Jazz session operation is already pending"));
    failedRevalidation = undefined;
    const token = ++generation;
    const task = Promise.resolve().then(() => perform(operation, mutate, token, revalidate));
    busy = task;
    void task
      .finally(() => {
        if (busy === task) busy = undefined;
      })
      .catch(() => {});
    return task;
  };
  const login = (operation: "loginJWT" | "loginOrRegisterJWT", auth: JWTAuth): Promise<void> => {
    // Hosts that retain the selected account re-admit the same identity in
    // place. Anything else is an ordinary transition; the enrollment reuses
    // the provider token the revalidation already fetched.
    const revalidate: InPlaceRevalidation | undefined = accounts.revalidatesInPlace
      ? {
          check: (isCurrent) => accounts.revalidateJWT(operation, auth, isCurrent),
          retry: () => login(operation, auth),
        }
      : undefined;
    return run(operation, () => accounts.enrollJWT(operation, auth), revalidate);
  };
  const session: JazzSession<Client> = {
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    createLocalFirst: () => run("createLocalFirst", () => accounts.createLocalFirst()),
    restoreLocalFirst: (secret) =>
      run("restoreLocalFirst", () => accounts.restoreLocalFirst(secret)),
    becomeBackend: (auth) => run("becomeBackend", () => accounts.becomeBackend(auth)),
    loginWithBackend: () => run("loginWithBackend", () => accounts.loginWithBackend()),
    registerJWT: (auth) => run("registerJWT", () => accounts.registerJWT(auth)),
    loginJWT: (auth) => login("loginJWT", auth),
    loginOrRegisterJWT: (auth) => login("loginOrRegisterJWT", auth),
    linkJWT: (auth) => run("linkJWT", () => accounts.linkJWT(auth)),
    retry: () => {
      const again = failedRevalidation;
      return again ? again() : run("retry", () => selected);
    },
    logout: () => {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      if (loggingOut) return loggingOut;
      failedRevalidation = undefined;
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
  needsLoginWarning.update();
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
  return Object.freeze(session);
}
