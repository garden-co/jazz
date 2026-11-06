import {
  attachJazzSessionConsumer,
  type JazzSession,
  type JazzSessionActions,
  type JazzSessionSnapshot,
  type JazzSessionConsumer,
} from "./state.js";
import type { AccountHandle } from "../accounts/state.js";
import { createListeners, toError } from "../runtime/util.js";

/**
 * Who your backend says is signed in: its `authenticate` answer, as Jazz
 * last received it.
 */
export interface JazzUser {
  /** Your user's id (`authenticate`'s `userId`). */
  readonly id: string;
  /** The issuer Jazz records for your backend's users. */
  readonly issuer: string;
  /**
   * The extra claims `authenticate` returned (`session.claims` in
   * permissions), as of this user's sign-in. Jazz doesn't update them while
   * the same user stays signed in: claims that changed (a role or an org
   * switch) show after the next sign-in, or a page reload.
   */
  readonly claims: Readonly<Record<string, unknown>>;
}

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

/** The backend user an account belongs to, or null for a guest or the system. */
export function userOfAccount(account: AccountHandle | undefined): JazzUser | null {
  const identity = account?.identity;
  if (!identity || identity.issuer === LOCAL_FIRST_ISSUER || identity.issuer === SYSTEM_ISSUER)
    return null;
  return Object.freeze({
    id: identity.subject,
    issuer: identity.issuer,
    claims: account?.claims ?? Object.freeze({}),
  });
}

export interface JazzAppSnapshot<Client> {
  readonly status: "starting" | "signed-out" | "transitioning" | "ready" | "error";
  readonly client?: Client;
  readonly account?: JazzSessionSnapshot<Client>["account"];
  /** Who your backend says is signed in; null for a guest or when no one is. */
  readonly user: JazzUser | null;
  readonly error?: Error;
  /** Action retries repeat a failed enrollment; session retries only recover the selected client. */
  readonly recovery?: "action" | "session";
}
export interface JazzApp<Client> {
  getSnapshot(): JazzAppSnapshot<Client>;
  subscribe(listener: () => void): () => void;
  start(): Promise<void>;
  /**
   * Your auth changed: ask your backend who is signed in now and follow its
   * answer. Call it after your own sign-in, sign-out or account switch.
   */
  reauthenticate(): Promise<void>;
  retry(): Promise<void>;
  logout(): Promise<void>;
  dispose(): Promise<void>;
  readonly sessionActions: JazzSessionActions;
  /** Acknowledge only after the rendered view has detached its old subscriptions. */
  attachConsumer(): { acknowledge(snapshot: JazzAppSnapshot<Client>): void; release(): void };
}

type ReauthenticatingSession = { reauthenticate?(): Promise<void> };

/**
 * Host-independent owner. Configuration is captured once; your backend
 * decides who is signed in (see `JazzApp.reauthenticate`).
 */
export function createJazzAppOwner<Config, Client>(
  hostConfig: Config,
  factory: (config: Config) => Promise<JazzSession<Client>>,
  options: { start?: boolean } = {},
): JazzApp<Client> {
  let session: JazzSession<Client> | undefined;
  let unsubscribeSession: (() => void) | undefined;
  let pending: Promise<void> | undefined;
  let disposal: Promise<void> | undefined;
  let disposed = false;
  let failure: Error | undefined;
  let manualRetry: (() => Promise<void>) | undefined;
  // The session as a failed reauthenticate left it. Its failure (and its
  // retry, which asks the backend again) stands until the session moves on,
  // say a later check follows the backend.
  let reauthFailedAt: JazzSessionSnapshot<Client> | undefined;
  let actionGeneration = 0;
  let manualActionPending = false;
  let pendingLogouts = 0;
  let retrying: Promise<void> | undefined;
  let snapshot: JazzAppSnapshot<Client> = Object.freeze({
    status: "starting",
    user: null,
  });
  const versions = new WeakMap<object, JazzSessionSnapshot<Client>>();
  const listeners = createListeners("Jazz app observer failed");
  const consumers = new Set<{ lease?: JazzSessionConsumer<Client> }>();
  function publish(force = false) {
    if (disposed && !force) return;
    const current = session?.getSnapshot();
    if (reauthFailedAt && current !== reauthFailedAt) {
      reauthFailedAt = undefined;
      failure = undefined;
      if (manualRetry === app.reauthenticate) manualRetry = undefined;
    }
    const error = failure ?? current?.error;
    const status: JazzAppSnapshot<Client>["status"] = error
      ? "error"
      : !current
        ? "starting"
        : !disposed && current.status === "ready"
          ? "ready"
          : current.status === "signed-out"
            ? "signed-out"
            : "transitioning";
    const recovery: JazzAppSnapshot<Client>["recovery"] = error
      ? (current?.recovery ??
        (current && (current.status === "ready" || current.status === "signed-out")
          ? "action"
          : "session"))
      : undefined;
    const next = {
      status,
      recovery,
      client: status === "ready" ? current?.client : undefined,
      account: current?.account,
      user: snapshot.account === current?.account ? snapshot.user : userOfAccount(current?.account),
      error,
    };
    // Keep the raw snapshot version even when the app hides a still-ready client.
    if (
      snapshot.status === next.status &&
      snapshot.client === next.client &&
      snapshot.account === next.account &&
      snapshot.error === next.error &&
      snapshot.recovery === next.recovery &&
      (!current || versions.get(snapshot) === current)
    )
      return;
    snapshot = Object.freeze(next);
    if (current) versions.set(snapshot, current);
    listeners.notify();
  }
  const requireSession = () => {
    if (disposed) throw new Error("Jazz app is disposed");
    if (!session) throw new Error("Jazz app is starting; retry initialization first");
    return session;
  };
  const sessionActions = {} as JazzSessionActions;
  for (const name of [
    "createLocalFirst",
    "restoreLocalFirst",
    "becomeBackend",
    "loginWithBackend",
    "linkWithBackend",
  ] as const) {
    (sessionActions as any)[name] = async (...args: unknown[]) => {
      const current = requireSession();
      if (manualActionPending || pendingLogouts)
        throw new Error("A Jazz session operation is already pending");
      const generation = ++actionGeneration;
      const invoke = async () => {
        if (disposed || generation !== actionGeneration)
          throw new Error("Jazz app action was superseded");
        if (manualActionPending || pendingLogouts)
          throw new Error("A Jazz session operation is already pending");
        manualActionPending = true;
        manualRetry = undefined;
        failure = undefined;
        try {
          await (current[name] as (...args: unknown[]) => Promise<void>)(...args);
          if (generation === actionGeneration) manualRetry = undefined;
        } catch (cause) {
          const failed = current.getSnapshot();
          if (
            generation === actionGeneration &&
            failed.error &&
            (failed.recovery === "action" ||
              failed.status === "ready" ||
              failed.status === "signed-out")
          )
            manualRetry = invoke;
          throw cause;
        } finally {
          manualActionPending = false;
          publish();
        }
      };
      return invoke();
    };
  }
  const app: JazzApp<Client> = {
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    start() {
      if (disposed) return Promise.reject(new Error("Jazz app is disposed"));
      if (pending) return pending;
      if (session) return Promise.resolve();
      failure = undefined;
      publish();
      const task = Promise.resolve()
        .then(() => factory(hostConfig as Config))
        .then(async (created) => {
          if (disposed) {
            await created.close();
            return;
          }
          session = created;
          for (const consumer of consumers) consumer.lease = attachJazzSessionConsumer(created);
          unsubscribeSession = created.subscribe(() => publish(disposed));
          publish();
        })
        .catch((cause) => {
          failure = toError(cause);
          publish();
          throw cause;
        })
        .finally(() => {
          if (pending === task) pending = undefined;
        });
      pending = task;
      return task;
    },
    async reauthenticate() {
      failure = undefined;
      reauthFailedAt = undefined;
      if (manualRetry === app.reauthenticate) manualRetry = undefined;
      try {
        // Asked while starting: follow the backend once the session exists.
        if (!session && pending) await pending;
        const current = requireSession() as JazzSession<Client> & ReauthenticatingSession;
        if (!current.reauthenticate)
          throw new Error(
            "This Jazz session does not follow a backend: reauthenticate is not supported",
          );
        await current.reauthenticate();
      } catch (cause) {
        const after = session?.getSnapshot();
        // A ready client stays: the backend couldn't be asked, so nothing
        // says its user changed. Otherwise the app shows the failure (or the
        // session's own, when following the backend failed), and retrying
        // asks the backend again rather than reopening the old account.
        if (after?.status !== "ready") {
          failure = after?.error ? undefined : toError(cause);
          if (after) {
            manualRetry = app.reauthenticate;
            reauthFailedAt = after;
          }
        }
        throw cause;
      } finally {
        publish();
      }
    },
    retry() {
      if (disposed) return Promise.reject(new Error("Jazz app is disposed"));
      if (!session) return app.start();
      if (retrying) return retrying;
      const current = requireSession();
      const generation = actionGeneration;
      failure = undefined;
      const task = Promise.resolve()
        .then(async () => {
          if (disposed || generation !== actionGeneration) return;
          try {
            if (
              manualRetry &&
              (snapshot.recovery === "action" || manualRetry === app.reauthenticate)
            )
              await manualRetry();
            else {
              await current.retry();
              manualRetry = undefined;
            }
          } catch (cause) {
            if (generation === actionGeneration) failure = toError(cause);
            throw cause;
          } finally {
            publish();
          }
        })
        .finally(() => {
          if (retrying === task) retrying = undefined;
        });
      retrying = task;
      return task;
    },
    async logout() {
      const current = requireSession();
      ++pendingLogouts;
      const generation = ++actionGeneration;
      manualRetry = undefined;
      failure = undefined;
      try {
        await current.logout();
        if (generation === actionGeneration) manualRetry = undefined;
      } catch (cause) {
        if (generation === actionGeneration) manualRetry = app.logout;
        if (generation === actionGeneration) failure = toError(cause);
        throw cause;
      } finally {
        --pendingLogouts;
        publish();
      }
    },
    dispose() {
      if (disposal) return disposal;
      disposed = true;
      ++actionGeneration;
      manualRetry = undefined;
      publish(true);
      disposal = (async () => {
        try {
          await pending;
          await session?.close();
        } catch (cause) {
          failure = toError(cause);
          publish(true);
          throw cause;
        } finally {
          unsubscribeSession?.();
          for (const consumer of consumers) consumer.lease?.release();
          consumers.clear();
          listeners.clear();
        }
      })();
      return disposal;
    },
    sessionActions,
    attachConsumer() {
      if (disposed) throw new Error("Jazz app is disposed");
      const consumer = { lease: session ? attachJazzSessionConsumer(session) : undefined };
      consumers.add(consumer);
      return {
        acknowledge(value) {
          const raw = versions.get(value);
          if (raw) consumer.lease?.acknowledge(raw);
        },
        release() {
          consumer.lease?.release();
          consumers.delete(consumer);
        },
      };
    },
  };
  sessionActions.retry = app.retry;
  sessionActions.logout = app.logout;
  sessionActions.close = app.dispose;
  if (options.start !== false) void app.start().catch(() => {});
  return app;
}
