import {
  attachSessionCoreConsumer,
  type SessionCore,
  type SessionCoreConsumer,
  type SessionCoreSnapshot,
} from "./state.js";
import type { AccountHandle } from "../accounts/state.js";
import { exportLocalFirstSecret } from "../accounts/enrollment.js";
import { createListeners, toError } from "../runtime/util.js";

const DEVICE_ISSUER = "urn:jazz:local-first";
const SYSTEM_ISSUER = "urn:jazz:system";

/**
 * Who a Jazz session is signed in as.
 *
 * - A **device account** (`kind: "device"`): an account whose key lives on
 *   this device. It needs no sign-in, works offline, and is how an app lets
 *   people start as guests. Link it to a user your backend signs in with
 *   `linkDeviceAccount()`.
 * - A **backend user** (`kind: "backend"`): the user your backend's
 *   `authenticate` names.
 */
export interface JazzUser {
  /**
   * The Jazz account: what your permissions see as `session.user`, and what
   * a row's owner column holds.
   */
  readonly accountId: string;
  readonly kind: "device" | "backend";
  /**
   * How the account signed in. For a backend user, `subject` is your own
   * user id (`authenticate`'s `userId`) and `issuer` the one Jazz records for
   * your backend's users.
   */
  readonly identity: { readonly issuer: string; readonly subject: string };
  /**
   * The extra claims `authenticate` returned (`session.claims` in
   * permissions), as of this user's sign-in. Empty for a device account.
   */
  readonly claims: Readonly<Record<string, unknown>>;
}

/** @internal The user a session's account handle stands for. */
export function userOfAccount(account: AccountHandle | undefined): JazzUser | null {
  const identity = account?.identity;
  if (!account || !identity || identity.issuer === SYSTEM_ISSUER) return null;
  return Object.freeze({
    accountId: account.id,
    kind: identity.issuer === DEVICE_ISSUER ? "device" : "backend",
    identity: Object.freeze({ issuer: identity.issuer, subject: identity.subject }),
    claims: Object.freeze({ ...account.claims }),
  });
}

/** An operation a session is running (`snapshot.pending`). */
export type JazzSessionOperation =
  | "createDeviceAccount"
  | "restoreDeviceAccount"
  | "linkDeviceAccount"
  | "refresh"
  | "signOut"
  | "retry";

const CORE_OPERATION: Record<string, JazzSessionOperation> = {
  createLocalFirst: "createDeviceAccount",
  restoreLocalFirst: "restoreDeviceAccount",
  linkWithBackend: "linkDeviceAccount",
  loginWithBackend: "refresh",
  logout: "signOut",
  retry: "retry",
};

export interface JazzSessionSnapshot<Client> {
  /**
   * - `starting`: opening; nothing to show yet.
   * - `ready`: `client` is usable. `error` may still be set: an operation
   *   failed and left the client as it was (see `recovery`).
   * - `signed-out`: no one is signed in. `error` may say why.
   * - `transitioning`: signing in, out, or switching users.
   * - `error`: no usable client (startup failed, or a sign-in or reopen
   *   failed). `retry()` tries again.
   * - `closed`: `close()` was called.
   */
  readonly status: "starting" | "ready" | "signed-out" | "transitioning" | "error" | "closed";
  /** Who the session is signed in as; null when no one is. */
  readonly user: JazzUser | null;
  /** Only while `status` is `ready`. */
  readonly client?: Client;
  readonly pending?: JazzSessionOperation;
  readonly error?: Error;
  /**
   * What `retry()` does about `error`: repeat the operation that failed
   * (`action`), or reopen the signed-in account (`session`).
   */
  readonly recovery?: "action" | "session";
}

/**
 * A Jazz session: who is signed in on this device, and the client that reads
 * and writes as them. Your backend decides who is signed in (its
 * `authenticate`); the session follows it.
 */
export interface JazzSession<Client> {
  getSnapshot(): JazzSessionSnapshot<Client>;
  subscribe(listener: () => void): () => void;
  /**
   * The signed-in user's Db: `getSnapshot().client.db`. Throws, naming the
   * status, while there is no usable client (starting, signed out, switching
   * users, failed or closed).
   *
   * The session replaces its Db when someone signs in or out (or switches
   * users): read `session.db` again after those rather than keeping it. A Db
   * kept from before throws `DbReplacedError` (the local-first upgrade keeps
   * the same Db).
   */
  readonly db: Client extends { db: infer TDb } ? TDb : unknown;
  /**
   * Sign in as a new device account: an account whose key lives on this
   * device. It needs no sign-in and works offline. It's how an app lets
   * people start as guests, and a local-first friendly way to sign in at
   * all. Link it to a user your backend signs in later with
   * `linkDeviceAccount()`, or keep its key with `exportDeviceKey()`.
   */
  createDeviceAccount(): Promise<void>;
  /** Sign in as the device account a key from `exportDeviceKey()` names. */
  restoreDeviceAccount(key: string): Promise<void>;
  /**
   * Keep the signed-in device account and its data when its user signs up:
   * after your own sign-up, your backend links this device account to the
   * user it now names, and the session continues as that user.
   */
  linkDeviceAccount(): Promise<void>;
  /**
   * Your auth changed: call this after your own sign-in, sign-out or account
   * switch. Jazz asks your backend who is signed in now (its `authenticate`
   * decides, from your session cookie or bearer) and follows its answer.
   * If your backend can't be asked (offline, or its `authenticate` failed),
   * it rejects and the snapshot shows the failure too: a usable client stays
   * `ready`, with `error` set and `recovery: "action"` (`retry()` asks again).
   */
  refresh(): Promise<void>;
  /**
   * The signed-in device account's key, to restore it elsewhere with
   * `restoreDeviceAccount(key)`. Anyone holding it is that account: keep it
   * secret. Throws when the session isn't signed in as a device account.
   */
  exportDeviceKey(): string;
  signOut(): Promise<void>;
  /** Try again after an error: see `snapshot.recovery`. Restarts a failed startup. */
  retry(): Promise<void>;
  /** Shut the client down; the session can't be used after. */
  close(): Promise<void>;
}

type RefreshingCore = { reauthenticate?(): Promise<void> };

interface SessionInternals<Client> {
  start(): Promise<void>;
  attachConsumer(): {
    acknowledge(snapshot: JazzSessionSnapshot<Client>): void;
    release(): void;
  };
}
const internals = new WeakMap<object, SessionInternals<unknown>>();

/** @internal Start a session made by `ownJazzSession` with `start: false`. */
export function startJazzSession(session: JazzSession<unknown>): Promise<void> {
  const own = internals.get(session);
  if (!own) return Promise.reject(new Error("Unknown Jazz session"));
  return own.start();
}

/**
 * @internal A framework view's lease on a session: acknowledge each snapshot
 * once the view has detached the subscriptions of the one before.
 */
export function attachJazzSessionConsumer<Client>(
  session: JazzSession<Client>,
): ReturnType<SessionInternals<Client>["attachConsumer"]> {
  const own = internals.get(session);
  if (!own) throw new Error("Unknown Jazz session");
  return (own as SessionInternals<Client>).attachConsumer();
}

/**
 * @internal Own a session over a host's session core: it exists at once
 * (`starting`), and startup failures and retries are part of its state.
 */
export function ownJazzSession<Config, Client>(
  hostConfig: Config,
  factory: (config: Config) => Promise<SessionCore<Client>>,
  options: { start?: boolean } = {},
): JazzSession<Client> {
  let core: SessionCore<Client> | undefined;
  let unsubscribeCore: (() => void) | undefined;
  let pending: Promise<void> | undefined;
  let closing: Promise<void> | undefined;
  let closed = false;
  let failure: Error | undefined;
  let manualRetry: (() => Promise<void>) | undefined;
  // The core as a failed refresh left it. Its failure (and its retry, which
  // asks the backend again) stands until the core moves on.
  let refreshFailedAt: SessionCoreSnapshot<Client> | undefined;
  let actionGeneration = 0;
  let manualActionPending = false;
  let pendingSignOuts = 0;
  let retrying: Promise<void> | undefined;
  let lastAccount: AccountHandle | undefined;
  let snapshot: JazzSessionSnapshot<Client> = Object.freeze({ status: "starting", user: null });
  const versions = new WeakMap<object, SessionCoreSnapshot<Client>>();
  const listeners = createListeners("Jazz session observer failed");
  const consumers = new Set<{ lease?: SessionCoreConsumer<Client> }>();

  function publish(force = false) {
    if (closed && !force) return;
    const current = core?.getSnapshot();
    if (refreshFailedAt && current !== refreshFailedAt) {
      refreshFailedAt = undefined;
      failure = undefined;
      if (manualRetry === session.refresh) manualRetry = undefined;
    }
    const error = failure ?? current?.error;
    const usable = current?.status === "ready" && current.client !== undefined;
    const status: JazzSessionSnapshot<Client>["status"] =
      closed || current?.status === "closed"
        ? "closed"
        : !current
          ? error
            ? "error"
            : "starting"
          : usable
            ? "ready"
            : current.status === "signed-out"
              ? "signed-out"
              : current.status === "error" || (error && current.status !== "transitioning")
                ? "error"
                : "transitioning";
    const recovery: JazzSessionSnapshot<Client>["recovery"] = error
      ? (current?.recovery ??
        (current && (current.status === "ready" || current.status === "signed-out")
          ? "action"
          : "session"))
      : undefined;
    const account = current?.account;
    const next: JazzSessionSnapshot<Client> = {
      status,
      user:
        account === lastAccount && snapshot.user !== undefined
          ? snapshot.user
          : userOfAccount(account),
      ...(status === "ready" ? { client: current?.client } : {}),
      ...(current?.pending ? { pending: CORE_OPERATION[current.pending] } : {}),
      ...(error ? { error } : {}),
      ...(recovery ? { recovery } : {}),
    };
    lastAccount = account;
    if (
      snapshot.status === next.status &&
      snapshot.client === next.client &&
      snapshot.user === next.user &&
      snapshot.pending === next.pending &&
      snapshot.error === next.error &&
      snapshot.recovery === next.recovery &&
      (!current || versions.get(snapshot) === current)
    )
      return;
    snapshot = Object.freeze(next);
    if (current) versions.set(snapshot, current);
    listeners.notify();
  }

  const requireCore = () => {
    if (closed) throw new Error("Jazz session is closed");
    if (!core) throw new Error("Jazz session is starting; retry initialization first");
    return core;
  };

  /** Run one of the core's sign-in operations, serialized and retryable. */
  const action =
    <Args extends unknown[]>(
      run: (current: SessionCore<Client>, ...args: Args) => Promise<void>,
    ): ((...args: Args) => Promise<void>) =>
    async (...args: Args) => {
      if (!core && pending) await pending;
      const current = requireCore();
      if (manualActionPending || pendingSignOuts)
        throw new Error("A Jazz session operation is already pending");
      const generation = ++actionGeneration;
      const invoke = async () => {
        if (closed || generation !== actionGeneration)
          throw new Error("Jazz session operation was superseded");
        if (manualActionPending || pendingSignOuts)
          throw new Error("A Jazz session operation is already pending");
        manualActionPending = true;
        manualRetry = undefined;
        failure = undefined;
        try {
          await run(current, ...args);
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

  const start = () => {
    if (closed) return Promise.reject(new Error("Jazz session is closed"));
    if (pending) return pending;
    if (core) return Promise.resolve();
    failure = undefined;
    publish();
    const task = Promise.resolve()
      .then(() => factory(hostConfig))
      .then(async (created) => {
        if (closed) {
          await created.close();
          return;
        }
        core = created;
        for (const consumer of consumers) consumer.lease = attachSessionCoreConsumer(created);
        unsubscribeCore = created.subscribe(() => publish(closed));
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
  };

  const session: JazzSession<Client> = {
    get db() {
      const current = snapshot;
      const client = current.client as { db?: unknown } | undefined;
      if (current.status !== "ready" || !client?.db)
        throw new Error(
          `session.db: there is no Db while the session is ${current.status}` +
            (current.status === "signed-out"
              ? ": sign in first (createDeviceAccount(), or your backend's user)"
              : current.status === "starting" || current.status === "transitioning"
                ? ': wait for status "ready" (subscribe to the session)'
                : current.error
                  ? ` (${current.error.message})`
                  : ""),
        );
      return client.db as JazzSession<Client>["db"];
    },
    getSnapshot: () => snapshot,
    subscribe: listeners.subscribe,
    createDeviceAccount: action((current) => current.createLocalFirst()),
    restoreDeviceAccount: action((current, key: string) => current.restoreLocalFirst(key)),
    linkDeviceAccount: action((current) => current.linkWithBackend()),
    async refresh() {
      failure = undefined;
      refreshFailedAt = undefined;
      if (manualRetry === session.refresh) manualRetry = undefined;
      try {
        // Asked while starting: follow the backend once the session exists.
        if (!core && pending) await pending;
        const current = requireCore() as SessionCore<Client> & RefreshingCore;
        if (current.reauthenticate) await current.reauthenticate();
        else await current.loginWithBackend();
      } catch (cause) {
        const after = core?.getSnapshot();
        // The session shows the failure (or the core's own), and retrying
        // asks the backend again rather than reopening the old account. A
        // ready client stays ready (the backend couldn't be asked, so nothing
        // says its user changed), with the error and `recovery: "action"`.
        failure = after?.error ? undefined : toError(cause);
        if (after) {
          manualRetry = session.refresh;
          refreshFailedAt = after;
        }
        throw cause;
      } finally {
        publish();
      }
    },
    exportDeviceKey() {
      const account = core?.getSnapshot().account;
      if (!account || account.identity.issuer !== DEVICE_ISSUER)
        throw new Error("exportDeviceKey(): this session isn't signed in as a device account");
      return exportLocalFirstSecret(account);
    },
    async signOut() {
      if (!core && pending) await pending;
      const current = requireCore();
      ++pendingSignOuts;
      const generation = ++actionGeneration;
      manualRetry = undefined;
      failure = undefined;
      try {
        await current.logout();
        if (generation === actionGeneration) manualRetry = undefined;
      } catch (cause) {
        if (generation === actionGeneration) {
          manualRetry = session.signOut;
          failure = toError(cause);
        }
        throw cause;
      } finally {
        --pendingSignOuts;
        publish();
      }
    },
    retry() {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      if (!core) return start();
      if (retrying) return retrying;
      const current = core;
      const generation = actionGeneration;
      failure = undefined;
      const task = Promise.resolve()
        .then(async () => {
          if (closed || generation !== actionGeneration) return;
          try {
            if (manualRetry && (snapshot.recovery === "action" || manualRetry === session.refresh))
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
    close() {
      if (closing) return closing;
      closed = true;
      ++actionGeneration;
      manualRetry = undefined;
      publish(true);
      closing = (async () => {
        try {
          await pending?.catch(() => {});
          await core?.close();
        } catch (cause) {
          failure = toError(cause);
          publish(true);
          throw cause;
        } finally {
          unsubscribeCore?.();
          for (const consumer of consumers) consumer.lease?.release();
          consumers.clear();
          listeners.clear();
        }
      })();
      return closing;
    },
  };

  internals.set(session, {
    start,
    attachConsumer() {
      if (closed) throw new Error("Jazz session is closed");
      const consumer = { lease: core ? attachSessionCoreConsumer(core) : undefined };
      consumers.add(consumer);
      return {
        acknowledge(value: JazzSessionSnapshot<Client>) {
          const raw = versions.get(value);
          if (raw) consumer.lease?.acknowledge(raw);
        },
        release() {
          consumer.lease?.release();
          consumers.delete(consumer);
        },
      };
    },
  } as SessionInternals<unknown>);

  if (options.start !== false) void start().catch(() => {});
  return session;
}

/**
 * @internal The public `createJazzClient` of a host: own a session over its
 * core, and resolve once it started (rejecting, and closing it, if startup
 * failed).
 */
export async function createStartedJazzSession<Config, Client>(
  config: Config,
  factory: (config: Config) => Promise<SessionCore<Client>>,
): Promise<JazzSession<Client>> {
  const session = ownJazzSession(config, factory, { start: false });
  try {
    await startJazzSession(session as JazzSession<unknown>);
  } catch (error) {
    await session.close().catch(() => {});
    throw error;
  }
  return session;
}

/** @internal `initial` as the session core takes it, refusing a removed value by name. */
export function coreInitial(initial: unknown): "local-first" | "signed-out" | undefined {
  if (initial === undefined || initial === "signed-out") return initial;
  if (initial === "device") return "local-first";
  if (initial === "local-first")
    throw new Error('initial: "local-first" is now initial: "device" (a device account)');
  throw new Error(`Unknown initial ${JSON.stringify(initial)}; expected "device" or "signed-out"`);
}

/**
 * @internal A session's operations, bound and without its state, for the
 * framework hooks (never a spread: `db` is a getter that throws while there
 * is no client).
 */
export function jazzSessionActions<Client>(
  session: JazzSession<Client>,
): Omit<JazzSession<Client>, "getSnapshot" | "subscribe" | "db"> {
  return {
    createDeviceAccount: session.createDeviceAccount,
    restoreDeviceAccount: session.restoreDeviceAccount,
    linkDeviceAccount: session.linkDeviceAccount,
    refresh: session.refresh,
    exportDeviceKey: session.exportDeviceKey,
    signOut: session.signOut,
    retry: session.retry,
    close: session.close,
  };
}

/**
 * @internal Session options that alpha.60 took and the session no longer
 * does. Refused by name: left in, they would reach the client config and be
 * ignored, quietly changing what the app asked for (an app that turned guest
 * linking off on shared devices would link again).
 */
export function refuseRemovedSessionOptions(config: object): void {
  const given = config as Record<string, unknown>;
  if ("linkGuestOnSignUp" in given)
    throw new Error(
      "createJazzClient: linkGuestOnSignUp is now linkDeviceAccountOnSignUp (guests are device accounts)",
    );
  if ("account" in given)
    throw new Error(
      'createJazzClient takes no account: the session signs in itself (initial: "device", createDeviceAccount(), or your backend\'s user). For a client you opened yourself, use <JazzClientProvider client>.',
    );
}
