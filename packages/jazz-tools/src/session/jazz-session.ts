import {
  attachSessionCoreConsumer,
  onJazzSessionClientUpgraded,
  type SessionCore,
  type SessionCoreConsumer,
  type SessionCoreSnapshot,
} from "./state.js";
import type { AccountHandle } from "../accounts/state.js";
import { NoKeptDeviceAccountError } from "./no-kept-device-account-error.js";
import { JazzSessionOperationError } from "./session-operation-error.js";
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

function clientIsLocalFirst(client: unknown): boolean {
  const db = (client as { db?: { isLocalFirst?(): boolean } } | undefined)?.db;
  return db?.isLocalFirst?.() === true;
}

/** An operation a session is running (`snapshot.pending`). */
export type JazzSessionOperation =
  | "createDeviceAccount"
  | "restoreDeviceAccount"
  | "reopenDeviceAccount"
  | "linkDeviceAccount"
  | "refresh"
  | "signOut"
  | "retry";

const CORE_OPERATION: Record<string, JazzSessionOperation> = {
  createLocalFirst: "createDeviceAccount",
  restoreLocalFirst: "restoreDeviceAccount",
  reopenLocalFirst: "reopenDeviceAccount",
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
   * - `signing-out`: signed out on this device, but the server hasn't
   *   taken the signed-out user's last writes yet (offline). Their client
   *   is closed to the app and keeps uploading; the session tries again
   *   with backoff and on reconnect (`retry()` tries now), then becomes
   *   `signed-out`. `error` says why it waits. No one can sign in until
   *   then: sign-ins and `refresh()` reject with a
   *   `JazzSessionOperationError` (`reason: "signing-out"`).
   * - `error`: no usable client (startup failed, or a sign-in or reopen
   *   failed). `retry()` tries again.
   * - `closed`: `close()` was called.
   */
  readonly status:
    | "starting"
    | "ready"
    | "signed-out"
    | "transitioning"
    | "signing-out"
    | "error"
    | "closed";
  /** Who the session is signed in as; null when no one is. */
  readonly user: JazzUser | null;
  /** Only while `status` is `ready`. */
  readonly client?: Client;
  /**
   * Whether `client` reads and writes through a local store, so it works
   * offline: `client.db.isLocalFirst()`, as a field you can subscribe to. A
   * thin client turns local-first in place when the session upgrades it (the
   * same `client` and `db` carry on). False while there is no client.
   */
  readonly localFirst: boolean;
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
   * Sign in again as the device account that was signed in last on this
   * device, with its id and its data: signing out keeps a device account's
   * key (except with `clearOnSignOut`, which forgets it and wipes its data).
   * Rejects when the device keeps none.
   */
  reopenDeviceAccount(): Promise<void>;
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
   * Refreshes asked while one runs share one more, made after them all; one
   * asked during a sign-in or link waits for it. While signing out it
   * rejects (`JazzSessionOperationError`, `reason: "signing-out"`).
   */
  refresh(): Promise<void>;
  /**
   * The signed-in device account's key, to restore it elsewhere with
   * `restoreDeviceAccount(key)`. Anyone holding it is that account: keep it
   * secret. While signed out, the key of the device account
   * `reopenDeviceAccount()` would reopen. Throws when signed in as someone
   * else, or when signed out and the device keeps no device account.
   */
  exportDeviceKey(): string;
  /**
   * Sign out on this device. It takes over from a sign-in, link or refresh
   * still running: those reject with a `JazzSessionOperationError`
   * (`reason: "superseded"`), and nothing they finish later signs anyone
   * back in. Calls while one runs share it. Resolves once signed out here:
   * if the server can't take the user's last writes yet (offline), the
   * session is `signing-out` until it has.
   *
   * `discardUnsynced: true` doesn't wait for the server: it **loses the
   * signed-out user's writes the server hasn't confirmed**, wipes their local
   * data from this device, and signs out at once, to `signed-out`. It also
   * ends a `signing-out` session (offline) that way, so someone else can
   * sign in. What the server already has stays there.
   */
  signOut(options?: { discardUnsynced?: boolean }): Promise<void>;
  /** Try again after an error: see `snapshot.recovery`. Restarts a failed startup. */
  retry(): Promise<void>;
  /** Shut the client down; the session can't be used after. */
  close(): Promise<void>;
}

type RefreshingCore = { reauthenticate?(): Promise<void> };

/** An operation holding a session's slot (see `ownJazzSession`). */
type Busy = {
  readonly kind: "acting" | "refreshing" | "signing-out";
  readonly id: number;
  readonly done: Promise<void>;
  readonly cancel: (reason: Error) => void;
  /** Refreshing: the one more refresh that refreshes asked meanwhile share. */
  next?: Promise<void>;
};
type Slot = { readonly kind: "idle" } | Busy;
const IDLE: Slot = Object.freeze({ kind: "idle" });
type Failure<Client> = {
  readonly error?: Error;
  readonly repeat?: () => Promise<void>;
  readonly until?: SessionCoreSnapshot<Client>;
};
const noop = () => {};

interface SessionInternals<Client> {
  start(): Promise<void>;
  attachConsumer(name?: string): {
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
  /** Names the view in the warning when it misses a transition's deadline. */
  name?: string,
): ReturnType<SessionInternals<Client>["attachConsumer"]> {
  const own = internals.get(session);
  if (!own) throw new Error("Unknown Jazz session");
  return (own as SessionInternals<Client>).attachConsumer(name);
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
  /**
   * The one operation the session runs: a sign-in or link (`acting`), a
   * `refresh()` (`refreshing`) or a `signOut()` (`signing-out`). Only
   * `operate` moves it, and an operation's completion counts only while it
   * still holds the slot (its `id`): one a sign-out (or `close()`) took over
   * from neither publishes its outcome nor leaves a retry. What may start
   * while another runs: see `refuse`, `refresh` and `signOut`.
   */
  let slot: Slot = IDLE;
  let lastId = 0;
  // Sign-outs started: a refresh asked before one never runs after it.
  let signOuts = 0;
  /**
   * The last operation's failure, as the snapshot shows it (`error`, or the
   * core's own error when unset) and as `retry()` repeats it (`repeat`). A
   * failed refresh's stands only while the core is as the refresh left it
   * (`until`).
   */
  let failure: Failure<Client> | undefined;
  let retrying: Promise<void> | undefined;
  let lastAccount: AccountHandle | undefined;
  let snapshot: JazzSessionSnapshot<Client> = Object.freeze({
    status: "starting",
    user: null,
    localFirst: false,
  });
  const versions = new WeakMap<object, SessionCoreSnapshot<Client>>();
  const listeners = createListeners("Jazz session observer failed");
  const consumers = new Set<{ name?: string; lease?: SessionCoreConsumer<Client> }>();

  function publish(force = false) {
    if (closed && !force) return;
    const current = core?.getSnapshot();
    if (failure?.until && current !== failure.until) failure = undefined;
    const error = failure?.error ?? current?.error;
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
            : current.status === "signed-out" || current.status === "signing-out"
              ? current.status
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
      localFirst: status === "ready" && clientIsLocalFirst(current?.client),
      ...(current?.pending ? { pending: CORE_OPERATION[current.pending] } : {}),
      ...(error ? { error } : {}),
      ...(recovery ? { recovery } : {}),
    };
    lastAccount = account;
    if (
      snapshot.status === next.status &&
      snapshot.client === next.client &&
      snapshot.user === next.user &&
      snapshot.localFirst === next.localFirst &&
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

  /** Throws unless a sign-in, link or refresh may start now. */
  const refuse = () => {
    if (closed) throw new Error("Jazz session is closed");
    // A sign-out runs, or waits for the server (`signing-out`): no one
    // signs in until it ends.
    if (slot.kind === "signing-out" || core?.getSnapshot().status === "signing-out")
      throw new JazzSessionOperationError("signing-out");
    if (slot.kind !== "idle") throw new JazzSessionOperationError("pending");
  };

  /**
   * The slot's only transition: take it for `work`, and give it back when
   * `work` settles, if this operation still holds it. A sign-out takes over
   * with `cancel`: the caller of the operation it took over from gets a
   * `superseded` rejection at once, and that operation's late completion
   * changes nothing.
   */
  const operate = (
    kind: Busy["kind"],
    work: () => Promise<void>,
    failed: (error: Error) => Failure<Client> | undefined,
  ): Promise<void> => {
    if (slot.kind !== "idle") slot.cancel(new JazzSessionOperationError("superseded"));
    const id = ++lastId;
    let cancel!: (reason: Error) => void;
    const cancelled = new Promise<never>((_, reject) => (cancel = reject));
    cancelled.catch(() => {});
    let settle!: (outcome: Promise<void>) => void;
    const done = new Promise<void>((resolve) => (settle = resolve));
    const held = () => slot.kind !== "idle" && slot.id === id;
    // Held before `work` starts: the core publishes its transition at once.
    slot = { kind, id, done, cancel };
    failure = undefined;
    settle(
      (async () => {
        try {
          await Promise.race([work(), cancelled]);
        } catch (cause) {
          if (held()) failure = failed(toError(cause));
          throw cause;
        } finally {
          if (held()) slot = IDLE;
          publish();
        }
      })(),
    );
    return done;
  };

  /** One of the core's sign-in operations, in the slot, and retryable. */
  const action =
    <Args extends unknown[]>(
      run: (current: SessionCore<Client>, ...args: Args) => Promise<void>,
    ): ((...args: Args) => Promise<void>) =>
    async (...args: Args) => {
      if (!core && pending) await pending;
      const current = requireCore();
      const invoke = (): Promise<void> => {
        refuse();
        return operate(
          "acting",
          () => run(current, ...args),
          () => {
            // The core shows the error; `retry()` repeats the action while
            // it left the session where it was.
            const after = current.getSnapshot();
            return after.error &&
              (after.recovery === "action" ||
                after.status === "ready" ||
                after.status === "signed-out")
              ? { repeat: invoke }
              : undefined;
          },
        );
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
        for (const consumer of consumers)
          consumer.lease = attachSessionCoreConsumer(created, consumer.name);
        const stopCore = created.subscribe(() => publish(closed));
        const stopUpgrades = onJazzSessionClientUpgraded(created, () => publish(closed));
        unsubscribeCore = () => {
          stopCore();
          stopUpgrades();
        };
        publish();
      })
      .catch((cause) => {
        failure = { error: toError(cause) };
        publish();
        throw cause;
      })
      .finally(() => {
        if (pending === task) pending = undefined;
      });
    pending = task;
    return task;
  };

  /** A refresh started before a sign-out never runs after it. */
  const unlessSignedOutSince = (since: number) => {
    if (signOuts !== since) throw new JazzSessionOperationError("superseded");
  };

  const session: JazzSession<Client> = {
    get db() {
      const current = snapshot;
      const client = current.client as { db?: unknown } | undefined;
      if (current.status !== "ready" || !client?.db)
        throw new Error(
          `session.db: there is no Db while the session is ${current.status}` +
            (current.status === "signed-out" || current.status === "signing-out"
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
    reopenDeviceAccount: action((current) => current.reopenLocalFirst()),
    linkDeviceAccount: action((current) => current.linkWithBackend()),
    refresh(): Promise<void> {
      const since = signOuts;
      // Refreshes asked while one runs share one more, which asks the
      // backend after them all.
      if (slot.kind === "refreshing") {
        const running = slot;
        running.next ??= running.done
          .then(noop, noop)
          .then(() => unlessSignedOutSince(since))
          .then(() => session.refresh());
        return running.next;
      }
      // A refresh waits for an action to finish, then follows the backend.
      if (slot.kind === "acting")
        return slot.done
          .then(noop, noop)
          .then(() => unlessSignedOutSince(since))
          .then(() => session.refresh());
      try {
        refuse();
      } catch (error) {
        return Promise.reject(error);
      }
      return operate(
        "refreshing",
        async () => {
          // Asked while starting: follow the backend once the session exists.
          if (!core && pending) await pending;
          const current = requireCore() as SessionCore<Client> & RefreshingCore;
          if (current.reauthenticate) await current.reauthenticate();
          else await current.loginWithBackend();
        },
        (error) => {
          const after = core?.getSnapshot();
          // The session shows the failure (or the core's own), and retrying
          // asks the backend again rather than reopening the old account. A
          // ready client stays ready (the backend couldn't be asked, so
          // nothing says its user changed), with the error and `recovery:
          // "action"`. It stands until the core moves on.
          return {
            ...(after?.error ? {} : { error }),
            ...(after ? { repeat: session.refresh, until: after } : {}),
          };
        },
      );
    },
    exportDeviceKey() {
      const account = core?.getSnapshot().account;
      if (!account) {
        const kept = core?.keptLocalFirstKey();
        if (!kept) throw new NoKeptDeviceAccountError("exportDeviceKey()");
        return kept;
      }
      if (account.identity.issuer !== DEVICE_ISSUER)
        throw new Error("exportDeviceKey(): this session isn't signed in as a device account");
      return exportLocalFirstSecret(account);
    },
    async signOut(options: { discardUnsynced?: boolean } = {}) {
      const discard = options.discardUnsynced === true;
      // A sign-out running: share it, or (discarding) follow it with one
      // that drops what it would still wait to upload.
      const join = async (running: Busy) => {
        if (!discard) return running.done;
        await running.done.catch(noop);
        return session.signOut(options);
      };
      if (slot.kind === "signing-out") return join(slot);
      if (!core && pending) await pending;
      const current = requireCore();
      // Joined, if one started while this waited for startup.
      const now = slot as Slot;
      if (now.kind === "signing-out") return join(now);
      // Sign-out takes over from any operation running (see `operate`).
      ++signOuts;
      return operate(
        "signing-out",
        () => current.logout({ discardUnsynced: discard }),
        (error) => ({ error, repeat: () => session.signOut(options) }),
      );
    },
    retry() {
      if (closed) return Promise.reject(new Error("Jazz session is closed"));
      if (!core) return start();
      if (retrying) return retrying;
      const current = core;
      const asked = { id: lastId };
      const task = Promise.resolve()
        .then(async () => {
          // Another operation started meanwhile: it decides what's next.
          if (closed || lastId !== asked.id) return;
          const repeat = failure?.repeat;
          if (repeat && (snapshot.recovery === "action" || repeat === session.refresh))
            return repeat();
          // Signing out: try the upload now (the snapshot shows how it went).
          if (current.getSnapshot().status === "signing-out") return current.retry();
          refuse();
          return operate(
            "acting",
            () => current.retry(),
            (error) => ({ error }),
          );
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
      if (slot.kind !== "idle") slot.cancel(new JazzSessionOperationError("superseded"));
      slot = IDLE;
      failure = failure?.error ? { error: failure.error } : undefined;
      publish(true);
      closing = (async () => {
        try {
          await pending?.catch(() => {});
          await core?.close();
        } catch (cause) {
          failure = { error: toError(cause) };
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
    attachConsumer(name?: string) {
      if (closed) throw new Error("Jazz session is closed");
      const consumer = { name, lease: core ? attachSessionCoreConsumer(core, name) : undefined };
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
    reopenDeviceAccount: session.reopenDeviceAccount,
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
