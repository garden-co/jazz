import type { SignedOutDecision } from "./backend-watch.js";
import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import type { AccountHandle } from "../accounts/state.js";
import {
  clientRegistryUrl,
  createAccountManagerWithMinter,
  watchStoredSelection,
} from "../accounts/account-manager-core.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import type { ShutdownOptions } from "../runtime/db.js";
import { bearerFetch } from "../runtime/credentialed-transport.js";
import { sessionRefreshLockName } from "../runtime/native-runtime/session-renewal-wire.js";
import {
  createRegisteredJazzClient,
  type CreateAccountDb,
  type JazzClient,
} from "../web/registered-jazz-client.js";
import type { OpenJazzClientConfig } from "../web/create-jazz-client.js";
import {
  createClient as createThinDb,
  mintLocalFirstTokenInJs,
  prefetchThinQueries,
  handOffThinWrites,
} from "../thin/index.js";
import {
  setLocalFirstUpgrader,
  type LocalFirstLoader,
  type LocalFirstModule,
} from "../runtime/local-first-upgrade.js";
import { createJazzSessionOwner, replaceJazzSessionClient } from "./state.js";
import { coreInitial, createStartedJazzSession, type JazzSession } from "./jazz-session.js";
import { DEFAULT_SERVER_URL, refuseClientAppId } from "../runtime/client-scope.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import {
  backendAnswered,
  firstBackendAnswer,
  followUser,
  signedInIdentity,
  type UserFollowingSession,
} from "./follow-user.js";
import { replicaLedger, replicaLifecycle, type ReplicaLifecycle } from "./replica-ledger.js";
import { refuseInvalidSessionConfig } from "./config-options.js";
import { warnWindowJazzConflict } from "../runtime/page-handle.js";
import { accountForgotten, storedSelectedAccount } from "../accounts/persistence.js";
import { rememberBackendUser } from "../accounts/enrollment.js";

export type { LocalFirstLoader };

export type JazzClientConfig = Omit<OpenJazzClientConfig, "account" | "serverUrl" | "appId"> &
  Omit<AccountManagerConfig, "appId"> & {
    /**
     * Not for a client: the app id belongs to your backend's store
     * (`remote({ appId })`, or the id a store keeps). A client is scoped by its
     * `serverUrl`; use `scope` to keep local-first users created under an
     * app id.
     */
    appId?: never;
    /**
     * Your app's Jazz endpoint: by default `"/jazz"`, where `createJazz` is
     * mounted in your own backend. `null` keeps Jazz on this device only: no
     * server, no sign-in, one device account per browser profile. That needs
     * the local-first client (`client`).
     */
    serverUrl?: string | null;
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"device"` account: an
     * account whose key lives on this device, which is how you let people
     * start as guests, and which can later link to your backend's user
     * (`linkDeviceAccount()`). Your backend must accept device accounts
     * (`acceptDeviceAccounts`). A device account works offline once its
     * client is the local-first one (`client`).
     */
    initial?: "device" | "signed-out";
    /**
     * When a device account is on this device and the user who signs in has
     * no Jazz account yet (they just signed up, or they are an existing user
     * signing in to Jazz for the first time), link the device account to
     * them: the account and everything it wrote become theirs. On by
     * default. Turn it off on shared devices, where the device account may be
     * someone else's guest: the session then leaves it as it is, and your
     * app decides after its own sign-in, with `linkDeviceAccount()` (its data
     * becomes the user's) or `refresh()` (an account of the user's own; the
     * device account stays on this device).
     */
    linkDeviceAccountOnSignUp?: boolean;
    /**
     * Called when your backend says no one is signed in any more (confirmed
     * by a second answer a moment later), before the session signs the
     * user out. Return `"keep"` to stay signed in for now (your app refreshed
     * its session itself, or redirects to sign-in first): the session asks
     * your backend again on the next focus, refusal or `refresh()`.
     * Return `"sign-out"` (or nothing) to sign out. A hook that throws signs
     * out.
     */
    onSignedOut?: () => SignedOutDecision | void | Promise<SignedOutDecision | void>;
    /**
     * On sign-out (or a switch to another user), wipe the account's local
     * storage instead of keeping it for that user's next sign-in. For shared
     * devices. Jazz first waits up to 5 seconds for pending writes to upload;
     * writes still unsynced then are lost. A sign-out the page doesn't see
     * (on your server, or another user signing in, before a reload) is
     * wiped on the next page load, once your backend names someone else or
     * no one.
     */
    clearOnSignOut?: boolean;
    /**
     * Load the local-first client, for example
     * `() => import("jazz-tools/local-first")`. Without it the app runs on the
     * thin client: the server evaluates every query and runs every write, and
     * no WASM, worker or local storage loads. With it, the app starts thin and
     * upgrades to local-first in the background once the page has loaded; a
     * browser that upgraded this account before opens local-first straight
     * away, so it works offline.
     */
    client?: LocalFirstLoader;
    /**
     * When the local-first client (`client`) opens:
     * - `"background"` (the default): start thin and upgrade once the page
     *   has loaded, unless this account upgraded in this browser before.
     *   The first paint needs no WASM.
     * - `"now"`: open the local-first client straight away, for every
     *   account. For apps that must work offline from the first render, or
     *   that use `db.transaction`, which only the local-first client has.
     *   If the local-first client can't load, the session fails (`retry()`
     *   tries again) rather than running thin.
     *
     * Needs `client`: `"now"` without it is refused.
     */
    localFirst?: "now" | "background";
  };

/**
 * Every option `createJazzClient` takes in a browser, checked against the
 * config type both ways (`appId` and `backendSecret` are refused by type, and
 * `appId` by name too).
 */
const BROWSER_SESSION_OPTIONS = {
  serverUrl: true,
  initial: true,
  linkDeviceAccountOnSignUp: true,
  onSignedOut: true,
  clearOnSignOut: true,
  client: true,
  localFirst: true,
  scope: true,
  driver: true,
  env: true,
  runtimeSources: true,
  dbName: true,
  initialSyncFlushEvery: true,
  logLevel: true,
  telemetryCollectorUrl: true,
  devMode: true,
  slowQueryThresholdMs: true,
  store: true,
  fetch: true,
  bearer: true,
} as const satisfies Record<Exclude<keyof JazzClientConfig, "appId" | "backendSecret">, true>;

/** @internal A browser session's core, which follows your auth's signed-in user. */
export type BrowserJazzSession = UserFollowingSession<JazzClient>;

/**
 * A client keeps its options as configuration (`db.getConfig()` clones them),
 * so one that holds a function or class could only fail later, in whatever
 * first reads the config (a sign-in). No client option takes one: name it now.
 */
function refuseUnkeptOptions(clientConfig: Record<string, unknown>): void {
  for (const [key, value] of Object.entries(clientConfig)) {
    let kept = value;
    if (key === "runtimeSources" && value && typeof value === "object") {
      // Live handles the Db leaves out of its config.
      const {
        browserWorkerPort: _port,
        browserWorkerSession: _session,
        ...sources
      } = value as Record<string, unknown>;
      kept = sources;
    }
    try {
      structuredClone(kept);
    } catch {
      throw new TypeError(
        `createJazzClient has no option "${key}" that takes a function or class` +
          (key === "WebSocket"
            ? ": the client uses the platform's own WebSocket (Node has one from 22)"
            : ""),
      );
    }
  }
}

/**
 * Open a Jazz session in this browser: who is signed in, and the client that
 * reads and writes as them. Resolves once it started; rejects if it can't.
 * Your backend decides who is signed in; call `session.refresh()` after your
 * own sign-in or sign-out.
 */
export function createJazzClient(config: JazzClientConfig): Promise<JazzSession<JazzClient>> {
  return createStartedJazzSession(config, openBrowserSessionCore);
}

/** @internal The session core `createJazzClient` and `<JazzProvider>` own. */
export async function openBrowserSessionCore(
  config: JazzClientConfig,
): Promise<BrowserJazzSession> {
  if (config && typeof config === "object") refuseClientAppId(config);
  refuseInvalidSessionConfig(config, BROWSER_SESSION_OPTIONS);
  // `window.__jazz` is Jazz's page handle: an app's own global there is left
  // alone (Jazz goes without the handle), and said so now rather than at the
  // upgrade to local-first.
  const page = (globalThis as { window?: Window }).window;
  if (page && typeof page.document !== "undefined") warnWindowJazzConflict(page);
  if (config.serverUrl === null) return createDeviceOnlySession(config);
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const {
    initial,
    clearOnSignOut,
    linkDeviceAccountOnSignUp,
    onSignedOut,
    client: loadLocalFirst,
    localFirst,
    // The account manager's own: neither is client configuration, and a Db
    // keeps only cloneable data (db.getConfig()).
    store: _store,
    fetch: _fetch,
    bearer,
    ...clientConfig
  } = { ...config, serverUrl };
  refuseUnkeptOptions(clientConfig);
  refuseLocalFirstTiming(localFirst, loadLocalFirst);
  const accounts = await createAccountManagerWithMinter(
    { ...config, serverUrl },
    mintLocalFirstTokenInJs,
    // A shared device: never reopen the last user before the backend answers.
    { rememberBackendUser: !clearOnSignOut },
  );
  const registry = clientRegistryUrl({ ...config, serverUrl });
  const upgrades = localFirstMemory(registry);
  // A memory-only client has no storage to wipe.
  const wipesOnSignOut = !!clearOnSignOut && (config.driver?.type ?? "persistent") === "persistent";
  // The lifecycle of this device's per-account stores (see `replicaLifecycle`).
  const replicas = wipesOnSignOut && loadLocalFirst ? openReplicaLifecycle() : undefined;
  // Accounts this page is opening a local-first client for.
  const opening = new Set<string>();
  function openReplicaLifecycle() {
    const fetcher = bearer ? bearerFetch(bearer, config.fetch) : config.fetch;
    return replicaLifecycle({
      registry,
      ledger: replicaLedger(registry),
      stopped: () => session?.getSnapshot().status === "closed",
      heldHere: (account) => {
        if (opening.has(account)) return true;
        const snapshot = session?.getSnapshot();
        return snapshot?.status !== "closed" && snapshot?.account?.id === account;
      },
      deleteStore: async (account) => {
        // `jazz-tools/local-first` brings it in the app's own chunk; a loader
        // that gives only `createClient` gets it loaded here.
        const deleteLocalFirstStore =
          (await loadLocalFirst!()).deleteLocalFirstStore ??
          (await import("../runtime/delete-local-first-store.js")).deleteLocalFirstStore;
        return deleteLocalFirstStore({
          ...clientConfig,
          accountId: account,
          accountRegistryAuthority: registry,
        });
      },
      backendUser: async () => {
        const user = await signedInIdentity(registry, fetcher);
        return user ? { issuer: user.issuer, subject: user.subject } : null;
      },
      selectedDeviceAccount: () => storedSelectedAccount(accounts),
    });
  }
  /**
   * Open `account`'s local-first client, held open against every tab's
   * wipe, and recorded in the ledger so a later page wipes what a sign-out
   * this page doesn't see leaves behind.
   */
  const openReplica = async (createDb: CreateAccountDb, account: AccountHandle) => {
    if (!replicas) return openLocalFirst(createDb, account);
    opening.add(account.id);
    try {
      const release = await replicas.hold({
        account: account.id,
        kind: rememberBackendUser(account) ? "backend" : "device",
        identity: account.identity,
      });
      let client: JazzClient;
      try {
        client = await openLocalFirst(createDb, account);
      } catch (error) {
        await release();
        throw error;
      }
      const shutdown = client.shutdown.bind(client);
      client.shutdown = async (options) => {
        try {
          await shutdown(options);
        } finally {
          await release();
        }
      };
      return client;
    } finally {
      opening.delete(account.id);
    }
  };
  /**
   * How this page signs a local-first account out under `clearOnSignOut`:
   * upload what the server still accepts, count what it still holds (every
   * tab's writes), close this tab's client (letting go of its hold), then
   * wipe as the last tab out. While another tab holds the store it doesn't:
   * that tab decides. Resolves whether the store is gone.
   */
  const wipeSignedOut = async (client: JazzClient, account: AccountHandle) => {
    // Loaded first: a chunk that won't load fails the sign-out while the
    // client still works, rather than after it closed.
    await loadLocalFirst!();
    await client.db.waitForSync({ timeoutMs: 5_000 }).catch(() => {});
    const pendingUploads = await client.db
      .storePendingUploads()
      .catch(() => client.db.getSyncStatus().pendingUploads);
    await client.shutdown();
    return (await replicas!.signedOut(account.id, pendingUploads)) !== "held";
  };
  let session: BrowserJazzSession | undefined;

  const openLocalFirst = (createDb: CreateAccountDb, account: AccountHandle) =>
    createRegisteredJazzClient("web", { ...clientConfig, account }, createDb);

  const upgrade = async (loader: LocalFirstLoader) => {
    let upgraded: AccountHandle | undefined;
    await replaceJazzSessionClient<JazzClient>(
      session!,
      async (current, account) => {
        const { createClient } = await loader();
        const next = await openReplica(createClient, account);
        try {
          await prefetchThinQueries(current.db, next.db);
          await handOffThinWrites(current.db, next.db);
        } catch (error) {
          await next.shutdown().catch(() => {});
          throw error;
        }
        upgraded = account;
        return next;
      },
      (current, next) => upgradesInPlace.get(current)!(next),
    );
    // Only once the session took the new client: a superseded upgrade
    // (a logout or account switch meanwhile) is not remembered.
    if (upgraded) upgrades.remember(upgraded);
  };

  const openThin = async (account: AccountHandle) => {
    const thin = await createRegisteredJazzClient(
      "web-thin",
      { ...clientConfig, account },
      createThinDb,
    );
    let pending: Promise<void> | undefined;
    setLocalFirstUpgrader(thin.db, (loader) => {
      pending ??= upgrade(loader).finally(() => {
        pending = undefined;
      });
      return pending;
    });
    return upgradableClient(thin);
  };

  const openClient = async (account: AccountHandle) => {
    if (loadLocalFirst && (localFirst === "now" || upgrades.has(account))) {
      let module: LocalFirstModule;
      try {
        module = await loadLocalFirst();
      } catch (error) {
        // Asked for local-first now: running thin would quietly break what
        // the app needs it for (offline, db.transaction).
        if (localFirst === "now") throw error;
        // A chunk that no longer loads (renamed by a deploy, or the network)
        // costs the app its offline start, not the session: it runs thin,
        // and the background upgrade tries the loader again.
        console.warn("Jazz could not load the local-first client; starting thin.", error);
        return openThin(account);
      }
      return openReplica(module.createClient, account);
    }
    return openThin(account);
  };

  session = await createJazzSessionOwner({
    accounts,
    initial: coreInitial(initial),
    // A memory-only client has no storage to wipe.
    clearOnSignOut: wipesOnSignOut,
    ...(replicas ? { wipeSignedOut } : {}),
    openClient,
    decorate: followUser({
      registry,
      // Checks and the sync connection's renewals may both refresh the
      // app's session: one lock for both, so its cookie rotates once.
      events: browserBackendWatchEvents(
        `jazz-account:${registry}`,
        sessionRefreshLockName(registry),
      ),
      linkGuest: linkDeviceAccountOnSignUp,
      forgotten: (account) => accountForgotten(accounts, account.id),
      watchAccounts: (changed) => watchStoredSelection(accounts, changed),
      // Ask "who is signed in" the way sign-in asks: through the app's own
      // `fetch` when it gives one, and with its bearer when it has no cookies.
      ...(bearer
        ? { fetch: bearerFetch(bearer, config.fetch) }
        : config.fetch
          ? { fetch: config.fetch }
          : {}),
      onSignedOut,
    }),
  });
  if (loadLocalFirst) upgradeInBackground(session, loadLocalFirst);
  if (replicas) watchReplicas(session, replicas);
  await firstBackendAnswer(session);
  return session;
}

/**
 * Run `replicas.reconcile()` (wipe what signed-out users left on this
 * device) once the session has followed the backend's first answer, and
 * again whenever something may have changed: the session settles after a
 * transition, another tab changes the ledger, or the page becomes visible.
 * Nothing polls.
 */
function watchReplicas(session: BrowserJazzSession, replicas: ReplicaLifecycle) {
  let started = false;
  let lastStatus: string | undefined;
  const reconcile = () => {
    if (!started || session.getSnapshot().status === "closed") return;
    void replicas.reconcile();
  };
  const onStorage = (event: StorageEvent) => {
    if (
      event.key === null ||
      event.key === replicas.ledger.key ||
      event.key === replicas.ledger.legacyKey
    )
      reconcile();
  };
  const onVisibility = () => {
    if (globalThis.document?.visibilityState === "visible") reconcile();
  };
  globalThis.addEventListener?.("storage", onStorage);
  globalThis.document?.addEventListener?.("visibilitychange", onVisibility);
  const unsubscribe = session.subscribe(() => {
    const { status } = session.getSnapshot();
    if (status === "closed") {
      unsubscribe();
      globalThis.removeEventListener?.("storage", onStorage);
      globalThis.document?.removeEventListener?.("visibilitychange", onVisibility);
      return;
    }
    const settled = status === "ready" || status === "signed-out";
    if (settled && status !== lastStatus) reconcile();
    lastStatus = status;
  });
  void backendAnswered(session)
    .then(() => settledSession(session))
    .then(() => {
      started = true;
      lastStatus = session.getSnapshot().status;
      reconcile();
    });
}

/** Resolves once `session` is ready or signed out. */
function settledSession(session: BrowserJazzSession): Promise<void> {
  return new Promise((resolve) => {
    const check = () => {
      const { status } = session.getSnapshot();
      if (status !== "ready" && status !== "signed-out" && status !== "closed") return false;
      stop?.();
      resolve();
      return true;
    };
    let stop: (() => void) | undefined;
    if (!check()) stop = session.subscribe(() => void check());
  });
}

const upgradesInPlace = new WeakMap<JazzClient, (next: JazzClient) => void>();

/**
 * The session's client while it is thin, and after it upgrades: the upgrade
 * moves its work to the local-first client in place, so its `db`, its query
 * cache and the app's component state all carry on. The thin `Db` becomes a
 * handle on the local-first one (see `Db.adoptSuccessor`), and its own
 * runtime stops once its writes are uploaded.
 */
function upgradableClient(thin: JazzClient): JazzClient {
  let inner = thin;
  const client: JazzClient = attachSubscriptionStore(
    {
      db: thin.db,
      get session() {
        return inner.session;
      },
      async shutdown(options?: ShutdownOptions) {
        if (inner === thin) return thin.shutdown(options);
        // Both runtimes stop the same way: with `waitForSync`, after their
        // writes upload; without, straight away (a close, or a sign-out
        // that wipes this device).
        await Promise.all([inner.shutdown(options), thin.db.stopOwnRuntime(options)]);
        // Its query cache served the app until now.
        await thin.shutdown();
      },
    },
    getSubscriptionStore(thin),
  );
  upgradesInPlace.set(client, (next) => {
    // The thin Db starts uploading what it still holds and stops once done.
    thin.db.adoptSuccessor(next.db);
    inner = next;
  });
  return client;
}

/**
 * `serverUrl: null`: Jazz on this device only. There is no backend to name a
 * user or to run a thin client against, so the session opens the local-first
 * client straight away, on this browser's local-first account.
 */
async function createDeviceOnlySession(config: JazzClientConfig): Promise<BrowserJazzSession> {
  const {
    initial,
    clearOnSignOut,
    linkDeviceAccountOnSignUp: _linkDeviceAccountOnSignUp,
    localFirst: _localFirst,
    onSignedOut: _onSignedOut,
    client: loadLocalFirst,
    store: _store,
    fetch: _fetch,
    ...rest
  } = config;
  if (!loadLocalFirst) {
    throw new Error(
      'serverUrl: null keeps Jazz on this device, which needs the local-first client: pass client: () => import("jazz-tools/local-first")',
    );
  }
  if (coreInitial(initial) === "signed-out") {
    throw new Error(
      'serverUrl: null has no backend to sign in through, so it always opens this device\'s device account: leave out initial or set it to "device"',
    );
  }
  const { serverUrl: _serverUrl, ...clientConfig } = rest;
  refuseUnkeptOptions(clientConfig);
  const accounts = await createAccountManagerWithMinter(config, mintLocalFirstTokenInJs);
  const { createClient } = await loadLocalFirst();
  return createJazzSessionOwner({
    accounts,
    initial: "local-first",
    clearOnSignOut: clearOnSignOut && (config.driver?.type ?? "persistent") === "persistent",
    openClient: (account) =>
      createRegisteredJazzClient("web", { ...clientConfig, account }, createClient),
    // No backend says who is signed in: there is nothing to ask again.
    decorate: () => ({ reauthenticate: async () => {} }),
  });
}

/**
 * Upgrade each thin client the session opens, once the page has loaded and
 * the browser is idle. A failed upgrade leaves the app on the thin client.
 */
function upgradeInBackground(session: BrowserJazzSession, loader: LocalFirstLoader) {
  const attempted = new WeakSet<object>();
  const check = () => {
    const { status, client } = session.getSnapshot();
    if (status !== "ready" || !client || attempted.has(client)) return;
    attempted.add(client);
    void afterPageLoad().then(() => {
      if (session.getSnapshot().client !== client) return;
      return client.db.upgradeToLocalFirst(loader).catch((error: unknown) => {
        if (session.getSnapshot().client !== client) return;
        console.warn("Jazz could not upgrade to the local-first client; staying thin.", error);
      });
    });
  };
  const stop = session.subscribe(() => {
    if (session.getSnapshot().status === "closed") {
      stop();
      return;
    }
    check();
  });
  check();
}

function afterPageLoad(): Promise<void> {
  if (typeof window === "undefined" || typeof document === "undefined") return Promise.resolve();
  const loaded =
    document.readyState === "complete"
      ? Promise.resolve()
      : new Promise<void>((resolve) =>
          window.addEventListener("load", () => resolve(), { once: true }),
        );
  return loaded.then(
    () =>
      new Promise<void>((resolve) => {
        if (typeof window.requestIdleCallback === "function")
          window.requestIdleCallback(() => resolve(), { timeout: 2_000 });
        else setTimeout(resolve, 0);
      }),
  );
}

/**
 * Which accounts this browser has upgraded to local-first, so their next
 * visit opens local-first straight away (and works offline).
 */
function localFirstMemory(registry: string) {
  const key = `jazz-local-first-v1:${encodeURIComponent(registry)}`;
  const read = (): string[] => {
    try {
      const value = JSON.parse(globalThis.localStorage?.getItem(key) ?? "[]");
      return Array.isArray(value) ? value.filter((id) => typeof id === "string") : [];
    } catch {
      return [];
    }
  };
  return {
    has: (account: AccountHandle) => read().includes(account.id),
    remember(account: AccountHandle) {
      const ids = read();
      if (ids.includes(account.id)) return;
      try {
        globalThis.localStorage?.setItem(key, JSON.stringify([...ids, account.id]));
      } catch {
        // Without storage the next visit starts thin again and upgrades.
      }
    },
  };
}

/** `localFirst`, checked: a timing the session can't honour fails by name. */
function refuseLocalFirstTiming(localFirst: unknown, loader: LocalFirstLoader | undefined): void {
  if (localFirst === undefined || localFirst === "background") return;
  if (localFirst !== "now")
    throw new Error(
      `Unknown localFirst ${JSON.stringify(localFirst)}; expected "now" or "background"`,
    );
  if (!loader)
    throw new Error(
      'localFirst: "now" opens the local-first client, which needs the `client` loader: pass client: () => import("jazz-tools/local-first")',
    );
}
