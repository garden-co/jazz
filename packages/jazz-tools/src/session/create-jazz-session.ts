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
import {
  coreInitial,
  createStartedJazzSession,
  refuseRemovedSessionOptions,
  type JazzSession,
} from "./jazz-session.js";
import { DEFAULT_SERVER_URL, refuseClientAppId } from "../runtime/client-scope.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import {
  backendAnswered,
  firstBackendAnswer,
  followUser,
  signedInIdentity,
  type UserFollowingSession,
} from "./follow-user.js";
import {
  holdReplica,
  leftBehindReplicas,
  whileNoTabHolds,
  wipeLeftBehindReplicas,
  type ReplicaOwner,
} from "./left-behind-replicas.js";
import { accountForgotten, type AccountStore } from "../accounts/persistence.js";
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
  refuseClientAppId(config);
  refuseRemovedSessionOptions(config);
  if (config.serverUrl === null) return createDeviceOnlySession(config);
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const {
    initial,
    clearOnSignOut,
    linkDeviceAccountOnSignUp,
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
  // Local-first replicas a sign-out may leave behind (see `leftBehindReplicas`).
  const leftBehind = wipesOnSignOut && loadLocalFirst ? leftBehindReplicas(registry) : undefined;
  // Accounts whose local-first client is (or was) the session's own in this
  // page: signing them out here wiped their replica.
  const inSession = new Set<string>();
  // Accounts this page is opening a local-first client for, and replicas
  // being wiped: an account's open waits for its wipe to finish.
  const opening = new Set<string>();
  const wiping = new Map<string, Promise<void>>();
  // Accounts signed out in this page, whose entry goes once no tab holds
  // their replica (another tab may still have it open).
  const signedOutHere = new Set<string>();
  const forgetSignedOut = (id: string) => {
    if (!leftBehind || !signedOutHere.has(id)) return;
    void whileNoTabHolds(registry, id, async () => {
      if (!signedOutHere.delete(id) || opening.has(id) || inSession.has(id)) return;
      leftBehind.forget(id);
    }).catch(() => {});
  };
  /** Open `account`'s local-first client, recorded and held open against other tabs' wipes. */
  const openReplica = async (createDb: CreateAccountDb, account: AccountHandle) => {
    // Only a user the backend signed in: a guest's replica is the device's own.
    if (!leftBehind || !rememberBackendUser(account)) return openLocalFirst(createDb, account);
    opening.add(account.id);
    signedOutHere.delete(account.id);
    try {
      await wiping.get(account.id)?.catch(() => {});
      leftBehind.remember(account);
      const release = await holdReplica(registry, account.id);
      // Again once held: another tab's forget may have run before it was.
      leftBehind.remember(account);
      let client: JazzClient;
      try {
        client = await openLocalFirst(createDb, account);
      } catch (error) {
        release();
        throw error;
      }
      const shutdown = client.shutdown.bind(client);
      client.shutdown = async (options) => {
        try {
          await shutdown(options);
        } finally {
          release();
          forgetSignedOut(account.id);
        }
      };
      return client;
    } finally {
      opening.delete(account.id);
    }
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
    if (upgraded) {
      upgrades.remember(upgraded);
      inSession.add(upgraded.id);
    }
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
      const client = await openReplica(module.createClient, account);
      inSession.add(account.id);
      return client;
    }
    return openThin(account);
  };

  session = await createJazzSessionOwner({
    accounts,
    initial: coreInitial(initial),
    // A memory-only client has no storage to wipe.
    clearOnSignOut: wipesOnSignOut,
    openClient,
    decorate: followUser({
      registry,
      events: browserBackendWatchEvents(`jazz-account:${registry}`),
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
    }),
  });
  if (loadLocalFirst) upgradeInBackground(session, loadLocalFirst);
  if (leftBehind && loadLocalFirst) {
    const record = leftBehind;
    const following = session;
    // Signed out in this page: the session wiped the replica as it did.
    following.subscribe(() => {
      const { status, account } = following.getSnapshot();
      if (status !== "ready" && status !== "signed-out") return;
      for (const id of inSession) {
        if (id === account?.id) continue;
        inSession.delete(id);
        signedOutHere.add(id);
        forgetSignedOut(id);
      }
    });
    const stillLeftBehind = (account: string) =>
      following.getSnapshot().status !== "closed" &&
      !opening.has(account) &&
      following.getSnapshot().account?.id !== account;
    const wipe = async (owner: ReplicaOwner, signal: AbortSignal) => {
      const remembered = await createAccountManagerWithMinter(
        { ...config, serverUrl, store: rememberedUserStore(owner) },
        mintLocalFirstTokenInJs,
      );
      const account = remembered.getLoggedIn();
      if (!account) throw new Error("Jazz could not reopen the signed-out user to wipe");
      const { createClient } = await loadLocalFirst();
      return whileNoTabHolds(registry, owner.account, async () => {
        // Checked and claimed in one step: an open of this account from now
        // on waits for the wipe.
        if (!stillLeftBehind(owner.account)) throw new Error("signed in again");
        let deleting = false;
        const run = (async () => {
          // Its socket carries another user's cookie (or none), so the
          // backend refuses it: nothing of this replica uploads.
          const client = await openLocalFirst(createClient, account);
          try {
            // Given up on while it opened: leave the replica for a later load.
            if (signal.aborted) return;
            deleting = true;
            await client.db.deleteClientStorage();
          } finally {
            await client.shutdown();
          }
          record.forget(owner.account);
        })();
        wiping.set(owner.account, run);
        // Given up on before its delete began: let go of the lock and the
        // account at once, so an open of it doesn't wait on an open that
        // hangs. A delete that began is waited for, however long it takes:
        // an open of the account meanwhile would get half-wiped storage.
        const givenUp = new Promise<void>((resolve) => {
          const giveUp = () => {
            if (!deleting) resolve();
          };
          if (signal.aborted) giveUp();
          signal.addEventListener("abort", giveUp, { once: true });
        });
        try {
          await Promise.race([run, givenUp]);
        } finally {
          wiping.delete(owner.account);
        }
      }).catch((error: unknown) => {
        if (error instanceof Error && error.message === "signed in again") return false;
        throw error;
      });
    };
    // Signed out where an earlier page didn't see it (on the server, or
    // another user signed in through the app): wipe what it left, once the
    // session has followed the backend.
    void backendAnswered(following)
      .then(() => settledSession(following))
      .then(() =>
        wipeLeftBehindReplicas({
          record,
          current: () => following.getSnapshot().account?.id,
          signedIn: () => {
            const identity = following.getSnapshot().account?.identity;
            return identity !== undefined && identity.issuer !== LOCAL_FIRST_ISSUER;
          },
          confirmSignedOut: async () => {
            await new Promise((resolve) => setTimeout(resolve, CONFIRM_SIGNED_OUT_MS));
            return (await signedInIdentity(registry, config.fetch)) === null;
          },
          stillLeftBehind,
          wipe,
        }),
      );
  }
  await firstBackendAnswer(session);
  return session;
}

const LOCAL_FIRST_ISSUER = "urn:jazz:local-first";

/** As the backend watch confirms a signed-out answer before it signs anyone out. */
const CONFIRM_SIGNED_OUT_MS = 2_000;

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

/** An account store holding `owner` as the remembered backend user, and nothing else. */
function rememberedUserStore(owner: ReplicaOwner): AccountStore {
  let stored: string | null = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [],
    selected: null,
    backendUser: { format: "jazz-backend-user-v1", ...owner, claims: {} },
  });
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
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
