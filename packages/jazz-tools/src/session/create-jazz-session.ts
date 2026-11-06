import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import type { AccountHandle } from "../accounts/state.js";
import {
  clientRegistryUrl,
  createAccountManagerWithMinter,
} from "../accounts/account-manager-core.js";
import { attachSubscriptionStore, getSubscriptionStore } from "../subscription-store-internal.js";
import type { ShutdownOptions } from "../runtime/db.js";
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
} from "../thin/index.js";
import {
  setLocalFirstUpgrader,
  type LocalFirstLoader,
  type LocalFirstModule,
} from "../runtime/local-first-upgrade.js";
import { createJazzSessionOwner, replaceJazzSessionClient } from "./state.js";
import { DEFAULT_SERVER_URL, refuseClientAppId } from "../runtime/client-scope.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import {
  firstBackendAnswer,
  followUser,
  signedInIdentity,
  type UserFollowingSession,
} from "./follow-user.js";
import { leftBehindReplicas, wipeLeftBehindReplicas } from "./left-behind-replicas.js";
import type { RememberedBackendUser } from "../accounts/enrollment.js";
import type { AccountStore } from "../accounts/persistence.js";

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
     * server, no sign-in, one local-first account per browser profile. That
     * needs the local-first client (`client`).
     */
    serverUrl?: string | null;
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"local-first"` guest, an
     * account on this device that can later link to your backend's user
     * (`linkWithBackend`). Your backend must accept guests
     * (`acceptLocalFirstAuth`). A guest works offline once its client is the
     * local-first one (`client`).
     */
    initial?: "local-first" | "signed-out";
    /**
     * When a local-first guest is on this device and the user who signs in
     * has no Jazz account yet (they just signed up, or they are an existing
     * user signing in to Jazz for the first time), link the guest to them:
     * the account and everything the guest wrote become theirs. On by
     * default. Turn it off on shared devices, where the guest may be someone
     * else's: the session then leaves such a guest as it is, and your app
     * decides after its own sign-in, with `linkWithBackend()` (the guest's
     * data becomes the user's) or `loginWithBackend()` (an account of the
     * user's own; the guest stays on this device).
     */
    linkGuestOnSignUp?: boolean;
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
  };

/** A browser's Jazz session, which follows your auth's signed-in user. */
export type BrowserJazzSession = UserFollowingSession<JazzClient>;

/** Own one configured client and serialize its account lifecycle. */
export async function createJazzClient(config: JazzClientConfig): Promise<BrowserJazzSession> {
  refuseClientAppId(config);
  if (config.serverUrl === null) return createDeviceOnlySession(config);
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const {
    initial,
    clearOnSignOut,
    linkGuestOnSignUp,
    client: loadLocalFirst,
    // The account manager's own: neither is client configuration, and a Db
    // keeps only cloneable data (db.getConfig()).
    store: _store,
    fetch: _fetch,
    ...clientConfig
  } = { ...config, serverUrl };
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
  // Accounts whose local-first client this page opened: signing them out here wipes them.
  const openedHere = new Set<string>();
  const holdsReplica = (account: AccountHandle) => {
    if (!leftBehind) return;
    leftBehind.remember(account);
    openedHere.add(account.id);
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
        const next = await openLocalFirst(createClient, account);
        try {
          await prefetchThinQueries(current.db, next.db);
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
      holdsReplica(upgraded);
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
    if (loadLocalFirst && upgrades.has(account)) {
      let module: LocalFirstModule;
      try {
        module = await loadLocalFirst();
      } catch (error) {
        // A chunk that no longer loads (renamed by a deploy, or the network)
        // costs the app its offline start, not the session: it runs thin,
        // and the background upgrade tries the loader again.
        console.warn("Jazz could not load the local-first client; starting thin.", error);
        return openThin(account);
      }
      const client = await openLocalFirst(module.createClient, account);
      holdsReplica(account);
      return client;
    }
    return openThin(account);
  };

  session = await createJazzSessionOwner({
    accounts,
    initial,
    // A memory-only client has no storage to wipe.
    clearOnSignOut: wipesOnSignOut,
    openClient,
    decorate: followUser({
      registry,
      events: browserBackendWatchEvents(`jazz-account:${registry}`),
      linkGuest: linkGuestOnSignUp,
    }),
  });
  if (loadLocalFirst) upgradeInBackground(session, loadLocalFirst);
  if (leftBehind && loadLocalFirst) {
    const record = leftBehind;
    // Signed out in this page: the session wiped the replica as it did.
    session.subscribe(() => {
      const { status, account } = session.getSnapshot();
      if (status !== "ready" && status !== "signed-out") return;
      for (const id of openedHere) {
        if (id === account?.id) continue;
        openedHere.delete(id);
        record.forget(id);
      }
    });
    // Signed out where this page didn't see it (on the server, or another
    // user signed in through the app) before this load: wipe what is left.
    void wipeLeftBehindReplicas({
      record,
      signedIn: () => signedInIdentity(registry),
      wipe: async (user) => {
        const remembered = await createAccountManagerWithMinter(
          { ...config, serverUrl, store: rememberedUserStore(user) },
          mintLocalFirstTokenInJs,
        );
        const account = remembered.getLoggedIn();
        if (!account) throw new Error("Jazz could not reopen the signed-out user to wipe");
        const { createClient } = await loadLocalFirst();
        const client = await openLocalFirst(createClient, account);
        try {
          await client.db.deleteClientStorage();
        } finally {
          await client.shutdown();
        }
      },
    });
  }
  await firstBackendAnswer(session);
  return session;
}

/** An account store holding `user` as the remembered backend user, and nothing else. */
function rememberedUserStore(user: RememberedBackendUser): AccountStore {
  let stored: string | null = JSON.stringify({
    format: "jazz-account-selection-v1",
    roots: [],
    selected: null,
    backendUser: { format: "jazz-backend-user-v1", ...user },
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
    linkGuestOnSignUp: _linkGuestOnSignUp,
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
  if (initial !== undefined && initial !== "local-first") {
    throw new Error(
      'serverUrl: null has no backend to sign in through, so it always opens this device\'s local-first account: leave out initial or set it to "local-first"',
    );
  }
  const { serverUrl: _serverUrl, ...clientConfig } = rest;
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
