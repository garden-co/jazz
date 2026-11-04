import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import type { AccountHandle } from "../accounts/state.js";
import { createAccountManagerWithMinter } from "../accounts/account-manager-core.js";
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
import { clientScope, DEFAULT_SERVER_URL } from "../runtime/client-scope.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { browserBackendWatchEvents } from "./backend-watch.js";
import { followUser, type UserFollowingSession } from "./follow-user.js";

export type { LocalFirstLoader };

export type JazzClientConfig = Omit<OpenJazzClientConfig, "account"> &
  AccountManagerConfig & {
    /**
     * Who the app starts as before your backend names a user: signed out
     * (the default, also `"signed-out"`), or a `"local-first"` guest, an
     * account on this device that works offline and can later link to your
     * backend's user (`linkWithBackend`).
     */
    initial?: "local-first" | "signed-out";
    /**
     * On sign-out (or a switch to another user), wipe the account's local
     * storage instead of keeping it for that user's next sign-in. For shared
     * devices. Jazz first waits up to 5 seconds for pending writes to upload;
     * writes still unsynced then are lost.
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
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const {
    initial,
    clearOnSignOut,
    client: loadLocalFirst,
    store: _store,
    ...clientConfig
  } = { ...config, serverUrl };
  const accounts = await createAccountManagerWithMinter(
    { ...config, serverUrl },
    mintLocalFirstTokenInJs,
  );
  const registry = accountRegistryUrl(serverUrl, clientScope({ ...config, serverUrl }));
  const upgrades = localFirstMemory(registry);
  let session: BrowserJazzSession | undefined;

  const openLocalFirst = (createDb: CreateAccountDb, account: AccountHandle) =>
    createRegisteredJazzClient("web", { ...clientConfig, account }, createDb);

  const upgrade = async (loader: LocalFirstLoader) => {
    let upgraded: AccountHandle | undefined;
    await replaceJazzSessionClient<JazzClient>(session!, async (current, account) => {
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
    });
    // Only once the session took the new client: a superseded upgrade
    // (a logout or account switch meanwhile) is not remembered.
    if (upgraded) upgrades.remember(upgraded);
  };

  const openThin = async (account: AccountHandle) => {
    const client = await createRegisteredJazzClient(
      "web-thin",
      { ...clientConfig, account },
      createThinDb,
    );
    let pending: Promise<void> | undefined;
    setLocalFirstUpgrader(client.db, (loader) => {
      pending ??= upgrade(loader).finally(() => {
        pending = undefined;
      });
      return pending;
    });
    return client;
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
      return openLocalFirst(module.createClient, account);
    }
    return openThin(account);
  };

  session = await createJazzSessionOwner({
    accounts,
    initial,
    // A memory-only client has no storage to wipe.
    clearOnSignOut: clearOnSignOut && (config.driver?.type ?? "persistent") === "persistent",
    openClient,
    decorate: followUser({
      registry,
      events: browserBackendWatchEvents(`jazz-account:${registry}`),
    }),
  });
  if (loadLocalFirst) upgradeInBackground(session, loadLocalFirst);
  return session;
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
