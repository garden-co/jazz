import { accountRegistryUrl } from "./context.js";
import { clientScope, DEFAULT_SERVER_URL } from "../runtime/client-scope.js";
import { prepareAccountManager, type AccountStore } from "./persistence.js";
import type { AccountManagerConfig } from "./create-account-manager.js";

/** Signs a local-first account's token for `audience` (see `prepareAccountManager`). */
export type LocalFirstTokenMinter = (secret: string, audience: string) => string;

/**
 * @internal The browser account manager, with token signing supplied by the
 * caller: the WASM runtime for `jazz-tools`, plain JS for the thin client.
 */
export async function createAccountManagerWithMinter(
  config: AccountManagerConfig,
  mintToken: LocalFirstTokenMinter,
  options: { rememberBackendUser?: boolean } = {},
) {
  const registry = clientRegistryUrl(config);
  const scope = clientScope({ ...config, serverUrl: resolvedServerUrl(config) });
  const store = config.store ?? browserAccountStore(registry, config.env ?? "dev");
  return prepareAccountManager({
    appId: scope,
    registry,
    store,
    mintToken,
    fetch:
      config.serverUrl === null ? deviceOnlyFetch : sendsCookies() ? config.fetch : cookielessFetch,
    rememberBackendUser: options.rememberBackendUser,
  });
}

/**
 * Where a device-only client (`serverUrl: null`) says its accounts are
 * registered. Nothing is ever sent there: such a client only creates and
 * opens local-first accounts, which need no registry request.
 */
const DEVICE_ONLY_ENDPOINT = "https://device-only.invalid/jazz";

/** `serverUrl` as a browser client resolves it: `"/jazz"` when left out, `null` for device-only. */
export function resolvedServerUrl(config: { serverUrl?: string | null }): string | null {
  return config.serverUrl === null ? null : (config.serverUrl ?? DEFAULT_SERVER_URL);
}

/** @internal The account registry a browser client's accounts belong to. */
export function clientRegistryUrl(config: {
  appId?: string;
  scope?: string;
  serverUrl?: string | null;
}): string {
  const serverUrl = resolvedServerUrl(config);
  return accountRegistryUrl(
    serverUrl ?? DEVICE_ONLY_ENDPOINT,
    clientScope({ ...config, serverUrl }),
  );
}

/** @internal What sign-in requests of a device-only client get: a clear refusal. */
export const deviceOnlyFetch: typeof fetch = () =>
  Promise.reject(
    new Error(
      "Jazz runs on this device only (serverUrl: null), so it can't sign in through a backend. Set serverUrl to use loginWithBackend or linkWithBackend.",
    ),
  );

/**
 * Whether this host sends the app's cookies on Jazz's sync connection, as a
 * browser page or worker does. A user the backend signs in holds no Jazz
 * credential of their own: the backend's cookie is what names them on every
 * connection, so on a host without one the session would report signed in
 * and never sync.
 */
function sendsCookies(): boolean {
  return (
    typeof document !== "undefined" ||
    typeof (globalThis as { WorkerGlobalScope?: unknown }).WorkerGlobalScope !== "undefined"
  );
}

/**
 * @internal What sign-in requests of a host without cookies get (Node, Deno,
 * Bun, a CLI): a clear refusal, naming the hosts that sign in with a bearer.
 */
export const cookielessFetch: typeof fetch = () =>
  Promise.reject(
    new Error(
      "loginWithBackend and linkWithBackend need a browser: the app's backend names the signed-in user by the cookie a browser sends on Jazz's sync connection, and this host sends none. With a bearer token instead: on React Native, pass bearer to createAccountManager; on Node, sync through the app's backend as a backend client (createJazzClient from jazz-tools/backend, with credential). Other hosts are not supported yet.",
    ),
  );

function browserAccountStore(registry: string, env: string): AccountStore {
  if (typeof localStorage === "undefined") {
    throw new Error("createAccountManager requires an AccountStore outside the browser");
  }
  if (typeof navigator === "undefined" || !navigator.locks) {
    throw new Error("The browser account store requires Web Locks; supply an atomic AccountStore");
  }
  const key = `jazz-account-selection-v1:${encodeURIComponent(registry)}:${encodeURIComponent(env)}`;
  return {
    async read() {
      return localStorage.getItem(key);
    },
    async update(transform) {
      await navigator.locks.request(key, () => {
        localStorage.setItem(key, transform(localStorage.getItem(key)));
      });
    },
  };
}
