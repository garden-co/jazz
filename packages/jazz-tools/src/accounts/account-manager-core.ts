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
) {
  const registry = clientRegistryUrl(config);
  const scope = clientScope({ ...config, serverUrl: resolvedServerUrl(config) });
  const store = config.store ?? browserAccountStore(registry, config.env ?? "dev");
  return prepareAccountManager({
    appId: scope,
    registry,
    store,
    mintToken,
    fetch: config.serverUrl === null ? deviceOnlyFetch : config.fetch,
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
