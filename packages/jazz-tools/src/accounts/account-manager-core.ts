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
  const serverUrl = config.serverUrl ?? DEFAULT_SERVER_URL;
  const scope = clientScope({ ...config, serverUrl });
  const registry = accountRegistryUrl(serverUrl, scope);
  const store = config.store ?? browserAccountStore(registry, config.env ?? "dev");
  return prepareAccountManager({ appId: scope, registry, store, mintToken });
}

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
