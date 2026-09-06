import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { RuntimeSourcesConfig } from "../runtime/context.js";
import { accountRegistryUrl } from "./context.js";
import { prepareAccountManager, type AccountStore } from "./persistence.js";

export interface AccountManagerConfig {
  appId: string;
  serverUrl: string;
  env?: string;
  runtimeSources?: RuntimeSourcesConfig;
  /** Supply durable storage on native/server hosts; browser defaults to localStorage. */
  store?: AccountStore;
}

/** Prepare crypto and restore selection; createLocalFirst() is synchronous afterward. */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  const source = new DefaultRuntimeSource();
  await source.load({ appId: config.appId, runtimeSources: config.runtimeSources });
  const store = config.store ?? browserAccountStore(registry, config.env ?? "dev");
  return prepareAccountManager({
    appId: config.appId,
    registry,
    store,
    mintToken(secret, audience) {
      return source.mintLocalFirstToken({
        secret: authSecretSeedForMinting(secret),
        audience,
        ttlSeconds: 3600,
        nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
      });
    },
  });
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
