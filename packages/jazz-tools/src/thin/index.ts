/**
 * `jazz-tools/thin`: a browser client with no local engine (SPEC 8.14).
 *
 * The server evaluates every query under the signed-in session and sends
 * finished results; writes run on the server. The `Db` it returns has the
 * same query and write API as `jazz-tools`' local-first `createDb`, without
 * WASM, workers or local storage.
 *
 * Not supported yet: offline use, transactions, branch and snapshot views,
 * large-value streaming. Upgrade to the local-first client for those.
 */
import type { AccountManagerConfig } from "../accounts/create-account-manager.js";
import {
  accountRegistryUrl,
  createAccountDbWithRuntimeSource,
  type AccountDbConfig,
} from "../accounts/context.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { Db } from "../runtime/db.js";
import { LOCAL_FIRST_ISSUER, mintSelfSignedToken } from "./self-signed-token.js";
import { ThinRuntimeSource } from "./thin-runtime-source.js";
import type { ThinRuntimeOptions } from "./thin-runtime.js";

export { ThinRuntimeSource } from "./thin-runtime-source.js";
export {
  ThinConnectionLostError,
  ThinRemoteError,
  type ThinRuntimeOptions,
} from "./thin-runtime.js";
export type {
  AccountHandle,
  AccountIdentity,
  AccountManager,
  AccountSnapshot,
} from "../accounts/state.js";
export { AccountAuthError, type JWTAuth } from "../accounts/enrollment.js";
export type { AccountStore } from "../accounts/persistence.js";
export type { Db } from "../runtime/db.js";

export type ThinDbConfig = AccountDbConfig & { thin?: ThinRuntimeOptions };

/** Open a thin `Db` for a signed-in account. `serverUrl` is required. */
export async function createDb(config: ThinDbConfig): Promise<Db> {
  const { thin, ...dbConfig } = config;
  return await createAccountDbWithRuntimeSource(
    { ...dbConfig, driver: { type: "memory" } },
    new ThinRuntimeSource(thin),
  );
}

/**
 * The account manager of `jazz-tools`, signing local-first identities in
 * JavaScript instead of WASM.
 */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  const store = config.store ?? browserAccountStore(registry, config.env ?? "dev");
  return prepareAccountManager({
    appId: config.appId,
    registry,
    store,
    // A thin client has no local data to reopen before the provider answers.
    retainAccountAssignment: false,
    mintToken(secret, audience) {
      return mintSelfSignedToken(LOCAL_FIRST_ISSUER, {
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
  const key = `jazz-account-selection-v1:${encodeURIComponent(registry)}:${encodeURIComponent(env)}`;
  return {
    async read() {
      return localStorage.getItem(key);
    },
    async update(transform) {
      const write = () => localStorage.setItem(key, transform(localStorage.getItem(key)));
      if (typeof navigator !== "undefined" && navigator.locks)
        await navigator.locks.request(key, write);
      else write();
    },
  };
}
