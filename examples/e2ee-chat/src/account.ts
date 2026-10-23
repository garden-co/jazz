import {
  createAccountManager,
  accountRegistryUrl,
  type AccountManager,
  type JWTAuth,
  type DbConfig,
} from "jazz-tools";
import { app } from "../schema.js";
import { createKeyStore } from "./account-store.js";

// The account manager's public browser adapter persists account selection with Web Locks.
const managers = new Map<string, Promise<AccountManager<JWTAuth>>>();

export async function prepareAccountConfig(): Promise<DbConfig> {
  const appId = import.meta.env.VITE_JAZZ_APP_ID;
  const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
  const env = import.meta.env.VITE_JAZZ_ENV ?? "dev";
  if (!appId || !serverUrl)
    throw new Error("Start with pnpm dev or configure VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL");
  const registry = accountRegistryUrl(serverUrl, appId);
  const scope = JSON.stringify([registry, env]);
  let pending = managers.get(scope);
  if (!pending) {
    pending = createAccountManager({ appId, serverUrl, env });
    managers.set(scope, pending);
    void pending.catch(() => {
      if (managers.get(scope) === pending) managers.delete(scope);
    });
  }
  const manager = await pending;
  const account = manager.getLoggedIn() ?? manager.createLocalFirst();
  return {
    appId,
    serverUrl,
    env,
    account,
    e2ee: { app, store: createKeyStore({ registry, env, accountId: account.id }) },
  };
}
