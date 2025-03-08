import { KvStoreContext } from "../storage/kv-store-context.js";

export * from "./DemoAuthUI.js";

export function clearUserCredentials() {
  const kvStore = KvStoreContext.getInstance().getStorage();

  // TODO: Migrate the Auth methods to use the same storage key/interface
  return Promise.all([
    kvStore.delete("demo-auth-logged-in-secret"),
    kvStore.delete("jazz-clerk-auth"),
    kvStore.delete("jazz-logged-in-secret"),
  ]);
}
