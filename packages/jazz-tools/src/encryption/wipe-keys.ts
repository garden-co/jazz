/**
 * Wiping an account's encryption keys from this device's private store, for a
 * sign-out with `clearOnSignOut`. Loaded on demand by the session: it brings
 * the browser's store, and nothing that serves the runtime, so the thin
 * client's first load stays free of key-holding code.
 */
import { browserPrivateStore } from "./browser-store.js";
import type { PrivateStore } from "./private-store.js";
import { encryptionStoreScope } from "./store-scope.js";

/**
 * Wipe `accountId`'s keys from `store` (the app's, or by default the
 * browser's own): the device's keys, the space keys it holds and its floors.
 * The device cannot read the account's encrypted rows again until it is
 * approved anew or recovered; a runtime still open on the scope finds it
 * wiped. Wiping a scope that holds nothing does nothing.
 */
export async function wipeEncryptionKeys(options: {
  store?: PrivateStore;
  appId: string;
  environment: string;
  accountId: string;
}): Promise<void> {
  const store = options.store ?? browserPrivateStore();
  await store.delete(encryptionStoreScope(options.appId, options.environment, options.accountId));
}
