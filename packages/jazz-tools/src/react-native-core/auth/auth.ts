import { KvStoreContext } from "../storage/kv-store-context.js";

export * from "./DemoAuthUI.js";
export * from "./PasskeyAuth.js";
export * from "./usePasskeyAuth.js";
export * from "./PasskeyAuthBasicUI.js";

export async function clearUserCredentials() {
  const kvStore = KvStoreContext.getInstance().getStorage();

  // Read credentials to find the account ID used as the session storage key
  const credentialKeys = [
    "jazz-logged-in-secret",
    "jazz-clerk-auth",
    "demo-auth-logged-in-secret",
  ];

  const deletions: Promise<void>[] = [];

  for (const key of credentialKeys) {
    const value = await kvStore.get(key);
    if (value) {
      try {
        const parsed = JSON.parse(value);
        if (parsed.accountID) {
          deletions.push(kvStore.delete(parsed.accountID));
        }
      } catch {
        // Ignore parse errors
      }
    }
    deletions.push(kvStore.delete(key));
  }

  await Promise.all(deletions);
}
