import { createAccountManager } from "../../accounts/create-account-manager.js";
import type { AccountDbConfig } from "../../accounts/context.js";

/** A real offline account and runtime, with isolated ephemeral test storage. */
export async function localAccountConfig(appId: string): Promise<AccountDbConfig> {
  const serverUrl = "http://127.0.0.1:1";
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl,
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  return { appId, serverUrl, account: accounts.createLocalFirst(), driver: { type: "memory" } };
}
