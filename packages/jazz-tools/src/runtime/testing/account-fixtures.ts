import { createAccountManager } from "../../accounts/create-account-manager.js";
import { localFirstAccountId } from "../../accounts/local-first.js";
import type { AccountDbConfig } from "../../accounts/context.js";

/** A real offline account and runtime, with isolated ephemeral test storage. */
export async function localAccountConfig(
  appId: string,
  serverUrl?: string,
  secret?: string,
): Promise<AccountDbConfig> {
  let stored: string | null = null;
  const accounts = await createAccountManager({
    appId,
    serverUrl: serverUrl ?? "http://127.0.0.1:1",
    store: {
      async read() {
        return stored;
      },
      async update(transform) {
        stored = transform(stored);
      },
    },
  });
  return {
    appId,
    ...(serverUrl ? { serverUrl } : {}),
    account: secret ? accounts.restoreLocalFirst(secret) : accounts.createLocalFirst(),
    driver: { type: "memory" },
  };
}

/** A deterministic, non-system account for native runtime author fixtures. */
export function testAccountId(seed: string): string {
  return localFirstAccountId("jazz-runtime-test-account-fixtures", seed);
}

/** Canonical admitted author bytes for native runtime write fixtures. */
export function testAuthorBytes(seed: string): Uint8Array {
  return new TextEncoder().encode(JSON.stringify([testAccountId(seed), "urn:jazz:test", seed]));
}
