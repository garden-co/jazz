import {
  createAccountManagerWithMinter,
  resolvedServerUrl,
  type AccountManagerConfig,
} from "../../accounts/account-manager-core.js";
import { authSecretSeedForMinting } from "../auth-secret-codec.js";
import { clientScope } from "../client-scope.js";
import { DefaultRuntimeSource } from "../default-runtime-source.js";
import { localFirstAccountId } from "../../accounts/local-first.js";
import type { AccountDbConfig } from "../../accounts/context.js";

/**
 * An account manager whose local-first tokens the WASM runtime signs. Tests
 * only: apps get their accounts from the session (`createJazzClient`).
 */
export async function createTestAccountManager(config: AccountManagerConfig) {
  const scope = clientScope({ ...config, serverUrl: resolvedServerUrl(config) });
  const source = new DefaultRuntimeSource();
  await source.load({ appId: scope, runtimeSources: config.runtimeSources });
  return createAccountManagerWithMinter(config, (secret, audience) =>
    source.mintLocalFirstToken({
      secret: authSecretSeedForMinting(secret),
      audience,
      ttlSeconds: 3600,
      nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
    }),
  );
}

/** A real offline account and runtime, with isolated ephemeral test storage. */
export async function localAccountConfig(
  appId: string,
  serverUrl?: string,
  secret?: string,
): Promise<AccountDbConfig> {
  let stored: string | null = null;
  const accounts = await createTestAccountManager({
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
