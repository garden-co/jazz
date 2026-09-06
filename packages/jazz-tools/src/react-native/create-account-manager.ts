import { installTrustedReservedSessionEntropy } from "../runtime/client-session.js";
import { accountRegistryUrl } from "../accounts/context.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import { formatAuthSecret, parseAuthSecret } from "../runtime/auth-secret-codec.js";

export interface AccountManagerConfig {
  appId: string;
  serverUrl: string;
  /** Atomic OS-protected storage scoped to this application, registry, and profile. */
  store: AccountStore;
}

/** Prepare native crypto and retained selection without opening a Jazz context. */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
  const native = await import("jazz-rn/relay");
  const crypto = native.installNativeForegroundRuntime();
  const generateSecret = crypto.accountSecret;
  const mintToken = crypto.mintLocalFirstToken;
  if (typeof generateSecret !== "function" || typeof mintToken !== "function") {
    throw new Error(
      "React Native accounts require a matching native build with account crypto support",
    );
  }
  if (!config.store)
    throw new Error("React Native accounts require an atomic OS-protected AccountStore");
  installTrustedReservedSessionEntropy(generateSecret);
  return prepareAccountManager({
    appId: config.appId,
    registry,
    store: config.store,
    generateSecret: () => formatAuthSecret(generateSecret()),
    mintToken: (secret, audience) =>
      mintToken(parseAuthSecret(secret), audience, 3600, Math.floor(Date.now() / 1000)),
  });
}
