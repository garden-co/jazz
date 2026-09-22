import { getTrustedReservedSession } from "../runtime/db-internal-session.js";
import { resolveClientInternalSessionSync } from "../runtime/client-session.js";
import type { DbConfig } from "../runtime/db.js";
import type { Session } from "../runtime/context.js";
import { assertAccountConfig } from "../accounts/config-capability.js";
import type { NativeForegroundFactory } from "./native-foreground-db.js";

/** The same native account admission is used by contexts and device receipts. */
export function beginNativeAccountSession(
  factory: NativeForegroundFactory,
  config: DbConfig,
  session: Session,
): Uint8Array {
  assertAccountConfig(config);
  if (!config.accountId || !config.accountRegistryAuthority) {
    throw new Error("Native account admission requires an account handle");
  }
  if (
    !factory.beginAccountSession ||
    !factory.attachAccountSchema ||
    !factory.releaseAccountSession ||
    !factory.refreshAccountSession
  ) {
    throw new Error("React Native accounts require a matching native account-admission build");
  }
  return factory.beginAccountSession(
    JSON.stringify({
      registry: config.accountRegistryAuthority,
      app_id: config.appId,
      env: config.env ?? "dev",
      account_id: config.accountId,
      issuer: session.issuer,
      subject: session.user_id,
      jwt: config.jwtToken,
      claims: session.claims,
      server_url: config.serverUrl ?? null,
    }),
  );
}

/** Schema attachment consumes the one-shot setup on success or failure. */
export function attachNativeAccountSchema(
  factory: NativeForegroundFactory,
  pending: Uint8Array,
  schemaSource: string,
): Uint8Array {
  try {
    return factory.attachAccountSchema!(pending, schemaSource);
  } finally {
    factory.releaseAccountSession!(pending);
  }
}

export function resolveNativeSession(config: DbConfig) {
  const session = resolveClientInternalSessionSync({
    ...config,
    trustedReservedSession: getTrustedReservedSession(config),
  });
  if (!session) throw new Error("React Native account session could not be resolved");
  return session;
}
