import { admitAccountConfig } from "./config-capability.js";
import type { AccountHandle } from "./state.js";
import {
  accountRegistry,
  accountToken,
  AccountAuthError,
  onAccountInvalidated,
} from "./enrollment.js";
import { accountAppId } from "./local-first.js";
import { createDbWithRuntimeSource, type Db, type DbConfig } from "../runtime/db.js";
import type { RuntimeSource } from "../runtime/runtime-source.js";
import {
  internalSessionFromVerifiedReservedJwtPayload,
  parseJwtPayload,
} from "../runtime/client-session.js";
import { setTrustedReservedSession } from "../runtime/db-internal-session.js";

/** Public clients always select an enrolled account, never an unverified principal. */
export type AccountDbConfig = Omit<
  DbConfig,
  "secret" | "jwtToken" | "cookieSession" | "adminSecret" | "accountId" | "accountRegistryAuthority"
> & {
  account: AccountHandle;
};

/** One canonical application endpoint for enrollment and context scope validation. */
export function accountRegistryUrl(serverUrl: string, appId: string): string {
  const url = new URL(serverUrl);
  if (url.protocol === "ws:") url.protocol = "http:";
  if (url.protocol === "wss:") url.protocol = "https:";
  if (url.protocol !== "http:" && url.protocol !== "https:")
    throw new AccountAuthError("invalid_registry_url");
  if (url.username || url.password || url.search || url.hash)
    throw new AccountAuthError("invalid_registry_url");
  url.pathname = `${url.pathname.replace(/\/$/, "")}/apps/${accountAppId(appId)}/accounts`;
  return url.href;
}

/** @internal Environment factories provide their native runtime source. */
export async function createAccountDbWithRuntimeSource(
  config: AccountDbConfig,
  runtimeSource: RuntimeSource<DbConfig>,
): Promise<Db> {
  for (const key of [
    "secret",
    "jwtToken",
    "cookieSession",
    "adminSecret",
    "accountId",
    "accountRegistryAuthority",
  ]) {
    if (Object.hasOwn(config, key)) throw new AccountAuthError("account_handle_required");
  }
  const { account, ...runtimeConfig } = config;
  const registry = accountRegistry(account);
  // Offline creation still has a configured registry authority; no request is made.
  if (
    !new URL(registry).pathname.endsWith(`/apps/${accountAppId(config.appId)}/accounts`) ||
    (config.serverUrl !== undefined &&
      accountRegistryUrl(config.serverUrl, config.appId) !== registry)
  ) {
    throw new AccountAuthError("account_application_mismatch");
  }
  let invalidated = false;
  let db: Db | undefined;
  const unsubscribe = onAccountInvalidated(account, () => {
    invalidated = true;
    if (db) {
      const closing = db;
      closing.abortGracefulShutdown();
      void closing
        .shutdown()
        .catch(() => closing.shutdown())
        .catch((error) => console.error("Account context shutdown failed", error));
    }
  });
  try {
    const jwtToken = await accountToken(account, registry);
    const resolved: DbConfig = {
      ...runtimeConfig,
      jwtToken,
      accountId: account.id,
      accountRegistryAuthority: registry,
    };
    if (account.identity.issuer === "urn:jazz:local-first") {
      setTrustedReservedSession(
        resolved,
        internalSessionFromVerifiedReservedJwtPayload(
          parseJwtPayload(jwtToken) ?? {},
          "local-first",
        ),
      );
    }
    admitAccountConfig(resolved, account);
    db = await createDbWithRuntimeSource(resolved, runtimeSource);
    if (invalidated) {
      await db.shutdown();
      throw new AccountAuthError("account_logged_out");
    }
    const opened = db;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let refreshing = false;
    let stopped = false;
    const schedule = (token: string) => {
      if (timer) clearTimeout(timer);
      const expires = parseJwtPayload(token)?.exp;
      if (typeof expires !== "number" || !Number.isFinite(expires) || expires * 1000 <= Date.now())
        return;
      const delay = Math.max(1000, Math.min(2_147_483_647, (expires * 1000 - Date.now()) * 0.8));
      timer = setTimeout(() => {
        void refresh();
      }, delay);
      (timer as unknown as { unref?: () => void }).unref?.();
    };
    const refresh = async () => {
      if (refreshing || stopped) return;
      refreshing = true;
      try {
        const token = await opened.refreshAccountAuth(account);
        if (!stopped) schedule(token);
      } catch {
        // Db publishes the failure to its auth state; retry on the next
        // explicit reconnect/auth failure rather than spinning on bad tokens.
      } finally {
        refreshing = false;
      }
    };
    const stopAuth = opened.onAuthChanged((state) => {
      if (state.error === "expired" || state.error === "missing") void refresh();
    });
    opened.onShutdown(() => {
      stopped = true;
      if (timer) clearTimeout(timer);
      stopAuth();
      unsubscribe();
    });
    schedule(jwtToken);
    return opened;
  } catch (error) {
    unsubscribe();
    throw error;
  }
}
