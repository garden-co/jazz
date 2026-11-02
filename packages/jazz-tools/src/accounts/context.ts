import { PlatformURL } from "../runtime/platform-url.js";
import { resolveServerUrl } from "../runtime/url.js";
import { admitAccountConfig } from "./config-capability.js";
import type { AccountHandle } from "./state.js";
import {
  accountRegistry,
  accountToken,
  delegatedAccountClaims,
  AccountAuthError,
  isProvisionalAccount,
  onAccountCredentialBound,
  onAccountInvalidated,
} from "./enrollment.js";
import { accountAppId } from "./local-first.js";
import {
  createDbWithRuntimeSource,
  holdCredentialRenewal,
  releaseUnusedLease,
  startBrowserWorkerLease,
  type Db,
  type DbConfig,
} from "../runtime/db.js";
import type { RuntimeSource } from "../runtime/runtime-source.js";
import {
  internalSessionFromVerifiedReservedJwtPayload,
  parseJwtPayload,
  retainedAccountSession,
} from "../runtime/client-session.js";
import { setTrustedReservedSession } from "../runtime/db-internal-session.js";
import { AuthRenewalBackoff, authRetryDelay } from "../runtime/auth-renewal-backoff.js";
import { isRenewableAuthFailure } from "../runtime/auth-state.js";

/** Public clients always select an enrolled account, never an unverified principal. */
export type AccountDbConfig = Omit<
  DbConfig,
  "secret" | "jwtToken" | "cookieSession" | "adminSecret" | "accountId" | "accountRegistryAuthority"
> & {
  account: AccountHandle;
};

/** One canonical application endpoint for enrollment and context scope validation. */
export function accountRegistryUrl(serverUrl: string, appId: string): string {
  const url = new PlatformURL(resolveServerUrl(serverUrl));
  if (url.protocol === "ws:") url.protocol = "http:";
  if (url.protocol === "wss:") url.protocol = "https:";
  if (url.protocol !== "http:" && url.protocol !== "https:")
    throw new AccountAuthError("invalid_registry_url");
  if (url.username || url.password || url.search || url.hash)
    throw new AccountAuthError("invalid_registry_url");
  url.pathname = `${url.pathname.replace(/\/$/, "")}/apps/${accountAppId(appId)}/accounts`;
  return url.href;
}

function accountContextScope(config: AccountDbConfig): string {
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
  const { account } = config;
  const registry = accountRegistry(account);
  // Offline creation still has a configured registry authority; no request is made.
  if (
    !new PlatformURL(registry).pathname.endsWith(`/apps/${accountAppId(config.appId)}/accounts`) ||
    (config.serverUrl !== undefined &&
      accountRegistryUrl(config.serverUrl, config.appId) !== registry)
  ) {
    throw new AccountAuthError("account_application_mismatch");
  }
  return registry;
}

/** @internal Resolve a handle for a host adapter; never accept copied credentials. */
export async function resolveAccountRuntimeConfig(config: AccountDbConfig): Promise<DbConfig> {
  const registry = accountContextScope(config);
  const { account, ...runtimeConfig } = config;
  if (isProvisionalAccount(account)) {
    // Open the retained account's local data now. With no credential this
    // context does not connect upstream, and its writes stay local until the
    // first provider JWT replaces this session and starts its connection. In a
    // shared browser worker another tab's credential for the same account may
    // sync them sooner. Local policies see no provider claims until then.
    const retained: DbConfig = {
      ...runtimeConfig,
      accountId: account.id,
      accountRegistryAuthority: registry,
    };
    setTrustedReservedSession(retained, retainedAccountSession(account.identity));
    admitAccountConfig(retained, account);
    return retained;
  }
  const delegatedClaims = delegatedAccountClaims(account, registry);
  if (delegatedClaims) {
    // Signed in through the app's backend: no credential leaves this client.
    // The backend states the identity on every connection; this mirrored
    // session only shapes local permission checks.
    const delegated: DbConfig = {
      ...runtimeConfig,
      cookieSession: {
        account_id: account.id,
        issuer: account.identity.issuer,
        user_id: account.identity.subject,
        claims: { ...delegatedClaims },
        authMode: "external",
      },
      accountId: account.id,
      accountRegistryAuthority: registry,
    };
    admitAccountConfig(delegated, account);
    return delegated;
  }
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
      internalSessionFromVerifiedReservedJwtPayload(parseJwtPayload(jwtToken) ?? {}, "local-first"),
    );
  }
  admitAccountConfig(resolved, account);
  return resolved;
}

/** @internal Environment factories provide their native runtime source. */
export async function createAccountDbWithRuntimeSource(
  config: AccountDbConfig,
  runtimeSource: RuntimeSource<DbConfig>,
): Promise<Db> {
  accountContextScope(config);
  const { account } = config;
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
  let stopBound: (() => void) | undefined;
  let lease: ReturnType<typeof startBrowserWorkerLease>;
  try {
    // The browser root is named by the account alone: boot its worker while
    // the account's credential resolves.
    const { account: _account, ...scope } = config;
    lease = startBrowserWorkerLease(
      { ...scope, accountId: account.id, accountRegistryAuthority: accountRegistry(account) },
      runtimeSource,
    );
    const resolved = await resolveAccountRuntimeConfig(config);
    const jwtToken = resolved.jwtToken;
    // From here the runtime adopts the lease or returns it itself.
    const opening = lease;
    lease = undefined;
    db = await createDbWithRuntimeSource(resolved, runtimeSource, opening);
    if (invalidated) {
      await db.shutdown();
      throw new AccountAuthError("account_logged_out");
    }
    const opened = db;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let refreshing = false;
    let stopped = false;
    let failedRefreshes = 0;
    const renewals = new AuthRenewalBackoff();
    const scheduleIn = (delay: number, beforeRefresh?: () => void) => {
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        beforeRefresh?.();
        void refresh();
      }, delay);
      (timer as unknown as { unref?: () => void }).unref?.();
    };
    const schedule = (token: string) => {
      const expires = parseJwtPayload(token)?.exp;
      if (typeof expires !== "number" || !Number.isFinite(expires)) return;
      // Already expired, as when the app slept past its renewal timer (mobile
      // platforms suspend timers in the background): renew now rather than
      // leave nothing to renew it (#4248).
      if (expires * 1000 <= Date.now()) {
        renew();
        return;
      }
      // A token that lives to its scheduled refresh ends any renewal streak.
      scheduleIn(Math.max(1000, Math.min(2_147_483_647, (expires * 1000 - Date.now()) * 0.8)), () =>
        renewals.reset(),
      );
    };
    // Always through the timer, never inline: `schedule()` runs inside a
    // refresh, which an inline refresh would find still running and skip.
    const renew = () => scheduleIn(renewals.next());
    const refresh = async () => {
      // A retained account refreshes when revalidation binds its credential.
      if (refreshing || stopped || isProvisionalAccount(account)) return;
      refreshing = true;
      try {
        const token = await opened.refreshAccountAuth(account);
        failedRefreshes = 0;
        if (!stopped) schedule(token);
      } catch (error) {
        if (stopped) return;
        // A failed refresh must not end the refresh cycle: the current token
        // still expires, and nothing else would renew it before then.
        console.error("Account auth refresh failed", error);
        scheduleIn(authRetryDelay(failedRefreshes++));
      } finally {
        refreshing = false;
      }
    };
    // The refresh cycle below renews every refusal a new token can fix.
    const releaseRenewal = holdCredentialRenewal(opened);
    const stopAuth = opened.onAuthChanged((state) => {
      // A refused credential renews whatever the server called it: a
      // rotated signing key reads as "invalid" until a new token is issued.
      // A new token cannot fix an auth mode the app has turned off or a
      // revoked account identity.
      if (state.error && isRenewableAuthFailure(state.error)) renew();
    });
    stopBound = onAccountCredentialBound(account, () => {
      renewals.reset();
      failedRefreshes = 0;
      void refresh();
    });
    opened.onShutdown(() => {
      stopped = true;
      if (timer) clearTimeout(timer);
      stopAuth();
      releaseRenewal();
      stopBound?.();
      unsubscribe();
    });
    if (jwtToken) schedule(jwtToken);
    return opened;
  } catch (error) {
    stopBound?.();
    // Return a lease the runtime never received before tearing it down.
    await releaseUnusedLease(lease);
    unsubscribe();
    if (!db) await runtimeSource.shutdown();
    throw error;
  }
}
