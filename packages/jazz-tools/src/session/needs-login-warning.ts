import type { AccountHandle } from "../accounts/state.js";
import { observeLocalWrites } from "../runtime/local-write-observer.js";

/**
 * How long a write may wait for the app to start logging in before the
 * development warning fires. It covers the usual startup order, where a child
 * component writes before the effect that awaits the provider session and
 * calls `loginJWT` has run.
 */
export const NEEDS_LOGIN_WARNING_DELAY_MS = 5_000;

/** Bundlers inline `process.env.NODE_ENV`; a bare browser without `process` counts as production. */
export function isDevelopmentBuild(): boolean {
  try {
    return process.env.NODE_ENV !== "production";
  } catch {
    return false;
  }
}

export function needsLoginWarningMessage(account: AccountHandle): string {
  return (
    `[jazz] The app wrote to account ${account.id}, which was reopened after a reload and still ` +
    `has needsLogin: true. ` +
    `These writes stay on this device and will not sync until the app logs in again: ` +
    `call sessionActions.loginJWT(...) or sessionActions.loginOrRegisterJWT(...) for this user ` +
    `when needsLogin is true (from useJazzAuth() or the session snapshot). Do not treat a matching ` +
    `account identity as signed in. Apps using managed auth (JazzProvider auth=...) do this ` +
    `automatically. This warning appears only in development builds.`
  );
}

interface Observed {
  readonly account?: AccountHandle;
  readonly client?: unknown;
  readonly needsLogin?: boolean;
}

/**
 * @internal Development-only diagnostic for apps that drive the session
 * themselves. It watches the open client while its account needs a login and,
 * on a write, waits {@link NEEDS_LOGIN_WARNING_DELAY_MS}: if by then no account
 * operation is running and the account still needs a login, it warns once for
 * that account. Managed auth never triggers it.
 */
export function watchNeedsLogin(options: {
  current(): Observed;
  /** True when an auth provider owns the session's logins. */
  isManaged(): boolean;
  /** True while an account operation (such as a login) is in flight. */
  isBusy(): boolean;
}): { update(): void; stop(): void } {
  if (!isDevelopmentBuild()) return { update() {}, stop() {} };
  const warned = new WeakSet<AccountHandle>();
  let watched: { account: AccountHandle; db: object; stop(): void } | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const unwatch = () => {
    watched?.stop();
    watched = undefined;
    if (timer) clearTimeout(timer);
    timer = undefined;
  };
  const stillNeedsLogin = (account: AccountHandle) => {
    const now = options.current();
    return now.needsLogin === true && now.account === account && !options.isManaged();
  };
  const check = () => {
    timer = undefined;
    const account = watched?.account;
    if (!account || !stillNeedsLogin(account) || options.isBusy()) return;
    warned.add(account);
    unwatch();
    console.warn(needsLoginWarningMessage(account));
  };
  const update = () => {
    const now = options.current();
    const db = (now.client as { db?: unknown } | undefined)?.db;
    const target =
      now.needsLogin && now.account && !warned.has(now.account) && db && typeof db === "object"
        ? { account: now.account, db }
        : undefined;
    if (watched && target && watched.account === target.account && watched.db === target.db) return;
    unwatch();
    if (!target) return;
    const { account, db: observed } = target;
    watched = {
      account,
      db: observed,
      stop: observeLocalWrites(observed, () => {
        if (timer || options.isManaged()) return;
        timer = setTimeout(check, NEEDS_LOGIN_WARNING_DELAY_MS);
        (timer as unknown as { unref?: () => void }).unref?.();
      }),
    };
  };
  return { update, stop: unwatch };
}
