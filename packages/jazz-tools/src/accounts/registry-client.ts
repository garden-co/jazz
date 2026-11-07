import { fetchFor } from "../runtime/in-process-transport.js";
import { withAuthRequestDeadline } from "../runtime/auth-request-deadline.js";
import type { AccountIdentity } from "./state.js";

export class AccountAuthError extends Error {
  constructor(
    readonly code: string,
    /** The registry's HTTP status when it answered with an error. */
    readonly status?: number,
  ) {
    super(code);
    this.name = "AccountAuthError";
  }
}

/**
 * The app's backend vouches to the core's registry for an identity it
 * verified itself, with its backend secret: resolve its account
 * (`resolve`), create it on first sign-in (`login-or-register`), or found a
 * local-first guest's (`found-local-first`). `registry` is the app's
 * `/accounts` URL; these routes live under `/admin/accounts`.
 */
export async function requestVouchedAccount(
  registry: string,
  operation: "resolve" | "login-or-register" | "found-local-first",
  identity: AccountIdentity,
  backendSecret: string,
  fetcher: typeof fetch = fetchFor(registry),
): Promise<unknown> {
  const admin = registry.replace(/\/accounts\/?$/, "/admin/accounts");
  return withAuthRequestDeadline(async (signal) => {
    const response = await fetcher(`${admin}/${operation}`, {
      signal,
      method: "POST",
      credentials: "omit",
      redirect: "error",
      headers: { "X-Jazz-Backend-Secret": backendSecret, "Content-Type": "application/json" },
      body: JSON.stringify({ identity }),
    });
    if (!response.ok) {
      const code = await response.text();
      throw new AccountAuthError(
        /^[a-z_]{1,80}$/.test(code) ? code : "account_request_failed",
        response.status,
      );
    }
    return response.json();
  }, new AccountAuthError("account_request_timeout"));
}

/**
 * Sign in through the app's backend (`createJazz`), same-origin with the app's
 * cookies: the backend's `authenticate` decides, and the answer carries the
 * account, identity and claims but no credential. With `link`, a local-first
 * guest's proof for a challenge the backend issued (see
 * {@link requestBackendLinkChallenge}): the backend links that guest to the
 * user it names.
 */
export async function requestBackendSignIn(
  registry: string,
  fetcher: typeof fetch = fetchFor(registry),
  options: { link?: { challenge: string; proof: string; audience?: string } } = {},
): Promise<unknown> {
  return backendRequest(
    registry,
    options.link ? "backend-link" : "backend-sign-in",
    fetcher,
    options.link,
  );
}

/**
 * A challenge from the app's backend for a local-first guest to sign before
 * it links to the user the backend names. It names that user, expires
 * quickly and links once.
 */
export async function requestBackendLinkChallenge(
  registry: string,
  fetcher: typeof fetch = fetchFor(registry),
): Promise<string> {
  const value = (await backendRequest(registry, "backend-link/challenge", fetcher)) as {
    challenge?: unknown;
  } | null;
  if (typeof value?.challenge !== "string" || !value.challenge) {
    throw new AccountAuthError("invalid_link_challenge");
  }
  return value.challenge;
}

async function backendRequest(
  registry: string,
  path: string,
  fetcher: typeof fetch,
  body?: unknown,
): Promise<unknown> {
  return withAuthRequestDeadline(async (signal) => {
    const response = await fetcher(`${registry}/${path}`, {
      signal,
      method: "POST",
      credentials: "same-origin",
      redirect: "error",
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      const code = await response.text();
      throw new AccountAuthError(
        /^[a-z_]{1,80}$/.test(code) ? code : "account_request_failed",
        response.status,
      );
    }
    return response.json();
  }, new AccountAuthError("account_request_timeout"));
}

export function readAccountAssignment(value: unknown, expected: AccountIdentity): string {
  if (!value || typeof value !== "object") throw new AccountAuthError("invalid_account_response");
  const result = value as { account?: unknown; identity?: AccountIdentity };
  if (
    typeof result.account !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(result.account) ||
    !result.identity ||
    result.identity.issuer !== expected.issuer ||
    result.identity.subject !== expected.subject
  ) {
    throw new AccountAuthError("invalid_account_response");
  }
  return result.account;
}
