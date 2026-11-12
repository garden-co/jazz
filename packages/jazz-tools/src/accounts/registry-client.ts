import { fetchFor } from "../runtime/in-process-transport.js";
import { withAuthRequestDeadline } from "../runtime/auth-request-deadline.js";
import type { AccountIdentity } from "./state.js";

export class AccountAuthError extends Error {
  constructor(
    readonly code: string,
    /** The registry's HTTP status when it answered with an error. */
    readonly status?: number,
    /** What the app can do about it, when the code alone doesn't say. */
    message: string = code,
  ) {
    super(message);
    this.name = "AccountAuthError";
  }
}

/**
 * `linkDeviceAccount()` was called without a device account to link: the
 * session is signed out, or its account came from your backend.
 */
export const LINK_NEEDS_GUEST_MESSAGE =
  "local_first_account_required: linkDeviceAccount() links this device's device account (a guest) to the user your backend signed in, and there is no device account to link: the session is signed out, or its account came from your backend. Call refresh() to open the backend user's account instead. To keep what a device account wrote, link while it is signed in.";

/**
 * `linkDeviceAccount()` was called with a device account whose key this
 * client no longer holds: it signed out meanwhile, or the handle comes from
 * another client.
 */
export const LINK_GUEST_KEY_MISSING_MESSAGE =
  "local_first_account_required: this device account's key isn't held by this client any more (it signed out, or comes from another Jazz client), so it can't prove itself to link. Link from the client where it is signed in, or call refresh() instead.";

/**
 * What `createJazz` answers a sign-in or `backend-user` request with when the
 * app's `authenticate` named a user without a valid `expiresAt`. The backend
 * logs the detail; the response carries only this code.
 */
export const AUTHENTICATE_EXPIRES_AT_MISSING = "authenticate_expires_at_missing";

/** What a client says on {@link AUTHENTICATE_EXPIRES_AT_MISSING}: the fix is in the backend. */
export const AUTHENTICATE_EXPIRES_AT_MISSING_MESSAGE =
  "authenticate_expires_at_missing: your backend's authenticate (createJazz) named a user without expiresAt, so Jazz can't tell how long that sign-in holds and can't sign them in. Return expiresAt with the user: when the answer stops holding, as Unix time in seconds (your session's expiry, or its token's exp). Your backend's log says what it returned. See https://jazz.tools/docs/setup/auth/backend#when-a-session-expires";

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
 * Ask the app's backend (`createJazz`) who is signed in, without signing them
 * in: the user its `authenticate` names, and their account if the core holds
 * one yet (`null` before their first sign-in). Unlike
 * {@link requestBackendSignIn} it never creates an account, so a local-first
 * guest can still link to a user who has none.
 */
export async function requestBackendUser(
  registry: string,
  fetcher: typeof fetch = fetchFor(registry),
): Promise<unknown> {
  return backendRequest(registry, "backend-user", fetcher);
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
      // No body, no content type: a host's JSON parser (Fastify's, Express's)
      // refuses or swallows an empty "JSON" body.
      ...(body === undefined
        ? {}
        : { headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) }),
    });
    if (!response.ok) {
      const code = await response.text();
      if (code === AUTHENTICATE_EXPIRES_AT_MISSING) {
        const error = new AccountAuthError(
          code,
          response.status,
          AUTHENTICATE_EXPIRES_AT_MISSING_MESSAGE,
        );
        // The session asks who is signed in in the background too, where a
        // failed answer is only retried: say it on the console as well, once.
        warnExpiresAtMissing(error);
        throw error;
      }
      throw new AccountAuthError(
        /^[a-z_]{1,80}$/.test(code) ? code : "account_request_failed",
        response.status,
      );
    }
    return response.json();
  }, new AccountAuthError("account_request_timeout"));
}

let warnedExpiresAtMissing = false;
function warnExpiresAtMissing(error: AccountAuthError): void {
  if (warnedExpiresAtMissing) return;
  warnedExpiresAtMissing = true;
  console.error(`Jazz: ${error.message}`);
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
