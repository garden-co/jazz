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

/** Shared bearer-only account protocol; never substitute a backend credential. */
export async function requestAccountRegistry(
  registry: string,
  path: string,
  token: string,
  body?: unknown,
  fetcher: typeof fetch = fetchFor(registry),
): Promise<unknown> {
  return withAuthRequestDeadline(async (signal) => {
    const response = await fetcher(`${registry}/${path}`, {
      signal,
      method: "POST",
      credentials: "omit",
      redirect: "error",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
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

/**
 * Ask the app's own backend (`createJazz`) who is signed in. Same-origin with
 * the app's cookies: the backend's `authenticate` decides, and the answer
 * carries the account, identity and claims but no credential.
 */
export async function requestBackendSignIn(
  registry: string,
  fetcher: typeof fetch = fetchFor(registry),
): Promise<unknown> {
  return withAuthRequestDeadline(async (signal) => {
    const response = await fetcher(`${registry}/backend-sign-in`, {
      signal,
      method: "POST",
      credentials: "same-origin",
      redirect: "error",
      headers: { "Content-Type": "application/json" },
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
