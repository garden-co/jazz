import type { AccountIdentity } from "./state.js";

export class AccountAuthError extends Error {
  constructor(readonly code: string) {
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
  fetcher: typeof fetch = globalThis.fetch,
): Promise<unknown> {
  const response = await fetcher(`${registry}/${path}`, {
    method: "POST",
    credentials: "omit",
    redirect: "error",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!response.ok) {
    const code = await response.text();
    throw new AccountAuthError(/^[a-z_]{1,80}$/.test(code) ? code : "account_request_failed");
  }
  return response.json();
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
