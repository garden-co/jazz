import type { Db } from "jazz-tools";
import { authJazzClient } from "@/src/lib/auth-jazz-client";

export type VerifiedRequest = {
  accountId: string;
  claims: Readonly<Record<string, unknown>>;
  /** Backend authority, with writes stamped as the verified caller. */
  db: Db;
};

/**
 * Verify the request's bearer through the backend client, which checks the
 * Better Auth JWT against its JWKS and resolves the caller's active Jazz
 * account. Returns null when the request carries no admitted account.
 */
export async function verifiedRequest(request: Request): Promise<VerifiedRequest | null> {
  const client = await authJazzClient();
  let requester: Db;
  try {
    requester = await client.forRequest(request);
  } catch {
    return null;
  }
  const session = requester.getAuthState().session;
  const accountId = session?.user.account;
  if (!session || !accountId) return null;
  return {
    accountId,
    claims: session.claims,
    db: await client.withAttributionForRequest(request),
  };
}
