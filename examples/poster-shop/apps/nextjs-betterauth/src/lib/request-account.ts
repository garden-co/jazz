import { AccountAuthError, type Db } from "jazz-tools";
import type { JazzClient } from "jazz-tools/backend";

export type VerifiedRequest = {
  accountId: string;
  claims: Readonly<Record<string, unknown>>;
  /** Backend authority, with writes stamped as the verified caller. */
  db: Db;
};

// Registry answers that mean "this caller has no usable account", as opposed
// to the registry itself failing (`account_request_failed`,
// `invalid_account_response`), which is an infrastructure error.
const INFRASTRUCTURE_ACCOUNT_CODES = new Set([
  "account_request_failed",
  "invalid_account_response",
]);

// jazz-tools rejects bad bearers with plain `Error`s carrying these exact
// verification messages (backend/request-auth.ts). Server-side faults such as
// "Invalid JWT public key" or "Unable to fetch JWKS" are deliberately absent.
// TODO: replace this list with a typed verification error once core has one.
const JWT_REJECTION_MESSAGES = new Set([
  "Invalid JWT header",
  "Invalid JWT payload",
  "JWT has expired",
  "JWT issuer does not match the configured issuer",
  "JWT audience does not match the configured audience",
  "No matching JWK found",
]);
// Signature failures are wrapped as `Invalid JWT: <reason>`.
const WRAPPED_SIGNATURE_REJECTION = "Invalid JWT: ";

/**
 * Verify the request's bearer once, through the backend client: it checks the
 * Better Auth JWT against its JWKS, resolves the caller's active Jazz account,
 * and returns a backend-authority `Db` that stamps writes as that caller.
 *
 * Returns null only when the caller is not authenticated (no bearer, a
 * rejected JWT, or no admitted account). Anything else, such as an
 * unreachable JWKS endpoint or account registry, is rethrown.
 */
export async function verifiedRequest(
  client: JazzClient,
  request: Request,
): Promise<VerifiedRequest | null> {
  if (!/^Bearer \S+$/.test(request.headers.get("authorization") ?? "")) return null;
  let db: Db;
  try {
    db = await client.withAttributionForRequest(request);
  } catch (error) {
    if (isAuthRejection(error)) return null;
    throw error;
  }
  const session = db.getAuthState().session;
  const accountId = session?.user.account;
  if (!session || !accountId) return null;
  return { accountId, claims: session.claims, db };
}

function isAuthRejection(error: unknown): boolean {
  if (error instanceof AccountAuthError) return !INFRASTRUCTURE_ACCOUNT_CODES.has(error.code);
  if (!(error instanceof Error)) return false;
  return (
    JWT_REJECTION_MESSAGES.has(error.message) ||
    error.message.startsWith(WRAPPED_SIGNATURE_REJECTION)
  );
}
