import type { Db } from "jazz-tools";
import type { Jazz } from "jazz-tools/backend";

export type VerifiedRequest = {
  accountId: string;
  claims: Readonly<Record<string, unknown>>;
  /** Backend authority, with writes stamped as the signed-in caller. */
  db: Db;
};

/**
 * The caller behind an API request, from its Better Auth session cookie (see
 * `authenticate` in jazz.server.ts): their Jazz account, and a
 * backend-authority `Db` that stamps writes as them.
 *
 * Returns null only when nobody is signed in. Anything else, such as an
 * unreachable sync server, is rethrown.
 */
export async function verifiedRequest(
  jazz: Jazz,
  request: Request,
): Promise<VerifiedRequest | null> {
  const accountId = await jazz.accountFor(request);
  if (!accountId) return null;
  const db = await jazz.withAttribution(request);
  return { accountId, claims: db.getAuthState().session?.claims ?? {}, db };
}
