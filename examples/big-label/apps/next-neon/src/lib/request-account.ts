import "server-only";
import { getUser } from "./auth";

/**
 * The signed-in caller of an API route, from their Managed Better Auth
 * session cookie. Resolves null when nobody is signed in, so routes answer 401.
 */
export async function requestAccount() {
  const user = await getUser();
  if (!user) return null;
  const { name, email } = user;
  return { accountId: user.id, displayName: name || email || user.id, email: email || null };
}
