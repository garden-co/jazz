import { jazz } from "./jazz.server";

/**
 * The signed-in caller of an API route, from their Better Auth session
 * cookie, and their Jazz account. Resolves null when nobody is signed in, so
 * routes answer 401. A failure on the server's side, such as an unreachable
 * sync server, is rethrown and becomes a logged 500.
 */
export async function requestAccount(request: Request) {
  const { auth } = await import("./auth");
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session) return null;
  const accountId = await (await jazz()).accountFor(request);
  if (!accountId) return null;
  const { name, email } = session.user;
  return { accountId, displayName: name || email || session.user.id, email: email || null };
}
