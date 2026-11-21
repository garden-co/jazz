/**
 * The signed-in caller of an API route, from their Better Auth session
 * cookie. Their account id is the Better Auth user id. Resolves null when
 * nobody is signed in, so routes answer 401.
 */
export async function requestAccount(request: Request) {
  const { auth } = await import("./auth");
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session) return null;
  const { id, name, email } = session.user;
  return { accountId: id, displayName: name || email || id, email: email || null };
}
