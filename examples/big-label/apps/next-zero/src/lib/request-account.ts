/**
 * The signed-in caller of an API route, from their Better Auth session
 * cookie. Resolves null when nobody is signed in, so routes answer 401.
 */
export async function requestAccount(request: Request) {
  const { auth } = await import("./auth");
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session) return null;
  const { name, email } = session.user;
  return {
    accountId: session.user.id,
    displayName: name || email || session.user.id,
    email: email || null,
  };
}
