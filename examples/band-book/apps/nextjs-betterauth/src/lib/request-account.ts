import { jazz } from "./jazz.server";

export type RequestAccount = { account: string; displayName: string };

/**
 * The signed-in caller of a server route: Jazz resolves their account from
 * the Better Auth session cookie (`authenticate` in jazz.server.ts).
 *
 * The bootstrap and invite routes write with backend authority on the
 * caller's behalf, so they need the caller's account id itself.
 */
export async function requireRequestAccount(request: Request): Promise<RequestAccount | Response> {
  const { auth } = await import("./auth");
  const session = await auth.api.getSession({ headers: request.headers });
  if (!session) return Response.json({ error: "sign in required" }, { status: 401 });
  const account = await (await jazz()).accountFor(request);
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const name = session.user.name;
  return { account, displayName: name.trim() ? name : "Band member" };
}
