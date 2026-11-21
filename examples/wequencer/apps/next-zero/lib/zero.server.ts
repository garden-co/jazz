import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { auth } from "@/lib/auth";
import { pool } from "@/lib/db.server";
import { schema, type AuthData } from "@/schema";

export const dbProvider = zeroNodePg(schema, pool);

/** The caller, from the Better Auth session cookie zero-cache forwards. */
export async function authData(request: Request): Promise<AuthData | undefined> {
  const session = await auth.api.getSession({ headers: request.headers });
  return session ? { userID: session.user.id } : undefined;
}
