"use server";

import { getUser } from "../src/lib/auth";
import { pool } from "../src/lib/db.server";
import { runMutation, type MutationResponse } from "../src/lib/mutators.server";

/**
 * Every write, as a Server Action (Neon's Next.js example): it checks the
 * session itself and returns a typed result (Next hides thrown messages in
 * production) with the transaction id to wait for.
 */
export async function write(name: string, args: unknown): Promise<MutationResponse> {
  const user = await getUser();
  if (!user) return { kind: "rejected", error: "UNAUTHENTICATED" };
  return runMutation(pool, user.id, name, args);
}
