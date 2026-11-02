import { ensureDemoWorkspace } from "@/src/lib/bootstrap";
import { jazz } from "@/src/lib/jazz.server";
import { requireRequestAccount } from "@/src/lib/request-account";

export const runtime = "nodejs";

/** Idempotent: safe to call on every sign-in and to retry after any failure. */
export async function POST(request: Request) {
  const caller = await requireRequestAccount(request);
  if (caller instanceof Response) return caller;
  const db = (await jazz()).db;
  const result = await ensureDemoWorkspace(db, caller.account, caller.displayName);
  return Response.json(result);
}
