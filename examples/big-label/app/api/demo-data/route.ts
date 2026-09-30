import { authJazzClient } from "../../../src/lib/auth-jazz-client";
import { isDemoProfile, loadDemoData } from "../../../src/lib/demo-data";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

/** Loads a deterministic fixture profile as demo labels administered by the caller. */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const { profile } = (await request.json().catch(() => ({}))) as { profile?: unknown };
  if (!isDemoProfile(profile)) return Response.json({ error: "unknown profile" }, { status: 400 });
  const db = (await authJazzClient()).db;
  return Response.json(await loadDemoData(db, account.accountId, profile));
}
