import { ensurePersonalOrganization } from "../../../src/lib/bootstrap";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

export async function POST() {
  // The Managed Better Auth session cookie names the caller.
  const account = await requestAccount();
  if (!account) return Response.json({ error: "account required" }, { status: 401 });
  const organization = await ensurePersonalOrganization(account.accountId, account.displayName, account.email);
  return Response.json({ organizationId: organization.id });
}
