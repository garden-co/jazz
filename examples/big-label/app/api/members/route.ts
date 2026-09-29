import { authJazzClient } from "../../../src/lib/auth-jazz-client";
import { addMemberByEmail } from "../../../src/lib/members";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

const statusCodes = {
  added: 201,
  "already-member": 409,
  "not-found": 404,
  forbidden: 403,
  "invalid-role": 400,
} as const;

/** Admins add an existing BigLabel user to their label by sign-in email. */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });
  const db = (await authJazzClient()).db;
  const result = await addMemberByEmail(db, account.accountId, { organizationId, email, role });
  return Response.json(result, { status: statusCodes[result.status] });
}
