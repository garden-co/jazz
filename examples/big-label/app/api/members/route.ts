import { app } from "../../../schema";
import { authJazzClient } from "../../../src/lib/auth-jazz-client";
import { addMember, findPersonByEmail } from "../../../src/lib/members";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

const statusCodes = { added: 201, "already-member": 409, forbidden: 403 } as const;

/**
 * Admins add an existing BigLabel user to their label by sign-in email. The
 * backend only resolves the email; the membership is written as the caller,
 * so `permissions.ts` decides whether they may add it.
 */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });

  const client = await authJazzClient();
  const caller = await client.forRequest(request);
  const person = await findPersonByEmail(client.db, email);
  if (!person) {
    // Only say an email is unknown to someone who could have added it, so
    // other users can't probe which emails have accounts.
    const isAdmin = await caller.one(
      app.memberships.where({ organizationId, userId: account.accountId, role: "admin" }),
    );
    return isAdmin
      ? Response.json({ status: "not-found" }, { status: 404 })
      : Response.json({ status: "forbidden" }, { status: 403 });
  }
  const status = await addMember(caller, { organizationId, person, role });
  return Response.json(status === "forbidden" ? { status } : { status, name: person.name }, {
    status: statusCodes[status],
  });
}
