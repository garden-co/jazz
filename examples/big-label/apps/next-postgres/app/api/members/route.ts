import { addMember, findPersonByEmail } from "../../../src/lib/members";
import { pool } from "../../../src/lib/db.server";
import { roleIn } from "../../../src/lib/permissions.server";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

const statusCodes = { added: 201, "already-member": 409, forbidden: 403 } as const;

/**
 * Admins add an existing BigLabel user to their label by sign-in email. The
 * email lookup is the only privileged read; the membership write checks the
 * caller's role on its own.
 */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });

  // Answer only admins, so nobody else can probe which emails have accounts
  // or who belongs to the label.
  if ((await roleIn(pool, account.accountId, organizationId)) !== "admin")
    return Response.json({ status: "forbidden" }, { status: 403 });
  const person = await findPersonByEmail(email);
  if (!person) return Response.json({ status: "not-found" }, { status: 404 });
  const status = await addMember(account.accountId, { organizationId, person, role });
  return Response.json(status === "forbidden" ? { status } : { status, name: person.name }, {
    status: statusCodes[status],
  });
}
