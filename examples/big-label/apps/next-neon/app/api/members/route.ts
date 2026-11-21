import { pool } from "../../../src/lib/db.server";
import { addMember, findPersonByEmail, isAdmin } from "../../../src/lib/members";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

const statusCodes = { added: 201, "already-member": 409, forbidden: 403 } as const;

/**
 * Admins add an existing BigLabel user to their label by sign-in email. The
 * server resolves the email (person_emails is server-only) and writes the
 * membership for the caller, checking in the same transaction that they
 * administer the label and that the role isn't admin.
 */
export async function POST(request: Request) {
  const account = await requestAccount();
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });

  // Answer only admins, so nobody else can probe which emails have accounts
  // or who belongs to the label. addMember checks again in its transaction.
  if (!(await isAdmin(pool, organizationId, account.accountId)))
    return Response.json({ status: "forbidden" }, { status: 403 });
  const person = await findPersonByEmail(pool, email);
  if (!person) return Response.json({ status: "not-found" }, { status: 404 });
  const status = await addMember(pool, account.accountId, { organizationId, person, role });
  return Response.json(status === "forbidden" ? { status } : { status, name: person.name }, {
    status: statusCodes[status],
  });
}
