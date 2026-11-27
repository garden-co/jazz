import { pool } from "../../../src/lib/db.server";
import { addMember, findPersonByEmail } from "../../../src/lib/members";
import { requestAccount } from "../../../src/lib/request-account";

export const runtime = "nodejs";

const statusCodes = { added: 201, "already-member": 409, forbidden: 403 } as const;

/**
 * Admins add an existing BigLabel user to their label by sign-in email. The
 * server only resolves the email; the membership is written by the
 * `memberships.add` mutator as the caller, so its checks decide.
 */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });

  // Answer only admins, so nobody else can probe which emails have accounts
  // or who belongs to the label. The mutator checks again on its own.
  const { rows } = await pool.query(
    `SELECT 1 FROM memberships WHERE "organizationId" = $1 AND "userId" = $2 AND role = 'admin'`,
    [organizationId, account.accountId],
  );
  if (rows.length === 0) return Response.json({ status: "forbidden" }, { status: 403 });
  const person = await findPersonByEmail(email);
  if (!person) return Response.json({ status: "not-found" }, { status: 404 });
  const status = await addMember(account.accountId, { organizationId, person, role });
  return Response.json(status === "forbidden" ? { status } : { status, name: person.name }, {
    status: statusCodes[status],
  });
}
