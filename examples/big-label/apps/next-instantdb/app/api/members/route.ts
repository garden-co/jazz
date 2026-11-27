import { addMember, findPersonByEmail } from "../../../src/lib/members";
import { adminDb, requestAccount } from "../../../src/lib/instant.server";

export const runtime = "nodejs";

const statusCodes = { added: 201, "already-member": 409, forbidden: 403 } as const;

/**
 * Admins add an existing BigLabel user to their label by sign-in email. The
 * admin db only resolves the email; the membership is written as the caller,
 * so `instant.perms.ts` decides whether they may add it.
 */
export async function POST(request: Request) {
  const account = await requestAccount(request);
  if (!account) return Response.json({ status: "unauthorized" }, { status: 401 });
  const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const { organizationId, email, role } = body;
  if (typeof organizationId !== "string" || typeof email !== "string" || typeof role !== "string")
    return Response.json({ status: "invalid-request" }, { status: 400 });

  // Answer only admins, read as the caller, so nobody else can probe which
  // emails have accounts or who belongs to the label. The membership write
  // below is still checked by instant.perms.ts on its own.
  const { memberships } = await account.db.query({
    memberships: {
      $: { where: { organization: organizationId, userId: account.accountId, role: "admin" } },
    },
  });
  if (memberships.length === 0) return Response.json({ status: "forbidden" }, { status: 403 });
  const person = await findPersonByEmail(adminDb, email);
  if (!person) return Response.json({ status: "not-found" }, { status: 404 });
  const status = await addMember(account.db, { organizationId, person, role });
  return Response.json(status === "forbidden" ? { status } : { status, name: person.name }, {
    status: statusCodes[status],
  });
}
