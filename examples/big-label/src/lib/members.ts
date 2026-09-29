import type { Db } from "jazz-tools";
import { app } from "../../schema";
import { normalizeEmail } from "./emails";

export type AddMemberResult =
  | { status: "added"; membershipId: string; name: string }
  | { status: "already-member"; name: string }
  | { status: "not-found" }
  | { status: "forbidden" }
  | { status: "invalid-role" };

/**
 * Adds the person who signed in with `email` to an organization. Runs on the
 * server, because emails are private: clients can't read `personEmails`, and
 * can only see people who already share a label with them.
 *
 * The caller must be an admin of the organization, and new members can be
 * editors or viewers only, the same rules `permissions.ts` applies to a
 * membership inserted by a client.
 */
export async function addMemberByEmail(
  db: Db,
  callerAccountId: string,
  input: { organizationId: string; email: string; role: string },
): Promise<AddMemberResult> {
  if (input.role !== "editor" && input.role !== "viewer") return { status: "invalid-role" };
  const callerIsAdmin = await db.one(
    app.memberships.where({
      organizationId: input.organizationId,
      userId: callerAccountId,
      role: "admin",
    }),
  );
  if (!callerIsAdmin) return { status: "forbidden" };

  const emails = await db.all(
    app.personEmails.where({ email: normalizeEmail(input.email) }).include({ person: true }),
  );
  const person = emails.length === 1 ? emails[0]!.person : null;
  if (!person) return { status: "not-found" };

  const existing = await db.one(
    app.memberships.where({ organizationId: input.organizationId, personId: person.id }),
  );
  if (existing) return { status: "already-member", name: person.name };

  const membership = db.insert(app.memberships, {
    organizationId: input.organizationId,
    personId: person.id,
    userId: person.userId,
    role: input.role,
  });
  await membership.wait({ tier: "global" });
  return { status: "added", membershipId: membership.value.id, name: person.name };
}
