import { randomUUID } from "node:crypto";
import { one, pool, transaction } from "./db.server";
import { normalizeEmail } from "./emails";
import { assertAdmin, Forbidden, isInvitableRole } from "./permissions.server";

/**
 * Looks up who signed in with `email`. Emails are private: only this server
 * path reads `personEmails`, and it is all the lookup does.
 */
export async function findPersonByEmail(email: string) {
  return (
    (await one<{ id: string; userId: string; name: string }>(
      pool,
      `SELECT p.id, p."userId", p.name FROM "personEmails" e JOIN people p ON p.id = e."personId" WHERE e.email = $1`,
      [normalizeEmail(email)],
    )) ?? null
  );
}

export type AddMemberStatus = "added" | "already-member" | "forbidden";

/**
 * Adds a person to a label as the caller: only admins add members, and never
 * as admins. The unique (label, person) constraint makes a double submit add
 * them once.
 */
export async function addMember(
  callerUserId: string,
  input: { organizationId: string; person: { id: string; userId: string }; role: string },
): Promise<AddMemberStatus> {
  try {
    return await transaction(async (tx) => {
      await assertAdmin(tx, callerUserId, input.organizationId);
      if (!isInvitableRole(input.role)) throw new Forbidden();
      const inserted = await tx.query(
        `INSERT INTO memberships (id, "organizationId", "personId", "userId", role) VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT ("organizationId", "personId") DO NOTHING`,
        [randomUUID(), input.organizationId, input.person.id, input.person.userId, input.role],
      );
      if (!inserted.rowCount) return "already-member" as const;
      tx.changed(`org:${input.organizationId}`, "memberships", "people");
      tx.changed(`me:${input.person.userId}`, "memberships");
      return "added" as const;
    });
  } catch (error) {
    if (error instanceof Forbidden) return "forbidden";
    throw error;
  }
}
