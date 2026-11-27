import { mustGetMutator } from "@rocicorp/zero";
import { mutators } from "../../mutators";
import { dbProvider, pool } from "./db.server";
import { normalizeEmail } from "./emails";

/**
 * Looks up who signed in with `email`. Emails are private: only the trusted
 * server reads `private.personEmails`, and this lookup is the only thing it
 * does with them when adding a member.
 */
export async function findPersonByEmail(email: string) {
  const { rows } = await pool.query(
    `SELECT p.id, p."userId", p.name FROM private."personEmails" e JOIN people p ON p.id = e."personId" WHERE e.email = $1`,
    [normalizeEmail(email)],
  );
  return rows.length === 1 ? (rows[0] as { id: string; userId: string; name: string }) : null;
}

export type AddMemberStatus = "added" | "already-member" | "forbidden";

/**
 * Adds a person to a label by running the `memberships.add` mutator as the
 * caller, so its checks decide: only admins add members, and never as
 * admins. A double submit adds them once: the mutator reads the person's
 * membership first, and the unique constraint on (organizationId, personId)
 * refuses a concurrent duplicate, which then reads as "already-member".
 */
export async function addMember(
  callerUserId: string,
  input: { organizationId: string; person: { id: string; userId: string }; role: string },
): Promise<AddMemberStatus> {
  const add = mustGetMutator(mutators, "memberships.add");
  try {
    const result = await dbProvider.transaction((tx) =>
      add.fn({
        tx,
        ctx: { userID: callerUserId },
        args: {
          id: crypto.randomUUID(),
          organizationId: input.organizationId,
          personId: input.person.id,
          userId: input.person.userId,
          role: input.role,
        },
      }),
    );
    return (result as { status: AddMemberStatus }).status;
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("permission_denied")) return "forbidden";
    if ((error as { code?: string }).code === "23505") return "already-member";
    throw error;
  }
}
