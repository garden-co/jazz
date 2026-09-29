import { PersistedWriteRejectedError, type Db } from "jazz-tools";
import { app } from "../../schema";
import { normalizeEmail } from "./emails";

/**
 * Looks up who signed in with `email`. Emails are private: only the trusted
 * server reads `personEmails`, so this takes the backend db, and it is the
 * only thing the backend does when adding a member.
 */
export async function findPersonByEmail(backend: Db, email: string) {
  const rows = await backend.all(
    app.personEmails.where({ email: normalizeEmail(email) }).include({ person: true }),
  );
  const person = rows.length === 1 ? rows[0]!.person : null;
  return person ? { id: person.id, userId: person.userId, name: person.name } : null;
}

export type AddMemberStatus = "added" | "already-member" | "forbidden";

/**
 * Adds a person to an organization, writing as the caller so `permissions.ts`
 * decides whether they may: only admins add members, and never as admins.
 *
 * The exclusive transaction reads the person's existing membership first, so
 * two submits racing each other can't both insert: the authority rejects the
 * one whose read went stale, and the retry finds the membership.
 */
export async function addMember(
  caller: Db,
  input: { organizationId: string; person: { id: string; userId: string }; role: string },
): Promise<AddMemberStatus> {
  for (let attempt = 0; ; attempt++) {
    try {
      const write = await caller.exclusiveTransaction(async (tx) => {
        const existing = await tx.all(
          app.memberships.where({
            organizationId: input.organizationId,
            personId: input.person.id,
          }),
        );
        if (existing.length > 0) return "already-member" as const;
        tx.insert(app.memberships, {
          organizationId: input.organizationId,
          personId: input.person.id,
          userId: input.person.userId,
          role: input.role,
        });
        return "added" as const;
      });
      return await write.wait();
    } catch (error) {
      if (error instanceof PersistedWriteRejectedError) return "forbidden";
      if (attempt < 3 && isExclusiveConflict(error)) continue;
      throw error;
    }
  }
}

/** Whether an exclusive transaction lost to a concurrent write and can be retried. */
export function isExclusiveConflict(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /exclusive_conflict|transaction_conflict|cascade_rejected/.test(message);
}
