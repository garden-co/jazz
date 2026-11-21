import { id } from "@instantdb/admin";
import { membershipFields } from "./membership";
import { normalizeEmail } from "./emails";
import {
  isPermissionDenied,
  isUniqueConflict,
  type AdminDb,
  type CallerDb,
} from "./instant.server";

/**
 * Looks up who signed in with `email`. Emails are private: only the server
 * reads `personEmails`, so this takes the admin db, and it is the only thing
 * the admin db does when adding a member.
 */
export async function findPersonByEmail(admin: AdminDb, email: string) {
  const { personEmails } = await admin.query({
    personEmails: { $: { where: { email: normalizeEmail(email) } }, person: {} },
  });
  const person = personEmails.length === 1 ? personEmails[0]!.person : null;
  return person ? { id: person.id, userId: person.userId, name: person.name } : null;
}

export type AddMemberStatus = "added" | "already-member" | "forbidden";

/**
 * Adds a person to an organization, writing as the caller so
 * `instant.perms.ts` decides whether they may: only admins add members, and
 * never as admins. The membership's key is unique per label and person, so a
 * double submit adds them once and the second write fails on it.
 */
export async function addMember(
  caller: CallerDb,
  input: { organizationId: string; person: { id: string; userId: string }; role: string },
): Promise<AddMemberStatus> {
  try {
    await caller.transact(
      caller.tx.memberships[id()]
        .create(membershipFields(input.organizationId, input.person.userId, input.role))
        .link({ organization: input.organizationId, person: input.person.id }),
    );
    return "added";
  } catch (error) {
    if (isUniqueConflict(error)) return "already-member";
    if (isPermissionDenied(error)) return "forbidden";
    throw error;
  }
}
