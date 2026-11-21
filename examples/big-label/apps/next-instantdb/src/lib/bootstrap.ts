import { id } from "@instantdb/admin";
import { adminDb, isUniqueConflict } from "./instant.server";
import { normalizeEmail } from "./emails";
import { membershipFields } from "./membership";

/**
 * Server-mediated first-tenant bootstrap. Ordinary clients never get admin
 * credentials and remain subject to the admin-only membership rules.
 */
const maxAttempts = 5;

export async function ensurePersonalOrganization(
  userId: string,
  name: string,
  email: string | null = null,
) {
  const slug = `personal-${userId}`;
  // Reads, then writes what's missing in one transaction. The profile, the
  // label's slug and the membership's key are unique, so a concurrent
  // bootstrap makes this transaction fail as a whole; the retry then reads
  // the winner's rows instead of adding duplicates.
  for (let attempt = 1; ; attempt++) {
    const { people, organizations } = await adminDb.query({
      people: { $: { where: { userId } }, emails: {} },
      organizations: { $: { where: { slug } } },
    });
    if (people.length > 1 || organizations.length > 1)
      throw new Error("Duplicate personal bootstrap rows");
    const person = people[0];
    const personId = person?.id ?? id();
    const organizationId = organizations[0]?.id ?? id();
    const { memberships } = await adminDb.query({
      memberships: { $: { where: { memberKey: `${organizationId}/${userId}` } } },
    });
    const chunks = [];
    if (!person) chunks.push(adminDb.tx.people[personId].create({ userId, name }));
    if (!organizations[0])
      chunks.push(
        adminDb.tx.organizations[organizationId].create({ name: `${name}'s label`, slug }),
      );
    // Record the verified sign-in email so admins can add this person by
    // email. Only the server writes or reads these rows.
    if (email) {
      const normalized = normalizeEmail(email);
      const current = person?.emails[0];
      if (!current)
        chunks.push(
          adminDb.tx.personEmails[id()].create({ email: normalized }).link({ person: personId }),
        );
      else if (current.email !== normalized)
        chunks.push(adminDb.tx.personEmails[current.id].update({ email: normalized }));
    }
    if (memberships.length === 0)
      chunks.push(
        adminDb.tx.memberships[id()]
          .create(membershipFields(organizationId, userId, "admin"))
          .link({ organization: organizationId, person: personId }),
      );
    try {
      if (chunks.length > 0) await adminDb.transact(chunks);
      return { id: organizationId };
    } catch (error) {
      if (!isUniqueConflict(error) || attempt >= maxAttempts) throw error;
    }
  }
}
