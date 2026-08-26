import { randomUUID } from "node:crypto";
import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";
import { planPersonalBootstrap } from "./bootstrap-state";

/**
 * Server-mediated first-tenant bootstrap. Ordinary clients never receive the
 * backend secret and remain subject to the admin-only membership policy.
 */
export async function ensurePersonalOrganization(userId: string, name: string) {
  const db = authJazzContext().asBackend(app);
  const slug = `personal-${userId}`;
  // Both the initial read and every retry happen inside the exclusive
  // transaction. A concurrent bootstrap can therefore only either commit the
  // complete triple or force this attempt to re-read the committed triple.
  for (;;) {
    try {
      const write = await db.exclusiveTransaction(async (tx) => {
        const [people, organizations] = await Promise.all([
          tx.all(app.people.where({ userId })),
          tx.all(app.organizations.where({ slug })),
        ]);
        const organization = organizations[0];
        const memberships = organization
          ? await tx.all(app.memberships.where({ organizationId: organization.id, userId }))
          : [];
        const plan = planPersonalBootstrap(userId, slug, { people, organizations, memberships });

        const personId = plan.person?.id ?? randomUUID();
        const organizationId = plan.organization?.id ?? randomUUID();
        if (!plan.person) tx.insert(app.people, { userId, name }, { id: personId });
        if (!plan.organization) {
          tx.insert(app.organizations, { name: `${name}'s label`, slug }, { id: organizationId });
        }
        if (!plan.membership) {
          tx.insert(
            app.memberships,
            { organizationId, personId, userId, role: "admin" },
            { id: randomUUID() },
          );
        }
        return organizationId;
      });
      const organizationId = await write.wait();
      const created = await db.one(app.organizations.where({ id: organizationId }));
      if (!created) throw new Error("Bootstrap was acknowledged without its organization");
      return created;
    } catch (error) {
      // The authority rejects a competing exclusive snapshot. Retry only once
      // it has become durable; all successful retry reads still pass the
      // duplicate/integrity checks above rather than selecting an arbitrary row.
      if (!isExclusiveConflict(error)) throw error;
    }
  }
}

function isExclusiveConflict(error: unknown) {
  const message = error instanceof Error ? error.message : String(error);
  return /exclusive_conflict|transaction_conflict|cascade_rejected/.test(message);
}
