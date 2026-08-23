import { randomUUID } from "node:crypto";
import { app } from "../../schema";
import { authJazzContext } from "./auth-jazz-context";

/**
 * Server-mediated first-tenant bootstrap. Ordinary clients never receive the
 * backend secret and remain subject to the admin-only membership policy.
 */
export async function ensurePersonalOrganization(userId: string, name: string) {
  const db = authJazzContext().asBackend(app);
  const slug = `personal-${userId}`;
  const existing = await db.one(app.organizations.where({ slug }));
  if (existing) return existing;

  const organizationId = randomUUID();
  const personId = randomUUID();
  const membershipId = randomUUID();
  try {
    const write = await db.exclusiveTransaction((tx) => {
      tx.insert(app.people, { userId, name }, { id: personId });
      tx.insert(app.organizations, { name: `${name}'s label`, slug }, { id: organizationId });
      tx.insert(
        app.memberships,
        { organizationId, personId, userId, role: "admin" },
        { id: membershipId },
      );
      return organizationId;
    });
    await write.wait();
  } catch (error) {
    // Concurrent authenticated requests may race the initial ensure. Re-read
    // the stable external-user key; only return a committed organization.
    const raced = await db.one(app.organizations.where({ slug }));
    if (raced) return raced;
    throw error;
  }
  const created = await db.one(app.organizations.where({ id: organizationId }));
  if (!created) throw new Error("Bootstrap was acknowledged without its organization");
  return created;
}
