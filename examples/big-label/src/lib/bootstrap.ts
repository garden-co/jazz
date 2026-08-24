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
  let organizationId: string;
  try {
    const write = await db.exclusiveTransaction(async (tx) => {
      // This lookup is part of the exclusive transaction's read set. Two
      // concurrent first requests therefore cannot both commit a tenant for
      // the same external-user key.
      const existing = await tx.one(app.organizations.where({ slug }));
      if (existing) return existing.id;

      const createdOrganizationId = randomUUID();
      const personId = randomUUID();
      const membershipId = randomUUID();
      tx.insert(app.people, { userId, name }, { id: personId });
      tx.insert(
        app.organizations,
        { name: `${name}'s label`, slug },
        { id: createdOrganizationId },
      );
      tx.insert(
        app.memberships,
        { organizationId: createdOrganizationId, personId, userId, role: "admin" },
        { id: membershipId },
      );
      return createdOrganizationId;
    });
    organizationId = await write.wait();
  } catch (error) {
    // An exclusive transaction that raced another committed bootstrap may be
    // rejected. Re-read the stable external-user key; only return a committed
    // organization, and preserve unrelated errors.
    const raced = await db.one(app.organizations.where({ slug }));
    if (raced) return raced;
    throw error;
  }
  const created = await db.one(app.organizations.where({ id: organizationId }));
  if (!created) throw new Error("Bootstrap was acknowledged without its organization");
  return created;
}
