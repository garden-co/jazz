import { randomUUID } from "node:crypto";
import type { Organization } from "../../schema";
import { one, transaction } from "./db.server";
import { normalizeEmail } from "./emails";

/**
 * Server-mediated first-tenant bootstrap: the account's person, their sign-in
 * email (for adding them by email) and their personal label with them as
 * admin. Browsers can't create any of these. Idempotent: unique constraints
 * on people."userId" and organizations.slug make a concurrent bootstrap
 * reuse the same rows instead of duplicating them.
 */
export async function ensurePersonalOrganization(userId: string, name: string, email: string | null = null) {
  const slug = `personal-${userId}`;
  return transaction(async (tx) => {
    const person = (await one<{ id: string }>(
      tx,
      `INSERT INTO people (id, "userId", name) VALUES ($1, $2, $3)
       ON CONFLICT ("userId") DO UPDATE SET "userId" = EXCLUDED."userId" RETURNING id`,
      [randomUUID(), userId, name],
    ))!;
    const organization = (await one<Organization>(
      tx,
      `INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3)
       ON CONFLICT (slug) DO UPDATE SET slug = EXCLUDED.slug RETURNING *`,
      [randomUUID(), `${name}'s label`, slug],
    ))!;
    if (email) {
      await tx.query(
        `INSERT INTO "personEmails" (id, "personId", email) VALUES ($1, $2, $3)
         ON CONFLICT ("personId") DO UPDATE SET email = EXCLUDED.email`,
        [randomUUID(), person.id, normalizeEmail(email)],
      );
    }
    await tx.query(
      `INSERT INTO memberships (id, "organizationId", "personId", "userId", role) VALUES ($1, $2, $3, $4, 'admin')
       ON CONFLICT ("organizationId", "personId") DO NOTHING`,
      [randomUUID(), organization.id, person.id, userId],
    );
    tx.changed(`me:${userId}`, "memberships");
    return organization;
  });
}
