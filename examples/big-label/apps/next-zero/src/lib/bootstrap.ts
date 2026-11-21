import { randomUUID } from "node:crypto";
import { pool } from "./db.server";
import { normalizeEmail } from "./emails";

/**
 * Server-mediated first-tenant bootstrap: one profile, one
 * `personal-<user id>` label and its admin membership per Better Auth user,
 * plus the verified sign-in email. Ordinary clients have no mutator that
 * creates people, labels or admins.
 *
 * The serializable transaction reads the triple and creates what's missing;
 * a concurrent bootstrap makes one of them fail with a serialization error
 * (40001), and the retry reads the winner's committed rows. The unique
 * constraints on people.userId, organizations.slug and
 * memberships(organizationId, personId) refuse duplicates outright.
 */
const maxAttempts = 5;

export async function ensurePersonalOrganization(userId: string, name: string, email: string | null = null) {
  const slug = `personal-${userId}`;
  for (let attempt = 1; ; attempt++) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      const person =
        (await client.query(`SELECT id FROM people WHERE "userId" = $1`, [userId])).rows[0] ??
        (await client.query(`INSERT INTO people (id, "userId", name) VALUES ($1, $2, $3) RETURNING id`, [randomUUID(), userId, name])).rows[0];
      const organization =
        (await client.query(`SELECT id, name FROM organizations WHERE slug = $1`, [slug])).rows[0] ??
        (await client.query(`INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3) RETURNING id, name`, [randomUUID(), `${name}'s label`, slug])).rows[0];
      const membership = (
        await client.query(`SELECT role FROM memberships WHERE "organizationId" = $1 AND "personId" = $2`, [organization.id, person.id])
      ).rows[0];
      if (!membership) {
        await client.query(
          `INSERT INTO memberships (id, "organizationId", "personId", "userId", role) VALUES ($1, $2, $3, $4, 'admin')`,
          [randomUUID(), organization.id, person.id, userId],
        );
      } else if (membership.role !== "admin") {
        throw new Error("Bootstrap found an invalid personal organization membership");
      }
      // Record the verified sign-in email so admins can add this person by
      // email. Only this trusted path writes the table.
      if (email) {
        await client.query(
          `INSERT INTO private."personEmails" (id, "personId", email) VALUES ($1, $2, $3)
           ON CONFLICT ("personId") DO UPDATE SET email = EXCLUDED.email`,
          [randomUUID(), person.id, normalizeEmail(email)],
        );
      }
      await client.query("COMMIT");
      return organization as { id: string; name: string };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      const code = (error as { code?: string }).code;
      // 40001: a concurrent bootstrap won; 23505: it committed the same row first.
      if (!(code === "40001" || code === "23505") || attempt >= maxAttempts) throw error;
    } finally {
      client.release();
    }
  }
}
