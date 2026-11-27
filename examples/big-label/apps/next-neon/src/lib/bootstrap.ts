import "server-only";
import { randomUUID } from "node:crypto";
import { pool } from "./db.server";
import { normalizeEmail } from "./emails";

/**
 * Server-mediated first-tenant bootstrap (POST /api/bootstrap): one profile,
 * one `personal-<user id>` label and its admin membership per Managed Better
 * Auth user, plus their verified sign-in email for adding them by email.
 * Browsers never create labels or people.
 *
 * One transaction. The unique constraints (people.user_id,
 * organizations.slug, memberships (organization_id, person_id),
 * person_emails.person_id) make every insert get-or-create: a concurrent
 * bootstrap waits on the other's insert, then reads the row it committed. A
 * personal membership that isn't an admin's is an explicit failure.
 */
export async function ensurePersonalOrganization(userId: string, name: string, email: string | null = null) {
  const slug = `personal-${userId}`;
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`INSERT INTO people (id, user_id, name) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING`, [
      randomUUID(),
      userId,
      name,
    ]);
    const person = (await client.query<{ id: string }>(`SELECT id FROM people WHERE user_id = $1`, [userId])).rows[0]!;
    await client.query(`INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3) ON CONFLICT (slug) DO NOTHING`, [
      randomUUID(),
      `${name}'s label`,
      slug,
    ]);
    const organization = (await client.query<{ id: string; name: string }>(`SELECT id, name FROM organizations WHERE slug = $1`, [slug]))
      .rows[0]!;
    // Record the verified sign-in email so admins can add this person by
    // email. Only this trusted path writes the table, only the member lookup reads it.
    if (email)
      await client.query(
        `INSERT INTO person_emails (id, person_id, email) VALUES ($1, $2, $3)
         ON CONFLICT (person_id) DO UPDATE SET email = EXCLUDED.email WHERE person_emails.email <> EXCLUDED.email`,
        [randomUUID(), person.id, normalizeEmail(email)],
      );
    await client.query(
      `INSERT INTO memberships (id, organization_id, person_id, user_id, role) VALUES ($1, $2, $3, $4, 'admin')
       ON CONFLICT (organization_id, person_id) DO NOTHING`,
      [randomUUID(), organization.id, person.id, userId],
    );
    const membership = (
      await client.query<{ role: string }>(`SELECT role FROM memberships WHERE organization_id = $1 AND person_id = $2`, [
        organization.id,
        person.id,
      ])
    ).rows[0];
    if (membership?.role !== "admin") throw new Error("Bootstrap found an invalid personal organization membership");
    await client.query("COMMIT");
    return organization;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
