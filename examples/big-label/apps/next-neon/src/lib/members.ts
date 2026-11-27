import "server-only";
import { randomUUID } from "node:crypto";
import type pg from "pg";
import { invitableRoles } from "../roles";
import { normalizeEmail } from "./emails";

/**
 * Looks up who signed in with `email`. Emails are private: only this server
 * code reads person_emails (no live query does).
 */
export async function findPersonByEmail(db: pg.Pool | pg.PoolClient, email: string) {
  const { rows } = await db.query<{ id: string; userId: string; name: string }>(
    `SELECT p.id, p.user_id AS "userId", p.name FROM person_emails e JOIN people p ON p.id = e.person_id WHERE e.email = $1`,
    [normalizeEmail(email)],
  );
  return rows.length === 1 ? rows[0]! : null;
}

/** Whether `userId` administers the label: the route answers only admins. */
export async function isAdmin(db: pg.Pool | pg.PoolClient, organizationId: string, userId: string) {
  const { rows } = await db.query(`SELECT 1 FROM memberships WHERE organization_id = $1 AND user_id = $2 AND role = 'admin'`, [
    organizationId,
    userId,
  ]);
  return rows.length > 0;
}

export type AddMemberStatus = "added" | "already-member" | "forbidden";

/**
 * Adds a person to a label for `caller`: only the label's admins add members,
 * and only as editors or viewers (admins are promoted later, never inserted).
 * One transaction; the unique (organization_id, person_id) constraint makes a
 * double submit add them once.
 */
export async function addMember(
  pool: pg.Pool,
  caller: string,
  input: { organizationId: string; person: { id: string; userId: string }; role: string },
): Promise<AddMemberStatus> {
  if (!(invitableRoles as readonly string[]).includes(input.role)) return "forbidden";
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (!(await isAdmin(client, input.organizationId, caller))) {
      await client.query("ROLLBACK");
      return "forbidden";
    }
    const { rowCount } = await client.query(
      `INSERT INTO memberships (id, organization_id, person_id, user_id, role) VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (organization_id, person_id) DO NOTHING`,
      [randomUUID(), input.organizationId, input.person.id, input.person.userId, input.role],
    );
    await client.query("COMMIT");
    return rowCount ? "added" : "already-member";
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
