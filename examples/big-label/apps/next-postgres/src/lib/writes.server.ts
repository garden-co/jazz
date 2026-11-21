// Writes: POST /api/rpc/<name>. `insert`, `update` and `delete` are generic
// single-row writes, checked per table against the README's roles (the same
// rules as the Jazz app's permissions.ts). `saveRelease` and the deletes are
// the app's multi-row writes. Each runs in one transaction and NOTIFYs which
// tables changed: the label's subscribers through `org:<id>`, and a person's
// label list through their `me` topic.
import { z } from "zod";
import { tables, type Membership, type TableName } from "../../schema";
import { one, rows, type Tx } from "./db.server";
import { assertAdmin, assertEditor, Forbidden, isInvitableRole, isRole } from "./permissions.server";

const org = (id: string) => `org:${id}`;
const me = (userId: string) => `me:${userId}`;

type Role = "admin" | "editor" | "never";
type Policy = { insert: Role; update: Role; delete: Role };
// Rows of the label that admins or editors may write (see src/roles.ts).
const policies: Partial<Record<TableName, Policy>> = {
  teams: { insert: "admin", update: "admin", delete: "admin" },
  teamAssignments: { insert: "admin", update: "admin", delete: "admin" },
  artists: { insert: "editor", update: "editor", delete: "admin" },
  catalogues: { insert: "admin", update: "admin", delete: "admin" },
  releases: { insert: "editor", update: "editor", delete: "admin" },
  releaseTeams: { insert: "editor", update: "never", delete: "editor" },
  releaseAssignments: { insert: "admin", update: "admin", delete: "admin" },
  memberships: { insert: "admin", update: "admin", delete: "admin" },
};

async function assertAllowed(tx: Tx, me: string, role: Role, organizationId: string) {
  if (role === "never") throw new Forbidden();
  await (role === "admin" ? assertAdmin : assertEditor)(tx, me, organizationId);
}

/** Only the table's own columns; ids and the tenant are never rewritten. */
function fields(table: TableName, row: Record<string, unknown>, immutable: string[]) {
  const allowed = (tables[table].columns as readonly string[]).filter((c) => !immutable.includes(c));
  const extra = Object.keys(row).filter((key) => !allowed.includes(key));
  if (extra.length) throw new Forbidden(`not writable: ${extra.join(", ")}`);
  return row;
}

function insertRow(tx: Tx, table: string, row: Record<string, unknown>) {
  const cols = Object.keys(row);
  return tx.query(
    `INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})`,
    Object.values(row),
  );
}

function updateRow(tx: Tx, table: string, id: string, patch: Record<string, unknown>) {
  const cols = Object.keys(patch);
  if (!cols.length) return;
  return tx.query(`UPDATE "${table}" SET ${cols.map((c, i) => `"${c}" = $${i + 2}`).join(", ")} WHERE id = $1`, [id, ...Object.values(patch)]);
}

/** Everyone in a label, for changes that show in their label list. */
async function tellMembers(tx: Tx, organizationId: string, ...changed: string[]) {
  for (const { userId } of await rows<{ userId: string }>(tx, `SELECT "userId" FROM memberships WHERE "organizationId" = $1`, [organizationId]))
    tx.changed(me(userId), ...changed);
}

/** A membership must name the person it belongs to, with a role the app knows. */
async function checkMembership(tx: Tx, row: Partial<Membership>, inserting: boolean) {
  if (row.role !== undefined && !(inserting ? isInvitableRole(row.role) : isRole(row.role))) throw new Forbidden();
  if (inserting && !(await one(tx, `SELECT 1 FROM people WHERE id = $1 AND "userId" = $2`, [row.personId, row.userId]))) throw new Forbidden();
}

const write = z.object({ table: z.enum(Object.keys(tables) as [TableName, ...TableName[]]), id: z.string(), row: z.record(z.string(), z.unknown()).optional() });

export const handlers: Record<string, (tx: Tx, me: string, args: unknown) => Promise<unknown>> = {
  async insert(tx, account, args) {
    const { table, id, row = {} } = write.parse(args);
    const policy = policies[table];
    const organizationId = row.organizationId as string;
    if (!policy || typeof organizationId !== "string") throw new Forbidden();
    await assertAllowed(tx, account, policy.insert, organizationId);
    if (table === "memberships") await checkMembership(tx, row as Partial<Membership>, true);
    await insertRow(tx, table, { ...fields(table, row, ["id"]), id });
    tx.changed(org(organizationId), table);
    if (table === "memberships") tx.changed(me(row.userId as string), "memberships");
  },

  async update(tx, account, args) {
    const { table, id, row = {} } = write.parse(args);
    if (table === "organizations") {
      await assertAdmin(tx, account, id);
      await updateRow(tx, table, id, fields(table, row, ["id", "slug"]));
      tx.changed(org(id), table);
      return tellMembers(tx, id, "organizations");
    }
    if (table === "people") {
      // Your own profile only.
      const person = await one<{ id: string }>(tx, `SELECT id FROM people WHERE id = $1 AND "userId" = $2`, [id, account]);
      if (!person) throw new Forbidden();
      await updateRow(tx, table, id, fields(table, row, ["id", "userId"]));
      tx.changed(me(account), "people");
      for (const { organizationId } of await rows<{ organizationId: string }>(tx, `SELECT "organizationId" FROM memberships WHERE "personId" = $1`, [id]))
        tx.changed(org(organizationId), "people");
      return;
    }
    const policy = policies[table];
    const existing = policy && (await one<Record<string, string>>(tx, `SELECT * FROM "${table}" WHERE id = $1 FOR UPDATE`, [id]));
    if (!policy || !existing) throw new Forbidden();
    await assertAllowed(tx, account, policy.update, existing.organizationId!);
    if (table === "memberships") await checkMembership(tx, row as Partial<Membership>, false);
    await updateRow(tx, table, id, fields(table, row, ["id", "organizationId", ...(table === "memberships" ? ["personId", "userId"] : [])]));
    tx.changed(org(existing.organizationId!), table);
    if (table === "memberships") tx.changed(me(existing.userId!), "memberships");
  },

  async delete(tx, account, args) {
    const { table, id } = write.parse(args);
    const policy = policies[table];
    const existing = policy && (await one<Record<string, string>>(tx, `SELECT * FROM "${table}" WHERE id = $1`, [id]));
    if (!policy || !existing) throw new Forbidden();
    await assertAllowed(tx, account, policy.delete, existing.organizationId!);
    // Assignments go with their release, team or member (ON DELETE CASCADE);
    // an artist with releases is refused (ON DELETE RESTRICT).
    await tx.query(`DELETE FROM "${table}" WHERE id = $1`, [id]);
    tx.changed(org(existing.organizationId!), table, "teamAssignments", "releaseTeams", "releaseAssignments");
    if (table === "memberships") {
      tx.changed(me(existing.userId!), "memberships");
      tx.revoke(existing.userId!, org(existing.organizationId!));
    }
  },

  /**
   * Inserts or updates a release. Catalogue numbers are unique per label: a
   * UNIQUE constraint holds the line, and this serializable transaction reads
   * the number's holders first, to answer with a clear error.
   */
  async saveRelease(tx, account, args) {
    const { organizationId, id, isNew, fields: row } = z
      .object({ organizationId: z.string(), id: z.string(), isNew: z.boolean(), fields: z.record(z.string(), z.unknown()) })
      .parse(args);
    await assertEditor(tx, account, organizationId);
    const holders = await rows<{ id: string }>(tx, `SELECT id FROM releases WHERE "organizationId" = $1 AND "catalogNumber" = $2`, [organizationId, row.catalogNumber]);
    if (holders.some((holder) => holder.id !== id)) throw new Conflict("catalog_number_taken", `${row.catalogNumber} is already in use`);
    const values = fields("releases", row, ["id", "organizationId"]);
    if (isNew) await insertRow(tx, "releases", { ...values, id, organizationId });
    else {
      const updated = await tx.query(`SELECT 1 FROM releases WHERE id = $1 AND "organizationId" = $2`, [id, organizationId]);
      if (!updated.rowCount) throw new Forbidden();
      await updateRow(tx, "releases", id, values);
    }
    tx.changed(org(organizationId), "releases");
  },
};

/** Which handlers run serializable (they read before they write). */
export const serializable = new Set(["saveRelease"]);

export class Conflict extends Error {
  constructor(
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

/**
 * Postgres constraint errors, as the app's answers: a duplicate, a delete
 * that would orphan rows, or a reference into another label (refused).
 */
export function asConflict(error: unknown, call: string) {
  const code = (error as { code?: string }).code;
  if (code === "23505") return new Conflict("already_exists", "That is already in use");
  if ((code === "23503" || code === "23001") && call === "delete") return new Conflict("still_referenced", "Delete this artist's releases first");
  if (code === "23503") return new Forbidden("permission_denied: not in this label");
  return undefined;
}
