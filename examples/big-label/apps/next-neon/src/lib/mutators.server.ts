import "server-only";
import type pg from "pg";
import { z } from "zod";
import { catalogueEditors, roles } from "../roles";

/**
 * Who can do what in BigLabel (reads are filtered in live-queries.server.ts),
 * the README's roles, the same rules as the Jazz app's permissions.ts:
 *
 * - admin: everything: members and roles, teams and their members,
 *   catalogues, renaming the label, deleting artists and releases.
 * - editor: add and edit artists and releases, assign releases to teams.
 * - viewer: read only.
 * - Nobody creates labels or people here: labels come from the bootstrap and
 *   demo-data routes, members from POST /api/members (admins only, never as
 *   admins; see members.ts).
 * - Every reference stays inside one label: release → artist/catalogue,
 *   team assignment → team/membership, release team → release/team. The
 *   handlers check the caller's role in the label the row belongs to, and the
 *   composite foreign keys on (organization_id, id) refuse a reference into
 *   another label, whoever writes it.
 * - Catalogue numbers are unique per label: the unique index
 *   (organization_id, catalog_number) decides, inside the write's
 *   transaction, so concurrent saves can't both take a number.
 *
 * Every write goes through these handlers (called from the Server Action in
 * app/actions.ts), as Neon's mutations guide does it: validate, authorize,
 * write in one transaction and return the transaction's id.
 */
export class Forbidden extends Error {
  constructor(message: string) {
    super(`permission_denied: ${message}`);
  }
}

export type Tx = {
  me: string;
  rows<T>(sql: string, params?: unknown[]): Promise<T[]>;
  /** A write; its RETURNING carries the transaction id, which the response returns. */
  write(sql: string, params?: unknown[]): Promise<number>;
};
const one = async <T = unknown>(tx: Tx, sql: string, params: unknown[]) => (await tx.rows<T>(sql, params))[0];

async function assertRole(tx: Tx, organizationId: string, allowed: readonly string[]) {
  const row = await one(tx, `SELECT 1 FROM memberships WHERE organization_id = $1 AND user_id = $2 AND role = ANY($3)`, [
    organizationId,
    tx.me,
    allowed,
  ]);
  if (!row) throw new Forbidden("your role in this label doesn't allow that");
}
const assertAdmin = (tx: Tx, organizationId: string) => assertRole(tx, organizationId, ["admin"]);
const assertEditor = (tx: Tx, organizationId: string) => assertRole(tx, organizationId, catalogueEditors);

/** The label a row belongs to, or undefined when it doesn't exist. */
async function labelOf(tx: Tx, table: string, id: string) {
  return (await one<{ organization_id: string }>(tx, `SELECT organization_id FROM ${table} WHERE id = $1`, [id]))
    ?.organization_id;
}

const searchKey = (...parts: string[]) => parts.join(" ").toLowerCase();
/** The trailing digits of a catalogue number: 13 for "NFC-013". */
export function catalogSequence(catalogNumber: string) {
  const digits = catalogNumber.match(/(\d+)$/)?.[1];
  return digits === undefined ? null : Number(digits);
}

const id = z.string().min(1);
const name = z.string().trim().min(1);
const artistFields = { name, genre: z.string().min(1), status: z.string().min(1) };
const releaseFields = {
  title: name,
  artistId: id,
  catalogueId: id.nullable(),
  catalogNumber: z.string().trim().min(1),
  format: z.string().min(1),
  releaseDate: z.number(),
  status: z.string().min(1),
};

type Mutator = { args: z.ZodType; run: (tx: Tx, args: never) => Promise<unknown> };
const mutator = <S extends z.ZodType>(args: S, run: (tx: Tx, args: z.infer<S>) => Promise<unknown>): Mutator => ({
  args,
  run: run as Mutator["run"],
});

/** Postgres refused a write for a constraint; the handlers turn these into the app's errors. */
const pgCode = (error: unknown) => (error as { code?: string }).code;

export const mutators: Record<string, Mutator> = {
  "organizations.rename": mutator(z.object({ id, name }), async (tx, args) => {
    await assertAdmin(tx, args.id);
    await tx.write(`UPDATE organizations SET name = $2 WHERE id = $1`, [args.id, args.name]);
  }),

  "artists.create": mutator(z.object({ id, organizationId: id, ...artistFields }), async (tx, a) => {
    await assertEditor(tx, a.organizationId);
    await tx.write(
      `INSERT INTO artists (id, organization_id, name, genre, status, search_key) VALUES ($1, $2, $3, $4, $5, $6)`,
      [a.id, a.organizationId, a.name, a.genre, a.status, searchKey(a.name, a.genre)],
    );
  }),
  "artists.update": mutator(z.object({ id, ...artistFields }), async (tx, a) => {
    const organizationId = await labelOf(tx, "artists", a.id);
    if (!organizationId) return;
    await assertEditor(tx, organizationId);
    await tx.write(`UPDATE artists SET name = $2, genre = $3, status = $4, search_key = $5 WHERE id = $1`, [
      a.id,
      a.name,
      a.genre,
      a.status,
      searchKey(a.name, a.genre),
    ]);
  }),
  /** Refused while the artist has releases (the releases' foreign key, NO ACTION). */
  "artists.delete": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "artists", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    try {
      await tx.write(`DELETE FROM artists WHERE id = $1`, [a.id]);
    } catch (error) {
      if (pgCode(error) === "23503") throw new Error("Delete this artist's releases first");
      throw error;
    }
  }),

  /**
   * Inserts or updates a release (the release form). The catalogue number must
   * be free in the label: the unique index decides inside this transaction,
   * so of two concurrent saves of one number, one is refused.
   */
  "releases.save": mutator(
    z.object({ id, isNew: z.boolean(), organizationId: id, ...releaseFields }),
    async (tx, r) => {
      if (!r.isNew && (await labelOf(tx, "releases", r.id)) !== r.organizationId)
        throw new Forbidden("that release isn't in this label");
      await assertEditor(tx, r.organizationId);
      const values = [
        r.id,
        r.organizationId,
        r.artistId,
        r.catalogueId,
        r.catalogNumber,
        catalogSequence(r.catalogNumber),
        r.title,
        r.format,
        r.releaseDate,
        r.status,
        searchKey(r.title, r.catalogNumber),
      ];
      try {
        await tx.write(
          r.isNew
            ? `INSERT INTO releases (id, organization_id, artist_id, catalogue_id, catalog_number, catalog_sequence, title,
                 format, release_date, status, search_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
            : `UPDATE releases SET artist_id = $3, catalogue_id = $4, catalog_number = $5, catalog_sequence = $6, title = $7,
                 format = $8, release_date = $9, status = $10, search_key = $11 WHERE id = $1 AND organization_id = $2`,
          values,
        );
      } catch (error) {
        if (pgCode(error) === "23505") throw new Error(`${r.catalogNumber} is already in use`);
        if (pgCode(error) === "23503") throw new Forbidden("the artist and catalogue must be in this label");
        throw error;
      }
    },
  ),
  /** Deletes a release; its team and per-person assignments go with it (ON DELETE CASCADE). */
  "releases.delete": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "releases", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`DELETE FROM releases WHERE id = $1`, [a.id]);
  }),

  /** Editors staff releases with teams; release and team must be in the label (foreign keys). */
  "releaseTeams.add": mutator(z.object({ id, organizationId: id, releaseId: id, teamId: id }), async (tx, a) => {
    await assertEditor(tx, a.organizationId);
    try {
      await tx.write(`INSERT INTO release_teams (id, organization_id, release_id, team_id) VALUES ($1, $2, $3, $4)`, [
        a.id,
        a.organizationId,
        a.releaseId,
        a.teamId,
      ]);
    } catch (error) {
      if (pgCode(error) === "23503") throw new Forbidden("the release and team must be in this label");
      throw error;
    }
  }),
  "releaseTeams.remove": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "release_teams", a.id);
    if (!organizationId) return;
    await assertEditor(tx, organizationId);
    await tx.write(`DELETE FROM release_teams WHERE id = $1`, [a.id]);
  }),

  "teams.create": mutator(z.object({ id, organizationId: id, name }), async (tx, a) => {
    await assertAdmin(tx, a.organizationId);
    await tx.write(`INSERT INTO teams (id, organization_id, name) VALUES ($1, $2, $3)`, [a.id, a.organizationId, a.name]);
  }),
  "teams.rename": mutator(z.object({ id, name }), async (tx, a) => {
    const organizationId = await labelOf(tx, "teams", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`UPDATE teams SET name = $2 WHERE id = $1`, [a.id, a.name]);
  }),
  /** Deletes a team; its memberships and release assignments go with it (ON DELETE CASCADE). */
  "teams.delete": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "teams", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`DELETE FROM teams WHERE id = $1`, [a.id]);
  }),

  /** Admins staff teams; team and membership must be in the label (foreign keys). */
  "teamAssignments.add": mutator(
    z.object({ id, organizationId: id, teamId: id, membershipId: id, role: z.enum(["lead", "member"]) }),
    async (tx, a) => {
      await assertAdmin(tx, a.organizationId);
      try {
        await tx.write(
          `INSERT INTO team_assignments (id, organization_id, team_id, membership_id, role) VALUES ($1, $2, $3, $4, $5)`,
          [a.id, a.organizationId, a.teamId, a.membershipId, a.role],
        );
      } catch (error) {
        if (pgCode(error) === "23503") throw new Forbidden("the team and member must be in this label");
        throw error;
      }
    },
  ),
  "teamAssignments.remove": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "team_assignments", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`DELETE FROM team_assignments WHERE id = $1`, [a.id]);
  }),

  "catalogues.create": mutator(z.object({ id, organizationId: id, name, code: name }), async (tx, a) => {
    await assertAdmin(tx, a.organizationId);
    await tx.write(`INSERT INTO catalogues (id, organization_id, name, code) VALUES ($1, $2, $3, $4)`, [
      a.id,
      a.organizationId,
      a.name,
      a.code.toUpperCase(),
    ]);
  }),
  "catalogues.update": mutator(z.object({ id, name, code: name }), async (tx, a) => {
    const organizationId = await labelOf(tx, "catalogues", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`UPDATE catalogues SET name = $2, code = $3 WHERE id = $1`, [a.id, a.name, a.code.toUpperCase()]);
  }),

  /** Admins change roles, to any role (promoting to admin happens here, never on insert). */
  "memberships.setRole": mutator(z.object({ id, role: z.enum(roles) }), async (tx, a) => {
    const organizationId = await labelOf(tx, "memberships", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`UPDATE memberships SET role = $2 WHERE id = $1`, [a.id, a.role]);
  }),
  /** Removes a member; their team and release assignments go with it (ON DELETE CASCADE). */
  "memberships.remove": mutator(z.object({ id }), async (tx, a) => {
    const organizationId = await labelOf(tx, "memberships", a.id);
    if (!organizationId) return;
    await assertAdmin(tx, organizationId);
    await tx.write(`DELETE FROM memberships WHERE id = $1`, [a.id]);
  }),
};

export type MutationResponse = { kind: "committed"; txid: string } | { kind: "unchanged" } | { kind: "rejected"; error: string };

/** Runs a mutator in one transaction and returns its txid, read from the writes' own RETURNING. */
export function runMutation(pool: pg.Pool, me: string, name: string, args: unknown): Promise<MutationResponse> {
  return runMutations(pool, me, [{ name, args }]);
}

/**
 * Runs several mutators, each with its own checks, in one transaction (the
 * seed writes a label's rows this way, as the Jazz seed's transactions do).
 */
export async function runMutations(pool: pg.Pool, me: string, writes: { name: string; args: unknown }[]): Promise<MutationResponse> {
  const runs: { def: Mutator; args: unknown }[] = [];
  for (const { name, args } of writes) {
    const def = mutators[name];
    if (!def) return { kind: "rejected", error: `No mutator ${name}` };
    const parsed = def.args.safeParse(args);
    if (!parsed.success) return { kind: "rejected", error: "invalid input" };
    runs.push({ def, args: parsed.data });
  }
  const client = await pool.connect();
  let txid: string | undefined;
  const tx: Tx = {
    me,
    rows: async (sql, params) => (await client.query(sql, params)).rows,
    write: async (sql, params) => {
      const result = await client.query<{ txid: string }>(`${sql} RETURNING pg_current_xact_id()::text AS txid`, params);
      txid ??= result.rows[0]?.txid;
      return result.rowCount ?? 0;
    },
  };
  try {
    await client.query("BEGIN");
    for (const { def, args } of runs) await def.run(tx, args as never);
    await client.query("COMMIT");
    return txid ? { kind: "committed", txid } : { kind: "unchanged" };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    return { kind: "rejected", error: error instanceof Error ? error.message : String(error) };
  } finally {
    client.release();
  }
}
