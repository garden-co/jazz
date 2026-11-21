import "server-only";
import { createHash } from "node:crypto";
import type pg from "pg";
import { pool } from "./db.server";
import { createFixture, type FixtureProfile } from "../fixtures";

export const demoProfiles = ["smoke", "small"] as const satisfies readonly FixtureProfile[];
export type DemoProfile = (typeof demoProfiles)[number];

export function isDemoProfile(value: unknown): value is DemoProfile {
  return (demoProfiles as readonly unknown[]).includes(value);
}

const snake = (key: string) => key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);
/** Inserts rows (camelCase keys, as the fixture has them) into a table in one statement. */
async function insertRows(client: pg.PoolClient, table: string, rows: Record<string, unknown>[]) {
  if (rows.length === 0) return;
  const json = rows.map((row) => Object.fromEntries(Object.entries(row).map(([k, v]) => [snake(k), v])));
  await client.query(`INSERT INTO ${table} SELECT * FROM json_populate_recordset(null::${table}, $1)`, [JSON.stringify(json)]);
}

/**
 * Loads a deterministic `createFixture(profile)` slice as new demo labels,
 * with the caller as the admin of each one. Like the personal bootstrap, this
 * is a trusted server path: browsers can never create organizations.
 *
 * Fixture IDs are mapped to UUIDs derived from the caller and the fixture ID,
 * so loading the same profile twice is a no-op and two callers never share
 * demo tenants. One transaction.
 */
export async function loadDemoData(userId: string, profile: DemoProfile, seed = 17) {
  const fixture = createFixture(profile, seed);
  const id = (fixtureId: string) => stableUuid(`${userId}:${profile}:${seed}:${fixtureId}`);
  const organizationIds = fixture.organizations.map((org) => id(org.id));

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const caller = (await client.query<{ id: string }>(`SELECT id FROM people WHERE user_id = $1`, [userId])).rows[0];
    if (!caller) throw new Error("Bootstrap the personal label before loading demo data");
    // Taking the first label's id serializes concurrent loads of the same profile.
    const { rowCount } = await client.query(
      `INSERT INTO organizations (id, name, slug) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`,
      [organizationIds[0], fixture.organizations[0]!.name, `demo-${profile}-${organizationIds[0]}`],
    );
    if (!rowCount) {
      await client.query("ROLLBACK");
      return { created: false, organizationIds };
    }
    await insertRows(
      client,
      "organizations",
      fixture.organizations.slice(1).map((org) => ({ id: id(org.id), name: org.name, slug: `demo-${profile}-${id(org.id)}` })),
    );
    // The synthetic members are profiles without accounts (and no email).
    await insertRows(client, "people", fixture.people.map((p) => ({ id: id(p.id), userId: id(p.userId), name: p.name })));
    await insertRows(client, "memberships", [
      ...fixture.organizations.map((org) => ({
        id: id(`${org.id}:caller`),
        organizationId: id(org.id),
        personId: caller.id,
        userId,
        role: "admin",
      })),
      ...fixture.memberships.map((m) => ({
        ...m,
        id: id(m.id),
        organizationId: id(m.organizationId),
        personId: id(m.personId),
        userId: id(m.userId),
      })),
    ]);
    const inLabel = <T extends { id: string; organizationId: string }>(row: T) => ({
      ...row,
      id: id(row.id),
      organizationId: id(row.organizationId),
    });
    await insertRows(client, "teams", fixture.teams.map(inLabel));
    await insertRows(
      client,
      "team_assignments",
      fixture.teamAssignments.map((a) => ({ ...inLabel(a), teamId: id(a.teamId), membershipId: id(a.membershipId) })),
    );
    await insertRows(client, "catalogues", fixture.catalogues.map(inLabel));
    await insertRows(client, "artists", fixture.artists.map(inLabel));
    await insertRows(
      client,
      "releases",
      fixture.releases.map((r) => ({
        ...inLabel(r),
        artistId: id(r.artistId),
        catalogueId: id(r.catalogueId),
        releaseDate: Date.parse(r.releaseDate),
      })),
    );
    await insertRows(
      client,
      "release_teams",
      fixture.releaseTeams.map((rt) => ({ ...inLabel(rt), releaseId: id(rt.releaseId), teamId: id(rt.teamId) })),
    );
    await client.query("COMMIT");
    return { created: true, organizationIds };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/** A name-based UUID (version 5 layout over SHA-256), stable across runs. */
function stableUuid(name: string) {
  const bytes = createHash("sha256").update(`big-label-demo:${name}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
