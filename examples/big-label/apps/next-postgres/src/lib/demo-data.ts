import { createHash } from "node:crypto";
import { one, transaction, type Tx } from "./db.server";
import { createFixture, type FixtureProfile } from "../fixtures";

export const demoProfiles = ["smoke", "small"] as const satisfies readonly FixtureProfile[];
export type DemoProfile = (typeof demoProfiles)[number];

export function isDemoProfile(value: unknown): value is DemoProfile {
  return (demoProfiles as readonly unknown[]).includes(value);
}

/**
 * Loads a deterministic `createFixture(profile)` slice as new demo labels,
 * with the caller as the admin of each one. Like the personal bootstrap, this
 * is a trusted server path: browsers can never create organizations.
 *
 * Fixture IDs are mapped to UUIDs derived from the caller and the fixture ID,
 * so loading the same profile twice is a no-op and two callers never share
 * demo tenants.
 */
export async function loadDemoData(userId: string, profile: DemoProfile, seed = 17) {
  const fixture = createFixture(profile, seed);
  const id = (fixtureId: string) => stableUuid(`${userId}:${profile}:${seed}:${fixtureId}`);
  const organizationIds = fixture.organizations.map((org) => id(org.id));

  return transaction(async (tx) => {
    const caller = await one<{ id: string }>(tx, `SELECT id FROM people WHERE "userId" = $1`, [userId]);
    if (!caller) throw new Error("Bootstrap the personal label before loading demo data");
    if (await one(tx, `SELECT 1 FROM organizations WHERE id = $1`, [organizationIds[0]]))
      return { created: false, organizationIds };

    const insert = (table: string, rows: Record<string, unknown>[]) => insertAll(tx, table, rows);
    await insert("organizations", fixture.organizations.map((org) => ({ id: id(org.id), name: org.name, slug: `demo-${profile}-${id(org.id)}` })));
    await insert("people", fixture.people.map((person) => ({ id: id(person.id), userId: id(person.userId), name: person.name })));
    await insert("memberships", [
      ...fixture.organizations.map((org) => ({ id: id(`${org.id}:caller`), organizationId: id(org.id), personId: caller.id, userId, role: "admin" })),
      ...fixture.memberships.map((m) => ({ id: id(m.id), organizationId: id(m.organizationId), personId: id(m.personId), userId: id(m.userId), role: m.role })),
    ]);
    await insert("teams", fixture.teams.map((team) => ({ id: id(team.id), organizationId: id(team.organizationId), name: team.name })));
    await insert("teamAssignments", fixture.teamAssignments.map((a) => ({ id: id(a.id), organizationId: id(a.organizationId), teamId: id(a.teamId), membershipId: id(a.membershipId), role: a.role })));
    await insert("catalogues", fixture.catalogues.map((c) => ({ id: id(c.id), organizationId: id(c.organizationId), name: c.name, code: c.code })));
    await insert("artists", fixture.artists.map(({ id: artistId, ...artist }) => ({ ...artist, id: id(artistId), organizationId: id(artist.organizationId) })));
    await insert("releases", fixture.releases.map(({ id: releaseId, ...release }) => ({
      ...release,
      id: id(releaseId),
      organizationId: id(release.organizationId),
      artistId: id(release.artistId),
      catalogueId: id(release.catalogueId),
    })));
    await insert("releaseTeams", fixture.releaseTeams.map((rt) => ({ id: id(rt.id), organizationId: id(rt.organizationId), releaseId: id(rt.releaseId), teamId: id(rt.teamId) })));
    tx.changed(`me:${userId}`, "memberships");
    return { created: true, organizationIds };
  });
}

/** Bulk insert: the rows as one JSON array, expanded by Postgres. */
function insertAll(tx: Tx, table: string, rows: Record<string, unknown>[]) {
  return tx.query(`INSERT INTO "${table}" SELECT * FROM json_populate_recordset(null::"${table}", $1)`, [JSON.stringify(rows)]);
}

/** A name-based UUID (version 5 layout over SHA-256), stable across runs. */
function stableUuid(name: string) {
  const bytes = createHash("sha256").update(`big-label-demo:${name}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
