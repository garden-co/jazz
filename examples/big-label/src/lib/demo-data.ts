import { createHash } from "node:crypto";
import type { Db } from "jazz-tools";
import { app } from "../../schema";
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
export async function loadDemoData(db: Db, userId: string, profile: DemoProfile, seed = 17) {
  const fixture = createFixture(profile, seed);
  const id = (fixtureId: string) => stableUuid(`${userId}:${profile}:${seed}:${fixtureId}`);

  const caller = await db.one(app.people.where({ userId }));
  if (!caller) throw new Error("Bootstrap the personal label before loading demo data");

  const firstOrganization = id(fixture.organizations[0]!.id);
  if (await db.one(app.organizations.where({ id: firstOrganization }))) {
    return { created: false, organizationIds: fixture.organizations.map((org) => id(org.id)) };
  }

  const write = await db.transaction((tx) => {
    for (const org of fixture.organizations) {
      tx.insert(
        app.organizations,
        { name: org.name, slug: `demo-${profile}-${id(org.id)}` },
        { id: id(org.id) },
      );
      tx.insert(
        app.memberships,
        { organizationId: id(org.id), personId: caller.id, userId, role: "admin" },
        { id: id(`${org.id}:caller`) },
      );
    }
    for (const person of fixture.people) {
      tx.insert(
        app.people,
        { userId: id(person.userId), name: person.name },
        { id: id(person.id) },
      );
    }
    for (const membership of fixture.memberships) {
      tx.insert(
        app.memberships,
        {
          organizationId: id(membership.organizationId),
          personId: id(membership.personId),
          userId: id(membership.userId),
          role: membership.role,
        },
        { id: id(membership.id) },
      );
    }
    for (const team of fixture.teams) {
      tx.insert(
        app.teams,
        { organizationId: id(team.organizationId), name: team.name },
        { id: id(team.id) },
      );
    }
    for (const assignment of fixture.teamAssignments) {
      tx.insert(
        app.teamAssignments,
        {
          organizationId: id(assignment.organizationId),
          teamId: id(assignment.teamId),
          membershipId: id(assignment.membershipId),
          role: assignment.role,
        },
        { id: id(assignment.id) },
      );
    }
    for (const catalogue of fixture.catalogues) {
      tx.insert(
        app.catalogues,
        {
          organizationId: id(catalogue.organizationId),
          name: catalogue.name,
          code: catalogue.code,
        },
        { id: id(catalogue.id) },
      );
    }
    for (const { id: artistId, ...artist } of fixture.artists) {
      tx.insert(
        app.artists,
        { ...artist, organizationId: id(artist.organizationId) },
        { id: id(artistId) },
      );
    }
    for (const { id: releaseId, ...release } of fixture.releases) {
      tx.insert(
        app.releases,
        {
          ...release,
          organizationId: id(release.organizationId),
          artistId: id(release.artistId),
          catalogueId: id(release.catalogueId),
          releaseDate: new Date(release.releaseDate),
        },
        { id: id(releaseId) },
      );
    }
    for (const releaseTeam of fixture.releaseTeams) {
      tx.insert(
        app.releaseTeams,
        {
          organizationId: id(releaseTeam.organizationId),
          releaseId: id(releaseTeam.releaseId),
          teamId: id(releaseTeam.teamId),
        },
        { id: id(releaseTeam.id) },
      );
    }
  });
  await write.wait({ tier: "global" });
  return { created: true, organizationIds: fixture.organizations.map((org) => id(org.id)) };
}

/** A name-based UUID (version 5 layout over SHA-256), stable across runs. */
function stableUuid(name: string) {
  const bytes = createHash("sha256").update(`big-label-demo:${name}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
