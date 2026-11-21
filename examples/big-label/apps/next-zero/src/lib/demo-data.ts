import { createHash } from "node:crypto";
import { zql } from "../../schema";
import { dbProvider } from "./db.server";
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

  return dbProvider.transaction(async (tx) => {
    const caller = await tx.run(zql.people.where("userId", userId).one());
    if (!caller) throw new Error("Bootstrap the personal label before loading demo data");

    const firstOrganization = id(fixture.organizations[0]!.id);
    if (await tx.run(zql.organizations.where("id", firstOrganization).one())) {
      return { created: false, organizationIds: fixture.organizations.map((org) => id(org.id)) };
    }

    for (const org of fixture.organizations) {
      await tx.mutate.organizations.insert({ id: id(org.id), name: org.name, slug: `demo-${profile}-${id(org.id)}` });
      await tx.mutate.memberships.insert({ id: id(`${org.id}:caller`), organizationId: id(org.id), personId: caller.id, userId, role: "admin" });
    }
    for (const person of fixture.people) {
      await tx.mutate.people.insert({ id: id(person.id), userId: id(person.userId), name: person.name });
    }
    for (const membership of fixture.memberships) {
      await tx.mutate.memberships.insert({ id: id(membership.id), organizationId: id(membership.organizationId),
          personId: id(membership.personId),
          userId: id(membership.userId),
          role: membership.role });
    }
    for (const team of fixture.teams) {
      await tx.mutate.teams.insert({ id: id(team.id), organizationId: id(team.organizationId), name: team.name });
    }
    for (const assignment of fixture.teamAssignments) {
      await tx.mutate.teamAssignments.insert({ id: id(assignment.id), organizationId: id(assignment.organizationId),
          teamId: id(assignment.teamId),
          membershipId: id(assignment.membershipId),
          role: assignment.role });
    }
    for (const catalogue of fixture.catalogues) {
      await tx.mutate.catalogues.insert({ id: id(catalogue.id), organizationId: id(catalogue.organizationId),
          name: catalogue.name,
          code: catalogue.code });
    }
    for (const { id: artistId, ...artist } of fixture.artists) {
      await tx.mutate.artists.insert({ id: id(artistId), ...artist, organizationId: id(artist.organizationId) });
    }
    for (const { id: releaseId, ...release } of fixture.releases) {
      await tx.mutate.releases.insert({ id: id(releaseId), ...release,
          organizationId: id(release.organizationId),
          artistId: id(release.artistId),
          catalogueId: id(release.catalogueId),
          releaseDate: new Date(release.releaseDate).getTime() });
    }
    for (const releaseTeam of fixture.releaseTeams) {
      await tx.mutate.releaseTeams.insert({ id: id(releaseTeam.id), organizationId: id(releaseTeam.organizationId),
          releaseId: id(releaseTeam.releaseId),
          teamId: id(releaseTeam.teamId) });
    }
    return { created: true, organizationIds: fixture.organizations.map((org) => id(org.id)) };
  });
}

/** A name-based UUID (version 5 layout over SHA-256), stable across runs. */
function stableUuid(name: string) {
  const bytes = createHash("sha256").update(`big-label-demo:${name}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
