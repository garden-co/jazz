import { createHash } from "node:crypto";
import { adminDb } from "./instant.server";
import { membershipFields } from "./membership";
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
  const { tx } = adminDb;

  const { people } = await adminDb.query({ people: { $: { where: { userId } } } });
  const caller = people[0];
  if (!caller) throw new Error("Bootstrap the personal label before loading demo data");

  const firstOrganization = id(fixture.organizations[0]!.id);
  const existing = await adminDb.query({
    organizations: { $: { where: { id: firstOrganization } } },
  });
  if (existing.organizations.length > 0) {
    return { created: false, organizationIds: fixture.organizations.map((org) => id(org.id)) };
  }

  const chunks = [];
  for (const org of fixture.organizations) {
    chunks.push(
      tx.organizations[id(org.id)].create({
        name: org.name,
        slug: `demo-${profile}-${id(org.id)}`,
      }),
      tx.memberships[id(`${org.id}:caller`)]
        .create(membershipFields(id(org.id), userId, "admin"))
        .link({ organization: id(org.id), person: caller.id }),
    );
  }
  for (const person of fixture.people)
    chunks.push(tx.people[id(person.id)].create({ userId: id(person.userId), name: person.name }));
  for (const membership of fixture.memberships) {
    const organizationId = id(membership.organizationId);
    chunks.push(
      tx.memberships[id(membership.id)]
        .create(membershipFields(organizationId, id(membership.userId), membership.role))
        .link({ organization: organizationId, person: id(membership.personId) }),
    );
  }
  for (const team of fixture.teams)
    chunks.push(
      tx.teams[id(team.id)]
        .create({ name: team.name })
        .link({ organization: id(team.organizationId) }),
    );
  for (const assignment of fixture.teamAssignments)
    chunks.push(
      tx.teamAssignments[id(assignment.id)].create({ role: assignment.role }).link({
        organization: id(assignment.organizationId),
        team: id(assignment.teamId),
        membership: id(assignment.membershipId),
      }),
    );
  for (const catalogue of fixture.catalogues)
    chunks.push(
      tx.catalogues[id(catalogue.id)]
        .create({ name: catalogue.name, code: catalogue.code })
        .link({ organization: id(catalogue.organizationId) }),
    );
  for (const { id: artistId, organizationId, ...artist } of fixture.artists)
    chunks.push(tx.artists[id(artistId)].create(artist).link({ organization: id(organizationId) }));
  for (const {
    id: releaseId,
    organizationId,
    artistId,
    catalogueId,
    ...release
  } of fixture.releases)
    chunks.push(
      tx.releases[id(releaseId)]
        .create({
          ...release,
          catalogKey: `${id(organizationId)}/${release.catalogNumber}`,
          releaseDate: new Date(release.releaseDate),
        })
        .link({
          organization: id(organizationId),
          artist: id(artistId),
          catalogue: id(catalogueId),
        }),
    );
  for (const releaseTeam of fixture.releaseTeams)
    chunks.push(
      tx.releaseTeams[id(releaseTeam.id)].create({}).link({
        organization: id(releaseTeam.organizationId),
        release: id(releaseTeam.releaseId),
        team: id(releaseTeam.teamId),
      }),
    );
  await adminDb.transact(chunks);
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
