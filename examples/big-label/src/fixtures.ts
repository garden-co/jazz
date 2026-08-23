/** Public, deterministic fixture data shared by the UI and headless receipts. */
export type FixtureProfile = "smoke" | "small" | "scaled";
export type Fixture = {
  organizations: { id: string; name: string; slug: string }[];
  people: { id: string; userId: string; name: string }[];
  teams: { id: string; organizationId: string; name: string }[];
  memberships: {
    id: string;
    organizationId: string;
    personId: string;
    userId: string;
    role: string;
  }[];
  artists: { id: string; organizationId: string; name: string; genre: string; status: string }[];
  releases: {
    id: string;
    organizationId: string;
    artistId: string;
    title: string;
    status: string;
  }[];
};

const sizes = { smoke: [2, 2, 3], small: [3, 4, 8], scaled: [24, 10, 60] } as const;
const genres = ["Electronic", "Indie", "Jazz", "Hip-hop"];

/** Seed controls names and IDs, so receipts are reproducible without private data. */
export function createFixture(profile: FixtureProfile = "small", seed = 17): Fixture {
  const [orgCount, membersPerOrg, artistsPerOrg] = sizes[profile];
  const fixture: Fixture = {
    organizations: [],
    people: [],
    teams: [],
    memberships: [],
    artists: [],
    releases: [],
  };
  let n = seed;
  const next = () => ((n = (n * 1664525 + 1013904223) >>> 0), n);
  for (let o = 0; o < orgCount; o++) {
    const organizationId = `org-${seed}-${o}`;
    fixture.organizations.push({
      id: organizationId,
      name: `Label ${o + 1}`,
      slug: `label-${o + 1}`,
    });
    fixture.teams.push({ id: `team-${seed}-${o}`, organizationId, name: "Release operations" });
    for (let m = 0; m < membersPerOrg; m++) {
      const personId = `person-${seed}-${o}-${m}`;
      fixture.people.push({
        id: personId,
        userId: `user-${seed}-${o}-${m}`,
        name: `Operator ${o + 1}.${m + 1}`,
      });
      fixture.memberships.push({
        id: `member-${seed}-${o}-${m}`,
        organizationId,
        personId,
        userId: `user-${seed}-${o}-${m}`,
        role: m === 0 ? "admin" : "editor",
      });
    }
    for (let a = 0; a < artistsPerOrg; a++) {
      const artistId = `artist-${seed}-${o}-${a}`;
      fixture.artists.push({
        id: artistId,
        organizationId,
        name: `Artist ${o + 1}-${a + 1}`,
        genre: genres[next() % genres.length]!,
        status: a % 3 === 0 ? "developing" : "active",
      });
      fixture.releases.push({
        id: `release-${seed}-${o}-${a}`,
        organizationId,
        artistId,
        title: `Release ${o + 1}-${a + 1}`,
        status: a % 4 === 0 ? "planning" : "scheduled",
      });
    }
  }
  return fixture;
}
