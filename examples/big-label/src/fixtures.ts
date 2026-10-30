import type { Role } from "./roles.js";

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
    role: Role;
  }[];
  teamAssignments: {
    id: string;
    organizationId: string;
    teamId: string;
    membershipId: string;
    role: string;
  }[];
  catalogues: { id: string; organizationId: string; name: string; code: string }[];
  artists: {
    id: string;
    organizationId: string;
    name: string;
    genre: string;
    status: string;
    searchKey: string;
  }[];
  releases: {
    id: string;
    organizationId: string;
    artistId: string;
    catalogueId: string;
    catalogNumber: string;
    catalogSequence: number;
    title: string;
    format: string;
    releaseDate: string;
    status: string;
    searchKey: string;
  }[];
  releaseTeams: { id: string; organizationId: string; releaseId: string; teamId: string }[];
};

export const fixtureProfiles: Record<
  FixtureProfile,
  { organizations: number; membersPerOrganization: number; artistsPerOrganization: number }
> = {
  smoke: { organizations: 2, membersPerOrganization: 2, artistsPerOrganization: 3 },
  small: { organizations: 3, membersPerOrganization: 4, artistsPerOrganization: 8 },
  scaled: { organizations: 24, membersPerOrganization: 10, artistsPerOrganization: 60 },
};

export const genres = ["Electronic", "Indie", "Jazz", "Hip-hop", "Folk", "Soul"];
export const artistStatuses = ["developing", "active", "on hiatus"];
export const releaseStatuses = ["planning", "scheduled", "released"];
export const releaseFormats = ["Single", "EP", "Album"];

const labelWords = ["Northwind", "Low Tide", "Paper Moon", "Cinder", "Blue Hour", "Field Notes"];
const firstWords = ["Velvet", "Quiet", "Neon", "Silver", "Hollow", "Golden", "Static", "Paper"];
const secondWords = [
  "Harbour",
  "Orchard",
  "Signal",
  "Rivers",
  "Lanterns",
  "Choir",
  "Echo",
  "Weather",
];
const titleWords = ["Night", "Glass", "Summer", "Distance", "Rooms", "Lights", "Tides", "Letters"];
const firstNames = ["Ada", "Bo", "Cleo", "Dev", "Ezra", "Fen", "Gia", "Hal", "Ines", "Jun"];
const lastNames = ["Okafor", "Lind", "Moreau", "Sato", "Reyes", "Novak", "Haas", "Quinn"];
const teamNames = ["Release operations", "A&R", "Marketing"];
const catalogueNames = [
  { name: "Front catalogue", code: "FC" },
  { name: "Reissues", code: "RE" },
];

/** Seed controls names and IDs, so receipts are reproducible without private data. */
export function createFixture(profile: FixtureProfile = "small", seed = 17): Fixture {
  const size = fixtureProfiles[profile];
  const fixture: Fixture = {
    organizations: [],
    people: [],
    teams: [],
    memberships: [],
    teamAssignments: [],
    catalogues: [],
    artists: [],
    releases: [],
    releaseTeams: [],
  };
  let n = seed;
  const next = () => ((n = (n * 1664525 + 1013904223) >>> 0), n);
  const pick = <T>(values: readonly T[]) => values[next() % values.length]!;
  for (let o = 0; o < size.organizations; o++) {
    const organizationId = `org-${seed}-${o}`;
    const labelWord = labelWords[o % labelWords.length]!;
    const round = Math.floor(o / labelWords.length);
    fixture.organizations.push({
      id: organizationId,
      name: `${labelWord} Records${round ? ` ${round + 1}` : ""}`,
      slug: `label-${o + 1}`,
    });
    const teams = teamNames.map((name, t) => ({
      id: t === 0 ? `team-${seed}-${o}` : `team-${seed}-${o}-${t}`,
      organizationId,
      name,
    }));
    fixture.teams.push(...teams);
    const catalogues = catalogueNames.map((catalogue, c) => ({
      id: `catalogue-${seed}-${o}-${c}`,
      organizationId,
      name: catalogue.name,
      code: `${labelWord.replace(/[^A-Z]/g, "")}${catalogue.code}`,
    }));
    fixture.catalogues.push(...catalogues);
    for (let m = 0; m < size.membersPerOrganization; m++) {
      const personId = `person-${seed}-${o}-${m}`;
      const membershipId = `member-${seed}-${o}-${m}`;
      const role: Role = m === 0 ? "admin" : m % 3 === 0 ? "viewer" : "editor";
      fixture.people.push({
        id: personId,
        userId: `user-${seed}-${o}-${m}`,
        name: `${pick(firstNames)} ${pick(lastNames)}`,
      });
      fixture.memberships.push({
        id: membershipId,
        organizationId,
        personId,
        userId: `user-${seed}-${o}-${m}`,
        role,
      });
      const team = teams[m % teams.length]!;
      fixture.teamAssignments.push({
        id: `team-member-${seed}-${o}-${m}`,
        organizationId,
        teamId: team.id,
        membershipId,
        role: m < teams.length ? "lead" : "member",
      });
    }
    for (let a = 0; a < size.artistsPerOrganization; a++) {
      const artistId = `artist-${seed}-${o}-${a}`;
      const name = `${pick(firstWords)} ${pick(secondWords)}`;
      const genre = pick(genres);
      fixture.artists.push({
        id: artistId,
        organizationId,
        name,
        genre,
        status: a % 3 === 0 ? "developing" : "active",
        searchKey: searchKey(name, genre),
      });
      const catalogue = catalogues[a % catalogues.length]!;
      const catalogNumber = formatCatalogNumber(catalogue.code, a + 1);
      const title = `${pick(titleWords)} ${pick(titleWords)}`;
      const releaseId = `release-${seed}-${o}-${a}`;
      fixture.releases.push({
        id: releaseId,
        organizationId,
        artistId,
        catalogueId: catalogue.id,
        catalogNumber,
        catalogSequence: a + 1,
        title,
        format: releaseFormats[a % releaseFormats.length]!,
        releaseDate: `2026-${String((a % 12) + 1).padStart(2, "0")}-01T00:00:00.000Z`,
        status: a % 4 === 0 ? "planning" : "scheduled",
        searchKey: searchKey(title, catalogNumber),
      });
      fixture.releaseTeams.push({
        id: `release-team-${seed}-${o}-${a}`,
        organizationId,
        releaseId,
        teamId: teams[0]!.id,
      });
    }
  }
  return fixture;
}

/** Lower-cased search text; the query uses a case-sensitive `contains`. */
export function searchKey(...parts: string[]) {
  return parts.join(" ").toLowerCase();
}

export function formatCatalogNumber(code: string, sequence: number) {
  return `${code}-${String(sequence).padStart(3, "0")}`;
}
