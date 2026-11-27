// BigLabel fixture: the catalogue benchmark's shape (big_label_label_load[4096]):
// 8 labels, each with 6 members, 3 teams, 2 catalogues, 64 artists and 512
// releases (4,096 in all), plus team memberships and release-team assignments.
// Based on BigLabel's createFixture (examples/big-label/src/fixtures.ts),
// scaled up, with every release date unique within its label so "newest
// first" has one order on every stack (ties would fall back to stack ids).
//
// Every member is a real, signed-up user (email + password). Harness users:
// "a" (admin of label 0, editor of label 1) and "b" (editor of label 0).
// Keys are fixture-local; each stack maps them to its own ids.
import { rng, pick } from "./rng.mjs";

export const LABELS = 8;
export const MEMBERS_PER_LABEL = 6;
export const ARTISTS_PER_LABEL = 64;
export const RELEASES_PER_ARTIST = 8; // 512 per label
export const PAGE_SIZE = 20; // the Releases table's page (useListControls)
export const BENCH_LABEL = "label-0";
export const SECOND_LABEL = "label-1";
/** What bl_search types, one per sample in turn: title words (catalogue numbers never contain them). */
export const SEARCHES = ["summer", "night", "glass", "tides", "rooms", "lights", "letters", "distance"];

export const genres = ["Electronic", "Indie", "Jazz", "Hip-hop", "Folk", "Soul"];
export const releaseStatuses = ["planning", "scheduled", "released"];
export const releaseFormats = ["Single", "EP", "Album"];
const labelWords = ["Northwind", "Low Tide", "Paper Moon", "Cinder", "Blue Hour", "Field Notes"];
const firstWords = ["Velvet", "Quiet", "Neon", "Silver", "Hollow", "Golden", "Static", "Paper"];
const secondWords = ["Harbour", "Orchard", "Signal", "Rivers", "Lanterns", "Choir", "Echo", "Weather"];
const titleWords = ["Night", "Glass", "Summer", "Distance", "Rooms", "Lights", "Tides", "Letters"];
const firstNames = ["Ada", "Bo", "Cleo", "Dev", "Ezra", "Fen", "Gia", "Hal", "Ines", "Jun"];
const lastNames = ["Okafor", "Lind", "Moreau", "Sato", "Reyes", "Novak", "Haas", "Quinn"];
const teamNames = ["Release operations", "A&R", "Marketing"];
const catalogueNames = [
  { name: "Front catalogue", code: "FC" },
  { name: "Reissues", code: "RE" },
];

const DAY = 86_400_000;
const FIRST_DAY = Date.UTC(2019, 0, 1);

export const searchKey = (...parts) => parts.join(" ").toLowerCase();
export const formatCatalogNumber = (code, sequence) => `${code}-${String(sequence).padStart(3, "0")}`;

const user = (key, name) => ({ key, name, email: `${key}@bench.invalid`, password: `bench-password-${key}` });

export function bigLabelFixture() {
  const r = rng(17);
  const users = { a: user("a", "Bench A"), b: user("b", "Bench B") };
  const labels = [];
  const memberships = []; // { labelKey, user, role }; the first per label is its admin
  const teams = [];
  const teamMembers = []; // { key, labelKey, teamKey, user, role }
  const catalogues = [];
  const artists = [];
  const releases = [];
  const releaseTeams = [];

  for (let o = 0; o < LABELS; o++) {
    const labelKey = `label-${o}`;
    const word = labelWords[o % labelWords.length];
    const round = Math.floor(o / labelWords.length);
    labels.push({ key: labelKey, name: `${word} Records${round ? ` ${round + 1}` : ""}`, slug: `label-${o + 1}` });

    const labelTeams = teamNames.map((name, t) => ({ key: `${labelKey}-team-${t}`, labelKey, name }));
    teams.push(...labelTeams);
    const labelCatalogues = catalogueNames.map((c, i) => ({
      key: `${labelKey}-catalogue-${i}`,
      labelKey,
      name: c.name,
      code: `${word.replace(/[^A-Z]/g, "")}${c.code}`,
    }));
    catalogues.push(...labelCatalogues);

    const members = [];
    for (let m = 0; m < MEMBERS_PER_LABEL; m++) {
      let key = `l${o}m${m}`;
      if (o === 0 && m === 0) key = "a";
      else if (o === 0 && m === 1) key = "b";
      else users[key] = user(key, `${pick(r, firstNames)} ${pick(r, lastNames)}`);
      const role = m === 0 ? "admin" : m % 3 === 0 ? "viewer" : "editor";
      members.push({ labelKey, user: key, role });
    }
    if (o === 1) members.push({ labelKey, user: "a", role: "editor" });
    memberships.push(...members);
    members.forEach((member, m) =>
      teamMembers.push({
        key: `${labelKey}-team-member-${m}`,
        labelKey,
        teamKey: labelTeams[m % labelTeams.length].key,
        user: member.user,
        role: m < labelTeams.length ? "lead" : "member",
      }),
    );

    // 512 distinct release days, shuffled over the label's releases.
    const days = Array.from({ length: ARTISTS_PER_LABEL * RELEASES_PER_ARTIST }, (_, i) => i * 4);
    for (let i = days.length - 1; i > 0; i--) {
      const j = Math.floor(r() * (i + 1));
      [days[i], days[j]] = [days[j], days[i]];
    }
    const sequence = labelCatalogues.map(() => 0);
    let n = 0;
    for (let a = 0; a < ARTISTS_PER_LABEL; a++) {
      const artistKey = `${labelKey}-artist-${a}`;
      const name = `${pick(r, firstWords)} ${pick(r, secondWords)}`;
      const genre = pick(r, genres);
      artists.push({ key: artistKey, labelKey, name, genre, status: a % 3 === 0 ? "developing" : "active", searchKey: searchKey(name, genre) });
      for (let k = 0; k < RELEASES_PER_ARTIST; k++, n++) {
        const c = n % labelCatalogues.length;
        const catalogue = labelCatalogues[c];
        const catalogSequence = ++sequence[c];
        const catalogNumber = formatCatalogNumber(catalogue.code, catalogSequence);
        const title = `${pick(r, titleWords)} ${pick(r, titleWords)}`;
        const releaseKey = `${labelKey}-release-${n}`;
        releases.push({
          key: releaseKey,
          labelKey,
          artistKey,
          catalogueKey: catalogue.key,
          catalogNumber,
          catalogSequence,
          title,
          format: releaseFormats[n % releaseFormats.length],
          releaseDate: new Date(FIRST_DAY + days[n] * DAY).toISOString(),
          status: pick(r, releaseStatuses),
          searchKey: searchKey(title, catalogNumber),
        });
        releaseTeams.push({ key: `${releaseKey}-team-0`, labelKey, releaseKey, teamKey: labelTeams[0].key });
        if (n % 4 === 0) releaseTeams.push({ key: `${releaseKey}-team-1`, labelKey, releaseKey, teamKey: labelTeams[1 + (n % 8 === 0 ? 1 : 0)].key });
      }
    }
  }
  return { users, labels, memberships, teams, teamMembers, catalogues, artists, releases, releaseTeams };
}

/** A label's releases as the Releases page orders them: newest first. */
export function releasesNewestFirst(fixture, labelKey, search = "") {
  const term = search.trim().toLowerCase();
  return fixture.releases
    .filter((r) => r.labelKey === labelKey && (!term || r.searchKey.includes(term)))
    .sort((x, y) => (x.releaseDate < y.releaseDate ? 1 : -1));
}

/** The Overview's counts for a label, as rendered ("Artists", "Releases", "Members", "Teams"). */
export function labelCounts(fixture, labelKey) {
  const of = (rows) => rows.filter((row) => row.labelKey === labelKey).length;
  return { artists: of(fixture.artists), releases: of(fixture.releases), members: of(fixture.memberships), teams: of(fixture.teams) };
}
