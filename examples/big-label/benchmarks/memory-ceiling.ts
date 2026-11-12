// BigLabel's adapter for the memory-ceiling benchmark
// (dev/benchmarks/memory-ceiling). It says how BigLabel grows, in its own
// units, and which queries its users run; the harness does the rest.
//
// Every transaction names the actor the app would write it as: tenant
// admission (organizations, people, memberships) is the trusted bootstrap
// path, as in `src/lib/bootstrap.ts`, and the catalogue is the label admin's,
// one release (with its team and people) per transaction, as the release
// form saves them. The harness grows data through the trusted path by
// default, as `src/lib/demo-data.ts` imports it, and measures what policy
// checked writes cost at each size with `probe`: user edits made through the
// deployed permissions.
import { createHash } from "node:crypto";
import { app } from "../schema";
import permissions from "../permissions";

type Op = { table: string; id: string; values: Record<string, unknown>; update?: boolean };
type Tx = { actor: "backend" | string; ops: Op[] };

const genres = ["Electronic", "Indie", "Jazz", "Hip-hop", "Folk", "Soul"];
const formats = ["Single", "EP", "Album"];
const statuses = ["planning", "scheduled", "released"];
const words = ["Velvet", "Quiet", "Neon", "Silver", "Hollow", "Golden", "Static", "Paper"];
const titles = ["Night", "Glass", "Summer", "Distance", "Rooms", "Lights", "Tides", "Letters"];
const teamNames = ["Release operations", "A&R", "Marketing", "Sync", "Digital", "Legal"];

/** A name-based UUID, stable across runs, so a scale always means the same rows. */
function uuid(...parts: (string | number)[]) {
  const bytes = createHash("sha256")
    .update(`big-label-memceil:${parts.join(":")}`)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Label sizes follow a power law, repeating every 16 labels: one large label
 * (120 artists, 1,200 releases, 40 members), three medium ones (30, 240, 12)
 * and twelve small ones (6, 30, 3). Every release has a team and two people
 * assigned (owner and A&R), and every member is on a team.
 */
export function labelShape(label: number) {
  const tier = label % 16 === 0 ? "large" : label % 16 <= 3 ? "medium" : "small";
  const size = {
    large: { artists: 120, releasesPerArtist: 10, members: 40, teams: 6, catalogues: 4 },
    medium: { artists: 30, releasesPerArtist: 8, members: 12, teams: 4, catalogues: 2 },
    small: { artists: 6, releasesPerArtist: 5, members: 3, teams: 3, catalogues: 2 },
  }[tier];
  return { tier, ...size, releases: size.artists * size.releasesPerArtist };
}

/** The flagship label: one label whose catalogue keeps growing. */
export function flagshipShape(releases: number) {
  return {
    tier: "flagship",
    artists: Math.max(1, Math.ceil(releases / 10)),
    releasesPerArtist: 10,
    members: 40,
    teams: 6,
    catalogues: 4,
    releases,
  };
}

const accountOf = (label: number, member: number) => uuid("account", label, member);
const orgOf = (label: number) => uuid("org", label);

/** Admission of one label: what the trusted bootstrap route writes. */
function* admitLabel(label: number, members: number): Generator<Tx> {
  const organizationId = orgOf(label);
  const ops: Op[] = [
    {
      table: "organizations",
      id: organizationId,
      values: { name: `Label ${label}`, slug: `label-${label}` },
    },
  ];
  for (let m = 0; m < members; m++) {
    const personId = uuid("person", label, m);
    ops.push({
      table: "people",
      id: personId,
      values: { userId: accountOf(label, m), name: `${words[m % 8]} ${titles[(m * 3) % 8]} ${m}` },
    });
    ops.push({
      table: "memberships",
      id: uuid("membership", label, m),
      values: {
        organizationId,
        personId,
        userId: accountOf(label, m),
        role: m === 0 ? "admin" : m % 3 === 0 ? "viewer" : "editor",
      },
    });
  }
  yield { actor: "backend", ops };
}

/** The label's own setup, written by its admin under the deployed policies. */
function* setUpLabel(label: number, shape: ReturnType<typeof labelShape>): Generator<Tx> {
  const organizationId = orgOf(label);
  const admin = accountOf(label, 0);
  const ops: Op[] = [];
  for (let t = 0; t < shape.teams; t++) {
    ops.push({
      table: "teams",
      id: uuid("team", label, t),
      values: { organizationId, name: teamNames[t % teamNames.length] },
    });
  }
  for (let c = 0; c < shape.catalogues; c++) {
    ops.push({
      table: "catalogues",
      id: uuid("catalogue", label, c),
      values: { organizationId, name: `Catalogue ${c + 1}`, code: `L${label}C${c}` },
    });
  }
  yield { actor: admin, ops };
  // Staffing: each member on one team.
  const staffing: Op[] = [];
  for (let m = 0; m < shape.members; m++) {
    staffing.push({
      table: "teamAssignments",
      id: uuid("teamAssignment", label, m),
      values: {
        organizationId,
        teamId: uuid("team", label, m % shape.teams),
        membershipId: uuid("membership", label, m),
        role: m < shape.teams ? "lead" : "member",
      },
    });
  }
  yield { actor: admin, ops: staffing };
}

function artistTx(label: number, a: number, admin: string): Tx {
  const name = `${words[a % 8]} ${titles[(a >> 3) % 8]} ${a}`;
  const genre = genres[a % genres.length]!;
  return {
    actor: admin,
    ops: [
      {
        table: "artists",
        id: uuid("artist", label, a),
        values: {
          organizationId: orgOf(label),
          name,
          genre,
          status: a % 3 === 0 ? "developing" : "active",
          searchKey: `${name} ${genre}`.toLowerCase(),
        },
      },
    ],
  };
}

function releaseTx(
  label: number,
  r: number,
  shape: ReturnType<typeof labelShape>,
  admin: string,
): Tx {
  const organizationId = orgOf(label);
  const releaseId = uuid("release", label, r);
  const catalogue = r % shape.catalogues;
  const catalogNumber = `L${label}C${catalogue}-${String(r + 1).padStart(4, "0")}`;
  const title = `${titles[r % 8]} ${titles[(r >> 3) % 8]} ${r}`;
  const day = r % 3650;
  return {
    actor: admin,
    ops: [
      {
        table: "releases",
        id: releaseId,
        values: {
          organizationId,
          artistId: uuid("artist", label, Math.floor(r / shape.releasesPerArtist)),
          catalogueId: uuid("catalogue", label, catalogue),
          catalogNumber,
          catalogSequence: r + 1,
          title,
          format: formats[r % formats.length],
          releaseDate: new Date(Date.UTC(2016, 0, 1) + day * 86_400_000),
          status: statuses[r % statuses.length],
          searchKey: `${title} ${catalogNumber}`.toLowerCase(),
        },
      },
      {
        table: "releaseTeams",
        id: uuid("releaseTeam", label, r),
        values: { organizationId, releaseId, teamId: uuid("team", label, r % shape.teams) },
      },
      ...["owner", "a&r"].map((role, i) => ({
        table: "releaseAssignments",
        id: uuid("releaseAssignment", label, r, role),
        values: {
          organizationId,
          releaseId,
          membershipId: uuid("membership", label, (r + i) % shape.members),
          role,
        },
      })),
    ],
  };
}

/** Releases [from, to) of a label, each artist written just before its first release. */
function* catalogue(
  label: number,
  shape: ReturnType<typeof labelShape>,
  from: number,
  to: number,
): Generator<Tx> {
  const admin = accountOf(label, 0);
  for (let r = from; r < to; r++) {
    if (r % shape.releasesPerArtist === 0)
      yield artistTx(label, r / shape.releasesPerArtist, admin);
    yield releaseTx(label, r, shape, admin);
  }
}

/**
 * A label admin's everyday edits: move a release on and hand it to another
 * owner. Updates, so probing never changes the counts.
 */
function* edits(
  label: number,
  shape: { releases: number; members: number },
  count: number,
): Generator<Tx> {
  const admin = accountOf(label, 0);
  for (let i = 0; i < count; i++) {
    const r = (i * 7919) % shape.releases;
    yield {
      actor: admin,
      ops: [
        {
          table: "releases",
          id: uuid("release", label, r),
          values: { status: statuses[(r + i + 1) % 3] },
          update: true,
        },
        {
          table: "releaseAssignments",
          id: uuid("releaseAssignment", label, r, "owner"),
          values: { membershipId: uuid("membership", label, (r + i + 1) % shape.members) },
          update: true,
        },
      ],
    };
  }
}

function labelCounts(shape: {
  artists: number;
  releases: number;
  members: number;
  teams: number;
  catalogues: number;
}) {
  return {
    artists: shape.artists,
    releases: shape.releases,
    members: shape.members,
    teams: shape.teams,
    catalogues: shape.catalogues,
    assignments: shape.members + shape.releases * 3,
  };
}

function sum(rows: Record<string, number>[]) {
  const out: Record<string, number> = {};
  for (const row of rows) for (const [k, v] of Object.entries(row)) out[k] = (out[k] ?? 0) + v;
  return out;
}

/** What the app loads first for a signed-in account: its labels (the switcher). */
function signIn(actor: string) {
  return [app.memberships.where({ userId: actor }).include({ organization: true })];
}

/** What a member's browser asks for when it opens a label (see src/screens). */
function labelReads(label: number, member = 0) {
  const organizationId = orgOf(label);
  const where = { organizationId };
  const reads = {
    labelSwitcher: app.memberships
      .where({ userId: accountOf(label, member) })
      .include({ organization: true }),
    overviewArtists: app.artists.where(where).select("id").limit(5000),
    overviewReleases: app.releases.where(where).select("status").limit(5000),
    overviewMembers: app.memberships.where(where).select("id").limit(5000),
    overviewTeams: app.teams.where(where).select("id").limit(5000),
    latestReleases: app.releases
      .where(where)
      .orderBy("releaseDate", "desc")
      .orderBy("id", "asc")
      .include({ artist: true })
      .limit(8),
    releasesPage: app.releases
      .where(where)
      .orderBy("releaseDate", "desc")
      .orderBy("id", "asc")
      .include({ artist: true })
      .limit(51),
    artistsPage: app.artists.where(where).orderBy("name", "asc").orderBy("id", "asc").limit(51),
    people: app.memberships.where(where).include({ person: true }).limit(500),
    teams: app.teams.where(where).orderBy("name", "asc"),
  };
  return Object.entries(reads).map(([name, query]) => ({ name, query }));
}

export default {
  name: "big-label",
  app,
  permissions,
  scenarios: {
    // Many tenants on one sync server: the shape of a hosted app's growth.
    labels: {
      unit: "labels",
      start: 16,
      describe: (labels: number) =>
        sum(
          Array.from({ length: labels }, (_, l) => ({ labels: 1, ...labelCounts(labelShape(l)) })),
        ),
      *transactions(from: number, to: number): Generator<Tx> {
        for (let l = from; l < to; l++) {
          const shape = labelShape(l);
          yield* admitLabel(l, shape.members);
          yield* setUpLabel(l, shape);
          yield* catalogue(l, shape, 0, shape.releases);
        }
      },
      // The admin of the first (large) label opens it; the crowd is the
      // admins of the next labels, reconnecting after a restart.
      signIn,
      probe: (_labels: number, count: number) => edits(0, labelShape(0), count),
      client: () => ({ actor: accountOf(0, 0), reads: labelReads(0) }),
      crowd: (labels: number, size: number) =>
        Array.from({ length: Math.min(size, labels - 1) }, (_, i) => ({
          actor: accountOf(i + 1, 0),
          reads: labelReads(i + 1),
        })),
    },
    // One label whose catalogue keeps growing: what one browser can hold.
    flagship: {
      unit: "releases",
      start: 1000,
      describe: (releases: number) => ({ labels: 1, ...labelCounts(flagshipShape(releases)) }),
      *transactions(from: number, to: number): Generator<Tx> {
        const shape = flagshipShape(Number.MAX_SAFE_INTEGER);
        if (from === 0) {
          yield* admitLabel(0, shape.members);
          yield* setUpLabel(0, shape);
        }
        yield* catalogue(0, shape, from, to);
      },
      signIn,
      probe: (releases: number, count: number) => edits(0, flagshipShape(releases), count),
      client: () => ({ actor: accountOf(0, 0), reads: labelReads(0) }),
      crowd: (_releases: number, size: number) =>
        Array.from({ length: Math.min(size, 39) }, (_, i) => ({
          actor: accountOf(0, i + 1),
          reads: labelReads(0, i + 1),
        })),
    },
  },
};
