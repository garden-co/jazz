// BigLabel's tables (db/001_init.sql), as rows and as the relations the
// generic read endpoint may follow. Shared by the server's query compiler and
// the client's query builder.
export type Organization = { id: string; name: string; slug: string };
export type Person = { id: string; userId: string; name: string };
export type Membership = { id: string; organizationId: string; personId: string; userId: string; role: string };
export type Team = { id: string; organizationId: string; name: string };
export type TeamAssignment = { id: string; organizationId: string; teamId: string; membershipId: string; role: string };
export type Artist = { id: string; organizationId: string; name: string; genre: string; status: string; searchKey: string };
export type Catalogue = { id: string; organizationId: string; name: string; code: string };
export type Release = {
  id: string;
  organizationId: string;
  artistId: string;
  catalogueId: string | null;
  catalogNumber: string;
  catalogSequence: number | null;
  title: string;
  format: string;
  releaseDate: Date;
  status: string;
  searchKey: string;
};
export type ReleaseTeam = { id: string; organizationId: string; releaseId: string; teamId: string };
export type ReleaseAssignment = { id: string; organizationId: string; releaseId: string; membershipId: string; role: string };

/** Each readable table: its columns, and its relations as [table, foreign key]. */
export const tables = {
  organizations: { columns: ["id", "name", "slug"], relations: {} },
  people: { columns: ["id", "userId", "name"], relations: {} },
  memberships: {
    columns: ["id", "organizationId", "personId", "userId", "role"],
    relations: { organization: ["organizations", "organizationId"], person: ["people", "personId"] },
  },
  teams: { columns: ["id", "organizationId", "name"], relations: {} },
  teamAssignments: {
    columns: ["id", "organizationId", "teamId", "membershipId", "role"],
    relations: { team: ["teams", "teamId"], membership: ["memberships", "membershipId"] },
  },
  artists: { columns: ["id", "organizationId", "name", "genre", "status", "searchKey"], relations: {} },
  catalogues: { columns: ["id", "organizationId", "name", "code"], relations: {} },
  releases: {
    columns: ["id", "organizationId", "artistId", "catalogueId", "catalogNumber", "catalogSequence", "title", "format", "releaseDate", "status", "searchKey"],
    relations: { artist: ["artists", "artistId"], catalogue: ["catalogues", "catalogueId"] },
  },
  releaseTeams: {
    columns: ["id", "organizationId", "releaseId", "teamId"],
    relations: { release: ["releases", "releaseId"], team: ["teams", "teamId"] },
  },
  releaseAssignments: {
    columns: ["id", "organizationId", "releaseId", "membershipId", "role"],
    relations: { release: ["releases", "releaseId"], membership: ["memberships", "membershipId"] },
  },
} as const satisfies Record<string, { columns: readonly string[]; relations: Record<string, readonly [string, string]> }>;
export type TableName = keyof typeof tables;

export type Rows = {
  organizations: Organization;
  people: Person;
  memberships: Membership;
  teams: Team;
  teamAssignments: TeamAssignment;
  artists: Artist;
  catalogues: Catalogue;
  releases: Release;
  releaseTeams: ReleaseTeam;
  releaseAssignments: ReleaseAssignment;
};

/** A filter value: equality, or one of the operators the screens use. */
export type Condition = string | number | boolean | null | { contains: string } | { gte: number } | { isNull: boolean };
export type Include = { [relation: string]: true | Include };
/** A read, as the client sends it to POST /api/query. */
export type QuerySpec = {
  table: TableName;
  where: Record<string, Condition>;
  orderBy: [column: string, direction: "asc" | "desc"][];
  include: Include;
  select?: string[];
  limit?: number;
  offset?: number;
};

/** What the WebSocket tells subscribers: these tables changed under this topic. */
export type ChangeEvent = { type: "change"; topic: string; tables: string[] };
