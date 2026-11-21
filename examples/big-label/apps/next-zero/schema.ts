import { createBuilder, createSchema, number, relationships, string, table, type Row } from "@rocicorp/zero";

// BigLabel's synced tables (Postgres: db/001_init.sql). Better Auth's tables
// and the sign-in emails live in Postgres schemas Zero doesn't replicate.

const organizations = table("organizations")
  .columns({ id: string(), name: string(), slug: string() })
  .primaryKey("id");

const people = table("people")
  .columns({ id: string(), userId: string(), name: string() })
  .primaryKey("id");

const memberships = table("memberships")
  .columns({
    id: string(),
    organizationId: string(),
    personId: string(),
    // Denormalized from the person, so membership checks are one indexed lookup.
    userId: string(),
    role: string(),
  })
  .primaryKey("id");

const teams = table("teams")
  .columns({ id: string(), organizationId: string(), name: string() })
  .primaryKey("id");

const teamAssignments = table("teamAssignments")
  .columns({ id: string(), organizationId: string(), teamId: string(), membershipId: string(), role: string() })
  .primaryKey("id");

const artists = table("artists")
  .columns({
    id: string(),
    organizationId: string(),
    name: string(),
    genre: string(),
    status: string(),
    // Lower-cased name and genre, for case-insensitive search.
    searchKey: string(),
  })
  .primaryKey("id");

const catalogues = table("catalogues")
  .columns({ id: string(), organizationId: string(), name: string(), code: string() })
  .primaryKey("id");

const releases = table("releases")
  .columns({
    id: string(),
    organizationId: string(),
    artistId: string(),
    catalogueId: string().optional(),
    catalogNumber: string(),
    // The catalogue number's trailing digits, so "NFC-1000" sorts after "NFC-999".
    catalogSequence: number().optional(),
    title: string(),
    format: string(),
    releaseDate: number(),
    status: string(),
    // Lower-cased title and catalogue number, for case-insensitive search.
    searchKey: string(),
  })
  .primaryKey("id");

// Which teams work on a release. Both sides must belong to the same tenant.
const releaseTeams = table("releaseTeams")
  .columns({ id: string(), organizationId: string(), releaseId: string(), teamId: string() })
  .primaryKey("id");

const releaseAssignments = table("releaseAssignments")
  .columns({ id: string(), organizationId: string(), releaseId: string(), membershipId: string(), role: string() })
  .primaryKey("id");

// `tenantMembers` (a tenant row's label's memberships) carries the read rule
// in queries.ts: a row is visible to members of its label.
const tenantMembers = {
  sourceField: ["organizationId"],
  destSchema: memberships,
  destField: ["organizationId"],
} as const;
const organizationOf = { sourceField: ["organizationId"], destSchema: organizations, destField: ["id"] } as const;

const organizationRelationships = relationships(organizations, ({ many }) => ({
  tenantMembers: many({ sourceField: ["id"], destSchema: memberships, destField: ["organizationId"] }),
}));
const membershipRelationships = relationships(memberships, ({ one, many }) => ({
  organization: one(organizationOf),
  person: one({ sourceField: ["personId"], destSchema: people, destField: ["id"] }),
  tenantMembers: many(tenantMembers),
}));
const teamRelationships = relationships(teams, ({ many }) => ({ tenantMembers: many(tenantMembers) }));
const teamAssignmentRelationships = relationships(teamAssignments, ({ one, many }) => ({
  tenantMembers: many(tenantMembers),
  team: one({ sourceField: ["teamId"], destSchema: teams, destField: ["id"] }),
  membership: one({ sourceField: ["membershipId"], destSchema: memberships, destField: ["id"] }),
}));
const artistRelationships = relationships(artists, ({ many }) => ({ tenantMembers: many(tenantMembers) }));
const catalogueRelationships = relationships(catalogues, ({ many }) => ({ tenantMembers: many(tenantMembers) }));
const releaseRelationships = relationships(releases, ({ one, many }) => ({
  tenantMembers: many(tenantMembers),
  artist: one({ sourceField: ["artistId"], destSchema: artists, destField: ["id"] }),
  catalogue: one({ sourceField: ["catalogueId"], destSchema: catalogues, destField: ["id"] }),
}));
const releaseTeamRelationships = relationships(releaseTeams, ({ one, many }) => ({
  tenantMembers: many(tenantMembers),
  release: one({ sourceField: ["releaseId"], destSchema: releases, destField: ["id"] }),
  team: one({ sourceField: ["teamId"], destSchema: teams, destField: ["id"] }),
}));
const releaseAssignmentRelationships = relationships(releaseAssignments, ({ many }) => ({
  tenantMembers: many(tenantMembers),
}));

export const schema = createSchema({
  tables: [
    organizations,
    people,
    memberships,
    teams,
    teamAssignments,
    artists,
    catalogues,
    releases,
    releaseTeams,
    releaseAssignments,
  ],
  relationships: [
    organizationRelationships,
    membershipRelationships,
    teamRelationships,
    teamAssignmentRelationships,
    artistRelationships,
    catalogueRelationships,
    releaseRelationships,
    releaseTeamRelationships,
    releaseAssignmentRelationships,
  ],
  enableLegacyQueries: false,
  enableLegacyMutators: false,
});

export const zql = createBuilder(schema);
export type Schema = typeof schema;

/** Who is asking: the Better Auth user from the session cookie. */
export type AuthData = { userID: string };

declare module "@rocicorp/zero" {
  interface DefaultTypes {
    schema: typeof schema;
    context: AuthData | undefined;
  }
}

export type Release = Row<typeof schema.tables.releases>;
export type Membership = Row<typeof schema.tables.memberships>;
