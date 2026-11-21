import { i } from "@instantdb/react";

/**
 * BigLabel: the same tables as the Jazz app. Every tenant row links to its
 * organization, and relations are links, so `instant.perms.ts` can follow them
 * with `data.ref`. A few attributes exist only so the rules and Instant's
 * unique constraints can do what the Jazz app does in serializable
 * transactions; each says why.
 */
const _schema = i.schema({
  entities: {
    $users: i.entity({
      email: i.string().unique().indexed().optional(),
    }),
    // Email + password sign-in (src/lib/instant.server.ts). Admin-only.
    credentials: i.entity({
      email: i.string().unique().indexed(),
      name: i.string(),
      passwordHash: i.string(),
    }),
    organizations: i.entity({
      name: i.string(),
      slug: i.string().unique().indexed(),
    }),
    people: i.entity({
      // One profile per user: unique, so concurrent bootstraps can't split it.
      userId: i.string().unique().indexed(),
      name: i.string(),
    }),
    // Sign-in emails, written only by the server (bootstrap) and read only by
    // it (adding a member by email). No client rule allows them.
    personEmails: i.entity({
      email: i.string().unique().indexed(),
    }),
    memberships: i.entity({
      userId: i.string().indexed(),
      role: i.string().indexed(),
      // `userId` when the role is admin (adminUserId) or may edit the catalogue
      // (editorUserId), else null. Rules can't filter a linked list by role,
      // so they check these lists; the membership's own rule keeps them true
      // to `role`.
      adminUserId: i.string().indexed().optional(),
      editorUserId: i.string().indexed().optional(),
      // "<organizationId>/<userId>", unique: a person joins a label once.
      memberKey: i.string().unique().indexed(),
    }),
    teams: i.entity({
      name: i.string().indexed(),
    }),
    teamAssignments: i.entity({
      role: i.string(),
    }),
    artists: i.entity({
      name: i.string().indexed(),
      genre: i.string().indexed(),
      status: i.string().indexed(),
      // Lower-cased name and genre, for case-insensitive search.
      searchKey: i.string().indexed(),
    }),
    catalogues: i.entity({
      name: i.string().indexed(),
      code: i.string(),
    }),
    releases: i.entity({
      catalogNumber: i.string().indexed(),
      // The catalogue number's trailing digits, so "NFC-1000" sorts after
      // "NFC-999" when suggesting the next number.
      catalogSequence: i.number().indexed().optional(),
      // "<organizationId>/<catalogNumber>", unique: catalogue numbers are
      // unique per label, enforced by the server for every write.
      catalogKey: i.string().unique().indexed(),
      title: i.string().indexed(),
      format: i.string(),
      releaseDate: i.date().indexed(),
      status: i.string().indexed(),
      // Lower-cased title and catalogue number, for case-insensitive search.
      searchKey: i.string().indexed(),
    }),
    releaseTeams: i.entity({}),
    releaseAssignments: i.entity({
      role: i.string(),
    }),
  },
  links: {
    personEmailsPerson: {
      forward: {
        on: "personEmails",
        has: "one",
        label: "person",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "people", has: "many", label: "emails" },
    },
    membershipsOrganization: {
      forward: {
        on: "memberships",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "memberships" },
    },
    membershipsPerson: {
      forward: {
        on: "memberships",
        has: "one",
        label: "person",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "people", has: "many", label: "memberships" },
    },
    teamsOrganization: {
      forward: {
        on: "teams",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "teams" },
    },
    teamAssignmentsOrganization: {
      forward: {
        on: "teamAssignments",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "teamAssignments" },
    },
    teamAssignmentsTeam: {
      forward: {
        on: "teamAssignments",
        has: "one",
        label: "team",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "teams", has: "many", label: "assignments" },
    },
    teamAssignmentsMembership: {
      forward: {
        on: "teamAssignments",
        has: "one",
        label: "membership",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "memberships", has: "many", label: "teamAssignments" },
    },
    artistsOrganization: {
      forward: {
        on: "artists",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "artists" },
    },
    cataloguesOrganization: {
      forward: {
        on: "catalogues",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "catalogues" },
    },
    releasesOrganization: {
      forward: {
        on: "releases",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "releases" },
    },
    releasesArtist: {
      forward: { on: "releases", has: "one", label: "artist", required: true },
      reverse: { on: "artists", has: "many", label: "releases" },
    },
    releasesCatalogue: {
      forward: { on: "releases", has: "one", label: "catalogue" },
      reverse: { on: "catalogues", has: "many", label: "releases" },
    },
    releaseTeamsOrganization: {
      forward: {
        on: "releaseTeams",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "releaseTeams" },
    },
    releaseTeamsRelease: {
      forward: {
        on: "releaseTeams",
        has: "one",
        label: "release",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "releases", has: "many", label: "releaseTeams" },
    },
    releaseTeamsTeam: {
      forward: {
        on: "releaseTeams",
        has: "one",
        label: "team",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "teams", has: "many", label: "releaseTeams" },
    },
    releaseAssignmentsOrganization: {
      forward: {
        on: "releaseAssignments",
        has: "one",
        label: "organization",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "organizations", has: "many", label: "releaseAssignments" },
    },
    releaseAssignmentsRelease: {
      forward: {
        on: "releaseAssignments",
        has: "one",
        label: "release",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "releases", has: "many", label: "assignments" },
    },
    releaseAssignmentsMembership: {
      forward: {
        on: "releaseAssignments",
        has: "one",
        label: "membership",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "memberships", has: "many", label: "releaseAssignments" },
    },
  },
});

// This helps TypeScript display better IntelliSense
type _AppSchema = typeof _schema;
interface AppSchema extends _AppSchema {}
const schema: AppSchema = _schema;

export type { AppSchema };
export default schema;
