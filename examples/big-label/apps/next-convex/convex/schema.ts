import { defineSchema, defineTable } from "convex/server";
import { authTables } from "@convex-dev/auth/server";
import { v } from "convex/values";

/**
 * BigLabel: the Jazz app's tables. `userId` is the Convex Auth user id. Who
 * may read or write what is checked inside each function (lib/access.ts).
 */
export default defineSchema({
  ...authTables,
  organizations: defineTable({ name: v.string(), slug: v.string() }).index("by_slug", ["slug"]),
  people: defineTable({ userId: v.id("users"), name: v.string() }).index("by_user", ["userId"]),
  // Sign-in emails, written by the bootstrap and read only by the add-member
  // mutation. No query returns them.
  personEmails: defineTable({ personId: v.id("people"), email: v.string() })
    .index("by_person", ["personId"])
    .index("by_email", ["email"]),
  teams: defineTable({ organizationId: v.id("organizations"), name: v.string() }).index(
    "by_org_name",
    ["organizationId", "name"],
  ),
  memberships: defineTable({
    organizationId: v.id("organizations"),
    personId: v.id("people"),
    // Denormalized so a member check is one indexed lookup.
    userId: v.id("users"),
    role: v.string(),
  })
    .index("by_user", ["userId"])
    .index("by_org_user", ["organizationId", "userId"])
    .index("by_org_person", ["organizationId", "personId"]),
  teamAssignments: defineTable({
    organizationId: v.id("organizations"),
    teamId: v.id("teams"),
    membershipId: v.id("memberships"),
    role: v.string(),
  })
    .index("by_org_team", ["organizationId", "teamId"])
    .index("by_org_membership", ["organizationId", "membershipId"]),
  artists: defineTable({
    organizationId: v.id("organizations"),
    name: v.string(),
    genre: v.string(),
    status: v.string(),
    // Lower-cased name and genre, for case-insensitive search.
    searchKey: v.string(),
  })
    .index("by_org_name", ["organizationId", "name"])
    .index("by_org_genre", ["organizationId", "genre"])
    .index("by_org_status", ["organizationId", "status"]),
  catalogues: defineTable({
    organizationId: v.id("organizations"),
    name: v.string(),
    code: v.string(),
  }).index("by_org_name", ["organizationId", "name"]),
  releases: defineTable({
    organizationId: v.id("organizations"),
    artistId: v.id("artists"),
    catalogueId: v.optional(v.id("catalogues")),
    catalogNumber: v.string(),
    // The catalogue number's trailing digits, so "NFC-1000" sorts after "NFC-999".
    catalogSequence: v.optional(v.number()),
    title: v.string(),
    format: v.string(),
    releaseDate: v.number(),
    status: v.string(),
    // Lower-cased title and catalogue number, for case-insensitive search.
    searchKey: v.string(),
  })
    // One index per sortable column of the Releases table.
    .index("by_org_releaseDate", ["organizationId", "releaseDate"])
    .index("by_org_title", ["organizationId", "title"])
    .index("by_org_catalogNumber", ["organizationId", "catalogNumber"])
    .index("by_org_status", ["organizationId", "status"])
    .index("by_org_artist_releaseDate", ["organizationId", "artistId", "releaseDate"])
    .index("by_org_catalogue_releaseDate", ["organizationId", "catalogueId", "releaseDate"])
    .index("by_org_catalogue_title", ["organizationId", "catalogueId", "title"])
    .index("by_org_catalogue_catalogNumber", ["organizationId", "catalogueId", "catalogNumber"])
    .index("by_org_catalogue_status", ["organizationId", "catalogueId", "status"])
    .index("by_org_catalogue_sequence", ["organizationId", "catalogueId", "catalogSequence"]),
  // Which teams work on a release. Both sides must belong to the same tenant.
  releaseTeams: defineTable({
    organizationId: v.id("organizations"),
    releaseId: v.id("releases"),
    teamId: v.id("teams"),
  })
    .index("by_org_release", ["organizationId", "releaseId"])
    .index("by_org_team", ["organizationId", "teamId"]),
});
