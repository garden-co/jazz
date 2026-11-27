import type { InstantRules } from "@instantdb/react";

/**
 * BigLabel's rules, the same as the Jazz app's permissions.ts (roles in
 * src/roles.ts): admins run the organization, editors maintain the catalogue,
 * viewers read it. Labels and people are created only by the server
 * (bootstrap); members are added by admins, never as admins. Every relation
 * must stay inside the row's own label.
 */
const member = (org: string) => `auth.id in data.ref('${org}.memberships.userId')`;
const admin = (org: string) => `auth.id in data.ref('${org}.memberships.adminUserId')`;
const editor = (org: string) => `auth.id in data.ref('${org}.memberships.editorUserId')`;
/** A linked row (`path`) belongs to the same label as this row. */
const sameLabel = (path: string) =>
  `data.ref('${path}.organization.id') == data.ref('organization.id')`;
/** Role bookkeeping on a membership (`row` is data or newData). */
const roleFieldsMatch = (row: string) => `(
  (${row}.role == 'admin' && ${row}.adminUserId == ${row}.userId && ${row}.editorUserId == ${row}.userId) ||
  (${row}.role == 'editor' && ${row}.adminUserId == null && ${row}.editorUserId == ${row}.userId) ||
  (${row}.role == 'viewer' && ${row}.adminUserId == null && ${row}.editorUserId == null))`;
const labelRows = (writer: (org: string) => string, deleter = admin) => ({
  view: member("organization"),
  create: writer("organization"),
  update: writer("organization"),
  delete: deleter("organization"),
});

const rules = {
  attrs: { allow: { $default: "false" } },
  // Everything not listed below, including `credentials` and `personEmails`,
  // is admin-only (the server).
  $default: { allow: { $default: "false" } },
  $users: { allow: { view: "auth.id == data.id" } },

  // You see your own profile and the people who share a label with you.
  // Profiles are created only by the bootstrap route; you may rename yours.
  people: {
    allow: {
      view: "isMine || auth.id in data.ref('memberships.organization.memberships.userId')",
      create: "false",
      update: "isMine && newData.userId == data.userId",
      delete: "false",
    },
    bind: { isMine: "auth.id != null && data.userId == auth.id" },
  },

  organizations: {
    allow: {
      view: "auth.id in data.ref('memberships.userId')",
      create: "false",
      update: "auth.id in data.ref('memberships.adminUserId') && newData.slug == data.slug",
      delete: "auth.id in data.ref('memberships.adminUserId')",
    },
  },

  // Only admins add members, only as editors or viewers, naming the person's
  // own user id; admins promote later. You can't change your own membership
  // unless you're its admin already, so nobody promotes themselves.
  memberships: {
    allow: {
      view: member("organization"),
      create: `${admin("organization")} && personMatches && keyMatches
        && data.role in ['editor', 'viewer'] && ${roleFieldsMatch("data")}`,
      update: `${admin("organization")} && (data.userId != auth.id || data.adminUserId == auth.id)
        && newData.userId == data.userId && newData.memberKey == data.memberKey
        && newData.role in ['admin', 'editor', 'viewer'] && ${roleFieldsMatch("newData")}`,
      delete: admin("organization"),
      // The admin can't see someone who isn't a member yet, so Instant's
      // default link check (view the person) would refuse every invite. This
      // rule replaces it: the linked person must be the membership's user.
      link: { person: "linkedData.userId == data.userId" },
    },
    bind: {
      personMatches: "data.userId in data.ref('person.userId')",
      keyMatches: "data.memberKey == data.ref('organization.id')[0] + '/' + data.userId",
    },
  },

  teams: { allow: labelRows(admin) },
  catalogues: { allow: labelRows(admin) },
  // An artist with releases can't be deleted (the Jazz app checks this in a
  // serializable transaction; here the server checks it on every delete).
  artists: {
    allow: {
      ...labelRows(editor),
      delete: `${admin("organization")} && data.ref('releases.id') == []`,
    },
  },

  // A release's artist and catalogue are in its label, and its catalogue key
  // is "<label>/<number>", which is unique (see instant.schema.ts).
  releases: {
    allow: {
      view: member("organization"),
      create: `${editor("organization")} && relationsInLabel
        && data.catalogKey == data.ref('organization.id')[0] + '/' + data.catalogNumber`,
      update: `${editor("organization")} && relationsInLabel
        && newData.catalogKey == data.ref('organization.id')[0] + '/' + newData.catalogNumber`,
      delete: admin("organization"),
    },
    bind: {
      relationsInLabel: `${sameLabel("artist")}
        && (data.ref('catalogue.id') == [] || ${sameLabel("catalogue")})`,
    },
  },

  teamAssignments: {
    allow: {
      view: member("organization"),
      create: `${admin("organization")} && inLabel`,
      update: `${admin("organization")} && inLabel`,
      delete: admin("organization"),
    },
    bind: { inLabel: `${sameLabel("team")} && ${sameLabel("membership")}` },
  },

  // Per-person release roles grant accountability, so only admins hand them out.
  releaseAssignments: {
    allow: {
      view: member("organization"),
      create: `${admin("organization")} && inLabel`,
      update: `${admin("organization")} && inLabel`,
      delete: admin("organization"),
    },
    bind: { inLabel: `${sameLabel("release")} && ${sameLabel("membership")}` },
  },

  // Editors staff releases with teams from the same label.
  releaseTeams: {
    allow: {
      view: member("organization"),
      create: `${editor("organization")} && ${sameLabel("release")} && ${sameLabel("team")}`,
      update: "false",
      delete: editor("organization"),
    },
  },
} satisfies InstantRules;

export default rules;
