import type { InstantRules } from "@instantdb/react";

/**
 * Wequencer's rules, the same as the Jazz app's permissions.ts. The session
 * creator administers it; owners and editors change tracks, pads and the
 * transport; viewers only read. A step's track and pattern must belong to the
 * same session. Rows reach their session through links, which `data.ref` follows.
 */
const isMember = (session: string) => `auth.id in data.ref('${session}.members.member_author')`;
const canEdit = (session: string) => `auth.id in data.ref('${session}.members.editor_author')`;
const isCreator = (session: string) => `auth.id in data.ref('${session}.creator')`;
const sessionRows = (session = "session") => ({
  view: isMember(session),
  create: canEdit(session),
  update: canEdit(session),
  delete: isCreator(session),
});

const rules = {
  attrs: { allow: { $default: "false" } },
  // Everything not listed below, including `credentials`, is admin-only.
  $default: { allow: { $default: "false" } },
  $users: { allow: { view: "auth.id == data.id" } },

  // Bandmates see each other's display name once that profile has shown
  // presence in a session they are members of. Profiles are written by the
  // server (app/api/bootstrap), like the Jazz app's.
  profiles: {
    allow: {
      view: `isMine || ${isMember("presence.session")}`,
      create: "isMine",
      update: "isMine && newData.author == auth.id",
      delete: "isMine",
    },
    bind: { isMine: "auth.id != null && data.author == auth.id" },
  },

  sessions: {
    allow: {
      view: "isCreator || auth.id in data.ref('members.member_author')",
      create: "isCreator",
      update: "isCreator && newData.creator == data.creator",
      delete: "isCreator",
    },
    bind: { isCreator: "auth.id != null && data.creator == auth.id" },
  },

  // Only the creator adds and removes members. `editor_author` must match the role.
  session_members: {
    allow: {
      view: `${isMember("session")} || ${isCreator("session")}`,
      create: `${isCreator("session")} && editorMatchesRole`,
      update: "false",
      delete: isCreator("session"),
    },
    bind: {
      editorMatchesRole: `(data.role in ['owner', 'editor'] && data.editor_author == data.member_author)
        || (data.role == 'viewer' && data.editor_author == null)`,
    },
  },

  tracks: { allow: sessionRows() },
  patterns: { allow: sessionRows() },

  // An editor of the track's session writes pads; the pattern must be in the same session.
  steps: {
    allow: {
      view: isMember("track.session"),
      create: `${canEdit("track.session")} && fitsSession`,
      update: `${canEdit("track.session")} && fitsSession`,
      delete: canEdit("track.session"),
    },
    bind: { fitsSession: "data.ref('track.session.id') == data.ref('pattern.session.id')" },
  },

  // An observation may name the playing pattern, which must be this session's.
  transport_observations: {
    allow: {
      view: isMember("session"),
      create: `${canEdit("session")} && (data.ref('pattern.id') == [] || data.ref('pattern.session.id') == data.ref('session.id'))`,
      update: canEdit("session"),
      delete: canEdit("session"),
    },
  },

  presence: {
    allow: {
      view: isMember("session"),
      create: `${isMember("session")} && isMine`,
      update: `${isMember("session")} && isMine`,
      delete: "isMine",
    },
    bind: { isMine: "auth.id in data.ref('profile.author')" },
  },
} satisfies InstantRules;

export default rules;
