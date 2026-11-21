import type { InstantRules } from "@instantdb/react";

/**
 * BandChat's rules, the same as the Jazz app's permissions.ts: rooms are
 * private to their members; only a room's creator admits people, and only
 * people who asked to join (or the creator themselves). Rows of a room reach
 * it through their `room` link, which `data.ref` follows.
 */
const me = "auth.id != null && auth.id";
const isMemberOf = (room: string) => `auth.id in data.ref('${room}.members.memberAuthor')`;
const isCreatorOf = (room: string) => `auth.id in data.ref('${room}.creator')`;

const rules = {
  attrs: { allow: { $default: "false" } },
  // Everything not listed below, including `credentials`, is admin-only.
  $default: { allow: { $default: "false" } },
  $users: { allow: { view: "auth.id == data.id" } },

  // Profiles are visible to their owner, to co-members and to a room creator
  // reviewing their join request. (The Jazz rule also shows the sender of any
  // message you can read; following every message a profile sent made each
  // profile check take seconds, so a former member shows as "Bandmate".)
  profiles: {
    allow: {
      view: `isMine
        || ${isMemberOf("memberships.room")}
        || ${isCreatorOf("joinRequests.room")}`,
      create: "isMine",
      update: "isMine && newData.author == auth.id",
      delete: "isMine",
    },
    bind: { isMine: `${me} == data.author` },
  },

  // The creator may rename the room. Any member may record new activity, but
  // the name stays unchanged. Anyone may create a room (they are its creator).
  rooms: {
    allow: {
      view: "isCreator || isMember",
      create: "isCreator",
      update:
        "newData.creator == data.creator && (isCreator || (isMember && newData.name == data.name))",
      delete: "isCreator",
    },
    bind: {
      isCreator: `${me} == data.creator`,
      isMember: "auth.id in data.ref('members.memberAuthor')",
    },
  },

  // Only the room creator admits members: the membership names the admitted
  // account's own profile, and that account is the creator or asked to join.
  roomMembers: {
    allow: {
      view: `${isMemberOf("room")} || ${isCreatorOf("room")}`,
      create: `${isCreatorOf("room")}
        && data.memberAuthor in data.ref('memberProfile.author')
        && (data.memberAuthor == auth.id || data.memberAuthor in data.ref('room.joinRequests.requester'))`,
      update: "false",
      // The creator removes members; a member may also leave on their own.
      delete: `${isCreatorOf("room")} || ${me} == data.memberAuthor`,
    },
  },

  joinRequests: {
    allow: {
      view: `isMine || ${isCreatorOf("room")}`,
      create: "isMine && auth.id in data.ref('profile.author')",
      update: "false",
      delete: `isMine || ${isCreatorOf("room")}`,
      // Asking needs only the room's id: the requester can't see the room yet,
      // so this replaces the default check that they can view it.
      link: { room: "isMine" },
    },
    bind: { isMine: `${me} == data.requester` },
  },

  readMarkers: {
    allow: {
      view: "isMine",
      create: `isMine && ${isMemberOf("room")}`,
      update: `isMine && newData.reader == auth.id && ${isMemberOf("room")}`,
      delete: "isMine",
    },
    bind: { isMine: `${me} == data.reader` },
  },

  messages: {
    allow: {
      view: isMemberOf("room"),
      create: `${isMemberOf("room")}
        && auth.id in data.ref('sender.author')
        && (data.ref('canvas.id') == [] || data.ref('canvas.room.id') == data.ref('room.id'))`,
      update: "false",
      delete: "auth.id in data.ref('sender.author')",
    },
  },

  reactions: {
    allow: {
      view: isMemberOf("message.room"),
      create: "canMutate",
      update: "false",
      delete: "canMutate",
    },
    bind: { canMutate: `${me} == data.author && ${isMemberOf("message.room")}` },
  },

  canvases: {
    allow: {
      view: isMemberOf("room"),
      create: isMemberOf("room"),
      update: "false",
      delete: "false",
    },
  },

  // A removed member can no longer draw.
  strokes: {
    allow: {
      view: isMemberOf("canvas.room"),
      create: "canMutate",
      update: "false",
      delete: "canMutate",
    },
    bind: { canMutate: `${me} == data.author && ${isMemberOf("canvas.room")}` },
  },

  // Attachments: uploaded into the sender's own folder, readable by members
  // of a room where a message carries them.
  $files: {
    allow: {
      view: `isMine || ${isMemberOf("messages.room")}`,
      create: "isMine",
      update: "false",
      delete: "isMine",
    },
    bind: { isMine: "auth.id != null && data.path.startsWith(auth.id + '/')" },
  },
} satisfies InstantRules;

export default rules;
