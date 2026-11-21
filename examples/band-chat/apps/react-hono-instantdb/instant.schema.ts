import { i } from "@instantdb/react";

const _schema = i.schema({
  entities: {
    $users: i.entity({
      email: i.string().unique().indexed().optional(),
      // The name given at sign-up; the profile setup starts from it.
      name: i.string().optional(),
    }),
    $files: i.entity({
      path: i.string().unique().indexed(),
      url: i.string(),
    }),
    // Email + password sign-in (server/auth.ts). Admin-only: no client rule allows it.
    credentials: i.entity({
      email: i.string().unique().indexed(),
      passwordHash: i.string(),
    }),
    profiles: i.entity({
      author: i.string().indexed(),
      displayName: i.string(),
      // A small, client-downscaled avatar image, as a data URL. Large media
      // belongs in a message attachment, which lives in Instant Storage.
      avatar: i.string().optional(),
      avatarType: i.string().optional(),
      createdAt: i.date().indexed(),
    }),
    rooms: i.entity({
      name: i.string(),
      creator: i.string().indexed(),
      createdAt: i.date(),
      // Denormalized "last activity" carrier, written by members when they
      // post. Unread state is derived per reader from `readMarkers`.
      lastActivityAt: i.date().optional(),
    }),
    roomMembers: i.entity({
      memberAuthor: i.string().indexed(),
    }),
    // "Ask to join": anyone holding a room link may ask; only the room creator
    // can see the request and admit the requester.
    joinRequests: i.entity({
      requester: i.string().indexed(),
    }),
    // One private row per reader and room: everything newer is unread.
    readMarkers: i.entity({
      reader: i.string().indexed(),
      lastReadAt: i.date(),
    }),
    messages: i.entity({
      text: i.string(),
      attachmentName: i.string().optional(),
      attachmentType: i.string().optional(),
      attachmentSize: i.number().optional(),
      createdAt: i.date().indexed(),
    }),
    reactions: i.entity({
      author: i.string().indexed(),
      emoji: i.string(),
    }),
    canvases: i.entity({
      title: i.string(),
    }),
    strokes: i.entity({
      author: i.string().indexed(),
      color: i.string(),
      width: i.number(),
      // Flat [x0, y0, x1, y1, …] in canvas units (0–1000 on both axes).
      points: i.json<number[]>(),
      createdAt: i.date().indexed(),
    }),
  },
  links: {
    roomMembersRoom: {
      forward: {
        on: "roomMembers",
        has: "one",
        label: "room",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "rooms", has: "many", label: "members" },
    },
    roomMembersProfile: {
      forward: {
        on: "roomMembers",
        has: "one",
        label: "memberProfile",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "profiles", has: "many", label: "memberships" },
    },
    joinRequestsRoom: {
      forward: {
        on: "joinRequests",
        has: "one",
        label: "room",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "rooms", has: "many", label: "joinRequests" },
    },
    joinRequestsProfile: {
      forward: {
        on: "joinRequests",
        has: "one",
        label: "profile",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "profiles", has: "many", label: "joinRequests" },
    },
    readMarkersRoom: {
      forward: {
        on: "readMarkers",
        has: "one",
        label: "room",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "rooms", has: "many", label: "readMarkers" },
    },
    messagesRoom: {
      forward: { on: "messages", has: "one", label: "room", required: true, onDelete: "cascade" },
      reverse: { on: "rooms", has: "many", label: "messages" },
    },
    messagesSender: {
      forward: { on: "messages", has: "one", label: "sender", required: true },
      reverse: { on: "profiles", has: "many", label: "messages" },
    },
    messagesCanvas: {
      forward: { on: "messages", has: "one", label: "canvas" },
      reverse: { on: "canvases", has: "many", label: "messages" },
    },
    messagesAttachment: {
      forward: { on: "messages", has: "one", label: "attachment" },
      reverse: { on: "$files", has: "many", label: "messages" },
    },
    reactionsMessage: {
      forward: {
        on: "reactions",
        has: "one",
        label: "message",
        required: true,
        onDelete: "cascade",
      },
      reverse: { on: "messages", has: "many", label: "reactions" },
    },
    canvasesRoom: {
      forward: { on: "canvases", has: "one", label: "room", required: true, onDelete: "cascade" },
      reverse: { on: "rooms", has: "many", label: "canvases" },
    },
    strokesCanvas: {
      forward: { on: "strokes", has: "one", label: "canvas", required: true, onDelete: "cascade" },
      reverse: { on: "canvases", has: "many", label: "strokes" },
    },
  },
});

// This helps TypeScript display better IntelliSense
type _AppSchema = typeof _schema;
interface AppSchema extends _AppSchema {}
const schema: AppSchema = _schema;

export type { AppSchema };
export default schema;
