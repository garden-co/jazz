import { defineSchema, defineTable } from "convex/server";
import { authTables } from "@convex-dev/auth/server";
import { v } from "convex/values";

/**
 * BandChat: the Jazz app's tables. `author`, `memberAuthor`, `requester` and
 * `reader` hold the Convex Auth user id. Who may read or write what is
 * checked inside each function (see lib/access.ts).
 */
export default defineSchema({
  ...authTables,
  profiles: defineTable({
    author: v.id("users"),
    displayName: v.string(),
    // A small, client-downscaled avatar image. Large media belongs in a
    // message attachment (file storage).
    avatar: v.optional(v.bytes()),
    avatarType: v.optional(v.string()),
  }).index("by_author", ["author"]),
  rooms: defineTable({
    name: v.string(),
    creator: v.id("users"),
    // Denormalized "last activity" carrier, written by members when they
    // post. Unread state is derived per reader from `readMarkers`.
    lastActivityAt: v.optional(v.number()),
  }).index("by_creator", ["creator"]),
  roomMembers: defineTable({
    roomId: v.id("rooms"),
    memberAuthor: v.id("users"),
    memberProfileId: v.id("profiles"),
  })
    .index("by_room_author", ["roomId", "memberAuthor"])
    .index("by_author", ["memberAuthor"]),
  // "Ask to join": anyone holding a room link may ask; only the room creator
  // can see the request and admit the requester.
  joinRequests: defineTable({
    roomId: v.id("rooms"),
    requester: v.id("users"),
    profileId: v.id("profiles"),
  })
    .index("by_room_requester", ["roomId", "requester"])
    .index("by_requester", ["requester"]),
  // One private row per reader and room: everything newer is unread.
  readMarkers: defineTable({
    roomId: v.id("rooms"),
    reader: v.id("users"),
    lastReadAt: v.number(),
  })
    .index("by_reader_room", ["reader", "roomId"]),
  messages: defineTable({
    roomId: v.id("rooms"),
    senderId: v.id("profiles"),
    text: v.string(),
    attachment: v.optional(v.id("_storage")),
    attachmentName: v.optional(v.string()),
    attachmentType: v.optional(v.string()),
    attachmentSize: v.optional(v.number()),
    // A message can carry a shared sketch instead of (or as well as) a file.
    canvasId: v.optional(v.id("canvases")),
  }).index("by_room", ["roomId"]),
  reactions: defineTable({
    roomId: v.id("rooms"),
    messageId: v.id("messages"),
    author: v.id("users"),
    emoji: v.string(),
  })
    .index("by_room", ["roomId"])
    .index("by_message", ["messageId"]),
  canvases: defineTable({ roomId: v.id("rooms"), title: v.string() }),
  strokes: defineTable({
    canvasId: v.id("canvases"),
    roomId: v.id("rooms"),
    author: v.id("users"),
    color: v.string(),
    width: v.number(),
    // Flat [x0, y0, x1, y1, …] in canvas units (0–1000 on both axes).
    points: v.array(v.number()),
  }).index("by_canvas", ["canvasId"]),
});
