import {
  createBuilder,
  createSchema,
  json,
  number,
  relationships,
  string,
  table,
  type Row,
} from "@rocicorp/zero";

// BandChat's synced tables (Postgres: db/001_init.sql). Better Auth's tables
// live in Postgres' "auth" schema and are not synced.

const profiles = table("profiles")
  .columns({ id: string(), author: string(), displayName: string(), createdAt: number() })
  .primaryKey("id");

const rooms = table("rooms")
  .columns({
    id: string(),
    name: string(),
    createdBy: string(),
    createdAt: number(),
    // Denormalized "last activity" carrier, written by members when they
    // post. Unread state is derived per reader from `readMarkers`.
    lastActivityAt: number().optional(),
  })
  .primaryKey("id");

const roomMembers = table("roomMembers")
  .columns({
    id: string(),
    roomId: string(),
    memberAuthor: string(),
    // The admitted member's profile, so co-members can see their name.
    memberProfileId: string().optional(),
  })
  .primaryKey("id");

// "Ask to join": anyone holding a room link may ask; only the room creator
// can see the request and admit the requester.
const joinRequests = table("joinRequests")
  .columns({ id: string(), roomId: string(), requester: string(), profileId: string() })
  .primaryKey("id");

// One private row per reader and room: everything newer is unread.
const readMarkers = table("readMarkers")
  .columns({ id: string(), roomId: string(), reader: string(), lastReadAt: number() })
  .primaryKey("id");

const messages = table("messages")
  .columns({
    id: string(),
    roomId: string(),
    senderId: string(),
    text: string(),
    // A message can carry a shared sketch.
    canvasId: string().optional(),
    createdAt: number(),
  })
  .primaryKey("id");

const reactions = table("reactions")
  .columns({ id: string(), roomId: string(), messageId: string(), author: string(), emoji: string() })
  .primaryKey("id");

const canvases = table("canvases")
  .columns({ id: string(), roomId: string(), title: string() })
  .primaryKey("id");

const strokes = table("strokes")
  .columns({
    id: string(),
    canvasId: string(),
    roomId: string(),
    author: string(),
    color: string(),
    width: number(),
    // Flat [x0, y0, x1, y1, …] in canvas units (0–1000 on both axes).
    points: json<number[]>(),
    createdAt: number(),
  })
  .primaryKey("id");

const profileRelationships = relationships(profiles, ({ many }) => ({
  messagesViaSender: many({ sourceField: ["id"], destSchema: messages, destField: ["senderId"] }),
  membershipsViaProfile: many({ sourceField: ["id"], destSchema: roomMembers, destField: ["memberProfileId"] }),
  joinRequestsViaProfile: many({ sourceField: ["id"], destSchema: joinRequests, destField: ["profileId"] }),
}));

const roomRelationships = relationships(rooms, ({ many }) => ({
  members: many({ sourceField: ["id"], destSchema: roomMembers, destField: ["roomId"] }),
}));

// Every room-scoped table has a `room` relationship, for the membership checks.
const roomMembersRoom = relationships(roomMembers, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));
const joinRequestsRoom = relationships(joinRequests, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));
const messagesRoom = relationships(messages, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));
const reactionsRoom = relationships(reactions, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));
const canvasesRoom = relationships(canvases, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));
const strokesRoom = relationships(strokes, ({ one }) => ({
  room: one({ sourceField: ["roomId"], destSchema: rooms, destField: ["id"] }),
}));

export const schema = createSchema({
  tables: [profiles, rooms, roomMembers, joinRequests, readMarkers, messages, reactions, canvases, strokes],
  relationships: [
    profileRelationships,
    roomRelationships,
    roomMembersRoom,
    joinRequestsRoom,
    messagesRoom,
    reactionsRoom,
    canvasesRoom,
    strokesRoom,
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

export type Profile = Row<typeof schema.tables.profiles>;
export type Room = Row<typeof schema.tables.rooms>;
export type RoomMember = Row<typeof schema.tables.roomMembers>;
export type JoinRequest = Row<typeof schema.tables.joinRequests>;
export type ReadMarker = Row<typeof schema.tables.readMarkers>;
export type Message = Row<typeof schema.tables.messages>;
export type Reaction = Row<typeof schema.tables.reactions>;
export type Canvas = Row<typeof schema.tables.canvases>;
export type Stroke = Row<typeof schema.tables.strokes>;
