import { schema as s } from "jazz-tools";

// A collaborative code editor where Jazz is the entire sync backbone:
// it durably stores Yjs binary updates + periodic snapshots, while Yjs +
// Monaco handle live text editing on the client.
const schema = {
  // One editable document / room. `shareToken` is the opaque id used in the URL.
  rooms: s.table({
    shareToken: s.string(),
    title: s.string(),
    editorLanguage: s.string().default("plaintext"),
    creator_session_user_id: s.string(),
    createdAt: s.timestamp(),
  }),

  // "Rooms I've opened or created" — used to scope the dashboard list.
  roomParticipants: s.table({
    room_id: s.ref("rooms"),
    session_user_id: s.string(),
    lastAccessedAt: s.timestamp(),
  }),

  // Append-only log of Yjs document updates. Each row is one binary Y update.
  roomYjsUpdates: s.table({
    room_id: s.ref("rooms"),
    update: s.bytes(),
    session_user_id: s.string(),
    provider_instance_id: s.string(),
    createdAt: s.timestamp(),
  }),

  // Periodic full-document snapshots so a fresh client doesn't replay the
  // entire update log. Bootstrap applies the latest snapshot, then the updates.
  roomYjsSnapshots: s.table({
    room_id: s.ref("rooms"),
    state: s.bytes(),
    textHash: s.string().optional(),
    session_user_id: s.string().optional(),
    createdAt: s.timestamp(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);

export type Room = s.RowOf<typeof app.rooms>;
export type RoomParticipant = s.RowOf<typeof app.roomParticipants>;
export type RoomYjsUpdate = s.RowOf<typeof app.roomYjsUpdates>;
export type RoomYjsSnapshot = s.RowOf<typeof app.roomYjsSnapshots>;
