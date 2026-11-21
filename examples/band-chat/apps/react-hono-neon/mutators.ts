import { HISTORY_PAGE } from "./queries";
import type { Message, Profile, Reaction, ReadMarker, Room, RoomMember, Stroke } from "./schema";
import type { MutationDef, Overlay } from "./src/lib/live";

/**
 * BandChat's writes, as the components call them: `db.mutate(mutators.x.y(args))`.
 * Each names a server write handler (server/mutators.ts, where the checks
 * are) and says how it changes the live queries at once, until its
 * transaction has synced (Neon's optimistic-mutations pattern).
 */
type Rows = Parameters<Overlay[string]>[0];
type Row = Rows[number];
const def = (name: string, args: unknown, overlay?: (me: string) => Overlay): MutationDef => ({ name, args, overlay });

const has = (rows: Rows, id: string) => rows.some((row) => row.id === id);
const without = (rows: Rows, match: (row: Row) => boolean) => (rows.some(match) ? rows.filter((row) => !match(row)) : rows);
const patch = (rows: Rows, id: string, change: Record<string, unknown>) =>
  has(rows, id) ? rows.map((row) => (row.id === id ? { ...row, ...change } : row)) : rows;
const inRoom = (roomId: string, add: (rows: Rows) => Rows) => (rows: Rows, args: Record<string, unknown>) =>
  args.roomId === roomId ? add(rows) : rows;

/** A sent message, newest first, and the room's new activity. */
function sent(message: Message): Overlay {
  return {
    "messages.newest": inRoom(message.roomId, (rows) =>
      has(rows, message.id) ? rows : [message, ...rows].slice(0, HISTORY_PAGE),
    ),
    "rooms.mine": (rows) => patch(rows, message.roomId, { lastActivityAt: message.createdAt }),
  };
}

type Id = { id: string };

export const mutators = {
  profiles: {
    create: (args: { id: string; displayName: string }) =>
      def("profiles.create", args, (me) => ({
        "profiles.mine": (rows) =>
          rows.length ? rows : [{ ...args, author: me, createdAt: Date.now() } satisfies Profile],
      })),
    update: (args: { id: string; displayName: string }) =>
      def("profiles.update", args, () => ({
        "profiles.mine": (rows) => patch(rows, args.id, { displayName: args.displayName }),
        "profiles.known": (rows) => patch(rows, args.id, { displayName: args.displayName }),
      })),
  },

  rooms: {
    /** A room and its creator's membership, in one transaction. */
    create: (args: { id: string; memberId: string; name: string; profileId: string }) =>
      def("rooms.create", args, (me) => {
        const room: Room = { id: args.id, name: args.name, createdBy: me, createdAt: Date.now(), lastActivityAt: null };
        const member: RoomMember = { id: args.memberId, roomId: args.id, memberAuthor: me, memberProfileId: args.profileId };
        return {
          "rooms.mine": (rows) => (has(rows, room.id) ? rows : [...rows, room]),
          "roomMembers.mine": (rows) => (has(rows, member.id) ? rows : [...rows, member]),
          "roomMembers.byRoom": inRoom(args.id, (rows) => (has(rows, member.id) ? rows : [...rows, member])),
        };
      }),
    rename: (args: { id: string; name: string }) =>
      def("rooms.rename", args, () => ({ "rooms.mine": (rows) => patch(rows, args.id, { name: args.name }) })),
  },

  members: {
    /** Admits someone who asked to join (or the creator themselves), clearing their request. */
    admit: (args: { id: string; roomId: string; memberAuthor: string; memberProfileId: string }) =>
      def("members.admit", args, (me) => ({
        "roomMembers.byRoom": inRoom(args.roomId, (rows) => (has(rows, args.id) ? rows : [...rows, args])),
        "roomMembers.mine": (rows) => (args.memberAuthor !== me || has(rows, args.id) ? rows : [...rows, args]),
        "joinRequests.byRoom": inRoom(args.roomId, (rows) => without(rows, (row) => row.requester === args.memberAuthor)),
      })),
    /** The creator removes a member; a member may also leave. */
    remove: (args: Id) =>
      def("members.remove", args, () => ({
        "roomMembers.byRoom": (rows) => without(rows, (row) => row.id === args.id),
        "roomMembers.mine": (rows) => without(rows, (row) => row.id === args.id),
      })),
  },

  joinRequests: {
    create: (args: { id: string; roomId: string; profileId: string }) =>
      def("joinRequests.create", args, (me) => ({
        "joinRequests.mine": inRoom(args.roomId, (rows) => (has(rows, args.id) ? rows : [...rows, { ...args, requester: me }])),
      })),
    delete: (args: Id) =>
      def("joinRequests.delete", args, () => ({
        "joinRequests.mine": (rows) => without(rows, (row) => row.id === args.id),
        "joinRequests.byRoom": (rows) => without(rows, (row) => row.id === args.id),
      })),
  },

  readMarkers: {
    /** Moves this reader's marker for a room forward (creating it the first time). */
    mark: (args: { id: string; roomId: string; lastReadAt: number }) =>
      def("readMarkers.mark", args, (me) => ({
        "readMarkers.mine": (rows) =>
          has(rows, args.id)
            ? patch(rows, args.id, { lastReadAt: args.lastReadAt })
            : [...rows, { ...args, reader: me } satisfies ReadMarker],
      })),
  },

  messages: {
    /** A message and the room's new activity, together. */
    send: (args: { id: string; roomId: string; senderId: string; text: string }) =>
      def("messages.send", args, () => sent({ ...args, canvasId: null, createdAt: Date.now() })),
    delete: (args: Id) =>
      def("messages.delete", args, () => ({ "messages.newest": (rows) => without(rows, (row) => row.id === args.id) })),
  },

  reactions: {
    /** Adds this author's emoji to a message, or takes it back. */
    toggle: (args: { id: string; roomId: string; messageId: string; emoji: string }) =>
      def("reactions.toggle", args, (me) => ({
        "reactions.byRoom": inRoom(args.roomId, (rows) => {
          const mine = (row: Row) => row.messageId === args.messageId && row.author === me && row.emoji === args.emoji;
          return rows.some(mine) ? rows.filter((row) => !mine(row)) : [...rows, { ...args, author: me } satisfies Reaction];
        }),
      })),
  },

  sketches: {
    /** The canvas, its message and the room's activity commit together. */
    start: (args: { canvasId: string; messageId: string; roomId: string; senderId: string }) =>
      def("sketches.start", args, () =>
        sent({ id: args.messageId, roomId: args.roomId, senderId: args.senderId, text: "", canvasId: args.canvasId, createdAt: Date.now() }),
      ),
    draw: (args: { id: string; canvasId: string; roomId: string; color: string; width: number; points: number[] }) =>
      def("sketches.draw", args, (me) => ({
        "strokes.byCanvas": (rows, query) =>
          query.canvasId !== args.canvasId || has(rows, args.id)
            ? rows
            : [...rows, { ...args, author: me, createdAt: Date.now() } satisfies Stroke],
      })),
    undo: (args: Id) =>
      def("sketches.undo", args, () => ({ "strokes.byCanvas": (rows) => without(rows, (row) => row.id === args.id) })),
  },
};
