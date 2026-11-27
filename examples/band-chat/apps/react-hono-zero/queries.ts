import { defineQueries, defineQuery, type ExpressionBuilder } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData, type Schema } from "./schema";

/**
 * Who can read what in BandChat (writes are checked in mutators.ts):
 *
 * - A room, its members, messages, reactions and sketches are for its
 *   members; the room's creator also sees the room itself (and its join
 *   requests) without being a member.
 * - Join requests are for the requester and the room's creator.
 * - Read markers are private to their reader.
 * - A profile is visible to its owner, to anyone who can read a message they
 *   sent or a membership that names them, and to a room creator who can read
 *   their join request.
 *
 * zero-cache resolves each query on the server with the caller's context, so
 * these filters are enforced there.
 */
const me = (ctx: AuthData | undefined) => ctx?.userID ?? "";

type Where<T extends keyof Schema["tables"] & string> = ExpressionBuilder<T, Schema>;

const isMember = (ctx: AuthData | undefined) => ({ exists }: Where<"rooms">) =>
  exists("members", (member) => member.where("memberAuthor", me(ctx)));
const canReadRoom = (ctx: AuthData | undefined) => (eb: Where<"rooms">) =>
  eb.or(eb.cmp("createdBy", me(ctx)), isMember(ctx)(eb));

const roomArgs = z.object({ roomId: z.string() });
// The newest page of a room's history.
export const HISTORY_PAGE = 200;
export const UNREAD_CAP = 100;

export const queries = defineQueries({
  profiles: {
    mine: defineQuery(({ ctx }) =>
      zql.profiles.where("author", me(ctx)).orderBy("createdAt", "asc").limit(1),
    ),
    /** Everyone this account may know: one subscription feeds every name. */
    known: defineQuery(({ ctx }) =>
      zql.profiles.where(({ cmp, exists, or }) =>
        or(
          cmp("author", me(ctx)),
          exists("messagesViaSender", (m) => m.whereExists("room", (r) => r.where(isMember(ctx)))),
          exists("membershipsViaProfile", (m) => m.whereExists("room", (r) => r.where(canReadRoom(ctx)))),
          exists("joinRequestsViaProfile", (j) => j.whereExists("room", (r) => r.where("createdBy", me(ctx)))),
        ),
      ),
    ),
  },
  rooms: {
    mine: defineQuery(({ ctx }) => zql.rooms.where(canReadRoom(ctx))),
  },
  roomMembers: {
    mine: defineQuery(({ ctx }) => zql.roomMembers.where("memberAuthor", me(ctx))),
    byRoom: defineQuery(roomArgs, ({ ctx, args }) =>
      zql.roomMembers.where("roomId", args.roomId).whereExists("room", (r) => r.where(canReadRoom(ctx))),
    ),
  },
  readMarkers: {
    mine: defineQuery(({ ctx }) => zql.readMarkers.where("reader", me(ctx))),
  },
  joinRequests: {
    /** The room creator's view of who asked. */
    byRoom: defineQuery(roomArgs, ({ ctx, args }) =>
      zql.joinRequests
        .where("roomId", args.roomId)
        .whereExists("room", (r) => r.where("createdBy", me(ctx))),
    ),
    /** The requester's own pending request. */
    mine: defineQuery(roomArgs, ({ ctx, args }) =>
      zql.joinRequests.where("roomId", args.roomId).where("requester", me(ctx)),
    ),
  },
  messages: {
    /** A room's newest messages, newest first. */
    newest: defineQuery(roomArgs, ({ ctx, args }) =>
      zql.messages
        .where("roomId", args.roomId)
        .whereExists("room", (r) => r.where(isMember(ctx)))
        .orderBy("createdAt", "desc")
        .limit(HISTORY_PAGE),
    ),
    /** Messages from others since this reader's marker, for the unread badge. */
    unread: defineQuery(
      z.object({ roomId: z.string(), myProfileId: z.string(), readAt: z.number().optional() }),
      ({ ctx, args }) => {
        let q = zql.messages
          .where("roomId", args.roomId)
          .where("senderId", "!=", args.myProfileId)
          .whereExists("room", (r) => r.where(isMember(ctx)));
        if (args.readAt !== undefined) q = q.where("createdAt", ">", args.readAt);
        return q.limit(UNREAD_CAP);
      },
    ),
  },
  reactions: {
    byRoom: defineQuery(roomArgs, ({ ctx, args }) =>
      zql.reactions.where("roomId", args.roomId).whereExists("room", (r) => r.where(isMember(ctx))),
    ),
  },
  strokes: {
    byCanvas: defineQuery(z.object({ canvasId: z.string() }), ({ ctx, args }) =>
      zql.strokes
        .where("canvasId", args.canvasId)
        .whereExists("room", (r) => r.where(isMember(ctx)))
        .orderBy("createdAt", "asc"),
    ),
  },
});
