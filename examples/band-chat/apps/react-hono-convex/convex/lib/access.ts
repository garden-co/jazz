import { getAuthUserId } from "@convex-dev/auth/server";
import { ConvexError } from "convex/values";
import type { Doc, Id } from "../_generated/dataModel";
import type { QueryCtx } from "../_generated/server";

/**
 * Who can do what in BandChat (the Jazz app's permissions.ts, as checks the
 * functions run before they read or write):
 *
 * - Rooms, their messages, reactions, sketches and member lists are visible
 *   to the room's members (and the room to its creator).
 * - Only the creator renames a room and admits members, and only people who
 *   asked to join (or the creator themselves). Members leave on their own;
 *   the creator removes anyone.
 * - Members post as their own profile and bump the room's activity; senders
 *   delete their own messages; reactions and strokes are their author's.
 * - Join requests are visible to the requester and the room creator; read
 *   markers only to their reader.
 * - Profiles are visible to their owner, co-members and creators reviewing
 *   a request.
 */
export const denied = () => new ConvexError("permission_denied");

export async function requireAuthor(ctx: QueryCtx) {
  const author = await getAuthUserId(ctx);
  if (!author) throw denied();
  return author;
}

export function myProfile(ctx: QueryCtx, author: Id<"users">) {
  return ctx.db
    .query("profiles")
    .withIndex("by_author", (q) => q.eq("author", author))
    .first();
}

export async function requireProfile(ctx: QueryCtx, author: Id<"users">) {
  const profile = await myProfile(ctx, author);
  if (!profile) throw denied();
  return profile;
}

export function membership(ctx: QueryCtx, roomId: Id<"rooms">, author: Id<"users">) {
  return ctx.db
    .query("roomMembers")
    .withIndex("by_room_author", (q) => q.eq("roomId", roomId).eq("memberAuthor", author))
    .first();
}

export async function requireMember(ctx: QueryCtx, roomId: Id<"rooms">, author: Id<"users">) {
  if (!(await membership(ctx, roomId, author))) throw denied();
}

/** The room, if `author` may read it: its creator or a member. */
export async function readableRoom(ctx: QueryCtx, roomId: Id<"rooms">, author: Id<"users">) {
  const room = await ctx.db.get(roomId);
  if (!room) return null;
  if (room.creator === author || (await membership(ctx, roomId, author))) return room;
  return null;
}

export async function requireReadableRoom(ctx: QueryCtx, roomId: Id<"rooms">, author: Id<"users">) {
  const room = await readableRoom(ctx, roomId, author);
  if (!room) throw denied();
  return room;
}

export async function requireCreator(ctx: QueryCtx, roomId: Id<"rooms">, author: Id<"users">) {
  const room = await ctx.db.get(roomId);
  if (!room || room.creator !== author) throw denied();
  return room;
}

/** A message whose room `author` belongs to. */
export async function requireMessageInMyRoom(
  ctx: QueryCtx,
  messageId: Id<"messages">,
  author: Id<"users">,
): Promise<Doc<"messages">> {
  const message = await ctx.db.get(messageId);
  if (!message) throw denied();
  await requireMember(ctx, message.roomId, author);
  return message;
}
