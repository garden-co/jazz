import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import {
  denied,
  requireAuthor,
  requireMember,
  requireMessageInMyRoom,
  requireProfile,
} from "./lib/access";

// The newest page of a room's history. Attachment bytes live in file storage
// and load through `attachmentUrl` once the attachment is needed.
const HISTORY_PAGE = 200;
// Counting stops here; the badge then reads "99+".
const UNREAD_CAP = 100;

/** A room's newest messages, newest first. */
export const page = query({
  args: { roomId: v.id("rooms") },
  handler: async (ctx, { roomId }) => {
    await requireMember(ctx, roomId, await requireAuthor(ctx));
    return ctx.db
      .query("messages")
      .withIndex("by_room", (q) => q.eq("roomId", roomId))
      .order("desc")
      .take(HISTORY_PAGE);
  },
});

/**
 * Unread counts for the room list: per room, messages from others newer than
 * your marker, up to the cap. One query for the whole list rather than one
 * subscription per room, so opening BandChat doesn't start ~256 queries at
 * once (the local backend times some of them out under that burst).
 */
export const unreadCounts = query({
  args: { rooms: v.array(v.object({ roomId: v.id("rooms"), since: v.optional(v.number()) })) },
  handler: async (ctx, { rooms }) => {
    const author = await requireAuthor(ctx);
    const me = await requireProfile(ctx, author);
    const counted = await Promise.all(
      rooms.map(async ({ roomId, since }) => {
        await requireMember(ctx, roomId, author);
        const newer = await ctx.db
          .query("messages")
          .withIndex("by_room", (q) =>
            since === undefined ? q.eq("roomId", roomId) : q.eq("roomId", roomId).gt("_creationTime", since),
          )
          .filter((q) => q.neq(q.field("senderId"), me._id))
          .take(UNREAD_CAP);
        return [roomId, newer.length] as const;
      }),
    );
    return Object.fromEntries(counted) as Record<string, number>;
  },
});

/** Members post as their own profile; the room's activity moves with it. */
export const send = mutation({
  args: {
    roomId: v.id("rooms"),
    text: v.string(),
    attachment: v.optional(v.id("_storage")),
    attachmentName: v.optional(v.string()),
    attachmentType: v.optional(v.string()),
    attachmentSize: v.optional(v.number()),
  },
  handler: async (ctx, { roomId, ...message }) => {
    const author = await requireAuthor(ctx);
    await requireMember(ctx, roomId, author);
    const sender = await requireProfile(ctx, author);
    await ctx.db.insert("messages", { roomId, senderId: sender._id, ...message });
    await ctx.db.patch(roomId, { lastActivityAt: Date.now() });
  },
});

/** The canvas, its message and the room's activity, in one transaction. */
export const startSketch = mutation({
  args: { roomId: v.id("rooms") },
  handler: async (ctx, { roomId }) => {
    const author = await requireAuthor(ctx);
    await requireMember(ctx, roomId, author);
    const sender = await requireProfile(ctx, author);
    const canvasId = await ctx.db.insert("canvases", { roomId, title: "Sketch" });
    await ctx.db.insert("messages", { roomId, senderId: sender._id, text: "", canvasId });
    await ctx.db.patch(roomId, { lastActivityAt: Date.now() });
  },
});

/** Senders delete their own messages. */
export const remove = mutation({
  args: { messageId: v.id("messages") },
  handler: async (ctx, { messageId }) => {
    const author = await requireAuthor(ctx);
    const message = await ctx.db.get(messageId);
    const sender = message && (await ctx.db.get(message.senderId));
    if (!message || sender?.author !== author) throw denied();
    if (message.attachment) await ctx.storage.delete(message.attachment);
    await ctx.db.delete(messageId);
  },
});

/** An upload URL for an attachment (Convex file storage). */
export const generateUploadUrl = mutation({
  args: {},
  handler: async (ctx) => {
    await requireAuthor(ctx);
    return ctx.storage.generateUploadUrl();
  },
});

/** Where to fetch a message's attachment, for the room's members. */
export const attachmentUrl = query({
  args: { messageId: v.id("messages") },
  handler: async (ctx, { messageId }) => {
    const message = await requireMessageInMyRoom(ctx, messageId, await requireAuthor(ctx));
    return message.attachment ? ctx.storage.getUrl(message.attachment) : null;
  },
});
