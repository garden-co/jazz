// Bench plumbing for scripts/seed.mjs: the parts of the BandChat fixture that
// the app itself never writes this way (run with `npx convex run`).
import { v } from "convex/values";
import { internal } from "./_generated/api";
import type { Id, TableNames } from "./_generated/dataModel";
import { internalAction, internalMutation } from "./_generated/server";
import { bandChatFixture } from "../../../../../dev/app-comparisons/fixtures/band-chat.mjs";

/** The local deployment limits writes to 4 MiB/s: when a batch hits it, wait and retry. */
async function throttled<T>(write: () => Promise<T>): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await write();
    } catch (error) {
      if (attempt >= 60 || !String(error).includes("Too many writes")) throw error;
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

const TABLES: TableNames[] = [
  "strokes", "canvases", "reactions", "messages", "readMarkers", "joinRequests", "roomMembers",
  "rooms", "profiles", "authRefreshTokens", "authSessions", "authAccounts",
  "authVerificationCodes", "authVerifiers", "authRateLimits", "users",
];

export const clear = internalMutation({
  args: { table: v.string() },
  handler: async (ctx, { table }) => {
    const docs = await ctx.db.query(table as TableNames).take(4000);
    for (const doc of docs) await ctx.db.delete(doc._id);
    return docs.length === 4000;
  },
});

export const reset = internalAction({
  args: {},
  handler: async (ctx) => {
    for (const table of TABLES) while (await throttled(() => ctx.runMutation(internal.seed.clear, { table }))) {}
  },
});

/**
 * The seed user's rooms, each with its creator's membership (as
 * `rooms.create` writes them) and the fixture's last activity time.
 */
export const createRooms = internalMutation({
  args: { creator: v.id("users"), profileId: v.id("profiles") },
  handler: async (ctx, { creator, profileId }) => {
    const ids: Record<string, Id<"rooms">> = {};
    for (const room of bandChatFixture().rooms) {
      const roomId = await ctx.db.insert("rooms", {
        name: room.name,
        creator,
        lastActivityAt: room.createdAtMs,
      });
      await ctx.db.insert("roomMembers", { roomId, memberAuthor: creator, memberProfileId: profileId });
      ids[room.key] = roomId;
    }
    return ids;
  },
});

export const insertMessages = internalMutation({
  args: { messages: v.array(v.object({ roomId: v.id("rooms"), senderId: v.id("profiles"), text: v.string() })) },
  handler: async (ctx, { messages }) => {
    for (const message of messages) await ctx.db.insert("messages", message);
  },
});

/** The fixture's messages, sent as their senders' profiles, in fixture order. */
export const writeMessages = internalAction({
  args: { rooms: v.record(v.string(), v.id("rooms")), profiles: v.record(v.string(), v.id("profiles")) },
  handler: async (ctx, { rooms, profiles }) => {
    const all = bandChatFixture().messages.map((m: { roomKey: string; sender: string; text: string }) => ({
      roomId: rooms[m.roomKey]!,
      senderId: profiles[m.sender]!,
      text: m.text,
    }));
    for (let i = 0; i < all.length; i += 500) {
      await throttled(() => ctx.runMutation(internal.seed.insertMessages, { messages: all.slice(i, i + 500) }));
    }
  },
});
