import { defineMutator, defineMutators, type Transaction } from "@rocicorp/zero";
import { z } from "zod";
import { zql, type AuthData } from "./schema";

/**
 * Who can do what in BandChat (reads are filtered in queries.ts):
 *
 * - Anyone signed in creates rooms; the creator renames the room and admits
 *   members, and only people who asked to join (or the creator) can be
 *   admitted, each with their own profile.
 * - The creator removes members; a member may also leave on their own.
 * - Members post messages (as their own profile), react, start sketches and
 *   draw; senders delete their own messages, authors their own reactions and
 *   strokes. Posting bumps the room's last activity, never its name.
 * - Anyone with a room link may ask to join; the requester or the creator
 *   withdraws the request.
 * - Read markers and profiles are written by their owner only.
 *
 * Mutators run optimistically on the client and again on the server; the
 * checks run on the server, where reads see all data.
 */
function mustUser(ctx: AuthData | undefined) {
  if (!ctx?.userID) throw new Error("Not signed in");
  return ctx.userID;
}

const onServer = (tx: Transaction) => tx.location === "server";

async function assertMember(tx: Transaction, me: string, roomId: string) {
  if (!onServer(tx)) return;
  const member = await tx.run(zql.roomMembers.where("roomId", roomId).where("memberAuthor", me).one());
  if (!member) throw new Error("You are not a member of this room");
}

async function assertCreator(tx: Transaction, me: string, roomId: string) {
  if (!onServer(tx)) return;
  const room = await tx.run(zql.rooms.where("id", roomId).where("createdBy", me).one());
  if (!room) throw new Error("Only the room creator can do that");
}

async function assertMyProfile(tx: Transaction, me: string, profileId: string) {
  if (!onServer(tx)) return;
  const profile = await tx.run(zql.profiles.where("id", profileId).where("author", me).one());
  if (!profile) throw new Error("That is not your profile");
}

const id = z.string();

export const mutators = defineMutators({
  profiles: {
    create: defineMutator(z.object({ id, displayName: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await tx.mutate.profiles.insert({ ...args, author: mustUser(ctx), createdAt: Date.now() });
    }),
    update: defineMutator(z.object({ id, displayName: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await assertMyProfile(tx, mustUser(ctx), args.id);
      await tx.mutate.profiles.update(args);
    }),
  },

  rooms: {
    /** A room and its creator's membership, in one transaction. */
    create: defineMutator(
      z.object({ id, memberId: id, name: z.string().min(1), profileId: id }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        if (onServer(tx) && (await tx.run(zql.rooms.where("id", args.id).one()))) throw new Error("Room exists");
        await assertMyProfile(tx, me, args.profileId);
        await tx.mutate.rooms.insert({ id: args.id, name: args.name, createdBy: me, createdAt: Date.now() });
        await tx.mutate.roomMembers.insert({ id: args.memberId, roomId: args.id, memberAuthor: me, memberProfileId: args.profileId });
      },
    ),
    rename: defineMutator(z.object({ id, name: z.string().min(1) }), async ({ tx, ctx, args }) => {
      await assertCreator(tx, mustUser(ctx), args.id);
      await tx.mutate.rooms.update(args);
    }),
  },

  members: {
    /**
     * Admits someone who asked to join (or the creator themselves), with
     * their own profile, and clears their request in the same transaction.
     */
    admit: defineMutator(
      z.object({ id, roomId: id, memberAuthor: id, memberProfileId: id }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertCreator(tx, me, args.roomId);
        const requests = await tx.run(
          zql.joinRequests.where("roomId", args.roomId).where("requester", args.memberAuthor),
        );
        if (onServer(tx)) {
          const profile = await tx.run(
            zql.profiles.where("id", args.memberProfileId).where("author", args.memberAuthor).one(),
          );
          if (!profile) throw new Error("That profile doesn't belong to them");
          if (args.memberAuthor !== me && requests.length === 0) throw new Error("They haven't asked to join");
        }
        await tx.mutate.roomMembers.insert(args);
        for (const request of requests) await tx.mutate.joinRequests.delete({ id: request.id });
      },
    ),
    /** The creator removes a member; a member may also leave (dropping their read markers). */
    remove: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const member = await tx.run(zql.roomMembers.where("id", args.id).one());
      if (!member) return;
      if (member.memberAuthor === me) {
        const markers = await tx.run(zql.readMarkers.where("roomId", member.roomId).where("reader", me));
        for (const marker of markers) await tx.mutate.readMarkers.delete({ id: marker.id });
      } else {
        await assertCreator(tx, me, member.roomId);
      }
      await tx.mutate.roomMembers.delete({ id: member.id });
    }),
  },

  joinRequests: {
    create: defineMutator(z.object({ id, roomId: id, profileId: id }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      await assertMyProfile(tx, me, args.profileId);
      await tx.mutate.joinRequests.insert({ ...args, requester: me });
    }),
    delete: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const request = await tx.run(zql.joinRequests.where("id", args.id).one());
      if (!request) return;
      if (request.requester !== me) await assertCreator(tx, me, request.roomId);
      await tx.mutate.joinRequests.delete(args);
    }),
  },

  readMarkers: {
    /** Moves this reader's marker for a room forward (creating it the first time). */
    mark: defineMutator(z.object({ id, roomId: id, lastReadAt: z.number() }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      await assertMember(tx, me, args.roomId);
      const existing = await tx.run(zql.readMarkers.where("id", args.id).one());
      if (existing && existing.reader !== me) throw new Error("Not your read marker");
      await tx.mutate.readMarkers.upsert({ ...args, reader: me });
    }),
  },

  messages: {
    /** A message and the room's new activity, together. */
    send: defineMutator(
      z.object({ id, roomId: id, senderId: id, text: z.string().min(1) }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertMember(tx, me, args.roomId);
        await assertMyProfile(tx, me, args.senderId);
        const now = Date.now();
        await tx.mutate.messages.insert({ ...args, createdAt: now });
        await tx.mutate.rooms.update({ id: args.roomId, lastActivityAt: now });
      },
    ),
    delete: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const message = await tx.run(zql.messages.where("id", args.id).one());
      if (!message) return;
      await assertMyProfile(tx, mustUser(ctx), message.senderId);
      await tx.mutate.messages.delete(args);
    }),
  },

  reactions: {
    /** Adds this author's emoji to a message, or takes it back. */
    toggle: defineMutator(
      z.object({ id, roomId: id, messageId: id, emoji: z.string().min(1) }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertMember(tx, me, args.roomId);
        const message = await tx.run(zql.messages.where("id", args.messageId).one());
        if (!message || message.roomId !== args.roomId) throw new Error("No such message in this room");
        const mine = await tx.run(
          zql.reactions.where("messageId", args.messageId).where("author", me).where("emoji", args.emoji).one(),
        );
        if (mine) await tx.mutate.reactions.delete({ id: mine.id });
        else await tx.mutate.reactions.insert({ ...args, author: me });
      },
    ),
  },

  sketches: {
    /** The canvas, its message and the room's activity commit together. */
    start: defineMutator(
      z.object({ canvasId: id, messageId: id, roomId: id, senderId: id }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertMember(tx, me, args.roomId);
        await assertMyProfile(tx, me, args.senderId);
        const now = Date.now();
        await tx.mutate.canvases.insert({ id: args.canvasId, roomId: args.roomId, title: "Sketch" });
        await tx.mutate.messages.insert({
          id: args.messageId,
          roomId: args.roomId,
          senderId: args.senderId,
          text: "",
          canvasId: args.canvasId,
          createdAt: now,
        });
        await tx.mutate.rooms.update({ id: args.roomId, lastActivityAt: now });
      },
    ),
    draw: defineMutator(
      z.object({ id, canvasId: id, roomId: id, color: z.string(), width: z.number().int(), points: z.array(z.number().int()) }),
      async ({ tx, ctx, args }) => {
        const me = mustUser(ctx);
        await assertMember(tx, me, args.roomId);
        const canvas = await tx.run(zql.canvases.where("id", args.canvasId).one());
        if (onServer(tx) && canvas?.roomId !== args.roomId) throw new Error("No such sketch in this room");
        await tx.mutate.strokes.insert({ ...args, author: me, createdAt: Date.now() });
      },
    ),
    undo: defineMutator(z.object({ id }), async ({ tx, ctx, args }) => {
      const me = mustUser(ctx);
      const stroke = await tx.run(zql.strokes.where("id", args.id).one());
      if (!stroke) return;
      if (stroke.author !== me) throw new Error("Not your stroke");
      await assertMember(tx, me, stroke.roomId);
      await tx.mutate.strokes.delete(args);
    }),
  },
});
