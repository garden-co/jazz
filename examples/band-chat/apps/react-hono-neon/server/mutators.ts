import type pg from "pg";
import { z } from "zod";

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
 * Every write goes through these handlers on the app's server, as Neon's
 * mutations guide does it: validate, authorize, write in one transaction and
 * return the transaction's id, which the client waits for on its live queries.
 */
export class Forbidden extends Error {}

export type Tx = {
  me: string;
  rows<T>(sql: string, params?: unknown[]): Promise<T[]>;
  /** A write; its RETURNING carries the transaction id, which the response returns. */
  write(sql: string, params?: unknown[]): Promise<number>;
};

const one = async <T = unknown>(tx: Tx, sql: string, params: unknown[]) => (await tx.rows<T>(sql, params))[0];

async function assertMember(tx: Tx, roomId: string) {
  const member = await one(tx, `SELECT 1 FROM "roomMembers" WHERE "roomId" = $1 AND "memberAuthor" = $2`, [roomId, tx.me]);
  if (!member) throw new Forbidden("You are not a member of this room");
}
async function assertCreator(tx: Tx, roomId: string) {
  const room = await one(tx, `SELECT 1 FROM "rooms" WHERE "id" = $1 AND "createdBy" = $2`, [roomId, tx.me]);
  if (!room) throw new Forbidden("Only the room creator can do that");
}
async function assertMyProfile(tx: Tx, profileId: string) {
  const profile = await one(tx, `SELECT 1 FROM "profiles" WHERE "id" = $1 AND "author" = $2`, [profileId, tx.me]);
  if (!profile) throw new Forbidden("That is not your profile");
}

const id = z.string();
type Mutator = { args: z.ZodType; run: (tx: Tx, args: never) => Promise<unknown> };
const mutator = <S extends z.ZodType>(args: S, run: (tx: Tx, args: z.infer<S>) => Promise<unknown>): Mutator => ({
  args,
  run: run as Mutator["run"],
});

/** A message (or sketch message) and the room's new activity. */
async function post(tx: Tx, message: { id: string; roomId: string; senderId: string; text: string; canvasId: string | null }) {
  await assertMember(tx, message.roomId);
  await assertMyProfile(tx, message.senderId);
  const now = Date.now();
  await tx.write(
    `INSERT INTO "messages" ("id", "roomId", "senderId", "text", "canvasId", "createdAt") VALUES ($1, $2, $3, $4, $5, $6)`,
    [message.id, message.roomId, message.senderId, message.text, message.canvasId, now],
  );
  await tx.write(`UPDATE "rooms" SET "lastActivityAt" = $2 WHERE "id" = $1`, [message.roomId, now]);
}

export const mutators: Record<string, Mutator> = {
  "profiles.create": mutator(z.object({ id, displayName: z.string().min(1) }), (tx, args) =>
    tx.write(`INSERT INTO "profiles" ("id", "author", "displayName", "createdAt") VALUES ($1, $2, $3, $4)`, [
      args.id,
      tx.me,
      args.displayName,
      Date.now(),
    ]),
  ),
  "profiles.update": mutator(z.object({ id, displayName: z.string().min(1) }), async (tx, args) => {
    const changed = await tx.write(`UPDATE "profiles" SET "displayName" = $2 WHERE "id" = $1 AND "author" = $3`, [
      args.id,
      args.displayName,
      tx.me,
    ]);
    if (!changed) throw new Forbidden("That is not your profile");
  }),

  /** A room and its creator's membership, in one transaction. */
  "rooms.create": mutator(z.object({ id, memberId: id, name: z.string().min(1), profileId: id }), async (tx, args) => {
    await assertMyProfile(tx, args.profileId);
    const created = await tx.write(
      `INSERT INTO "rooms" ("id", "name", "createdBy", "createdAt") VALUES ($1, $2, $3, $4) ON CONFLICT ("id") DO NOTHING`,
      [args.id, args.name, tx.me, Date.now()],
    );
    if (!created) throw new Forbidden("Room exists");
    await tx.write(`INSERT INTO "roomMembers" ("id", "roomId", "memberAuthor", "memberProfileId") VALUES ($1, $2, $3, $4)`, [
      args.memberId,
      args.id,
      tx.me,
      args.profileId,
    ]);
  }),
  "rooms.rename": mutator(z.object({ id, name: z.string().min(1) }), async (tx, args) => {
    await assertCreator(tx, args.id);
    await tx.write(`UPDATE "rooms" SET "name" = $2 WHERE "id" = $1`, [args.id, args.name]);
  }),

  /** Admits someone who asked to join (or the creator themselves), with their own profile, clearing the request. */
  "members.admit": mutator(z.object({ id, roomId: id, memberAuthor: id, memberProfileId: id }), async (tx, args) => {
    await assertCreator(tx, args.roomId);
    const profile = await one(tx, `SELECT 1 FROM "profiles" WHERE "id" = $1 AND "author" = $2`, [args.memberProfileId, args.memberAuthor]);
    if (!profile) throw new Forbidden("That profile doesn't belong to them");
    const requests = await tx.rows(`SELECT 1 FROM "joinRequests" WHERE "roomId" = $1 AND "requester" = $2`, [args.roomId, args.memberAuthor]);
    if (args.memberAuthor !== tx.me && requests.length === 0) throw new Forbidden("They haven't asked to join");
    await tx.write(`INSERT INTO "roomMembers" ("id", "roomId", "memberAuthor", "memberProfileId") VALUES ($1, $2, $3, $4)`, [
      args.id,
      args.roomId,
      args.memberAuthor,
      args.memberProfileId,
    ]);
    await tx.write(`DELETE FROM "joinRequests" WHERE "roomId" = $1 AND "requester" = $2`, [args.roomId, args.memberAuthor]);
  }),
  /** The creator removes a member; a member may also leave (dropping their read markers). */
  "members.remove": mutator(z.object({ id }), async (tx, args) => {
    const member = await one<{ roomId: string; memberAuthor: string }>(
      tx,
      `SELECT "roomId", "memberAuthor" FROM "roomMembers" WHERE "id" = $1`,
      [args.id],
    );
    if (!member) return;
    if (member.memberAuthor === tx.me) {
      await tx.write(`DELETE FROM "readMarkers" WHERE "roomId" = $1 AND "reader" = $2`, [member.roomId, tx.me]);
    } else {
      await assertCreator(tx, member.roomId);
    }
    await tx.write(`DELETE FROM "roomMembers" WHERE "id" = $1`, [args.id]);
  }),

  "joinRequests.create": mutator(z.object({ id, roomId: id, profileId: id }), async (tx, args) => {
    await assertMyProfile(tx, args.profileId);
    await tx.write(`INSERT INTO "joinRequests" ("id", "roomId", "requester", "profileId") VALUES ($1, $2, $3, $4)`, [
      args.id,
      args.roomId,
      tx.me,
      args.profileId,
    ]);
  }),
  "joinRequests.delete": mutator(z.object({ id }), async (tx, args) => {
    const request = await one<{ roomId: string; requester: string }>(
      tx,
      `SELECT "roomId", "requester" FROM "joinRequests" WHERE "id" = $1`,
      [args.id],
    );
    if (!request) return;
    if (request.requester !== tx.me) await assertCreator(tx, request.roomId);
    await tx.write(`DELETE FROM "joinRequests" WHERE "id" = $1`, [args.id]);
  }),

  /** Moves this reader's marker for a room forward (creating it the first time). */
  "readMarkers.mark": mutator(z.object({ id, roomId: id, lastReadAt: z.number() }), async (tx, args) => {
    await assertMember(tx, args.roomId);
    const written = await tx.write(
      `INSERT INTO "readMarkers" ("id", "roomId", "reader", "lastReadAt") VALUES ($1, $2, $3, $4)
       ON CONFLICT ("id") DO UPDATE SET "lastReadAt" = EXCLUDED."lastReadAt" WHERE "readMarkers"."reader" = EXCLUDED."reader"`,
      [args.id, args.roomId, tx.me, args.lastReadAt],
    );
    if (!written) throw new Forbidden("Not your read marker");
  }),

  "messages.send": mutator(z.object({ id, roomId: id, senderId: id, text: z.string().min(1) }), (tx, args) =>
    post(tx, { ...args, canvasId: null }),
  ),
  "messages.delete": mutator(z.object({ id }), async (tx, args) => {
    const message = await one<{ senderId: string }>(tx, `SELECT "senderId" FROM "messages" WHERE "id" = $1`, [args.id]);
    if (!message) return;
    await assertMyProfile(tx, message.senderId);
    await tx.write(`DELETE FROM "messages" WHERE "id" = $1`, [args.id]);
  }),

  /** Adds this author's emoji to a message, or takes it back. */
  "reactions.toggle": mutator(z.object({ id, roomId: id, messageId: id, emoji: z.string().min(1) }), async (tx, args) => {
    await assertMember(tx, args.roomId);
    const message = await one(tx, `SELECT 1 FROM "messages" WHERE "id" = $1 AND "roomId" = $2`, [args.messageId, args.roomId]);
    if (!message) throw new Forbidden("No such message in this room");
    const removed = await tx.write(`DELETE FROM "reactions" WHERE "messageId" = $1 AND "author" = $2 AND "emoji" = $3`, [
      args.messageId,
      tx.me,
      args.emoji,
    ]);
    if (!removed) {
      await tx.write(`INSERT INTO "reactions" ("id", "roomId", "messageId", "author", "emoji") VALUES ($1, $2, $3, $4, $5)`, [
        args.id,
        args.roomId,
        args.messageId,
        tx.me,
        args.emoji,
      ]);
    }
  }),

  /** The canvas, its message and the room's activity commit together. */
  "sketches.start": mutator(z.object({ canvasId: id, messageId: id, roomId: id, senderId: id }), async (tx, args) => {
    await assertMember(tx, args.roomId);
    await tx.write(`INSERT INTO "canvases" ("id", "roomId", "title") VALUES ($1, $2, 'Sketch')`, [args.canvasId, args.roomId]);
    await post(tx, { id: args.messageId, roomId: args.roomId, senderId: args.senderId, text: "", canvasId: args.canvasId });
  }),
  "sketches.draw": mutator(
    z.object({ id, canvasId: id, roomId: id, color: z.string(), width: z.number().int(), points: z.array(z.number().int()) }),
    async (tx, args) => {
      await assertMember(tx, args.roomId);
      const canvas = await one(tx, `SELECT 1 FROM "canvases" WHERE "id" = $1 AND "roomId" = $2`, [args.canvasId, args.roomId]);
      if (!canvas) throw new Forbidden("No such sketch in this room");
      await tx.write(
        `INSERT INTO "strokes" ("id", "canvasId", "roomId", "author", "color", "width", "points", "createdAt")
         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)`,
        [args.id, args.canvasId, args.roomId, tx.me, args.color, args.width, JSON.stringify(args.points), Date.now()],
      );
    },
  ),
  "sketches.undo": mutator(z.object({ id }), async (tx, args) => {
    const stroke = await one<{ author: string; roomId: string }>(tx, `SELECT "author", "roomId" FROM "strokes" WHERE "id" = $1`, [args.id]);
    if (!stroke) return;
    if (stroke.author !== tx.me) throw new Forbidden("Not your stroke");
    await assertMember(tx, stroke.roomId);
    await tx.write(`DELETE FROM "strokes" WHERE "id" = $1`, [args.id]);
  }),
};

export type MutationResponse = { kind: "committed"; txid: string; result?: unknown } | { kind: "unchanged"; result?: unknown };

/** Runs a mutator in one transaction and returns its txid, read from the writes' own RETURNING. */
export async function runMutation(pool: pg.Pool, me: string, name: string, rawArgs: unknown): Promise<MutationResponse> {
  const def = mutators[name];
  if (!def) throw new Forbidden(`No mutator ${name}`);
  const args = def.args.parse(rawArgs);
  const client = await pool.connect();
  let txid: string | undefined;
  const tx: Tx = {
    me,
    rows: async (sql, params) => (await client.query(sql, params)).rows,
    write: async (sql, params) => {
      const result = await client.query<{ txid: string }>(`${sql} RETURNING pg_current_xact_id()::text AS txid`, params);
      txid ??= result.rows[0]?.txid;
      return result.rowCount ?? 0;
    },
  };
  try {
    await client.query("BEGIN");
    const result = await def.run(tx, args as never);
    await client.query("COMMIT");
    return txid ? { kind: "committed", txid, result } : { kind: "unchanged", result };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
