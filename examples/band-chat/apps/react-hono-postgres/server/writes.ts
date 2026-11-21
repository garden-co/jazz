// RPC writes: POST /api/rpc/<name> with a JSON body. Each runs in one
// transaction with its permission checks, then tells the live hub which
// tables changed: the open room's subscribers through `room:<id>`, and each
// affected person's room list through their `me` topic.
import { Hono } from "hono";
import { z } from "zod";
import type { JoinRequest, RoomMember, Stroke } from "../shared/types";
import { one, rows, transaction, type Tx } from "./db";
import { meTopic as me } from "./live";
import { assertCreator, assertMember, assertMyProfile, Forbidden } from "./permissions";

const room = (id: string) => `room:${id}`;

function insert(tx: Tx, table: string, row: Record<string, unknown>) {
  const cols = Object.keys(row);
  return tx.query(
    `INSERT INTO "${table}" (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${cols.map((_, i) => `$${i + 1}`).join(", ")})`,
    Object.values(row),
  );
}

/** Everyone whose room list shows this room: its members and its creator. */
async function peopleIn(tx: Tx, roomId: string) {
  const people = await rows<{ account: string }>(
    tx,
    `SELECT "memberAuthor" AS account FROM "roomMembers" WHERE "roomId" = $1 UNION SELECT "createdBy" FROM rooms WHERE id = $1`,
    [roomId],
  );
  return people.map((p) => p.account);
}

async function tellPeopleIn(tx: Tx, roomId: string, ...tables: string[]) {
  for (const account of await peopleIn(tx, roomId)) tx.changed(me(account), ...tables);
}

type Handler = { input: z.ZodType; run: (tx: Tx, account: string, args: any) => Promise<unknown> };
const rpc = <S extends z.ZodType>(input: S, run: (tx: Tx, account: string, args: z.infer<S>) => Promise<unknown>): Handler => ({ input, run });
const id = z.string();

const handlers: Record<string, Handler> = {
  "profiles.create": rpc(z.object({ id, displayName: z.string().min(1) }), async (tx, account, args) => {
    await insert(tx, "profiles", { ...args, author: account, createdAt: Date.now() });
    tx.changed(me(account), "profiles");
  }),
  "profiles.update": rpc(z.object({ id, displayName: z.string().min(1) }), async (tx, account, args) => {
    await assertMyProfile(tx, account, args.id);
    await tx.query(`UPDATE profiles SET "displayName" = $2 WHERE id = $1`, [args.id, args.displayName]);
    // Everyone who may know this profile: co-members and creators of rooms asked to join.
    const people = await rows<{ account: string }>(
      tx,
      `SELECT m2."memberAuthor" AS account FROM "roomMembers" m JOIN "roomMembers" m2 ON m2."roomId" = m."roomId" WHERE m."memberAuthor" = $1
       UNION SELECT r."createdBy" FROM "joinRequests" j JOIN rooms r ON r.id = j."roomId" WHERE j.requester = $1
       UNION SELECT $1`,
      [account],
    );
    for (const p of people) tx.changed(me(p.account), "profiles");
  }),

  // A room and its creator's membership, in one transaction.
  "rooms.create": rpc(z.object({ id, memberId: id, name: z.string().min(1), profileId: id }), async (tx, account, args) => {
    await assertMyProfile(tx, account, args.profileId);
    await insert(tx, "rooms", { id: args.id, name: args.name, createdBy: account, createdAt: Date.now() });
    await insert(tx, "roomMembers", { id: args.memberId, roomId: args.id, memberAuthor: account, memberProfileId: args.profileId });
    tx.changed(me(account), "rooms", "roomMembers");
  }),
  "rooms.rename": rpc(z.object({ id, name: z.string().min(1) }), async (tx, account, args) => {
    await assertCreator(tx, account, args.id);
    await tx.query(`UPDATE rooms SET name = $2 WHERE id = $1`, [args.id, args.name]);
    await tellPeopleIn(tx, args.id, "rooms");
  }),

  // Admits someone who asked to join (or the creator themselves), with their
  // own profile, and clears their request in the same transaction.
  "members.admit": rpc(z.object({ id, roomId: id, memberAuthor: id, memberProfileId: id }), async (tx, account, args) => {
    await assertCreator(tx, account, args.roomId);
    const profile = await one(tx, `SELECT 1 FROM profiles WHERE id = $1 AND author = $2`, [args.memberProfileId, args.memberAuthor]);
    if (!profile) throw new Forbidden("That profile doesn't belong to them");
    const requests = await rows<JoinRequest>(tx, `DELETE FROM "joinRequests" WHERE "roomId" = $1 AND requester = $2 RETURNING *`, [args.roomId, args.memberAuthor]);
    if (args.memberAuthor !== account && requests.length === 0) throw new Forbidden("They haven't asked to join");
    await insert(tx, "roomMembers", args);
    tx.changed(room(args.roomId), "roomMembers", "joinRequests");
    await tellPeopleIn(tx, args.roomId, "profiles");
    tx.changed(me(args.memberAuthor), "rooms", "roomMembers", "joinRequests", "unread");
  }),
  // The creator removes a member; a member may also leave (dropping their read markers).
  "members.remove": rpc(z.object({ id }), async (tx, account, args) => {
    const member = await one<RoomMember>(tx, `SELECT * FROM "roomMembers" WHERE id = $1`, [args.id]);
    if (!member) return;
    if (member.memberAuthor === account) {
      await tx.query(`DELETE FROM "readMarkers" WHERE "roomId" = $1 AND reader = $2`, [member.roomId, account]);
    } else {
      await assertCreator(tx, account, member.roomId);
    }
    await tx.query(`DELETE FROM "roomMembers" WHERE id = $1`, [member.id]);
    tx.changed(room(member.roomId), "roomMembers");
    tx.changed(me(member.memberAuthor), "rooms", "roomMembers", "readMarkers", "unread", "profiles");
    const creator = await one<{ createdBy: string }>(tx, `SELECT "createdBy" FROM rooms WHERE id = $1`, [member.roomId]);
    if (creator?.createdBy !== member.memberAuthor) tx.revoke(member.memberAuthor, room(member.roomId));
  }),

  "joinRequests.create": rpc(z.object({ id, roomId: id, profileId: id }), async (tx, account, args) => {
    await assertMyProfile(tx, account, args.profileId);
    await insert(tx, "joinRequests", { ...args, requester: account });
    tx.changed(room(args.roomId), "joinRequests");
    tx.changed(me(account), "joinRequests");
    const creator = await one<{ createdBy: string }>(tx, `SELECT "createdBy" FROM rooms WHERE id = $1`, [args.roomId]);
    if (creator) tx.changed(me(creator.createdBy), "profiles");
  }),
  "joinRequests.delete": rpc(z.object({ id }), async (tx, account, args) => {
    const request = await one<JoinRequest>(tx, `SELECT * FROM "joinRequests" WHERE id = $1`, [args.id]);
    if (!request) return;
    if (request.requester !== account) await assertCreator(tx, account, request.roomId);
    await tx.query(`DELETE FROM "joinRequests" WHERE id = $1`, [args.id]);
    tx.changed(room(request.roomId), "joinRequests");
    tx.changed(me(request.requester), "joinRequests");
  }),

  // Moves this reader's marker for a room forward (creating it the first time).
  "readMarkers.mark": rpc(z.object({ id, roomId: id, lastReadAt: z.number() }), async (tx, account, args) => {
    await assertMember(tx, account, args.roomId);
    const r = await tx.query(
      `INSERT INTO "readMarkers" (id, "roomId", reader, "lastReadAt") VALUES ($1, $2, $3, $4)
       ON CONFLICT (id) DO UPDATE SET "lastReadAt" = EXCLUDED."lastReadAt" WHERE "readMarkers".reader = $3`,
      [args.id, args.roomId, account, args.lastReadAt],
    );
    if (!r.rowCount) throw new Forbidden("Not your read marker");
    tx.changed(me(account), "readMarkers", "unread");
  }),

  // A message and the room's new activity, together.
  "messages.send": rpc(z.object({ id, roomId: id, senderId: id, text: z.string().min(1) }), async (tx, account, args) => {
    await assertMember(tx, account, args.roomId);
    await assertMyProfile(tx, account, args.senderId);
    const now = Date.now();
    await insert(tx, "messages", { ...args, createdAt: now });
    await tx.query(`UPDATE rooms SET "lastActivityAt" = $2 WHERE id = $1`, [args.roomId, now]);
    tx.changed(room(args.roomId), "messages");
    await tellPeopleIn(tx, args.roomId, "rooms", "unread");
  }),
  "messages.delete": rpc(z.object({ id }), async (tx, account, args) => {
    const message = await one<{ roomId: string; senderId: string }>(tx, `SELECT * FROM messages WHERE id = $1`, [args.id]);
    if (!message) return;
    await assertMyProfile(tx, account, message.senderId);
    await tx.query(`DELETE FROM messages WHERE id = $1`, [args.id]);
    tx.changed(room(message.roomId), "messages", "reactions");
    await tellPeopleIn(tx, message.roomId, "unread");
  }),

  // Adds this author's emoji to a message, or takes it back.
  "reactions.toggle": rpc(z.object({ id, roomId: id, messageId: id, emoji: z.string().min(1) }), async (tx, account, args) => {
    await assertMember(tx, account, args.roomId);
    const message = await one(tx, `SELECT 1 FROM messages WHERE id = $1 AND "roomId" = $2`, [args.messageId, args.roomId]);
    if (!message) throw new Forbidden("No such message in this room");
    const removed = await tx.query(`DELETE FROM reactions WHERE "messageId" = $1 AND author = $2 AND emoji = $3`, [args.messageId, account, args.emoji]);
    if (!removed.rowCount) await insert(tx, "reactions", { ...args, author: account });
    tx.changed(room(args.roomId), "reactions");
  }),

  // The canvas, its message and the room's activity commit together.
  "sketches.start": rpc(z.object({ canvasId: id, messageId: id, roomId: id, senderId: id }), async (tx, account, args) => {
    await assertMember(tx, account, args.roomId);
    await assertMyProfile(tx, account, args.senderId);
    const now = Date.now();
    await insert(tx, "canvases", { id: args.canvasId, roomId: args.roomId, title: "Sketch" });
    await insert(tx, "messages", { id: args.messageId, roomId: args.roomId, senderId: args.senderId, text: "", canvasId: args.canvasId, createdAt: now });
    await tx.query(`UPDATE rooms SET "lastActivityAt" = $2 WHERE id = $1`, [args.roomId, now]);
    tx.changed(room(args.roomId), "messages");
    await tellPeopleIn(tx, args.roomId, "rooms", "unread");
  }),
  "sketches.draw": rpc(
    z.object({ id, canvasId: id, roomId: id, color: z.string(), width: z.number().int(), points: z.array(z.number().int()) }),
    async (tx, account, args) => {
      await assertMember(tx, account, args.roomId);
      const canvas = await one(tx, `SELECT 1 FROM canvases WHERE id = $1 AND "roomId" = $2`, [args.canvasId, args.roomId]);
      if (!canvas) throw new Forbidden("No such sketch in this room");
      await insert(tx, "strokes", { ...args, points: JSON.stringify(args.points), author: account, createdAt: Date.now() });
      tx.changed(room(args.roomId), "strokes");
    },
  ),
  "sketches.undo": rpc(z.object({ id }), async (tx, account, args) => {
    const stroke = await one<Stroke>(tx, `SELECT * FROM strokes WHERE id = $1`, [args.id]);
    if (!stroke) return;
    if (stroke.author !== account) throw new Forbidden("Not your stroke");
    await assertMember(tx, account, stroke.roomId);
    await tx.query(`DELETE FROM strokes WHERE id = $1`, [args.id]);
    tx.changed(room(stroke.roomId), "strokes");
  }),
};

type Env = { Variables: { me: string } };
export const writes = new Hono<Env>();

writes.post("/rpc/:name", async (c) => {
  const handler = handlers[c.req.param("name")];
  if (!handler) return c.json({ error: "No such call" }, 404);
  const parsed = handler.input.safeParse(await c.req.json());
  if (!parsed.success) return c.json({ error: parsed.error.message }, 400);
  try {
    const result = await transaction((tx) => handler.run(tx, c.var.me, parsed.data));
    return c.json({ result: result ?? null });
  } catch (error) {
    if (error instanceof Forbidden) return c.json({ error: error.message }, 403);
    throw error;
  }
});
