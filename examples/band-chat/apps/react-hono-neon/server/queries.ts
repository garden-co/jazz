import { rawSql } from "@neon/realtime/server";
import { z } from "zod";
import { HISTORY_PAGE, UNREAD_CAP } from "../queries";

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
 * Realtime authorizes the query, not the connection: each query is built for
 * the signed-in user ($1) and sealed. The membership checks are part of the
 * live query, so they stay live.
 */
const isMember = (roomId: string) =>
  `EXISTS (SELECT 1 FROM "roomMembers" mm WHERE mm."roomId" = ${roomId} AND mm."memberAuthor" = $1)`;
const canReadRoom = (roomId: string) =>
  `(${isMember(roomId)} OR EXISTS (SELECT 1 FROM "rooms" cr WHERE cr."id" = ${roomId} AND cr."createdBy" = $1))`;

const roomArgs = z.object({ roomId: z.string() });
type Params = (string | number | boolean | null)[];

const PROFILE = `p."id", p."author", p."displayName", p."createdAt"`;
const MESSAGE = `"id", "roomId", "senderId", "text", "canvasId", "createdAt"`;
const MEMBER = `"id", "roomId", "memberAuthor", "memberProfileId"`;

export const liveQueries: Record<string, (me: string, args: unknown) => ReturnType<typeof rawSql>> = {
  "profiles.mine": (me) =>
    rawSql(`SELECT ${PROFILE} FROM "profiles" p WHERE p."author" = $1 ORDER BY p."createdAt" ASC, p."id" LIMIT 1`, [me]),

  /** Everyone this account may know: one subscription feeds every name. */
  "profiles.known": (me) =>
    rawSql(
      `SELECT ${PROFILE} FROM "profiles" p WHERE p."author" = $1
       OR EXISTS (SELECT 1 FROM "messages" msg WHERE msg."senderId" = p."id" AND ${isMember(`msg."roomId"`)})
       OR EXISTS (SELECT 1 FROM "roomMembers" pm WHERE pm."memberProfileId" = p."id" AND ${canReadRoom(`pm."roomId"`)})
       OR EXISTS (SELECT 1 FROM "joinRequests" j JOIN "rooms" jr ON jr."id" = j."roomId"
                  WHERE j."profileId" = p."id" AND jr."createdBy" = $1)`,
      [me],
    ),

  "rooms.mine": (me) =>
    rawSql(
      `SELECT r."id", r."name", r."createdBy", r."createdAt", r."lastActivityAt" FROM "rooms" r
       WHERE r."createdBy" = $1 OR ${isMember(`r."id"`)}`,
      [me],
    ),

  "roomMembers.mine": (me) => rawSql(`SELECT ${MEMBER} FROM "roomMembers" WHERE "memberAuthor" = $1`, [me]),

  "roomMembers.byRoom": (me, args) => {
    const { roomId } = roomArgs.parse(args);
    return rawSql(`SELECT ${MEMBER} FROM "roomMembers" WHERE "roomId" = $2 AND ${canReadRoom("$2")}`, [me, roomId]);
  },

  "readMarkers.mine": (me) =>
    rawSql(`SELECT "id", "roomId", "reader", "lastReadAt" FROM "readMarkers" WHERE "reader" = $1`, [me]),

  /** The room creator's view of who asked. */
  "joinRequests.byRoom": (me, args) => {
    const { roomId } = roomArgs.parse(args);
    return rawSql(
      `SELECT "id", "roomId", "requester", "profileId" FROM "joinRequests"
       WHERE "roomId" = $2 AND EXISTS (SELECT 1 FROM "rooms" cr WHERE cr."id" = $2 AND cr."createdBy" = $1)`,
      [me, roomId],
    );
  },

  /** The requester's own pending request. */
  "joinRequests.mine": (me, args) => {
    const { roomId } = roomArgs.parse(args);
    return rawSql(
      `SELECT "id", "roomId", "requester", "profileId" FROM "joinRequests" WHERE "roomId" = $2 AND "requester" = $1`,
      [me, roomId],
    );
  },

  /** A room's newest messages, newest first. */
  "messages.newest": (me, args) => {
    const { roomId } = roomArgs.parse(args);
    return rawSql(
      `SELECT ${MESSAGE} FROM "messages" WHERE "roomId" = $2 AND ${isMember("$2")}
       ORDER BY "createdAt" DESC, "id" DESC LIMIT ${HISTORY_PAGE}`,
      [me, roomId],
    );
  },

  /** Messages from others since this reader's marker, for the unread badge. */
  "messages.unread": (me, args) => {
    const { roomId, myProfileId, readAt } = z
      .object({ roomId: z.string(), myProfileId: z.string(), readAt: z.number().optional() })
      .parse(args);
    const params: Params = [me, roomId, myProfileId];
    const since = readAt === undefined ? "" : `AND "createdAt" > $${params.push(readAt)}`;
    return rawSql(
      `SELECT ${MESSAGE} FROM "messages" WHERE "roomId" = $2 AND "senderId" <> $3 ${since} AND ${isMember("$2")}
       ORDER BY "createdAt" DESC, "id" DESC LIMIT ${UNREAD_CAP}`,
      params,
    );
  },

  "reactions.byRoom": (me, args) => {
    const { roomId } = roomArgs.parse(args);
    return rawSql(
      `SELECT "id", "roomId", "messageId", "author", "emoji" FROM "reactions" WHERE "roomId" = $2 AND ${isMember("$2")}`,
      [me, roomId],
    );
  },

  "strokes.byCanvas": (me, args) => {
    const { canvasId } = z.object({ canvasId: z.string() }).parse(args);
    return rawSql(
      `SELECT s."id", s."canvasId", s."roomId", s."author", s."color", s."width", s."points", s."createdAt"
       FROM "strokes" s WHERE s."canvasId" = $2 AND ${isMember(`s."roomId"`)} ORDER BY s."createdAt" ASC, s."id"`,
      [me, canvasId],
    );
  },
};
