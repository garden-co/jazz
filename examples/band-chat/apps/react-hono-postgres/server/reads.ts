// REST reads. Every query filters by what the caller may see (permissions.ts),
// so an outsider gets an empty list, never someone else's rows.
import { Hono } from "hono";
import { pool, rows } from "./db";
import { canReadRoomSql, isMemberSql } from "./permissions";

type Env = { Variables: { me: string } };
export const reads = new Hono<Env>();
export const HISTORY_PAGE = 200;
export const UNREAD_CAP = 100;

const list = (sql: string, params: (c: { var: { me: string }; req: { param(k: string): string } }) => unknown[]) =>
  async (c: any) => c.json(await rows(pool, sql, params(c)));

reads.get("/profiles/mine", list(`SELECT * FROM profiles WHERE author = $1 ORDER BY "createdAt" LIMIT 1`, (c) => [c.var.me]));

// Everyone this account may know: co-members of rooms it can read, senders in
// rooms it is in, and people asking to join its rooms.
reads.get(
  "/profiles/known",
  list(
    `SELECT p.* FROM profiles p WHERE p.author = $1
        OR EXISTS (SELECT 1 FROM "roomMembers" m JOIN rooms r ON r.id = m."roomId" WHERE m."memberProfileId" = p.id AND ${canReadRoomSql("$1")})
        OR EXISTS (SELECT 1 FROM "joinRequests" j JOIN rooms r ON r.id = j."roomId" WHERE j."profileId" = p.id AND r."createdBy" = $1)
        OR EXISTS (SELECT 1 FROM messages msg WHERE msg."senderId" = p.id AND ${isMemberSql('msg."roomId"', "$1")})`,
    (c) => [c.var.me],
  ),
);

reads.get("/rooms", list(`SELECT r.* FROM rooms r WHERE ${canReadRoomSql("$1")}`, (c) => [c.var.me]));
reads.get("/memberships", list(`SELECT * FROM "roomMembers" WHERE "memberAuthor" = $1`, (c) => [c.var.me]));
reads.get("/read-markers", list(`SELECT * FROM "readMarkers" WHERE reader = $1`, (c) => [c.var.me]));

// Unread counts for every room this account is in: messages from others since
// its read marker, capped. One query for the whole room list.
reads.get("/unread", async (c) => {
  const counts = await rows<{ roomId: string; count: number }>(
    pool,
    `SELECT m."roomId", (SELECT count(*)::int FROM (
         SELECT 1 FROM messages msg
          WHERE msg."roomId" = m."roomId" AND msg."senderId" IS DISTINCT FROM m."memberProfileId"
            AND msg."createdAt" > COALESCE((SELECT max("lastReadAt") FROM "readMarkers" rd WHERE rd."roomId" = m."roomId" AND rd.reader = $1), 0)
          LIMIT ${UNREAD_CAP}) capped) AS count
       FROM "roomMembers" m WHERE m."memberAuthor" = $1`,
    [c.var.me],
  );
  return c.json(Object.fromEntries(counts.map(({ roomId, count }) => [roomId, count])));
});

reads.get(
  "/rooms/:id/members",
  list(`SELECT m.* FROM "roomMembers" m JOIN rooms r ON r.id = m."roomId" WHERE m."roomId" = $2 AND ${canReadRoomSql("$1")}`, (c) => [c.var.me, c.req.param("id")]),
);
reads.get(
  "/rooms/:id/join-requests",
  list(`SELECT j.* FROM "joinRequests" j JOIN rooms r ON r.id = j."roomId" WHERE j."roomId" = $2 AND r."createdBy" = $1`, (c) => [c.var.me, c.req.param("id")]),
);
reads.get(
  "/rooms/:id/my-join-requests",
  list(`SELECT * FROM "joinRequests" WHERE "roomId" = $2 AND requester = $1`, (c) => [c.var.me, c.req.param("id")]),
);
// A room's newest page, newest first.
reads.get(
  "/rooms/:id/messages",
  list(
    `SELECT * FROM messages WHERE "roomId" = $2 AND ${isMemberSql("$2", "$1")} ORDER BY "createdAt" DESC LIMIT ${HISTORY_PAGE}`,
    (c) => [c.var.me, c.req.param("id")],
  ),
);
reads.get(
  "/rooms/:id/reactions",
  list(`SELECT * FROM reactions WHERE "roomId" = $2 AND ${isMemberSql("$2", "$1")}`, (c) => [c.var.me, c.req.param("id")]),
);
reads.get(
  "/rooms/:roomId/canvases/:id/strokes",
  list(
    `SELECT s.* FROM strokes s WHERE s."canvasId" = $2 AND ${isMemberSql('s."roomId"', "$1")} ORDER BY s."createdAt"`,
    (c) => [c.var.me, c.req.param("id")],
  ),
);
