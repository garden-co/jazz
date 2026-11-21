import { rawSql } from "@neon/realtime/server";
import { z } from "zod";

/**
 * Who can read what in StagePlan (the read half of the README's rules):
 *
 * - Crew profiles (names) are shared.
 * - A show, its tasks, comments, activity and crew list are for its crew
 *   (anyone with a showCrew row) and its chief; everyone else sees nothing.
 * - Invite codes are bearer secrets: only the chief reads them.
 * - Checklist items are private to their owner.
 *
 * Realtime authorizes the query, not the connection: each query is built here
 * for the signed-in account ($1) and sealed, so the browser can't change it.
 * The membership checks are part of the live query, so they stay live.
 */
const isCrew = (showId: string) => `(
  EXISTS (SELECT 1 FROM "shows" s2 WHERE s2."id" = ${showId} AND s2."chiefAccount" = $1)
  OR EXISTS (SELECT 1 FROM "showCrew" m WHERE m."showId" = ${showId} AND m."account" = $1))`;

const showArgs = z.object({ showId: z.string() });
const escapeLike = (s: string) => s.replace(/[\\%_]/g, (c) => `\\${c}`);
const crewJson = (c: string) => `jsonb_build_object('id', ${c}."id", 'account', ${c}."account", 'name', ${c}."name")`;
const SHOW = `s."id", s."name", s."venue", s."date", s."doors", s."chiefAccount", s."createdAt"`;

type Params = (string | number | boolean | null)[];

export const liveQueries: Record<string, (me: string, args: unknown) => ReturnType<typeof rawSql>> = {
  "crew.mine": (me) => rawSql(`SELECT "id", "account", "name" FROM "crew" WHERE "id" = $1`, [me]),

  // Read rules only return shows you crew on, so this is "my shows".
  "shows.mine": (me) =>
    rawSql(
      `SELECT ${SHOW}, COALESCE((SELECT jsonb_agg(jsonb_build_object('status', t."status"))
         FROM "tasks" t WHERE t."showId" = s."id"), '[]'::jsonb) AS "tasks"
       FROM "shows" s WHERE ${isCrew(`s."id"`)} ORDER BY s."date" ASC, s."id"`,
      [me],
    ),

  "shows.one": (me, args) => {
    const { showId } = showArgs.parse(args);
    return rawSql(`SELECT ${SHOW} FROM "shows" s WHERE s."id" = $2 AND ${isCrew("$2")}`, [me, showId]);
  },

  "showCrew.byShow": (me, args) => {
    const { showId } = showArgs.parse(args);
    return rawSql(
      `SELECT sc."id", sc."showId", sc."crewId", sc."account", sc."role", ${crewJson("c")} AS "crew"
       FROM "showCrew" sc JOIN "crew" c ON c."id" = sc."crewId"
       WHERE sc."showId" = $2 AND ${isCrew("$2")} ORDER BY sc."role" ASC, sc."id"`,
      [me, showId],
    );
  },

  "showInvites.byShow": (me, args) => {
    const { showId } = showArgs.parse(args);
    return rawSql(
      `SELECT i."id", i."showId", i."code" FROM "showInvites" i
       WHERE i."showId" = $2 AND EXISTS (SELECT 1 FROM "shows" s WHERE s."id" = $2 AND s."chiefAccount" = $1)`,
      [me, showId],
    );
  },

  "tasks.byShow": (me, args) => {
    const { showId } = showArgs.parse(args);
    return rawSql(
      `SELECT "id", "showId", "title", "status", "assigneeId", "notes", "rank", "createdBy", "createdAt", "updatedAt"
       FROM "tasks" WHERE "showId" = $2 AND ${isCrew("$2")} ORDER BY "rank" ASC, "id"`,
      [me, showId],
    );
  },

  "comments.byTask": (me, args) => {
    const { taskId } = z.object({ taskId: z.string() }).parse(args);
    return rawSql(
      `SELECT cm."id", cm."taskId", cm."authorId", cm."body", cm."createdAt", ${crewJson("c")} AS "author"
       FROM "comments" cm JOIN "tasks" t ON t."id" = cm."taskId" JOIN "crew" c ON c."id" = cm."authorId"
       WHERE cm."taskId" = $2 AND ${isCrew(`t."showId"`)} ORDER BY cm."createdAt" ASC, cm."id"`,
      [me, taskId],
    );
  },

  "activity.feed": (me, args) => {
    const { showId, taskId, limit } = z
      .object({ showId: z.string(), taskId: z.string().optional(), limit: z.number().int().min(1).max(100) })
      .parse(args);
    const params: Params = [me, showId, limit];
    const byTask = taskId ? `AND a."taskId" = $${params.push(taskId)}` : "";
    return rawSql(
      `SELECT a."id", a."showId", a."taskId", a."actorId", a."kind", a."detail", a."createdAt", ${crewJson("c")} AS "actor"
       FROM "activity" a LEFT JOIN "crew" c ON c."id" = a."actorId"
       WHERE a."showId" = $2 ${byTask} AND ${isCrew("$2")}
       ORDER BY a."createdAt" DESC, a."id" DESC LIMIT $3`,
      params,
    );
  },

  "checklist.mine": (me, args) => {
    const { search, filter } = z.object({ search: z.string(), filter: z.enum(["all", "open", "done"]) }).parse(args);
    const params: Params = [me];
    let where = `"ownerAccount" = $1`;
    if (search) where += ` AND "title" LIKE $${params.push(`%${escapeLike(search)}%`)}`;
    if (filter !== "all") where += ` AND "done" = $${params.push(filter === "done")}`;
    return rawSql(
      `SELECT "id", "title", "done", "ownerAccount" FROM "checklistItems" WHERE ${where} ORDER BY "title" ASC, "id"`,
      params,
    );
  },
};
