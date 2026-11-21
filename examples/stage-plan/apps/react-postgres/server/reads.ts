// REST reads. Every query filters by what the caller may see (permissions.ts),
// so a non-member gets an empty list or null, never someone else's rows.
import { Hono } from "hono";
import { pool, one, rows } from "./db.js";
import { canSeeShow } from "./permissions.js";

type Env = { Variables: { me: string } };
export const reads = new Hono<Env>();

reads.get("/me", async (c) => {
  const profile = await one(pool, `SELECT * FROM crew WHERE account = $1`, [c.var.me]);
  return c.json(profile ?? null);
});

reads.get("/shows", async (c) => {
  // Read permissions only return shows you crew on, so this is "my shows".
  const shows = await rows(
    pool,
    `SELECT s.*, COALESCE((SELECT json_agg(json_build_object('status', t.status)) FROM tasks t WHERE t."showId" = s.id), '[]') AS tasks
       FROM shows s WHERE ${canSeeShow("$1")} ORDER BY s.date`,
    [c.var.me],
  );
  return c.json(shows);
});

reads.get("/shows/:id", async (c) => {
  const show = await one(pool, `SELECT s.* FROM shows s WHERE s.id = $2 AND ${canSeeShow("$1")}`, [c.var.me, c.req.param("id")]);
  return c.json(show ?? null);
});

reads.get("/shows/:id/crew", async (c) => {
  const crew = await rows(
    pool,
    `SELECT sc.*, row_to_json(p) AS crew FROM "showCrew" sc
       JOIN shows s ON s.id = sc."showId" LEFT JOIN crew p ON p.id = sc."crewId"
      WHERE sc."showId" = $2 AND ${canSeeShow("$1")} ORDER BY sc.role`,
    [c.var.me, c.req.param("id")],
  );
  return c.json(crew);
});

reads.get("/shows/:id/invites", async (c) => {
  const invites = await rows(
    pool,
    `SELECT i.* FROM "showInvites" i JOIN shows s ON s.id = i."showId" WHERE i."showId" = $2 AND s."chiefAccount" = $1`,
    [c.var.me, c.req.param("id")],
  );
  return c.json(invites);
});

reads.get("/shows/:id/tasks", async (c) => {
  const tasks = await rows(
    pool,
    `SELECT t.* FROM tasks t JOIN shows s ON s.id = t."showId" WHERE t."showId" = $2 AND ${canSeeShow("$1")} ORDER BY t.rank`,
    [c.var.me, c.req.param("id")],
  );
  return c.json(tasks);
});

reads.get("/shows/:id/activity", async (c) => {
  const taskId = c.req.query("taskId") || null;
  const limit = Math.min(Number(c.req.query("limit") ?? 100), 100);
  const entries = await rows(
    pool,
    `SELECT a.*, row_to_json(p) AS actor FROM activity a
       JOIN shows s ON s.id = a."showId" LEFT JOIN crew p ON p.id = a."actorId"
      WHERE a."showId" = $2 AND ($3::text IS NULL OR a."taskId" = $3) AND ${canSeeShow("$1")}
      ORDER BY a."createdAt" DESC LIMIT $4`,
    [c.var.me, c.req.param("id"), taskId, limit],
  );
  return c.json(entries);
});

reads.get("/tasks/:id/comments", async (c) => {
  const comments = await rows(
    pool,
    `SELECT cm.*, row_to_json(p) AS author FROM comments cm
       JOIN tasks t ON t.id = cm."taskId" JOIN shows s ON s.id = t."showId" LEFT JOIN crew p ON p.id = cm."authorId"
      WHERE cm."taskId" = $2 AND ${canSeeShow("$1")} ORDER BY cm."createdAt"`,
    [c.var.me, c.req.param("id")],
  );
  return c.json(comments);
});

reads.get("/checklist", async (c) => {
  const search = c.req.query("search") ?? "";
  const filter = c.req.query("filter") ?? "all";
  const items = await rows(
    pool,
    `SELECT * FROM "checklistItems" WHERE "ownerAccount" = $1
        AND ($2 = '' OR title LIKE '%' || replace(replace(replace($2, '\\', '\\\\'), '%', '\\%'), '_', '\\_') || '%')
        AND ($3 = 'all' OR done = ($3 = 'done'))
      ORDER BY title`,
    [c.var.me, search, filter],
  );
  return c.json(items);
});
