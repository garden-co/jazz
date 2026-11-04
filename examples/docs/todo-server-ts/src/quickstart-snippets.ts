// #region quickstart-server-setup-ts
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createJazz, sqlite } from "jazz-tools/backend";
import { app as schemaApp } from "../schema.js";
import permissions from "../permissions.js";

// Your own auth decides who a request is (a session cookie, a header).
declare function currentUser(request: Request): Promise<{ id: string } | null>;

const jazz = await createJazz({
  app: schemaApp,
  permissions,
  store: sqlite({ path: "./data/jazz" }),
  async authenticate(request) {
    const user = await currentUser(request);
    return user ? { userId: user.id } : null;
  },
});
const api = new Hono();
// #endregion quickstart-server-setup-ts

// #region quickstart-server-write-ts
api.post("/api/todos", async (c) => {
  const account = await jazz.accountFor(c.req.raw);
  if (!account) return c.json({ error: "Account required" }, 401);
  const db = await jazz.forRequest(c.req.raw);
  const { title } = await c.req.json();

  const { value: todo } = db.insert(schemaApp.todos, {
    title,
    done: false,
    owner_id: account,
  });

  return c.json(todo, 201);
});
// #endregion quickstart-server-write-ts

// #region quickstart-server-read-ts
api.get("/api/todos", async (c) => {
  const db = await jazz.forRequest(c.req.raw);
  const todos = await db.all(
    schemaApp.todos.where({ done: false }).orderBy("title", "asc").limit(100),
  );
  return c.json(todos);
});
// #endregion quickstart-server-read-ts

// #region quickstart-server-update-ts
api.patch("/api/todos/:id", async (c) => {
  const db = await jazz.forRequest(c.req.raw);
  const { id } = c.req.param();
  const { done } = await c.req.json();
  db.update(schemaApp.todos, id, { done });
  return c.json({ ok: true });
});

api.delete("/api/todos/:id", async (c) => {
  const db = await jazz.forRequest(c.req.raw);
  const { id } = c.req.param();
  db.delete(schemaApp.todos, id);
  return c.json({ ok: true });
});
// #endregion quickstart-server-update-ts

// #region quickstart-server-listen-ts
serve({ fetch: api.fetch, port: 3000 }, (info) => {
  console.log(`Server running on http://localhost:${info.port}`);
});
// #endregion quickstart-server-listen-ts
