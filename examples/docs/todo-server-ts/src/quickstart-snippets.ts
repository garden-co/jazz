// #region quickstart-server-setup-ts
import { Hono } from "hono";
import { serve } from "@hono/node-server";
import { createJazzContext } from "jazz-tools/backend";
import { app as schema } from "../schema/app.js";

const context = createJazzContext({
  appId: "<your-app-id>",
  app: schema,
  driver: { type: "persistent", dataPath: "./data/jazz.db" },
});

const api = new Hono();
// #endregion quickstart-server-setup-ts

// #region quickstart-server-write-ts
api.post("/api/todos", async (c) => {
  const db = context.forRequest(c.req);
  const { title } = await c.req.json();

  const todo = db.insert(schema.todos, {
    title,
    done: false,
    owner_id: "system",
  });

  return c.json(todo, 201);
});
// #endregion quickstart-server-write-ts

// #region quickstart-server-read-ts
api.get("/api/todos", async (c) => {
  const db = context.forRequest(c.req);
  const todos = await db.all(
    schema.todos.where({ done: false }).orderBy("title", "asc").limit(100),
  );
  return c.json(todos);
});
// #endregion quickstart-server-read-ts

// #region quickstart-server-update-ts
api.patch("/api/todos/:id", async (c) => {
  const db = context.forRequest(c.req);
  const { id } = c.req.param();
  const { done } = await c.req.json();
  db.update(schema.todos, id, { done });
  return c.json({ ok: true });
});

api.delete("/api/todos/:id", async (c) => {
  const db = context.forRequest(c.req);
  const { id } = c.req.param();
  db.delete(schema.todos, id);
  return c.json({ ok: true });
});
// #endregion quickstart-server-update-ts

// #region quickstart-server-listen-ts
serve({ fetch: api.fetch, port: 3000 }, (info) => {
  console.log(`Server running on http://localhost:${info.port}`);
});
// #endregion quickstart-server-listen-ts
