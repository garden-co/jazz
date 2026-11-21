// The app's data API, a Hono app mounted at /api by
// app/api/[[...route]]/route.ts (Better Auth, the bootstrap, members and demo
// data keep their own routes): the generic read and the RPC writes.
import { Hono } from "hono";
import { pool, transaction } from "./db.server";
import { Forbidden } from "./permissions.server";
import { querySpec, runQuery } from "./query.server";
import { asConflict, Conflict, handlers, serializable } from "./writes.server";

type Env = { Variables: { me: string } };
export const api = new Hono<Env>().basePath("/api");

api.get("/health", (c) => c.text("ok"));

api.use("*", async (c, next) => {
  const { auth } = await import("./auth");
  const session = await auth.api.getSession({ headers: c.req.raw.headers });
  if (!session) return c.json({ error: "account required" }, 401);
  c.set("me", session.user.id);
  await next();
});

api.post("/query", async (c) => {
  const spec = querySpec.safeParse(await c.req.json());
  if (!spec.success) return c.json({ error: spec.error.message }, 400);
  return c.json(await runQuery(pool, c.var.me, spec.data));
});

api.post("/rpc/:name", async (c) => {
  const name = c.req.param("name");
  const handler = handlers[name];
  if (!handler) return c.json({ error: "No such call" }, 404);
  const args = await c.req.json();
  try {
    const result = await transaction((tx) => handler(tx, c.var.me, args), { serializable: serializable.has(name) });
    return c.json({ result: result ?? null });
  } catch (cause) {
    const error = asConflict(cause, name) ?? cause;
    if (error instanceof Forbidden) return c.json({ error: error.message, code: "permission_denied" }, 403);
    if (error instanceof Conflict) return c.json({ error: error.message, code: error.code }, 409);
    throw error;
  }
});
