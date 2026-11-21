// BandChat's server: Better Auth at /api/auth/*, Zero's query and mutate
// endpoints (zero-cache calls them with the browser's session cookie), and in
// production the built SPA.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { mustGetMutator, mustGetQuery } from "@rocicorp/zero";
import { handleMutateRequest, handleQueryRequest } from "@rocicorp/zero/server";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { Hono } from "hono";
import pg from "pg";
import { mutators } from "../mutators";
import { queries } from "../queries";
import { schema, type AuthData } from "../schema";
import { auth, DATABASE_URL } from "./auth";

const dbProvider = zeroNodePg(schema, new pg.Pool({ connectionString: DATABASE_URL }));

async function authData(request: Request): Promise<AuthData | undefined> {
  const session = await auth.api.getSession({ headers: request.headers });
  return session ? { userID: session.user.id } : undefined;
}

const app = new Hono();
app.get("/health", (c) => c.text("ok"));
// Better Auth owns sign-up, sign-in and the session cookie.
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));

app.post("/api/zero/query", async (c) => {
  const ctx = await authData(c.req.raw);
  if (!ctx) return c.json({ error: "Unauthorized" }, 401);
  const result = await handleQueryRequest({
    handler: (name, args) => mustGetQuery(queries, name).fn({ args, ctx }),
    schema,
    request: c.req.raw,
    userID: ctx.userID,
  });
  return c.json(result);
});

app.post("/api/zero/mutate", async (c) => {
  const ctx = await authData(c.req.raw);
  if (!ctx) return c.json({ error: "Unauthorized" }, 401);
  const result = await handleMutateRequest({
    dbProvider,
    handler: (transact) =>
      transact((tx, name, args) => mustGetMutator(mutators, name).fn({ tx, args, ctx })),
    request: c.req.raw,
    userID: ctx.userID,
  });
  return c.json(result);
});

if (process.env.NODE_ENV === "production") {
  app.use("/*", serveStatic({ root: "./dist" }));
  // A single-page app: every other path renders index.html.
  app.get("*", serveStatic({ path: "./dist/index.html" }));
}

const port = Number(process.env.PORT ?? 3001);
serve({ fetch: app.fetch, port, hostname: "127.0.0.1" }, (info) => {
  console.log(`BandChat listening on http://127.0.0.1:${info.port}`);
});
