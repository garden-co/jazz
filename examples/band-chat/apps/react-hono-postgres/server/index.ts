// BandChat's server: Better Auth at /api/auth/*, the REST reads and RPC
// writes, the live WebSocket, and in production the built SPA.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import type { Server } from "node:http";
import { Hono } from "hono";
import { auth, userIdFrom } from "./auth";
import { attachLive } from "./live";
import { reads } from "./reads";
import { writes } from "./writes";

type Env = { Variables: { me: string } };
const api = new Hono<Env>();
api.use("*", async (c, next) => {
  const me = await userIdFrom(c.req.raw.headers);
  if (!me) return c.json({ error: "Unauthorized" }, 401);
  c.set("me", me);
  await next();
});
api.route("/", reads);
api.route("/", writes);

const app = new Hono();
app.get("/health", (c) => c.text("ok"));
// Better Auth owns sign-up, sign-in and the session cookie.
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));
app.route("/api", api);

if (process.env.NODE_ENV === "production") {
  app.use("/*", serveStatic({ root: "./dist" }));
  // A single-page app: every other path renders index.html.
  app.get("*", serveStatic({ path: "./dist/index.html" }));
}

const port = Number(process.env.PORT ?? 3001);
const server = serve({ fetch: app.fetch, port, hostname: "127.0.0.1" }, (info) => {
  console.log(`BandChat listening on http://127.0.0.1:${info.port}`);
});
attachLive(server as Server);
