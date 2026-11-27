import type { Server } from "node:http";
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { assertConfiguration } from "../src/lib/config.mjs";
import { auth } from "./auth";
import { endOtherSignIns, jazz } from "./jazz";

const config = assertConfiguration();
const production = process.env.NODE_ENV === "production";
// In development Vite serves the browser app on the app origin and proxies
// /api and /jazz here; in production this server is the whole app.
const port = Number(process.env.PORT ?? (production ? new URL(config.origin).port || 3000 : 3001));

const app = new Hono();
app.get("/health", (c) => c.text("ok"));
// Better Auth owns sign-up, sign-in and the session cookie.
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));
// Before a lost device is removed: end every other sign-in of the user, so
// no open connection of that device stays signed in. Signed in as the user
// who asks; the page's own origin only.
app.post("/api/end-other-sign-ins", async (c) => {
  if (c.req.header("origin") !== config.origin) return c.text("Forbidden", 403);
  const session = await auth.api.getSession({ headers: c.req.raw.headers });
  if (!session) return c.text("Unauthorized", 401);
  const ended = await endOtherSignIns(session.user.id, session.session.token);
  return c.json({ ended });
});
// Sync and sign-in for the browser, on this app's own origin.
app.all("/jazz", async (c) => (await jazz()).handle(c.req.raw));
app.all("/jazz/*", async (c) => (await jazz()).handle(c.req.raw));
if (production) {
  app.use("/*", serveStatic({ root: "./dist" }));
  // A single-page app: every other path renders index.html.
  app.get("*", serveStatic({ path: "./dist/index.html" }));
}

const server = serve({ fetch: app.fetch, port }, (info) => {
  console.log(`BandChat listening on http://localhost:${info.port}`);
});
// Sync sockets upgrade on the same server.
await (await jazz()).attach(server as Server);
