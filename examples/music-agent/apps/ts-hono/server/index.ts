import type { Server } from "node:http";
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { agentProvider } from "@/src/agent/config";
import { startSweeper } from "@/src/agent/runner";
import { appOrigin } from "@/src/lib/app-origin";
import { auth } from "@/src/lib/auth";
import { backendJazzClient } from "@/src/lib/backend-jazz-client";
import * as routes from "@/src/server/routes";

const production = process.env.NODE_ENV === "production";
// In development Vite serves the browser app on the app origin and proxies
// /api and /jazz here; in production this server is the whole app.
const port = Number(process.env.PORT ?? (production ? new URL(appOrigin).port || 3000 : 3001));

// Fail at boot, not on the first reply, when the provider is misconfigured.
agentProvider();

const app = new Hono();
app.get("/health", (c) => c.text("ok"));
// Better Auth owns sign-up, sign-in and the session cookie.
app.on(["GET", "POST"], "/api/auth/*", (c) => auth.handler(c.req.raw));
app.get("/api/config", () => routes.config());
app.post("/api/bootstrap", (c) => routes.bootstrap(c.req.raw));
app.post("/api/turns", (c) => routes.requestReply(c.req.raw));
app.post("/api/turns/:id/regenerate", (c) => routes.regenerate(c.req.raw, c.req.param("id")));
app.post("/api/turns/:id/resume", (c) => routes.resume(c.req.raw, c.req.param("id")));
app.get("/api/attachments/:id", (c) => routes.attachment(c.req.raw, c.req.param("id")));
// Sync and sign-in for the browser, on this app's own origin.
app.all("/jazz", async (c) => (await backendJazzClient()).fetch(c.req.raw));
app.all("/jazz/*", async (c) => (await backendJazzClient()).fetch(c.req.raw));
if (production) {
  app.use("/*", serveStatic({ root: "./dist" }));
  // A single-page app: every other path renders index.html.
  app.get("*", serveStatic({ path: "./dist/index.html" }));
}

const server = serve({ fetch: app.fetch, port }, (info) => {
  console.log(`MusicAgent listening on http://localhost:${info.port}`);
});
// Sync sockets upgrade on the same server.
await (await backendJazzClient()).attach(server as Server);
// Replies left streaming by a previous server process are marked interrupted,
// so the conversation offers to resume or retry them.
startSweeper();
