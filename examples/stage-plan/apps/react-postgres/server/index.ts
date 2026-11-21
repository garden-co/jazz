// The app's server: anonymous sessions, the REST reads and RPC writes, the
// live WebSocket, and (in production) the built app.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import type { Server } from "node:http";
import { Hono } from "hono";
import { getCookie, setCookie } from "hono/cookie";
import type { Crew } from "../shared/types.js";
import { one, pool } from "./db.js";
import { attachLive } from "./live.js";
import { reads } from "./reads.js";
import { accountFromValue, COOKIE, sessionValue } from "./session.js";
import { writes } from "./writes.js";

type Env = { Variables: { me: string } };
const api = new Hono<Env>();

api.get("/health", (c) => c.text("ok"));

// No sign-up: every browser gets an anonymous account, kept in a cookie.
api.get("/session", async (c) => {
  let account = accountFromValue(getCookie(c, COOKIE));
  if (!account) {
    account = crypto.randomUUID();
    setCookie(c, COOKIE, sessionValue(account), { path: "/", httpOnly: true, sameSite: "Lax", maxAge: 365 * 24 * 3600 });
  }
  const profile = await one<Crew>(pool, `SELECT * FROM crew WHERE account = $1`, [account]);
  return c.json({ account, profile: profile ?? null });
});

api.use("*", async (c, next) => {
  const account = accountFromValue(getCookie(c, COOKIE));
  if (!account) return c.json({ error: "Not signed in" }, 401);
  c.set("me", account);
  await next();
});
api.route("/", reads);
api.route("/", writes);

export const app = new Hono();
app.route("/api", api);

// Production: serve the built app from the same origin.
if (process.env.SERVE_DIST) {
  app.use("/*", serveStatic({ root: process.env.SERVE_DIST }));
}

const port = Number(process.env.PORT ?? 4320);
const server = serve({ fetch: app.fetch, port, hostname: process.env.HOST ?? "127.0.0.1" });
attachLive(server as Server);
console.log(`stage-plan server on :${port}`);
