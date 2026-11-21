// The app's server, as in Neon's realtime-react template (Hono + pg): anonymous
// sign-in, the sealing route for live queries, the write route, and (in
// production) the built app.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { createRealtime } from "@neon/realtime/server";
import { Hono, type Context } from "hono";
import { getCookie, setCookie } from "hono/cookie";
import { jwtVerify, SignJWT } from "jose";
import pg from "pg";
import { Forbidden, runMutation } from "./mutators.js";
import { liveQueries } from "./queries.js";

const databaseUrl = process.env.DATABASE_URL!;
const pool = new pg.Pool({ connectionString: databaseUrl.replace("sslmode=require", "sslmode=verify-full"), max: 10 });
pool.on("error", (error) => console.error(error));
const realtime = createRealtime({
  secret: process.env.NEON_REALTIME_SECRET!,
  db: new URL(databaseUrl).pathname.slice(1),
});
const secret = new TextEncoder().encode(process.env.AUTH_SECRET ?? "stage-plan-dev-secret");

/** The anonymous account from the session cookie. */
async function account(c: Context): Promise<string | undefined> {
  const token = getCookie(c, "jwt");
  if (!token) return undefined;
  try {
    const { payload } = await jwtVerify(token, secret);
    return typeof payload.sub === "string" ? payload.sub : undefined;
  } catch {
    return undefined;
  }
}

export const app = new Hono();

// The harness's rtt: one round trip to the Neon database through this server.
app.get("/api/health", async (c) => c.json((await pool.query("SELECT 1 AS ok")).rows[0]));

// No sign-up: every browser gets an anonymous account, kept in a cookie.
app.get("/api/login", async (c) => {
  const existing = await account(c);
  if (existing) return c.json({ userID: existing });
  const userID = crypto.randomUUID();
  const jwt = await new SignJWT({ sub: userID }).setProtectedHeader({ alg: "HS256" }).setExpirationTime("365d").sign(secret);
  setCookie(c, "jwt", jwt, { path: "/", sameSite: "Lax", httpOnly: true, maxAge: 365 * 24 * 3600 });
  return c.json({ userID });
});

// Seals a named live query for the signed-in account. The sealed query is a
// bearer token, so it isn't served from a cacheable GET.
app.post("/api/live", async (c) => {
  const me = await account(c);
  if (!me) return c.json({ error: "Unauthorized" }, 401);
  const { name, args } = await c.req.json<{ name: string; args: unknown }>();
  const build = liveQueries[name];
  if (!build) return c.json({ error: `No query ${name}` }, 404);
  try {
    return c.json(await realtime.seal({ query: build(me, args) }));
  } catch (error) {
    return c.json({ error: String(error) }, 400);
  }
});

app.post("/api/mutate/:name", async (c) => {
  const me = await account(c);
  if (!me) return c.json({ error: "Unauthorized" }, 401);
  try {
    return c.json(await runMutation(pool, me, c.req.param("name"), await c.req.json()));
  } catch (error) {
    const status = error instanceof Forbidden ? 403 : 400;
    return c.json({ error: error instanceof Error ? error.message : String(error) }, status);
  }
});

// Production: serve the built app from the same origin.
if (process.env.SERVE_DIST) {
  app.use("/*", serveStatic({ root: process.env.SERVE_DIST }));
}

if (process.argv[1]?.endsWith("server/index.ts")) {
  const port = Number(process.env.PORT ?? 4320);
  serve({ fetch: app.fetch, port, hostname: process.env.HOST ?? "127.0.0.1" });
  console.log(`stage-plan server on :${port}`);
}
