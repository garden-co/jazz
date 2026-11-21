// BandChat's server, as in Neon's Realtime guides (Hono + pg + Managed Better
// Auth): the sealing route for live queries, the write route, and in
// production the built SPA. The browser signs in with Managed Better Auth
// directly and sends its JWT as a bearer token, which is verified here.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { createRealtime } from "@neon/realtime/server";
import { Hono, type Context } from "hono";
import { createRemoteJWKSet, jwtVerify } from "jose";
import pg from "pg";
import { Forbidden, runMutation } from "./mutators";
import { liveQueries } from "./queries";

const databaseUrl = process.env.DATABASE_URL!;
const pool = new pg.Pool({ connectionString: databaseUrl.replace("sslmode=require", "sslmode=verify-full"), max: 10 });
pool.on("error", (error) => console.error(error));
const realtime = createRealtime({
  secret: process.env.NEON_REALTIME_SECRET!,
  db: new URL(databaseUrl).pathname.slice(1),
});

const authUrl = process.env.NEON_AUTH_BASE_URL!;
const authOrigin = new URL(authUrl).origin;
const jwks = createRemoteJWKSet(new URL(`${authUrl}/.well-known/jwks.json`));

/** The Managed Better Auth user id from the request's bearer JWT. */
async function userId(c: Context): Promise<string | undefined> {
  const header = c.req.header("Authorization");
  const token = header?.startsWith("Bearer ") ? header.slice("Bearer ".length) : undefined;
  if (!token) return undefined;
  try {
    const { payload } = await jwtVerify(token, jwks, { issuer: authOrigin, audience: authOrigin });
    return payload.sub;
  } catch {
    return undefined;
  }
}

const app = new Hono();
// The harness's rtt: one round trip to the Neon database through this server.
app.get("/health", async (c) => c.json((await pool.query("SELECT 1 AS ok")).rows[0]));

// Seals a named live query for the signed-in user. The sealed query is a
// bearer token, so it isn't served from a cacheable GET.
app.post("/api/live", async (c) => {
  const me = await userId(c);
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
  const me = await userId(c);
  if (!me) return c.json({ error: "Unauthorized" }, 401);
  try {
    return c.json(await runMutation(pool, me, c.req.param("name"), await c.req.json()));
  } catch (error) {
    const status = error instanceof Forbidden ? 403 : 400;
    return c.json({ error: error instanceof Error ? error.message : String(error) }, status);
  }
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
