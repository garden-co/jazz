// The app's server: anonymous sign-in, Zero's query and mutate endpoints,
// and (in production) the built app. zero-cache calls the endpoints with the
// browser's cookies, as in Zero's hello-zero starter.
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { mustGetMutator, mustGetQuery } from "@rocicorp/zero";
import { handleMutateRequest, handleQueryRequest } from "@rocicorp/zero/server";
import { zeroNodePg } from "@rocicorp/zero/server/adapters/pg";
import { Hono, type Context } from "hono";
import { getCookie, setCookie } from "hono/cookie";
import { jwtVerify, SignJWT } from "jose";
import pg from "pg";
import { mutators } from "../mutators.js";
import { queries } from "../queries.js";
import { schema, type AuthData } from "../schema.js";

const secret = new TextEncoder().encode(process.env.AUTH_SECRET ?? "stage-plan-dev-secret");
const dbProvider = zeroNodePg(schema, new pg.Pool({ connectionString: process.env.ZERO_UPSTREAM_DB }));

async function authData(c: Context): Promise<AuthData | undefined> {
  const token = getCookie(c, "jwt");
  if (!token) return undefined;
  try {
    const { payload } = await jwtVerify(token, secret);
    return typeof payload.sub === "string" ? { userID: payload.sub } : undefined;
  } catch {
    return undefined;
  }
}

export const app = new Hono();

// No sign-up: every browser gets an anonymous account, kept in a cookie.
app.get("/api/login", async (c) => {
  const existing = await authData(c);
  if (existing) return c.json(existing);
  const userID = crypto.randomUUID();
  const jwt = await new SignJWT({ sub: userID })
    .setProtectedHeader({ alg: "HS256" })
    .setExpirationTime("365d")
    .sign(secret);
  setCookie(c, "jwt", jwt, { path: "/", sameSite: "Lax", maxAge: 365 * 24 * 3600 });
  return c.json({ userID });
});

app.post("/api/zero/query", async (c) => {
  const ctx = await authData(c);
  const result = await handleQueryRequest({
    handler: (name, args) => mustGetQuery(queries, name).fn({ args, ctx }),
    schema,
    request: c.req.raw,
    userID: ctx?.userID ?? null,
  });
  return c.json(result);
});

app.post("/api/zero/mutate", async (c) => {
  const ctx = await authData(c);
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

// Production: serve the built app from the same origin.
if (process.env.SERVE_DIST) {
  app.use("/*", serveStatic({ root: process.env.SERVE_DIST }));
}

if (process.argv[1]?.endsWith("server/index.ts")) {
  const port = Number(process.env.PORT ?? 4310);
  serve({ fetch: app.fetch, port, hostname: process.env.HOST ?? "127.0.0.1" });
  console.log(`stage-plan server on :${port}`);
}
