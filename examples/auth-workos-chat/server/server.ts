import express from "express";
import type { Request, Response } from "express";
import { createServer } from "node:http";
import { WorkOS } from "@workos-inc/node";
import { APP_ID, WORKOS_CLIENT_ID } from "../constants.js";
import { app as schema } from "../schema.js";
import permissions from "../permissions.js";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazz } from "jazz-tools/backend"`
import { createRequire } from "node:module";
const { createJazz, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

const PORT = 3001;
const APP_ORIGIN = process.env.APP_ORIGIN ?? "http://127.0.0.1:5173";
const COOKIE = "wos_session";
const apiKey = process.env.WORKOS_API_KEY;
const cookiePassword = process.env.WORKOS_COOKIE_PASSWORD ?? "";
if (!apiKey || cookiePassword.length < 32) {
  throw new Error(
    "Set WORKOS_API_KEY and WORKOS_COOKIE_PASSWORD (at least 32 characters) in .env.local.",
  );
}
const workos = new WorkOS(apiKey, { clientId: WORKOS_CLIENT_ID });

function readCookie(header: string | null | undefined): string | undefined {
  return header
    ?.split(";")
    .map((part) => part.trim().split("="))
    .find(([name]) => name === COOKIE)?.[1];
}

// #region workos-session
// The AuthKit session lives in a sealed, HttpOnly cookie. Its access token is
// short-lived: when it has expired, refresh once and remember the fresh seal
// for the stale one, so concurrent requests and sync sockets (which can't set
// a cookie) all see the same session. HTTP responses write the fresh cookie.
const refreshed = new Map<string, Promise<string | null>>();
async function readSession(sealed: string | undefined) {
  if (!sealed) return null;
  const current = (await refreshed.get(sealed)) ?? sealed;
  const session = workos.userManagement.loadSealedSession({ sessionData: current, cookiePassword });
  const result = await session.authenticate();
  if (result.authenticated) return { user: result.user, role: result.role, sealed: current };
  if (!refreshed.has(sealed))
    refreshed.set(
      sealed,
      session.refresh().then((next) => (next.authenticated && next.sealedSession) || null),
    );
  const fresh = await refreshed.get(sealed);
  if (!fresh) return null;
  const again = await workos.userManagement
    .loadSealedSession({ sessionData: fresh, cookiePassword })
    .authenticate();
  return again.authenticated ? { user: again.user, role: again.role, sealed: fresh } : null;
}
// #endregion workos-session

function setCookie(res: Response, sealed: string) {
  res.setHeader("Set-Cookie", `${COOKIE}=${sealed}; HttpOnly; Path=/; SameSite=Lax`);
}

// #region workos-jazz-server
// Jazz runs inside this server; `authenticate` reads the AuthKit cookie, and
// the organization role becomes the claim the chat permissions check.
const jazz = await createJazz({
  appId: APP_ID,
  app: schema,
  permissions,
  store: sqlite({ path: ".jazz" }),
  authenticate: async (request) => {
    const session = await readSession(readCookie(request.headers.get("cookie")));
    return session ? { userId: session.user.id, claims: { role: session.role ?? "" } } : null;
  },
});
// #endregion workos-jazz-server

const api = express();
// Keep the browser's cookie fresh on every HTTP request, Jazz's included.
api.use(async (req, res, next) => {
  const sealed = readCookie(req.headers.cookie);
  const session = await readSession(sealed).catch(() => null);
  if (session && session.sealed !== sealed) setCookie(res, session.sealed);
  next();
});
api.use((req, res, next) => jazz.nodeHandler(req, res, next));

api.get("/auth/sign-in", (_req: Request, res: Response) => {
  res.redirect(
    workos.userManagement.getAuthorizationUrl({
      provider: "authkit",
      clientId: WORKOS_CLIENT_ID,
      redirectUri: `${APP_ORIGIN}/auth/callback`,
    }),
  );
});

api.get("/auth/callback", async (req: Request, res: Response) => {
  const code = typeof req.query.code === "string" ? req.query.code : undefined;
  if (!code) return void res.status(400).send("Missing code");
  const { sealedSession } = await workos.userManagement.authenticateWithCode({
    code,
    clientId: WORKOS_CLIENT_ID,
    session: { sealSession: true, cookiePassword },
  });
  if (!sealedSession) return void res.status(500).send("No session");
  setCookie(res, sealedSession);
  res.redirect("/?signed-in=1");
});

api.get("/api/auth/session", async (req: Request, res: Response) => {
  const session = await readSession(readCookie(req.headers.cookie)).catch(() => null);
  res.json(
    session
      ? {
          firstName: session.user.firstName,
          lastName: session.user.lastName,
          email: session.user.email,
          role: session.role ?? null,
        }
      : null,
  );
});

api.post("/auth/sign-out", async (req: Request, res: Response) => {
  const session = await readSession(readCookie(req.headers.cookie)).catch(() => null);
  const logoutUrl = session
    ? await workos.userManagement
        .loadSealedSession({ sessionData: session.sealed, cookiePassword })
        .getLogoutUrl({ returnTo: APP_ORIGIN })
    : APP_ORIGIN;
  res.setHeader("Set-Cookie", `${COOKIE}=; HttpOnly; Path=/; Max-Age=0`);
  res.json({ logoutUrl });
});

const server = createServer(api);
await jazz.attach(server); // WebSocket sync on /jazz
server.listen(PORT, "127.0.0.1", () => {
  console.log(`WorkOS auth and Jazz ready at http://127.0.0.1:${PORT}`);
});
