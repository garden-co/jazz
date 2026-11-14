import express from "express";
import type { Request, Response } from "express";
import { createServer } from "node:http";
import { WorkOS } from "@workos-inc/node";
import { WORKOS_CLIENT_ID } from "../constants.js";
import { app as schema } from "../schema.js";
import permissions from "../jazz.permissions.json" with { type: "json" };
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
// The AuthKit session lives in a sealed, HttpOnly cookie, and its access
// token lasts minutes. Reading it never refreshes it: refreshing spends a
// single-use refresh token, so it happens in one place per request kind, in
// `refreshSession` below, and the rotated seal goes back to the browser as a
// cookie on that response.
async function readSession(sealed: string | undefined) {
  if (!sealed) return null;
  const session = await workos.userManagement
    .loadSealedSession({ sessionData: sealed, cookiePassword })
    .authenticate();
  return session.authenticated ? session : null;
}

/**
 * The rotated seal, when `sealed`'s access token has expired or expires
 * within `withinSeconds`; null when it doesn't need refreshing, or can't be
 * refreshed (signed out or revoked: `readSession` then says so).
 */
async function refreshSession(sealed: string | undefined, withinSeconds = 0) {
  if (!sealed) return null;
  const session = workos.userManagement.loadSealedSession({ sessionData: sealed, cookiePassword });
  const current = await session.authenticate();
  const expiresIn = current.authenticated ? expOf(current.accessToken) - Date.now() / 1000 : 0;
  if (expiresIn > withinSeconds) return null;
  const refreshed = await session.refresh();
  return refreshed.authenticated && refreshed.sealedSession ? refreshed.sealedSession : null;
}

/** When a WorkOS access token expires (its `exp`, Unix seconds). */
function expOf(accessToken: string): number {
  const payload = JSON.parse(Buffer.from(accessToken.split(".")[1]!, "base64url").toString());
  return payload.exp as number;
}

const sessionCookie = (sealed: string) => `${COOKIE}=${sealed}; HttpOnly; Path=/; SameSite=Lax`;

/** `cookieHeader` with the session cookie set to `sealed`, other cookies kept. */
function withSession(cookieHeader: string | null | undefined, sealed: string): string {
  const others = (cookieHeader ?? "")
    .split(";")
    .map((part) => part.trim())
    .filter((part) => part && !part.startsWith(`${COOKIE}=`));
  return [...others, `${COOKIE}=${sealed}`].join("; ");
}
// #endregion workos-session

// #region workos-jazz-server
// Jazz runs inside this server. `refresh` is the only place a sync session's
// cookie is refreshed: Jazz calls it on the request a browser makes to ask
// who is signed in, one tab at a time, before an open session reaches the
// `expiresAt` that `authenticate` returned, and renews that session in place.
const jazz = createJazz({
  app: schema,
  permissions,
  store: sqlite({ path: ".jazz" }),
  refresh: async (request, next) => {
    // An open session renews up to ~90 s before its expiry: refresh a token
    // that expires within two minutes too, not only an expired one.
    const cookie = request.headers.get("cookie");
    const fresh = await refreshSession(readCookie(cookie), 120).catch(() => null);
    if (!fresh) return next(request);
    const headers = new Headers(request.headers);
    headers.set("cookie", withSession(cookie, fresh));
    const response = await next(new Request(request, { headers }));
    response.headers.append("Set-Cookie", sessionCookie(fresh));
    return response;
  },
  // Read-only: it never refreshes, so it may run on every request. The
  // organization role becomes the claim the chat permissions check.
  authenticate: async (request) => {
    const session = await readSession(readCookie(request.headers.get("cookie")));
    return session
      ? {
          userId: session.user.id,
          claims: { role: session.role ?? "" },
          expiresAt: expOf(session.accessToken),
        }
      : null;
  },
});
// #endregion workos-jazz-server

const api = express();
// The app's own routes refresh an expired session as an AuthKit app's routes
// do, and write the rotated cookie. Jazz's routes (`/jazz`) are left to
// `refresh` above.
api.use(["/api", "/auth"], async (req, res, next) => {
  const fresh = await refreshSession(readCookie(req.headers.cookie)).catch(() => null);
  if (fresh) {
    req.headers.cookie = withSession(req.headers.cookie, fresh);
    res.setHeader("Set-Cookie", sessionCookie(fresh));
  }
  next();
});
api.use((req, res, next) => jazz.nodeHandler(req, res, next));

// WorkOS events: a session revoked elsewhere (sign-out on another device, an
// admin) ends this user's open sync sessions at once instead of at their next
// renewal. Optional: set WORKOS_WEBHOOK_SECRET and add the endpoint in WorkOS.
const webhookSecret = process.env.WORKOS_WEBHOOK_SECRET;
if (webhookSecret) {
  api.post("/webhooks/workos", express.json(), async (req: Request, res: Response) => {
    const event = await workos.webhooks
      .constructEvent({
        payload: req.body,
        sigHeader: req.header("workos-signature") ?? "",
        secret: webhookSecret,
      })
      .catch(() => null);
    if (!event) return void res.sendStatus(400);
    if (event.event === "session.revoked") await jazz.revokeUser(event.data.userId);
    res.sendStatus(200);
  });
}

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
  res.setHeader("Set-Cookie", sessionCookie(sealedSession));
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

// Signs out without redirecting, so the client can call Jazz's `refresh()`
// and every tab of this browser follows: ends the session at WorkOS (its
// refresh token stops working on every device) and clears the cookie.
api.post("/auth/sign-out", async (req: Request, res: Response) => {
  const session = await readSession(readCookie(req.headers.cookie)).catch(() => null);
  if (session) {
    await workos.userManagement.revokeSession({ sessionId: session.sessionId }).catch(() => {});
  }
  res.setHeader("Set-Cookie", `${COOKIE}=; HttpOnly; Path=/; Max-Age=0`);
  res.json({ ok: true });
});

const server = createServer(api);
await jazz.attach(server); // WebSocket sync on /jazz
server.listen(PORT, "127.0.0.1", () => {
  console.log(`WorkOS auth and Jazz ready at http://127.0.0.1:${PORT}`);
});
