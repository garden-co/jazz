import { Hono } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import { createMiddleware } from "hono/factory";
import { InvalidOperationError, parseOperationBatch } from "../operations.js";
import {
  bffSessionCookie,
  createBffSession,
  currentSession,
  invalidateBffSession,
  jazzJwks,
  jazzToken,
  oauth,
  oauthScope,
} from "./auth.js";
import {
  OperationError,
  getTimelineProjectionStatus,
  projectThread,
  projectTimelinePage,
  reconcileOperations,
} from "./bridge.js";

const configuredWebOrigin = process.env.WEB_ORIGIN ?? "http://127.0.0.1:5173";

type AuthenticatedSession = NonNullable<Awaited<ReturnType<typeof currentSession>>>;
type ServerEnvironment = {
  Variables: {
    authentication: AuthenticatedSession;
  };
};

export function createServer(webOrigin = configuredWebOrigin) {
  const server = new Hono<ServerEnvironment>();
  const secureCookies = new URL(webOrigin).protocol === "https:";
  const cookieOptions = {
    httpOnly: true,
    sameSite: "Lax" as const,
    secure: secureCookies,
    path: "/",
  };
  const requireSession = createMiddleware<ServerEnvironment>(async (c, next) => {
    const authentication = await currentSession(getCookie(c, bffSessionCookie));
    if (!authentication) return c.json({ error: "not signed in" }, 401);
    c.set("authentication", authentication);
    await next();
  });

  server.onError((error, c) => {
    const message = error instanceof Error ? error.message : String(error);
    const status = error instanceof OperationError || error instanceof InvalidOperationError
      ? error.status
      : 502;
    return c.json({ error: message }, status);
  });

  server.get("/health", (c) => c.json({ status: "ok" }));
  server.get("/api/health", (c) => c.json({ status: "ok" }));
  server.get("/.well-known/jazz-jwks.json", (c) => c.json(jazzJwks));

  server.use("/api/session", requireSession);
  server.use("/api/timeline", requireSession);
  server.use("/api/timeline/status", requireSession);
  server.use("/api/thread", requireSession);
  server.use("/api/operations", requireSession);

  server.get("/api/session", async (c) => {
    const { did } = c.var.authentication;
    return c.json({ did, token: await jazzToken(did) });
  });

  server.post("/api/auth/login", async (c) => {
    const { handle } = await c.req.json<{ handle?: string }>();
    if (!handle?.trim()) return c.json({ error: "handle is required" }, 400);
    const url = await oauth.authorize(handle.trim(), { scope: oauthScope });
    return c.json({ url: url.toString() });
  });

  server.get("/api/auth/callback", async (c) => {
    const { session } = await oauth.callback(new URL(c.req.url).searchParams);
    const sessionId = await createBffSession(session.did);
    setCookie(c, bffSessionCookie, sessionId, cookieOptions);
    return c.redirect(webOrigin);
  });

  server.post("/api/auth/logout", async (c) => {
    const sessionId = getCookie(c, bffSessionCookie);
    const did = sessionId ? await invalidateBffSession(sessionId) : undefined;
    if (did) await oauth.revoke(did).catch(() => undefined);
    deleteCookie(c, bffSessionCookie, { path: "/", secure: secureCookies });
    return c.json({ ok: true });
  });

  server.get("/api/timeline", async (c) => {
    const { did, session } = c.var.authentication;
    return c.json(await projectTimelinePage(did, session, c.req.query("cursor")));
  });

  server.get("/api/timeline/status", (c) => {
    const status = getTimelineProjectionStatus(c.var.authentication.did);
    return status ? c.json(status) : c.json({ state: "idle" as const });
  });

  server.get("/api/thread", async (c) => {
    const uri = c.req.query("uri");
    if (!uri?.startsWith("at://")) return c.json({ error: "invalid post URI" }, 400);
    const { did, session } = c.var.authentication;
    return c.json(await projectThread(did, session, uri));
  });

  server.post("/api/operations", async (c) => {
    const { did, session } = c.var.authentication;
    const operations = parseOperationBatch(await c.req.json<unknown>(), did);
    await reconcileOperations(did, session, operations);
    return c.json({ ok: true });
  });

  return server;
}

export const server = createServer();
