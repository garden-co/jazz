import { serveStatic } from "@hono/node-server/serve-static";
import { Hono } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import { createMiddleware } from "hono/factory";
import { parseAtRecordUri } from "../shared/identifiers.js";
import { InvalidOperationError, parseOperationBatch } from "../shared/pending-operations.js";
import {
  bffSessionCookie,
  createBffSession,
  createJazzToken,
  invalidateBffSession,
  jazzJwks,
  oauth,
  oauthScope,
  restoreBffSession,
} from "./auth.js";
import { projectThread, projectTimelinePage, reconcileOperations } from "./bridge.js";
import { OperationError } from "./bluesky.js";

const configuredWebOrigin = process.env.WEB_ORIGIN ?? "http://127.0.0.1:3001";

type ServerOptions = {
  staticRoot?: string;
  webOrigin?: string;
};

type AuthenticatedSession = NonNullable<Awaited<ReturnType<typeof restoreBffSession>>>;
type ServerEnvironment = {
  Variables: {
    authentication: AuthenticatedSession;
  };
};

export function createServer({ staticRoot, webOrigin = configuredWebOrigin }: ServerOptions = {}) {
  const server = new Hono<ServerEnvironment>();
  const secureCookies = new URL(webOrigin).protocol === "https:";
  const cookieOptions = {
    httpOnly: true,
    sameSite: "Lax" as const,
    secure: secureCookies,
    path: "/",
  };
  const requireSession = createMiddleware<ServerEnvironment>(async (c, next) => {
    const authentication = await restoreBffSession(getCookie(c, bffSessionCookie));
    if (!authentication) return c.json({ error: "not signed in" }, 401);
    c.set("authentication", authentication);
    await next();
  });

  server.onError((error, c) => {
    if (error instanceof OperationError || error instanceof InvalidOperationError) {
      return c.json({ error: error.message }, error.status);
    }
    console.error(error);
    return c.json({ error: "Unexpected server error" }, 502);
  });

  server.get("/api/health", (c) => c.json({ status: "ok" }));
  server.get("/.well-known/jazz-jwks.json", (c) => c.json(jazzJwks));

  server.use("/api/session", requireSession);
  server.use("/api/timeline", requireSession);
  server.use("/api/thread", requireSession);
  server.use("/api/operations", requireSession);

  // ATProto establishes identity; Jazz trusts the same DID through this short-lived JWT.
  server.get("/api/session", async (c) => {
    const { did } = c.var.authentication;
    return c.json({ did, token: await createJazzToken(did) });
  });

  server.post("/api/auth/login", async (c) => {
    const body = await c.req.json<{ handle?: string }>().catch(() => undefined);
    const handle = body?.handle?.trim();
    if (!handle) return c.json({ error: "handle is required" }, 400);
    const url = await oauth.authorize(handle, { scope: oauthScope });
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
    // Local logout must still succeed when remote token revocation is unavailable.
    if (did) await oauth.revoke(did).catch(() => undefined);
    deleteCookie(c, bffSessionCookie, { path: "/", secure: secureCookies });
    return c.json({ ok: true });
  });

  // HTTP triggers source reads and writes; projected data reaches React through Jazz.
  server.get("/api/timeline", async (c) => {
    const { did, session } = c.var.authentication;
    const { cursor, hasMore, count } = await projectTimelinePage(
      did,
      session,
      c.req.query("cursor"),
    );
    return c.json({ cursor, hasMore, count });
  });

  server.get("/api/thread", async (c) => {
    const uri = c.req.query("uri");
    const parsedUri = parseAtRecordUri(uri);
    if (!uri || !parsedUri || parsedUri.collection !== "app.bsky.feed.post") {
      return c.json({ error: "invalid post URI" }, 400);
    }
    const { did, session } = c.var.authentication;
    return c.json(await projectThread(did, session, uri));
  });

  server.post("/api/operations", async (c) => {
    const { did, session } = c.var.authentication;
    const body = await c.req.json<unknown>().catch(() => undefined);
    const operations = parseOperationBatch(body, did);
    await reconcileOperations(did, session, operations);
    return c.json({ ok: true });
  });

  if (staticRoot) {
    const isBackendPath = (path: string) =>
      path === "/api" || path.startsWith("/api/") || path.startsWith("/.well-known/");
    const serveAsset = serveStatic<ServerEnvironment>({ root: staticRoot });
    const serveIndex = serveStatic<ServerEnvironment>({ root: staticRoot, path: "index.html" });

    server.use("*", (c, next) => (isBackendPath(c.req.path) ? next() : serveAsset(c, next)));
    server.notFound(async (c) => {
      if (c.req.method !== "GET" || isBackendPath(c.req.path)) {
        return c.json({ error: "not found" }, 404);
      }
      const response = await serveIndex(c, async () => undefined);
      return response ?? c.json({ error: "not found" }, 404);
    });
  }

  return server;
}
