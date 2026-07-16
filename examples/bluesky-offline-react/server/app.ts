import { Hono } from "hono";
import { deleteCookie, getCookie, setCookie } from "hono/cookie";
import { currentSession, jazzJwks, jazzToken, oauth, oauthScope } from "./auth.js";
import {
  OperationError,
  projectThread,
  projectTimelinePage,
  reconcileOperations,
  type QueuedOperation,
} from "./bridge.js";

const webOrigin = process.env.WEB_ORIGIN ?? "http://127.0.0.1:5173";

export const server = new Hono();

server.get("/health", (c) => c.json({ status: "ok" }));
server.get("/.well-known/jazz-jwks.json", (c) => c.json(jazzJwks));

server.get("/api/session", async (c) => {
  const did = getCookie(c, "did");
  const session = await currentSession(did);
  return did && session
    ? c.json({ did, token: await jazzToken(did) })
    : c.json({ error: "not signed in" }, 401);
});

server.post("/api/auth/login", async (c) => {
  const { handle } = await c.req.json<{ handle?: string }>();
  if (!handle?.trim()) return c.json({ error: "handle is required" }, 400);
  const url = await oauth.authorize(handle.trim(), { scope: oauthScope });
  return c.json({ url: url.toString() });
});

server.get("/api/auth/callback", async (c) => {
  const { session } = await oauth.callback(new URL(c.req.url).searchParams);
  setCookie(c, "did", session.did, { httpOnly: true, sameSite: "Lax", secure: false, path: "/" });
  return c.redirect(webOrigin);
});

server.post("/api/auth/logout", async (c) => {
  const did = getCookie(c, "did");
  if (did) await oauth.revoke(did).catch(() => undefined);
  deleteCookie(c, "did", { path: "/" });
  return c.json({ ok: true });
});

server.get("/api/timeline", async (c) => {
  const did = getCookie(c, "did");
  const session = await currentSession(did);
  if (!did || !session) return c.json({ error: "not signed in" }, 401);
  try {
    return c.json(await projectTimelinePage(did, session, c.req.query("cursor")));
  } catch (error) {
    return c.json({ error: error instanceof Error ? error.message : String(error) }, 502);
  }
});

server.get("/api/thread", async (c) => {
  const did = getCookie(c, "did");
  const session = await currentSession(did);
  if (!did || !session) return c.json({ error: "not signed in" }, 401);
  const uri = c.req.query("uri");
  if (!uri?.startsWith("at://")) return c.json({ error: "invalid post URI" }, 400);
  try {
    return c.json(await projectThread(did, session, uri));
  } catch (error) {
    return c.json({ error: error instanceof Error ? error.message : String(error) }, 502);
  }
});

server.post("/api/operations", async (c) => {
  const did = getCookie(c, "did");
  const session = await currentSession(did);
  if (!did || !session) return c.json({ error: "not signed in" }, 401);
  const operations = await c.req.json<QueuedOperation[]>();
  if (!Array.isArray(operations) || operations.length > 100) return c.json({ error: "invalid operations" }, 400);
  if (operations.some((operation) => operation.ownerDid !== did)) return c.json({ error: "owner mismatch" }, 403);
  try {
    await reconcileOperations(did, session, operations);
    return c.json({ ok: true });
  } catch (error) {
    if (error instanceof OperationError) return c.json({ error: error.message }, error.status);
    return c.json({ error: error instanceof Error ? error.message : String(error) }, 502);
  }
});
