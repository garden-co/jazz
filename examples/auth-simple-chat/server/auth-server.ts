import express from "express";
import type { Request, Response } from "express";
import { randomUUID } from "node:crypto";
import { createServer, type Server as HttpServer } from "node:http";
import type { Jazz } from "jazz-tools/backend";
import { DEFAULT_APP_ID } from "../constants.ts";
import { app as schema } from "../schema.ts";
import permissions from "../permissions.ts";
import { findUser, createUser, type User } from "./users.ts";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazz } from "jazz-tools/backend"`
import { createRequire } from "node:module";
const { createJazz, memory, sqlite } = createRequire(import.meta.url)(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

export interface AuthServerOptions {
  port?: number;
  appId?: string;
  /** Keep Jazz's data in this directory; in memory when omitted. */
  dataDir?: string;
}

export interface AuthServerHandle {
  port: number;
  url: string;
  jazz: Jazz;
  stop: () => Promise<void>;
}

const SESSION_COOKIE = "chat_session";

export async function startAuthServer(options: AuthServerOptions = {}): Promise<AuthServerHandle> {
  const port = options.port ?? 3001;

  // 1. The app's own sessions: an opaque id in an HttpOnly cookie.
  // In production, keep them in your database instead of process memory.
  const sessions = new Map<string, User>();
  function userFromCookie(cookieHeader: string | null | undefined): User | null {
    const id = cookieHeader
      ?.split(";")
      .map((part) => part.trim().split("="))
      .find(([name]) => name === SESSION_COOKIE)?.[1];
    return (id && sessions.get(id)) || null;
  }
  function startSession(res: Response, user: User) {
    const id = randomUUID();
    sessions.set(id, user);
    res.setHeader("Set-Cookie", `${SESSION_COOKIE}=${id}; HttpOnly; Path=/; SameSite=Lax`);
    res.json({ username: user.username, role: user.role });
  }

  // 2. Jazz runs inside this server. `authenticate` reads the same cookie,
  // so the browser never holds a Jazz credential; the role becomes a claim.
  const jazz = await createJazz({
    appId: options.appId ?? DEFAULT_APP_ID,
    app: schema,
    permissions,
    store: options.dataDir ? sqlite({ path: options.dataDir }) : memory(),
    authenticate: (request) => {
      const user = userFromCookie(request.headers.get("cookie"));
      return user ? { userId: user.userId, claims: { role: user.role } } : null;
    },
  });

  const app = express();
  app.use((req, res, next) => jazz.nodeHandler(req, res, next));
  app.use(express.json());

  app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "ok" });
  });

  app.get("/api/auth/session", (req: Request, res: Response) => {
    const user = userFromCookie(req.headers.cookie);
    res.json(user ? { username: user.username, role: user.role } : null);
  });

  // 3a. Sign in to an existing account.
  app.post("/api/auth/sign-in", (req: Request, res: Response) => {
    const { email, password } = req.body as { email?: string; password?: string };
    if (!email?.trim() || !password) {
      res.status(400).json({ error: "Email and password are required." });
      return;
    }
    const user = findUser(email.trim(), password);
    if (!user) {
      res.status(401).json({ error: "Invalid email or password." });
      return;
    }
    startSession(res, user);
  });

  // 3b. Sign up: creates a new account.
  app.post("/api/auth/sign-up", (req: Request, res: Response) => {
    const { email, password } = req.body as { email?: string; password?: string };
    if (!email?.trim() || !password) {
      res.status(400).json({ error: "Email and password are required." });
      return;
    }
    const user = createUser(email.trim(), password);
    if (!user) {
      res.status(409).json({ error: "An account with this email already exists." });
      return;
    }
    startSession(res, user);
  });

  app.post("/api/auth/sign-out", (req: Request, res: Response) => {
    const id = req.headers.cookie
      ?.split(";")
      .map((part) => part.trim().split("="))
      .find(([name]) => name === SESSION_COOKIE)?.[1];
    if (id) sessions.delete(id);
    res.setHeader("Set-Cookie", `${SESSION_COOKIE}=; HttpOnly; Path=/; Max-Age=0`);
    res.json({});
  });

  const server: HttpServer = createServer(app);
  await jazz.attach(server); // WebSocket sync on /jazz
  await new Promise<void>((resolve, reject) => {
    server.listen(port, "127.0.0.1", (error?: unknown) => {
      if (error) reject(error);
      else resolve();
    });
  });

  return {
    port,
    url: `http://127.0.0.1:${port}`,
    jazz,
    stop: async () => {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
      await jazz.close();
    },
  };
}
