/**
 * Todo Server - TypeScript implementation using Jazz.
 *
 * This demonstrates how to use Jazz with Express to build a simple REST API.
 */

import express, { Request, Response, NextFunction } from "express";
import type { Application } from "express";
import type { Server } from "node:http";
import { tmpdir } from "node:os";
import { mkdtempSync } from "node:fs";
import { join } from "node:path";
import { createJazz, fromJWT, remote, sqlite, type Db } from "jazz-tools/backend";
import { app as schemaApp } from "../schema.js";
import permissions from "../permissions.js";

// ============================================================================
// Types
// ============================================================================

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  owner_id: string;
}

interface CreateTodoRequest {
  title: string;
  owner_id?: string;
}

interface UpdateTodoRequest {
  title?: string;
  done?: boolean;
}

export interface TodoServer {
  app: Application;
  db: Db;
  shutdown: () => Promise<void>;
}

export interface RunningServer extends TodoServer {
  server: Server;
  port: number;
  baseUrl: string;
}

export interface TodoServerConfig {
  /** The directory Jazz keeps its SQLite data in. */
  dataPath?: string;
  /** A Jazz core elsewhere to keep the data in instead, with its app id and admin secret. */
  serverUrl?: string;
  appId?: string;
  adminSecret?: string;
  /** The JWKS endpoint and issuer of the identity provider whose JWTs this API accepts. */
  jwksUrl?: string;
  jwtIssuer?: string;
}

/** The parts of an Express request `authenticate` reads, as a fetch Request. */
function toRequest(req: Request): globalThis.Request {
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    for (const item of [value ?? []].flat()) headers.append(name, item);
  }
  return new globalThis.Request(`http://${req.headers.host ?? "localhost"}${req.originalUrl}`, {
    headers,
  });
}

// ============================================================================
// Helpers
// ============================================================================

/**
 * Create a todo server.
 *
 * @param config Optional server config. Environment variables are used as fallback.
 * @returns TodoServer with app, db, and shutdown function
 */
export async function createServer(config: TodoServerConfig = {}): Promise<TodoServer> {
  const dataPath = config.dataPath ?? mkdtempSync(join(tmpdir(), "jazz-todo-"));
  const serverUrl = config.serverUrl ?? process.env.JAZZ_SERVER_URL?.trim();
  const appId = config.appId ?? process.env.JAZZ_APP_ID?.trim();
  const adminSecret = config.adminSecret ?? process.env.JAZZ_ADMIN_SECRET?.trim();
  const jwksUrl = config.jwksUrl ?? process.env.JAZZ_JWKS_URL?.trim();
  const jwtIssuer = config.jwtIssuer ?? process.env.JAZZ_JWT_ISSUER?.trim();

  if (serverUrl && (!appId || !adminSecret)) {
    throw new Error("JAZZ_SERVER_URL needs JAZZ_APP_ID and JAZZ_ADMIN_SECRET");
  }
  if (Boolean(jwksUrl) !== Boolean(jwtIssuer)) {
    throw new Error("JAZZ_JWKS_URL and JAZZ_JWT_ISSUER go together");
  }

  // #region context-setup-ts-backend
  // Who a request is, your backend decides: here, the subject of a JWT
  // your identity provider signed. Without one configured, nobody is signed in.
  const authenticate =
    jwksUrl && jwtIssuer
      ? fromJWT({ jwksUrl, issuer: jwtIssuer, audience: false })
      : async () => null;

  const jazz = createJazz({
    app: schemaApp,
    permissions,
    // Jazz Cloud (or your own core), or SQLite in this process.
    store: serverUrl
      ? remote({ url: serverUrl, appId: appId!, secret: adminSecret! })
      : sqlite({ path: dataPath }),
    authenticate,
  });
  // The backend's own database, with backend permissions.
  const db = jazz.db;
  // #endregion context-setup-ts-backend

  // Create Express app
  const app = express();
  app.use(express.json());

  // Sign in: returns the caller's Jazz account, which Jazz creates the first
  // time it sees a verified identity.
  app.post("/sign-in", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const account = await jazz.accountFor(toRequest(req));
      if (!account) {
        res.status(401).json({ error: "Unauthorized" });
        return;
      }
      res.json({ account });
    } catch (e) {
      next(e);
    }
  });

  // Track active SSE connections for live updates
  const sseConnections = new Set<Response>();

  // Helper to broadcast current todos to all SSE connections
  async function broadcastTodos() {
    if (sseConnections.size === 0) {
      return;
    }

    const todos = await db.all(schemaApp.todos);
    const data = `data: ${JSON.stringify(todos)}\n\n`;

    for (const res of sseConnections) {
      res.write(data);
    }
  }

  // ========================================================================
  // Routes
  // ========================================================================

  // Health check
  app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "healthy" });
  });

  // List all todos
  app.get("/todos", async (_req: Request, res: Response, next: NextFunction) => {
    try {
      const todos = await db.all(schemaApp.todos);
      res.json(todos);
    } catch (e) {
      next(e);
    }
  });

  // Create a todo
  app.post("/todos", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const body = req.body as CreateTodoRequest;

      if (!body.title) {
        res.status(400).json({ error: "title is required" });
        return;
      }

      const inserted = db.insert(schemaApp.todos, {
        title: body.title,
        done: false,
        owner_id: body.owner_id ?? "93c209ee-dbae-5071-a90d-02f8c0bbcf6a",
      });
      await inserted.wait({ tier: "global" });

      res.status(201).json(inserted.value);

      // Notify SSE connections
      await broadcastTodos();
    } catch (e) {
      next(e);
    }
  });

  // Verify the caller before checking that the requested account is their own.
  app.get("/todos/as/:userId", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const request = toRequest(req);
      const account = await jazz.accountFor(request);
      if (!account) {
        res.status(401).json({ error: "Unauthorized" });
        return;
      }
      if (account !== req.params.userId) {
        res.status(403).json({ error: "Account mismatch" });
        return;
      }
      const userDb = await jazz.forReqUser(request);
      const todos = await userDb.all(schemaApp.todos);
      res.json(todos);
    } catch (e) {
      next(e);
    }
  });

  // Live SSE stream of all todos (must be before /todos/:id)
  app.get("/todos/live", async (_req: Request, res: Response) => {
    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    // Register this connection
    sseConnections.add(res);

    // Send initial state
    const todos = await db.all(schemaApp.todos);
    res.write(`data: ${JSON.stringify(todos)}\n\n`);

    // Clean up on disconnect
    res.on("close", () => {
      sseConnections.delete(res);
    });
  });

  // Get a single todo
  app.get("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;

      const todo = await db.one(schemaApp.todos.where({ id }));
      if (!todo) {
        res.status(404).json({ error: "Todo not found" });
        return;
      }

      res.json(todo);
    } catch (e) {
      next(e);
    }
  });

  // Update a todo
  app.put("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const body = req.body as UpdateTodoRequest;

      const updates = {
        title: body.title,
        done: body.done,
      };

      if (Object.values(updates).every((value) => value === undefined)) {
        // No updates, just return the current todo
        const todo = await db.one(schemaApp.todos.where({ id }));
        if (!todo) {
          res.status(404).json({ error: "Todo not found" });
          return;
        }
        res.json(todo);
        return;
      }

      await db.update(schemaApp.todos, id, updates).wait({ tier: "global" });

      // Fetch updated todo
      const todo = await db.one(schemaApp.todos.where({ id }));
      if (!todo) {
        res.status(404).json({ error: "Todo not found after update" });
        return;
      }
      res.json(todo);

      // Notify SSE connections
      await broadcastTodos();
    } catch (e) {
      next(e);
    }
  });

  // Delete a todo
  app.delete("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;

      await db.delete(schemaApp.todos, id).wait({ tier: "global" });
      res.status(204).send();

      // Notify SSE connections
      await broadcastTodos();
    } catch (e) {
      const error = e as Error;
      if (error.message?.includes("NotFound")) {
        res.status(404).json({ error: "Todo not found" });
      } else {
        next(e);
      }
    }
  });

  // Error handler
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error("Error:", err);
    res.status(500).json({ error: err.message });
  });

  return {
    app,
    db,
    shutdown: async () => {
      await jazz.close();
    },
  };
}

/**
 * Start the server on a specific port.
 *
 * @param todoServer The server to start
 * @param port Port to listen on (0 for random available port)
 * @returns RunningServer with server instance and actual port
 */
export function startServer(todoServer: TodoServer, port: number = 0): Promise<RunningServer> {
  return new Promise((resolve) => {
    const server = todoServer.app.listen(port, () => {
      const address = server.address();
      const actualPort = typeof address === "object" && address ? address.port : port;
      resolve({
        ...todoServer,
        server,
        port: actualPort,
        baseUrl: `http://localhost:${actualPort}`,
      });
    });
  });
}

/**
 * Stop a running server.
 */
export async function stopServer(server: RunningServer): Promise<void> {
  await server.shutdown();
  await new Promise<void>((resolve, reject) => {
    server.server.close((err) => {
      if (err) reject(err);
      else resolve();
    });
  });
}

// ============================================================================
// CLI Entry Point
// ============================================================================

async function main() {
  const todoServer = await createServer();

  // Start server
  const port = parseInt(process.env.PORT ?? "3000", 10);
  const running = await startServer(todoServer, port);

  console.log(`Todo server listening on ${running.baseUrl}`);
  console.log(`  GET    /health`);
  console.log(`  GET    /todos`);
  console.log(`  POST   /todos`);
  console.log(`  GET    /todos/:id`);
  console.log(`  PUT    /todos/:id`);
  console.log(`  DELETE /todos/:id`);

  // Graceful shutdown
  process.on("SIGINT", async () => {
    console.log("\nShutting down...");
    await stopServer(running);
    process.exit(0);
  });
}

// Only run main if this is the entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
  });
}
