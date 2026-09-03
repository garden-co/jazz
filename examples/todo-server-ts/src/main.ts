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
import { createJazzContext, type Db } from "jazz-tools/backend";
import { app as schemaApp } from "../schema.js";
import permissions from "../permissions.js";

// ============================================================================
// Types
// ============================================================================

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  owner_id: string;
}

interface CreateTodoRequest {
  title: string;
  description?: string;
}

interface UpdateTodoRequest {
  title?: string;
  done?: boolean;
  description?: string;
}

export interface TodoServer {
  app: Application;
  db: Db;
  shutdown: () => Promise<void>;
  flush: () => void;
}

export interface RunningServer extends TodoServer {
  server: Server;
  port: number;
  baseUrl: string;
}

export interface TodoServerOptions {
  /** URL of the external issuer's JWKS endpoint for HTTP request authentication. */
  jwksUrl?: string;
}
interface ServerLifecycle {
  draining: boolean;
  beginDrain: () => void;
}

const serverLifecycles = new WeakMap<Application, ServerLifecycle>();
const stopPromises = new WeakMap<Server, Promise<void>>();

// ============================================================================
// Helpers
// ============================================================================

/**
 * Create a todo server.
 *
 * @param dataPath Optional path to local Fjall database file. If omitted, uses a temp directory.
 * @returns TodoServer with the Express app, administrative database handle, and lifecycle functions
 */
export async function createServer(
  dataPath?: string,
  options: TodoServerOptions = {},
): Promise<TodoServer> {
  const dbPath = dataPath ?? join(mkdtempSync(join(tmpdir(), "jazz-todo-")), "jazz.db");
  const appId = process.env.JAZZ_APP_ID ?? "019d4349-244c-74d4-8573-8e1b24cf21e2";

  const context = createJazzContext({
    appId,
    app: schemaApp,
    permissions,
    driver: { type: "persistent", dataPath: dbPath },
    env: "dev",
    jwksUrl: options.jwksUrl ?? process.env.JAZZ_JWKS_URL,
    jwtPublicKey: process.env.JAZZ_JWT_PUBLIC_KEY,
  });
  // Preserve the programmatic administrative handle for embedding and tests.
  // Network routes below exclusively use request-scoped databases.
  const db = context.asBackend();

  const app = express();

  const sseConnections = new Map<Response, Db>();
  const lifecycle: ServerLifecycle = {
    draining: false,
    beginDrain: () => {
      lifecycle.draining = true;
      const connections = Array.from(sseConnections.keys());
      sseConnections.clear();
      for (const res of connections) {
        if (!res.destroyed && !res.writableEnded) {
          res.end();
        }
      }
    },
  };
  const isActiveSseConnection = (res: Response) =>
    !lifecycle.draining && sseConnections.has(res) && !res.destroyed && !res.writableEnded;

  async function broadcastTodos() {
    if (lifecycle.draining) {
      return;
    }

    await Promise.all(
      Array.from(sseConnections, async ([res, requestDb]) => {
        if (!isActiveSseConnection(res)) {
          return;
        }
        const todos = await requestDb.all(schemaApp.todos);
        if (!isActiveSseConnection(res)) {
          return;
        }
        res.write(`data: ${JSON.stringify(todos)}\n\n`);
      }),
    );
  }

  function requestDb(res: Response): Db {
    const db = res.locals.requestDb as Db | undefined;
    if (!db) {
      throw new Error("Authenticated request database is unavailable");
    }
    return db;
  }

  // ========================================================================
  // Routes
  // ========================================================================

  // Health check
  app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "healthy" });
  });

  app.use("/todos", (_req: Request, res: Response, next: NextFunction) => {
    if (!lifecycle.draining) {
      next();
      return;
    }
    res.status(503).json({ error: "Server is shutting down" });
  });

  // Authenticate every todo request before selecting a session-scoped database.
  app.use("/todos", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const db = await context.forRequest(req);
      const session = db.getAuthState().session;
      if (!session) {
        res.status(401).json({ error: "Unauthorized" });
        return;
      }
      res.locals.requestDb = db;
      res.locals.userId = session.user;
      next();
    } catch {
      res.status(401).json({ error: "Unauthorized" });
    }
  });
  app.use(express.json());

  // List the authenticated caller's todos
  app.get("/todos", async (_req: Request, res: Response, next: NextFunction) => {
    try {
      const todos = await requestDb(res).all(schemaApp.todos);
      res.json(todos);
    } catch (e) {
      next(e);
    }
  });

  // Create a todo owned by the authenticated caller
  app.post("/todos", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const body = req.body as CreateTodoRequest;

      if (!body.title) {
        res.status(400).json({ error: "title is required" });
        return;
      }

      const inserted = requestDb(res).insert(schemaApp.todos, {
        title: body.title,
        done: false,
        description: body.description?.trim(),
        owner_id: res.locals.userId as string,
      });
      await inserted.wait({ tier: "local" });
      await broadcastTodos();

      res.status(201).json(inserted.value);
    } catch (e) {
      next(e);
    }
  });

  // Live SSE stream of the authenticated caller's todos (must be before /todos/:id)
  app.get("/todos/live", async (_req: Request, res: Response, next: NextFunction) => {
    const cleanup = () => {
      sseConnections.delete(res);
    };
    res.once("close", cleanup);

    try {
      if (lifecycle.draining) {
        res.status(503).json({ error: "Server is shutting down" });
        return;
      }

      const db = requestDb(res);
      res.setHeader("Content-Type", "text/event-stream");
      res.setHeader("Cache-Control", "no-cache");
      res.setHeader("Connection", "keep-alive");
      res.flushHeaders();

      sseConnections.set(res, db);

      const todos = await db.all(schemaApp.todos);
      if (!isActiveSseConnection(res)) {
        return;
      }
      res.write(`data: ${JSON.stringify(todos)}\n\n`);
    } catch (e) {
      if (res.destroyed || res.writableEnded) {
        return;
      }
      next(e);
    }
  });

  // Get a single todo visible to the authenticated caller
  app.get("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const todo = await requestDb(res).one(schemaApp.todos.where({ id }));
      if (!todo) {
        res.status(404).json({ error: "Todo not found" });
        return;
      }

      res.json(todo);
    } catch (e) {
      next(e);
    }
  });

  // Update a todo visible to the authenticated caller
  app.put("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const body = req.body as UpdateTodoRequest;
      const db = requestDb(res);
      const existing = await db.one(schemaApp.todos.where({ id }));
      if (!existing) {
        res.status(404).json({ error: "Todo not found" });
        return;
      }

      const updates = {
        title: body.title,
        done: body.done,
        description: body.description === undefined ? undefined : body.description.trim(),
      };

      if (Object.values(updates).some((value) => value !== undefined)) {
        await db.update(schemaApp.todos, id, updates).wait({ tier: "local" });
        await broadcastTodos();
      }

      const todo = await db.one(schemaApp.todos.where({ id }));
      if (!todo) {
        res.status(404).json({ error: "Todo not found after update" });
        return;
      }
      res.json(todo);
    } catch (e) {
      next(e);
    }
  });

  // Delete a todo visible to the authenticated caller
  app.delete("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;
      const db = requestDb(res);
      const existing = await db.one(schemaApp.todos.where({ id }));
      if (!existing) {
        res.status(404).json({ error: "Todo not found" });
        return;
      }

      await db.delete(schemaApp.todos, id).wait({ tier: "local" });
      await broadcastTodos();
      res.status(204).send();
    } catch (e) {
      next(e);
    }
  });

  // Error handler
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    console.error("Error:", err);
    res.status(500).json({ error: err.message });
  });

  serverLifecycles.set(app, lifecycle);

  let shutdownPromise: Promise<void> | undefined;
  const shutdown = async () => {
    shutdownPromise ??= context.shutdown();
    await shutdownPromise;
  };

  return {
    app,
    db,
    shutdown,
    flush: () => {
      context.flush();
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
  const existingStop = stopPromises.get(server.server);
  if (existingStop) {
    return existingStop;
  }

  const lifecycle = serverLifecycles.get(server.app);
  const stopPromise = (async () => {
    const httpClose = new Promise<void>((resolve, reject) => {
      try {
        server.server.close((error) => {
          if (error) reject(error);
          else resolve();
        });
      } catch (error) {
        reject(error);
      }
    });
    lifecycle?.beginDrain();

    let httpError: unknown;
    try {
      await httpClose;
    } catch (error) {
      httpError = error;
    }

    let shutdownError: unknown;
    try {
      await server.shutdown();
    } catch (error) {
      shutdownError = error;
    }
    if (httpError && shutdownError) {
      throw new AggregateError([httpError, shutdownError], "HTTP and Jazz shutdown both failed");
    }
    if (httpError) throw httpError;
    if (shutdownError) throw shutdownError;
  })();
  stopPromises.set(server.server, stopPromise);
  return stopPromise;
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
