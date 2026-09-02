/**
 * Todo Server - TypeScript implementation using Jazz.
 *
 * This demonstrates how to use Jazz with Express to build a simple REST API.
 */

import express, { Request, Response, NextFunction } from "express";
import type { Application } from "express";
import type { Server } from "node:http";
import { join } from "node:path";
import { createJazzSession, type Db } from "jazz-tools/backend";
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
  /** The deployed Jazz app used to admit external identities. */
  appId?: string;
  /** Upstream Jazz server that owns the account registry. */
  serverUrl?: string;
  /** Server-only credential for durable backend writes. */
  backendSecret?: string;
  adminSecret?: string;
}
interface ServerLifecycle {
  draining: boolean;
  beginDrain: () => void;
}

const serverLifecycles = new WeakMap<Application, ServerLifecycle>();
const stopPromises = new WeakMap<Server, Promise<void>>();

export type TodoServerStorage = { type: "persistent"; dataPath: string } | { type: "memory" };

// ============================================================================
// Helpers
// ============================================================================

/**
 * Create a todo server.
 *
 * @param storage Explicit persistent path or in-memory storage selector.
 * @returns TodoServer with the Express app, administrative database handle, and lifecycle functions
 */
export async function createServer(
  storage: TodoServerStorage,
  options: TodoServerOptions = {},
): Promise<TodoServer> {
  const appId = options.appId ?? process.env.JAZZ_APP_ID ?? "019d4349-244c-74d4-8573-8e1b24cf21e2";
  const serverUrl = options.serverUrl ?? process.env.JAZZ_SERVER_URL;
  const backendSecret = options.backendSecret ?? process.env.JAZZ_BACKEND_SECRET;
  const driver =
    storage.type === "persistent"
      ? { type: "persistent" as const, dataPath: storage.dataPath }
      : { type: "memory" as const };

  if (storage.type === "persistent" && storage.dataPath.trim() === "") {
    throw new Error("Persistent storage requires a non-empty dataPath");
  }

  if (!serverUrl || !backendSecret) {
    throw new Error("JAZZ_SERVER_URL and JAZZ_BACKEND_SECRET are required");
  }

  const session = await createJazzSession({
    appId,
    app: schemaApp,
    permissions,
    driver,
    serverUrl,
    initial: { backendSecret },
    env: "dev",
    jwksUrl: options.jwksUrl ?? process.env.JAZZ_JWKS_URL,
    jwtPublicKey: process.env.JAZZ_JWT_PUBLIC_KEY,
  });
  // Preserve the programmatic administrative handle for embedding and tests.
  // Network routes below exclusively use request-scoped databases.
  const snapshot = session.getSnapshot();
  if (snapshot.status !== "ready" || !snapshot.client) {
    throw snapshot.error ?? new Error("Backend session is not ready");
  }
  const client = snapshot.client;
  const db = client.db;

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
      const db = await client.forRequest(req);
      const session = db.getAuthState().session;
      if (!session) {
        res.status(401).json({ error: "Unauthorized" });
        return;
      }
      res.locals.requestDb = db;
      res.locals.userId = session.user.account;
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
    shutdownPromise ??= session.close();
    await shutdownPromise;
  };

  return {
    app,
    db,
    shutdown,
    flush: () => {
      client.flush();
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
  return new Promise((resolve, reject) => {
    const server = todoServer.app.listen(port);

    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };

    const onListening = () => {
      server.off("error", onError);
      const address = server.address();
      const actualPort = typeof address === "object" && address ? address.port : port;
      resolve({
        ...todoServer,
        server,
        port: actualPort,
        baseUrl: `http://localhost:${actualPort}`,
      });
    };

    server.once("error", onError);
    server.once("listening", onListening);
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

const DEFAULT_APP_ID = "019d4349-244c-74d4-8573-8e1b24cf21e2";

function resolveStorage(argv: string[], env: NodeJS.ProcessEnv): TodoServerStorage {
  let dataPath: string | undefined;
  let inMemory = false;

  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--data-path") {
      if (dataPath !== undefined) {
        throw new Error("The --data-path option may only be provided once");
      }
      const value = argv[index + 1];
      if (value === undefined || value.startsWith("--") || value.trim() === "") {
        throw new Error("--data-path requires a non-empty path");
      }
      dataPath = value;
      index += 1;
    } else if (argument === "--in-memory") {
      if (inMemory) {
        throw new Error("The --in-memory option may only be provided once");
      }
      inMemory = true;
    } else {
      throw new Error(`Unknown option: ${argument}`);
    }
  }

  const environmentPath = env.DB_PATH;
  if (environmentPath !== undefined && environmentPath.trim() === "") {
    throw new Error("DB_PATH must be non-empty when set");
  }
  if (inMemory && (dataPath !== undefined || environmentPath !== undefined)) {
    throw new Error("--in-memory conflicts with --data-path and DB_PATH");
  }
  if (dataPath !== undefined) {
    return { type: "persistent", dataPath };
  }
  if (environmentPath !== undefined) {
    return { type: "persistent", dataPath: environmentPath };
  }
  if (inMemory) {
    return { type: "memory" };
  }

  const appId = env.JAZZ_APP_ID ?? DEFAULT_APP_ID;
  const encodedAppId = Buffer.from(appId, "utf8").toString("base64url");
  return {
    type: "persistent",
    dataPath: join("data", "todos", encodedAppId, "jazz.db"),
  };
}
async function main() {
  const todoServer = await createServer(resolveStorage(process.argv.slice(2), process.env));

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
