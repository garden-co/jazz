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

// ============================================================================
// Types
// ============================================================================

export interface Todo {
  id: string;
  title: string;
  done: boolean;
  description?: string;
}

interface CreateTodoRequest {
  title: string;
  description?: string;
  owner_id?: string;
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

// ============================================================================
// Helpers
// ============================================================================

/**
 * Create a todo server.
 *
 * @param dataPath Optional path to local Fjall database file. If omitted, uses a temp directory.
 * @returns TodoServer with app, db, and shutdown function
 */
export async function createServer(dataPath?: string): Promise<TodoServer> {
  const dbPath = dataPath ?? join(mkdtempSync(join(tmpdir(), "jazz-todo-")), "jazz.db");
  const appId = process.env.JAZZ_APP_ID ?? "todo-server-ts";

  // #region context-setup-ts-backend
  const context = createJazzContext({
    appId,
    app: schemaApp,
    driver: { type: "persistent", dataPath: dbPath },
    env: "dev",
    userBranch: "main",
  });
  const db = context.db();
  // #endregion context-setup-ts-backend

  // Create Express app
  const app = express();
  app.use(express.json());

  // Track active SSE connections for live updates
  const sseConnections = new Set<Response>();

  // Helper to broadcast current todos to all SSE connections
  async function broadcastTodos() {
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

      const todo = db.insert(schemaApp.todos, {
        title: body.title,
        done: false,
        description: body.description?.trim(),
        owner_id: body.owner_id ?? "anonymous",
      });

      res.status(201).json(todo);

      // Notify SSE connections
      broadcastTodos();
    } catch (e) {
      next(e);
    }
  });

  // List todos as a specific session user (for policy verification/testing)
  app.get("/todos/as/:userId", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const userDb = context.forSession({
        user_id: req.params.userId,
        claims: {},
      });
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
        description: body.description === undefined ? undefined : body.description.trim(),
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

      await db.updateDurable(schemaApp.todos, id, updates);

      // Fetch updated todo
      const todo = await db.one(schemaApp.todos.where({ id }));
      if (!todo) {
        res.status(404).json({ error: "Todo not found after update" });
        return;
      }
      res.json(todo);

      // Notify SSE connections
      broadcastTodos();
    } catch (e) {
      next(e);
    }
  });

  // Delete a todo
  app.delete("/todos/:id", async (req: Request, res: Response, next: NextFunction) => {
    try {
      const { id } = req.params;

      await db.deleteDurable(schemaApp.todos, id);
      res.status(204).send();

      // Notify SSE connections
      broadcastTodos();
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
      await context.shutdown();
    },
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
