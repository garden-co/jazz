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
import { JazzClient } from "jazz-tools";
import type { Value, WasmSchema } from "jazz-tools";
import { NapiRuntime } from "jazz-napi";

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
}

interface UpdateTodoRequest {
  title?: string;
  done?: boolean;
  description?: string;
}

export interface TodoServer {
  app: Application;
  client: JazzClient;
  shutdown: () => Promise<void>;
  flush: () => void;
}

export interface RunningServer extends TodoServer {
  server: Server;
  port: number;
  baseUrl: string;
}

// ============================================================================
// Schema
// ============================================================================

export const schema: WasmSchema = {
  tables: {
    projects: {
      columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
    },
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
        { name: "description", column_type: { type: "Text" }, nullable: true },
        { name: "parent", column_type: { type: "Uuid" }, nullable: true, references: "todos" },
        {
          name: "project",
          column_type: { type: "Uuid" },
          nullable: true,
          references: "projects",
        },
      ],
    },
  },
};

// ============================================================================
// Helpers
// ============================================================================

function rowToTodo(id: string, values: Value[]): Todo | null {
  if (values.length < 2) return null;

  const titleVal = values[0];
  const doneVal = values[1];
  const descVal = values[2];

  if (titleVal.type !== "Text" || doneVal.type !== "Boolean") {
    return null;
  }

  return {
    id,
    title: titleVal.value,
    done: doneVal.value,
    description: descVal?.type === "Text" && descVal.value ? descVal.value : undefined,
  };
}

function buildQuery(table: string) {
  return JSON.stringify({
    table,
    branches: [],
    disjuncts: [{ conditions: [] }],
    order_by: [],
    offset: 0,
    include_deleted: false,
    array_subqueries: [],
    joins: [],
  });
}

// ============================================================================
// Server Factory
// ============================================================================

/**
 * Create a todo server.
 *
 * @param dataPath Optional path to local SurrealKV database file. If omitted, uses a temp directory.
 * @returns TodoServer with app, client, and shutdown function
 */
export async function createServer(dataPath?: string): Promise<TodoServer> {
  // Create SurrealKV-backed runtime via NAPI
  const dbPath = dataPath ?? join(mkdtempSync(join(tmpdir(), "jazz-todo-")), "jazz.db");
  const appId = process.env.JAZZ_APP_ID ?? "todo-server-ts";

  const runtime = new NapiRuntime(JSON.stringify(schema), appId, "dev", "main", dbPath);

  const client = JazzClient.connectWithRuntime(runtime, {
    appId,
    schema,
    env: "dev",
    userBranch: "main",
  });

  // Create Express app
  const app = express();
  app.use(express.json());

  // Track active SSE connections for live updates
  const sseConnections = new Set<Response>();

  // Helper to broadcast current todos to all SSE connections
  async function broadcastTodos() {
    const rows = await client.query(buildQuery("todos"));
    const todos = rows
      .map((row) => rowToTodo(row.id, row.values))
      .filter((t): t is Todo => t !== null);
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
      const rows = await client.query(buildQuery("todos"));
      const todos = rows
        .map((row) => rowToTodo(row.id, row.values))
        .filter((t): t is Todo => t !== null);

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

      const values: Value[] = [
        { type: "Text", value: body.title },
        { type: "Boolean", value: false },
        { type: "Text", value: body.description ?? "" },
        { type: "Null" },
        { type: "Null" },
      ];

      const id = await client.create("todos", values);

      const todo: Todo = {
        id,
        title: body.title,
        done: false,
        description: body.description,
      };

      res.status(201).json(todo);

      // Notify SSE connections
      broadcastTodos();
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
    const rows = await client.query(buildQuery("todos"));
    const todos = rows
      .map((row) => rowToTodo(row.id, row.values))
      .filter((t): t is Todo => t !== null);
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

      const rows = await client.query(buildQuery("todos"));
      const row = rows.find((r) => r.id === id);

      if (!row) {
        res.status(404).json({ error: "Todo not found" });
        return;
      }

      const todo = rowToTodo(row.id, row.values);
      if (!todo) {
        res.status(500).json({ error: "Failed to parse todo" });
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

      const updates: Record<string, Value> = {};

      if (body.title !== undefined) {
        updates.title = { type: "Text", value: body.title };
      }
      if (body.done !== undefined) {
        updates.done = { type: "Boolean", value: body.done };
      }
      if (body.description !== undefined) {
        updates.description = { type: "Text", value: body.description };
      }

      if (Object.keys(updates).length === 0) {
        // No updates, just return the current todo
        const rows = await client.query(buildQuery("todos"));
        const row = rows.find((r) => r.id === id);

        if (!row) {
          res.status(404).json({ error: "Todo not found" });
          return;
        }

        const todo = rowToTodo(row.id, row.values);
        res.json(todo);
        return;
      }

      await client.update(id, updates);

      // Fetch updated todo
      const rows = await client.query(buildQuery("todos"));
      const row = rows.find((r) => r.id === id);

      if (!row) {
        res.status(404).json({ error: "Todo not found after update" });
        return;
      }

      const todo = rowToTodo(row.id, row.values);
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

      await client.delete(id);
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
    client,
    shutdown: async () => {
      await client.shutdown();
    },
    flush: () => {
      runtime.flush();
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
