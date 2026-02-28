import type { Application, Request, Response } from "express";
import { createJazzContext, type Value } from "jazz-tools/backend";
import { app as schemaApp } from "../schema/app.js";

type AuthenticatedRequest = Request<
  Record<string, string>,
  unknown,
  { title?: string; description?: string }
> & { auth?: { sub?: string } };

// #region quickstart-server-context-ts
const context = createJazzContext({
  appId: "todo-server-ts",
  app: schemaApp,
  dataPath: "./data/jazz.db",
  serverUrl: process.env.JAZZ_SERVER_URL,
  backendSecret: process.env.JAZZ_BACKEND_SECRET,
});
const client = context.client();
// #endregion quickstart-server-context-ts

// #region quickstart-server-read-oneshot-ts
export async function getOpenTodos(req: Request, res: Response): Promise<void> {
  const requester = context.forRequest(req);
  const rows = await requester.query(
    schemaApp.todos.where({ done: false }).orderBy("title", "asc").limit(100),
  );
  res.json(rows);
}
// #endregion quickstart-server-read-oneshot-ts

// #region quickstart-server-read-streaming-ts
export async function streamOpenTodos(req: Request, res: Response): Promise<void> {
  const requester = context.forRequest(req);
  const query = schemaApp.todos.where({ done: false }).orderBy("title", "asc").limit(100);

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const snapshot = await requester.query(query);
  res.write(`data: ${JSON.stringify({ type: "snapshot", rows: snapshot })}\n\n`);

  const subscriptionId = requester.subscribe(query, (delta) => {
    res.write(`data: ${JSON.stringify({ type: "delta", delta })}\n\n`);
  });

  req.on("close", () => {
    client.unsubscribe(subscriptionId);
  });
}
// #endregion quickstart-server-read-streaming-ts

// #region quickstart-server-write-ts
export async function createTodo(req: AuthenticatedRequest, res: Response): Promise<void> {
  const userId = req.auth?.sub;
  if (!userId) {
    res.status(401).json({ error: "missing authenticated user" });
    return;
  }

  const title = req.body.title?.trim();
  if (!title) {
    res.status(400).json({ error: "title is required" });
    return;
  }

  const values: Value[] = [
    { type: "Text", value: title },
    { type: "Boolean", value: false },
    { type: "Text", value: req.body.description?.trim() ?? "" },
    { type: "Null" },
    { type: "Null" },
    { type: "Text", value: userId },
  ];

  const requester = context.forRequest(req);
  const id = await requester.create("todos", values);

  res.status(201).json({
    id,
    title,
    done: false,
    owner_id: userId,
  });
}
// #endregion quickstart-server-write-ts

// #region quickstart-server-register-routes-ts
export function registerTodoApi(api: Application): void {
  api.get("/api/todos", (req, res, next) => {
    void getOpenTodos(req, res).catch(next);
  });

  api.get("/api/todos/stream", (req, res, next) => {
    void streamOpenTodos(req, res).catch(next);
  });

  api.post("/api/todos", (req: AuthenticatedRequest, res, next) => {
    void createTodo(req, res).catch(next);
  });
}
// #endregion quickstart-server-register-routes-ts
